"""CCView.io Fee Scraper — async background scraper for actual network fees.

Setelah swap berhasil, scrape ccview.io counterparties API untuk mendapatkan
network fee aktual yang benar-benar dipotong oleh validator.

Reuses logic dari ccview_scraper/scraper.py tapi didesain untuk:
- Async background (tidak blocking swap flow)
- Lazy session init (sekali saja)
- Rate limit: cooldown-based (min 5s between scrapes per account)
- Trigger setiap swap hop sukses (bukan hanya per round)
- Graceful fallback jika gagal
- Periodic scrape fallback setiap N detik
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

import httpx


# --- Constants ---
COUNTERPARTIES_URL = "https://ccview.io/api/v1/internal/api/v1/parties/counterparties"
PARTY_INFO_URL = "https://ccview.io/api/v1/internal/api/v1/parties"
MAX_RETRIES = 2
REQUEST_TIMEOUT = 20.0
VALIDATOR_PATTERNS = ("cantex-validator", "Cantex-validator")
# Minimum seconds between scrapes for the same account (avoid spam)
MIN_SCRAPE_COOLDOWN_SECONDS = 5.0
# Delay before scraping after a swap (ccview.io needs time to index)
SCRAPE_DELAY_AFTER_SWAP_SECONDS = 5.0
# Periodic scrape interval (fallback) in seconds — reduced from 120s for faster updates
PERIODIC_SCRAPE_INTERVAL_SECONDS = 90.0


@dataclass
class ActualFeeResult:
    """Result dari scraping ccview.io untuk fee aktual."""

    party_id: str
    date: str
    validator_fee_total: Decimal = field(default_factory=lambda: Decimal("0"))
    validator_tx_count: int = 0
    avg_fee_per_swap: Decimal = field(default_factory=lambda: Decimal("0"))
    swap_tx_count: int = 0
    balance: Decimal = field(default_factory=lambda: Decimal("0"))
    success: bool = False
    error: str | None = None
    scraped_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class FeeScraper:
    """Scrape network fee aktual dari ccview.io setelah swap sukses.

    Features:
    - Lazy session init (visit homepage + /api/v1/session sekali)
    - Shared session untuk semua akun
    - Cooldown-based rate limit: min 5s between scrapes per account
    - Triggered after every successful swap hop (non-blocking)
    - Small delay before scrape (ccview.io needs indexing time)
    - Periodic scrape fallback
    - Graceful fallback jika gagal
    """

    def __init__(self) -> None:
        self.log = logging.getLogger("autoswap_bot.fee_scraper")
        self._client: httpx.AsyncClient | None = None
        self._session_initialized = False
        self._init_lock = asyncio.Lock()
        self._scrape_lock = asyncio.Lock()
        # Track last scrape time per account (monotonic) for cooldown
        self._last_scrape_time: dict[str, float] = {}
        # Store latest results per account
        self._latest_results: dict[str, ActualFeeResult] = {}
        # Background tasks
        self._background_tasks: set[asyncio.Task[None]] = set()
        # Periodic scrape tasks
        self._periodic_tasks: dict[str, asyncio.Task[None]] = {}

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Lazy init httpx client with session."""
        if self._client is not None and self._session_initialized:
            return self._client

        async with self._init_lock:
            if self._client is not None and self._session_initialized:
                return self._client

            if self._client is None:
                self._client = httpx.AsyncClient(
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/131.0.0.0 Safari/537.36"
                        ),
                        "Accept": "application/json, text/html, */*",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Origin": "https://ccview.io",
                        "Referer": "https://ccview.io/",
                    },
                    follow_redirects=True,
                    timeout=REQUEST_TIMEOUT,
                )

            if not self._session_initialized:
                try:
                    self.log.debug("Initializing ccview.io session...")
                    await self._client.get("https://ccview.io/", timeout=REQUEST_TIMEOUT)
                    resp = await self._client.get(
                        "https://ccview.io/api/v1/session", timeout=REQUEST_TIMEOUT
                    )
                    self._session_initialized = resp.status_code == 200
                    self.log.debug(
                        "Session init: HTTP %s, cookies=%s",
                        resp.status_code,
                        len(self._client.cookies),
                    )
                except Exception as exc:
                    self.log.warning("ccview.io session init failed: %s", exc)
                    # Still return client, API might work without session
                    self._session_initialized = True  # Don't retry every time

            return self._client

    async def fetch_actual_fee(self, party_id: str, date: str) -> ActualFeeResult:
        """Fetch counterparties data dan extract validator fee.

        Args:
            party_id: The party ID (address) of the account on ccview.io
            date: Date string in YYYY-MM-DD format

        Returns:
            ActualFeeResult with validator fee data
        """
        result = ActualFeeResult(party_id=party_id, date=date)

        try:
            client = await self._ensure_client()

            self.log.debug(
                "CCView fetch_actual_fee | party_id=%s | date=%s",
                party_id[:30] if party_id else "(empty)",
                date,
            )

            # Fetch counterparties
            params = {
                "party_id": party_id,
                "limit": "50",
                "offset": "0",
                "start": date,
                "end": date,
            }

            cp_data = await self._fetch_with_retry(client, COUNTERPARTIES_URL, params)
            if cp_data is None:
                result.error = f"Failed to fetch counterparties (party_id={party_id[:30]})"
                self.log.warning(
                    "CCView counterparties returned None | party_id=%s | date=%s",
                    party_id[:30] if party_id else "(empty)",
                    date,
                )
                return result

            # Parse counterparties and find validator
            items = cp_data.get("data", [])
            ans_binding = cp_data.get("ans_binding", {})

            validator_fee_total = Decimal("0")
            validator_tx_count = 0
            swap_tx_count = 0

            for item in items:
                cid = item.get("counterparty_id", "")
                # Check display name from ans_binding
                display_name = cid
                if cid in ans_binding:
                    bindings = ans_binding[cid]
                    if bindings and isinstance(bindings, list):
                        display_name = bindings[0].get("ans_name", cid)

                cid_lower = cid.lower()
                display_lower = display_name.lower()

                # Detect validator
                if any(
                    pattern.lower() in cid_lower or pattern.lower() in display_lower
                    for pattern in VALIDATOR_PATTERNS
                ):
                    validator_fee_total = Decimal(
                        item.get("transfers_out_volume", "0")
                    )
                    validator_tx_count = int(item.get("transfers_out_count", 0))

                # Detect pool-custodian (swap counterparty)
                if "pool-custodian" in cid_lower or "pool-custodian" in display_lower:
                    swap_tx_count = int(item.get("total_transfers_count", 0))

            avg_fee = (
                validator_fee_total / validator_tx_count
                if validator_tx_count > 0
                else Decimal("0")
            )

            result.validator_fee_total = validator_fee_total
            result.validator_tx_count = validator_tx_count
            result.avg_fee_per_swap = avg_fee
            result.swap_tx_count = swap_tx_count
            result.success = True

            # Optionally fetch balance
            try:
                balance_data = await self._fetch_with_retry(
                    client, f"{PARTY_INFO_URL}/{party_id}", {}
                )
                if balance_data is not None:
                    balance_info = balance_data.get("balance", {})
                    if isinstance(balance_info, dict):
                        result.balance = Decimal(
                            str(
                                balance_info.get("total_coin_holdings")
                                or balance_info.get("total_available_coin")
                                or "0"
                            )
                        )
            except Exception:
                pass  # Balance is optional

        except Exception as exc:
            result.error = str(exc)
            self.log.warning("Fee scrape failed for %s: %s", party_id[:20], exc)

        return result

    async def _fetch_with_retry(
        self,
        client: httpx.AsyncClient,
        url: str,
        params: dict,
    ) -> dict | None:
        """Fetch URL with retry logic."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await client.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    return response.json()
                if response.status_code == 404:
                    return None
                self.log.debug(
                    "ccview HTTP %s (attempt %s/%s): %s",
                    response.status_code,
                    attempt,
                    MAX_RETRIES,
                    response.text[:100],
                )
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
                self.log.debug(
                    "ccview request error (attempt %s/%s): %s",
                    attempt,
                    MAX_RETRIES,
                    exc,
                )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)
        return None

    def trigger_background_scrape(
        self,
        *,
        party_id: str,
        account_name: str,
        completed_round: int,
    ) -> None:
        """Trigger a background scrape (non-blocking).

        Cooldown-based: min 5s between scrapes for the same account.
        Called after every successful swap hop.
        """
        if not party_id:
            return

        # Cooldown check: skip if last scrape was too recent
        now = time.monotonic()
        last_time = self._last_scrape_time.get(account_name, 0.0)
        if now - last_time < MIN_SCRAPE_COOLDOWN_SECONDS:
            self.log.debug(
                "CCView scrape skipped for %s: cooldown (%.1fs since last)",
                account_name,
                now - last_time,
            )
            return

        self._last_scrape_time[account_name] = now

        # Create background task
        task = asyncio.create_task(
            self._background_scrape(
                party_id=party_id,
                account_name=account_name,
                completed_round=completed_round,
            )
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _background_scrape(
        self,
        *,
        party_id: str,
        account_name: str,
        completed_round: int,
    ) -> None:
        """Background scrape task with delay for ccview.io indexing."""
        try:
            # Wait a bit for ccview.io to index the transaction
            await asyncio.sleep(SCRAPE_DELAY_AFTER_SWAP_SECONDS)

            today = datetime.now(timezone.utc).date().isoformat()
            async with self._scrape_lock:
                result = await self.fetch_actual_fee(party_id, today)

            self._latest_results[account_name] = result

            if result.success:
                self.log.info(
                    "CCView scrape OK | %s | round=%s | "
                    "validator_fee=%s CC (%s tx) | avg=%s CC/swap | swaps=%s",
                    account_name,
                    completed_round,
                    result.validator_fee_total,
                    result.validator_tx_count,
                    result.avg_fee_per_swap,
                    result.swap_tx_count,
                )
            else:
                self.log.warning(
                    "CCView scrape failed | %s | round=%s | error=%s",
                    account_name,
                    completed_round,
                    result.error,
                )
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.log.warning(
                "CCView background scrape exception | %s | %s",
                account_name,
                exc,
            )

    def trigger_startup_scrape(
        self,
        *,
        party_id: str,
        account_name: str,
    ) -> None:
        """Trigger an immediate scrape at startup (non-blocking, no delay).

        Unlike trigger_background_scrape, this does NOT wait for ccview indexing
        because we're fetching historical data that's already indexed.
        """
        if not party_id:
            self.log.warning(
                "CCView trigger_startup_scrape SKIPPED: party_id kosong untuk %s",
                account_name,
            )
            return

        self.log.info(
            "CCView startup scrape triggered | %s | party_id=%s...",
            account_name,
            party_id[:20],
        )

        task = asyncio.create_task(
            self._startup_scrape(
                party_id=party_id,
                account_name=account_name,
            )
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _startup_scrape(
        self,
        *,
        party_id: str,
        account_name: str,
    ) -> None:
        """Startup scrape — no delay, immediate fetch for today's data."""
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            async with self._scrape_lock:
                result = await self.fetch_actual_fee(party_id, today)

            self._latest_results[account_name] = result
            self._last_scrape_time[account_name] = time.monotonic()

            if result.success:
                self.log.info(
                    "CCView startup scrape OK | %s | date=%s | "
                    "validator_fee=%s CC (%s tx) | avg=%s CC/swap | swaps=%s",
                    account_name,
                    today,
                    result.validator_fee_total,
                    result.validator_tx_count,
                    result.avg_fee_per_swap,
                    result.swap_tx_count,
                )
            else:
                self.log.warning(
                    "CCView startup scrape failed | %s | date=%s | error=%s",
                    account_name,
                    today,
                    result.error,
                )
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.log.warning(
                "CCView startup scrape exception | %s | %s",
                account_name,
                exc,
            )

    def start_periodic_scrape(
        self,
        *,
        party_id: str,
        account_name: str,
    ) -> None:
        """Start a periodic background scrape for an account (fallback).

        Scrapes every PERIODIC_SCRAPE_INTERVAL_SECONDS as a fallback
        in case per-hop triggers miss updates.
        """
        if not party_id:
            return
        if account_name in self._periodic_tasks:
            return  # Already running

        task = asyncio.create_task(
            self._periodic_scrape_loop(
                party_id=party_id,
                account_name=account_name,
            )
        )
        self._periodic_tasks[account_name] = task
        task.add_done_callback(lambda t: self._periodic_tasks.pop(account_name, None))

    async def _periodic_scrape_loop(
        self,
        *,
        party_id: str,
        account_name: str,
    ) -> None:
        """Periodic scrape loop — runs until cancelled."""
        self.log.info(
            "CCView periodic scrape loop started | %s | interval=%ss | party_id=%s...",
            account_name,
            PERIODIC_SCRAPE_INTERVAL_SECONDS,
            party_id[:20],
        )
        try:
            while True:
                await asyncio.sleep(PERIODIC_SCRAPE_INTERVAL_SECONDS)
                try:
                    today = datetime.now(timezone.utc).date().isoformat()
                    async with self._scrape_lock:
                        result = await self.fetch_actual_fee(party_id, today)
                    if result.success:
                        self._latest_results[account_name] = result
                        self._last_scrape_time[account_name] = time.monotonic()
                        self.log.info(
                            "CCView periodic scrape OK | %s | "
                            "validator_fee=%s CC (%s tx) | avg=%s CC/swap",
                            account_name,
                            result.validator_fee_total,
                            result.validator_tx_count,
                            result.avg_fee_per_swap,
                        )
                    else:
                        self.log.warning(
                            "CCView periodic scrape failed | %s | error=%s",
                            account_name,
                            result.error,
                        )
                except Exception as exc:
                    self.log.warning(
                        "CCView periodic scrape error | %s | %s",
                        account_name,
                        exc,
                    )
        except asyncio.CancelledError:
            self.log.debug("CCView periodic scrape loop cancelled | %s", account_name)

    def get_latest_result(self, account_name: str) -> ActualFeeResult | None:
        """Get the latest scrape result for an account."""
        return self._latest_results.get(account_name)

    async def scrape_now(
        self,
        *,
        party_id: str,
        account_name: str,
        force: bool = False,
    ) -> ActualFeeResult | None:
        """Synchronous (awaited) scrape — fetch ccview data immediately and return result.

        Unlike trigger_background_scrape, this blocks until the scrape completes.
        Use after swap progress confirmed or after refill to guarantee card update.
        Respects cooldown to avoid spamming unless force=True.
        """
        if not party_id:
            return None

        # Cooldown check (reduced to 3s for synchronous calls)
        now = time.monotonic()
        last_time = self._last_scrape_time.get(account_name, 0.0)
        if not force and now - last_time < 3.0:
            # Return cached result if available
            cached = self._latest_results.get(account_name)
            if cached is not None and cached.success:
                return cached
            # Otherwise wait out the cooldown
            await asyncio.sleep(3.0 - (now - last_time))

        try:
            today = datetime.now(timezone.utc).date().isoformat()
            async with self._scrape_lock:
                result = await self.fetch_actual_fee(party_id, today)

            if result.success:
                self._latest_results[account_name] = result
                self._last_scrape_time[account_name] = time.monotonic()
                self.log.info(
                    "CCView scrape_now OK | %s | fee=%s CC (%s tx) | avg=%s CC/swap",
                    account_name,
                    result.validator_fee_total,
                    result.validator_tx_count,
                    result.avg_fee_per_swap,
                )
            else:
                self.log.warning(
                    "CCView scrape_now failed | %s | error=%s",
                    account_name,
                    result.error,
                )
            return result
        except Exception as exc:
            self.log.warning("CCView scrape_now exception | %s | %s", account_name, exc)
            return None

    def get_actual_avg_fee(self, account_name: str) -> Decimal | None:
        """Get the actual average fee per swap from latest scrape.

        Returns None if no successful scrape available.
        """
        result = self._latest_results.get(account_name)
        if result is None or not result.success:
            return None
        if result.avg_fee_per_swap <= Decimal("0"):
            return None
        return result.avg_fee_per_swap

    async def close(self) -> None:
        """Close the HTTP client and cancel background tasks."""
        # Cancel periodic tasks
        for task in list(self._periodic_tasks.values()):
            task.cancel()
        self._periodic_tasks.clear()

        # Cancel all background tasks
        for task in list(self._background_tasks):
            task.cancel()
        self._background_tasks.clear()

        if self._client is not None:
            await self._client.aclose()
            self._client = None
            self._session_initialized = False
