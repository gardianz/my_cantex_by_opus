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
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx  # type: ignore[import]


# --- Constants ---
COUNTERPARTIES_URL = "https://ccview.io/api/v1/internal/api/v1/parties/counterparties"
PARTY_INFO_URL = "https://ccview.io/api/v1/internal/api/v1/parties"
MAX_RETRIES = 2
REQUEST_TIMEOUT = 20.0
VALIDATOR_PATTERNS = ("cantex-validator", "Cantex-validator")
# Minimum seconds between scrapes for the same account (avoid spam).
# Diturunkan dari 5s -> 2s sesudah perbaikan timestamp-after-completion,
# agar trigger berurutan tetap dilayani saat scrape sebelumnya benar-benar
# selesai sukses (bukan "slot hangus" karena timestamp di-set duluan).
MIN_SCRAPE_COOLDOWN_SECONDS = 2.0
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
    week_validator_fee_total: Decimal | None = None
    week_validator_tx_count: int | None = None
    week_avg_fee_per_swap: Decimal | None = None
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
        # Track last scrape COMPLETION time per account (monotonic) for cooldown.
        # Sengaja diisi setelah scrape selesai, bukan saat trigger, supaya
        # cooldown tidak menelan slot saat scrape gagal di tengah jalan.
        self._last_scrape_time: dict[str, float] = {}
        # Store latest results per account
        self._latest_results: dict[str, ActualFeeResult] = {}
        # Background tasks
        self._background_tasks: set[asyncio.Task[None]] = set()
        # Periodic scrape tasks
        self._periodic_tasks: dict[str, asyncio.Task[None]] = {}
        # Callback dipanggil setiap kali _latest_results[account_name] terisi
        # dengan result.success=True. Dipakai untuk memastikan periodic loop
        # tetap me-refresh card dashboard.
        # Signature: callback(account_name: str, result: ActualFeeResult) -> None
        # NOTE: callback adalah sync function; bila ada coroutine yang harus
        # diawait, scheduler caller bertanggung jawab create_task.
        self._on_result_callbacks: list = []

    def register_on_result(self, callback) -> None:
        """Daftarkan callback yang dipanggil saat ada result.success baru.

        Dipakai bot.AutoswapBot supaya periodic scrape juga merefresh
        dashboard card, tidak hanya per-hop trigger.
        """
        if callback not in self._on_result_callbacks:
            self._on_result_callbacks.append(callback)

    def _notify_result(self, account_name: str, result: ActualFeeResult) -> None:
        """Panggil semua callback registered. Gagal silently dengan log warning."""
        for cb in list(self._on_result_callbacks):
            try:
                cb(account_name, result)
            except Exception as exc:
                self.log.warning(
                    "FeeScraper on_result callback raised: %s", exc, exc_info=True
                )

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

    def _utc_today_and_week_dates(self) -> tuple[str, str]:
        today = datetime.now(timezone.utc).date()
        week_start = today - timedelta(days=today.weekday())
        return today.isoformat(), week_start.isoformat()

    async def fetch_actual_fee(self, party_id: str, date: str) -> ActualFeeResult:
        """Fetch counterparties data dan extract validator fee untuk satu hari."""
        return await self.fetch_actual_fee_range(
            party_id,
            start_date=date,
            end_date=date,
        )

    async def fetch_actual_fee_range(
        self,
        party_id: str,
        *,
        start_date: str,
        end_date: str,
        include_balance: bool = True,
    ) -> ActualFeeResult:
        """Fetch counterparties data dan extract validator fee untuk rentang tanggal.

        Args:
            party_id: The party ID (address) of the account on ccview.io
            start_date: Start date YYYY-MM-DD
            end_date: End date YYYY-MM-DD
            include_balance: Also fetch party balance detail

        Returns:
            ActualFeeResult with validator fee data
        """
        range_label = start_date if start_date == end_date else f"{start_date}..{end_date}"
        result = ActualFeeResult(party_id=party_id, date=range_label)

        try:
            client = await self._ensure_client()

            self.log.debug(
                "CCView fetch_actual_fee_range | party_id=%s | start=%s | end=%s",
                party_id[:30] if party_id else "(empty)",
                start_date,
                end_date,
            )

            # Fetch counterparties
            params = {
                "party_id": party_id,
                "limit": "50",
                "offset": "0",
                "start": start_date,
                "end": end_date,
            }

            cp_data = await self._fetch_with_retry(client, COUNTERPARTIES_URL, params)
            if cp_data is None:
                result.error = f"Failed to fetch counterparties (party_id={party_id[:30]})"
                self.log.warning(
                    "CCView counterparties returned None | party_id=%s | start=%s | end=%s",
                    party_id[:30] if party_id else "(empty)",
                    start_date,
                    end_date,
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
            if include_balance:
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

    async def fetch_actual_fee_today_and_week(self, party_id: str) -> ActualFeeResult:
        """Fetch today + Monday→today aggregates in one logical scrape result."""
        today, week_start = self._utc_today_and_week_dates()
        today_result = await self.fetch_actual_fee_range(
            party_id,
            start_date=today,
            end_date=today,
            include_balance=True,
        )
        if not today_result.success:
            return today_result

        if week_start == today:
            today_result.week_validator_fee_total = today_result.validator_fee_total
            today_result.week_validator_tx_count = today_result.validator_tx_count
            today_result.week_avg_fee_per_swap = today_result.avg_fee_per_swap
            return today_result

        week_result = await self.fetch_actual_fee_range(
            party_id,
            start_date=week_start,
            end_date=today,
            include_balance=False,
        )
        if week_result.success:
            today_result.week_validator_fee_total = week_result.validator_fee_total
            today_result.week_validator_tx_count = week_result.validator_tx_count
            today_result.week_avg_fee_per_swap = week_result.avg_fee_per_swap
        else:
            self.log.warning(
                "CCView weekly aggregate scrape failed | party_id=%s | start=%s | end=%s | error=%s",
                party_id[:30] if party_id else "(empty)",
                week_start,
                today,
                week_result.error,
            )
        return today_result

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

        Cooldown-based: min 2s between scrapes for the same account
        (set setelah scrape selesai sukses, supaya scrape gagal tidak
        menelan slot trigger berikutnya).
        Called after every successful swap hop.
        """
        if not party_id:
            return

        # Cooldown check: skip if last SUCCESSFUL scrape completed too recently.
        now = time.monotonic()
        last_time = self._last_scrape_time.get(account_name, 0.0)
        if now - last_time < MIN_SCRAPE_COOLDOWN_SECONDS:
            # [GAS DIAG] log keputusan trigger supaya bisa lihat kenapa akun
            # tertentu tidak meng-scrape ulang setelah progress.
            self.log.warning(
                "[GAS DIAG] CCView trigger SKIP cooldown | %s | round=%s | "
                "elapsed=%.1fs | min=%.1fs",
                account_name,
                completed_round,
                now - last_time,
                MIN_SCRAPE_COOLDOWN_SECONDS,
            )
            return
        self.log.info(
            "[GAS DIAG] CCView trigger ACCEPT | %s | round=%s | elapsed=%.1fs",
            account_name,
            completed_round,
            now - last_time,
        )
        # Sengaja TIDAK set _last_scrape_time di sini. Diset oleh
        # _background_scrape() setelah scrape sukses. Trigger berurutan saat
        # scrape pertama belum balik akan tetap dijaga oleh cooldown setelah
        # scrape sebelumnya selesai (refresh timestamp = waktu completion).

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

            # [GAS DIAG] track wait di scrape lock — bila banyak akun antre,
            # akan terlihat selisih waktu antara mulai await dan dapat lock.
            wait_start = time.monotonic()
            async with self._scrape_lock:
                wait_elapsed = time.monotonic() - wait_start
                if wait_elapsed > 1.0:
                    self.log.info(
                        "[GAS DIAG] CCView background scrape lock waited %.1fs | %s",
                        wait_elapsed,
                        account_name,
                    )
                result = await self.fetch_actual_fee_today_and_week(party_id)

            self._latest_results[account_name] = result

            if result.success:
                # Set timestamp HANYA setelah sukses, bukan saat trigger.
                # Trigger berikutnya tetap dilayani kalau scrape ini gagal.
                self._last_scrape_time[account_name] = time.monotonic()
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
                # Beritahu callback (mis. card update) bahwa ada hasil baru.
                self._notify_result(account_name, result)
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
            async with self._scrape_lock:
                result = await self.fetch_actual_fee_today_and_week(party_id)

            self._latest_results[account_name] = result

            if result.success:
                self._last_scrape_time[account_name] = time.monotonic()
                self.log.info(
                    "CCView startup scrape OK | %s | "
                    "today_fee=%s CC (%s tx) | week_fee=%s CC (%s tx) | avg=%s CC/swap | swaps=%s",
                    account_name,
                    result.validator_fee_total,
                    result.validator_tx_count,
                    result.week_validator_fee_total,
                    result.week_validator_tx_count,
                    result.avg_fee_per_swap,
                    result.swap_tx_count,
                )
                self._notify_result(account_name, result)
            else:
                self.log.warning(
                    "CCView startup scrape failed | %s | error=%s",
                    account_name,
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
                    async with self._scrape_lock:
                        result = await self.fetch_actual_fee_today_and_week(party_id)
                    if result.success:
                        self._latest_results[account_name] = result
                        self._last_scrape_time[account_name] = time.monotonic()
                        self.log.info(
                            "CCView periodic scrape OK | %s | "
                            "today_fee=%s CC (%s tx) | week_fee=%s CC (%s tx) | avg=%s CC/swap",
                            account_name,
                            result.validator_fee_total,
                            result.validator_tx_count,
                            result.week_validator_fee_total,
                            result.week_validator_tx_count,
                            result.avg_fee_per_swap,
                        )
                        # Pastikan dashboard card juga ikut refresh dari periodic.
                        self._notify_result(account_name, result)
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
            async with self._scrape_lock:
                result = await self.fetch_actual_fee_today_and_week(party_id)

            if result.success:
                self._latest_results[account_name] = result
                self._last_scrape_time[account_name] = time.monotonic()
                self.log.info(
                    "CCView scrape_now OK | %s | today_fee=%s CC (%s tx) | week_fee=%s CC (%s tx) | avg=%s CC/swap",
                    account_name,
                    result.validator_fee_total,
                    result.validator_tx_count,
                    result.week_validator_fee_total,
                    result.week_validator_tx_count,
                    result.avg_fee_per_swap,
                )
                self._notify_result(account_name, result)
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
