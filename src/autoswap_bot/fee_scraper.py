"""CCView.io Fee Scraper — async background scraper for actual network fees.

Setelah swap berhasil, scrape ccview.io counterparties API untuk mendapatkan
network fee aktual yang benar-benar dipotong oleh validator.

Reuses logic dari ccview_scraper/scraper.py tapi didesain untuk:
- Async background (tidak blocking swap flow)
- Lazy session init (sekali saja)
- Rate limit: max 1 scrape per akun per swap
- Graceful fallback jika gagal
"""

from __future__ import annotations

import asyncio
import logging
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
    - Rate limit: max 1 scrape per akun per trigger
    - Background async (tidak blocking swap flow)
    - Graceful fallback jika gagal
    """

    def __init__(self) -> None:
        self.log = logging.getLogger("autoswap_bot.fee_scraper")
        self._client: httpx.AsyncClient | None = None
        self._session_initialized = False
        self._init_lock = asyncio.Lock()
        self._scrape_lock = asyncio.Lock()
        # Track last scrape per account to avoid spam
        self._last_scrape_round: dict[str, int] = {}
        # Store latest results per account
        self._latest_results: dict[str, ActualFeeResult] = {}
        # Background tasks
        self._background_tasks: set[asyncio.Task] = set()

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
                result.error = "Failed to fetch counterparties"
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

        Rate limited: max 1 scrape per account per round.
        """
        if not party_id:
            return

        # Rate limit: skip if already scraped this round
        last_round = self._last_scrape_round.get(account_name, -1)
        if last_round >= completed_round:
            return

        self._last_scrape_round[account_name] = completed_round

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
        """Background scrape task."""
        try:
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
        except Exception as exc:
            self.log.warning(
                "CCView background scrape exception | %s | %s",
                account_name,
                exc,
            )

    def get_latest_result(self, account_name: str) -> ActualFeeResult | None:
        """Get the latest scrape result for an account."""
        return self._latest_results.get(account_name)

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
        # Cancel all background tasks
        for task in list(self._background_tasks):
            task.cancel()
        self._background_tasks.clear()

        if self._client is not None:
            await self._client.aclose()
            self._client = None
            self._session_initialized = False
