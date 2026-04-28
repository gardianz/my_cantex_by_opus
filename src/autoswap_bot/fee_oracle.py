"""Shared Fee Oracle — single polling task for network fee across all pairs.

Instead of every account polling its own quote to check fees, one
``SharedFeeOracle`` instance polls quotes for every tracked pair and
exposes the results to all account runners.  This reduces API traffic
from O(accounts × pairs) to O(pairs) per poll cycle.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict
from decimal import Decimal
from typing import Any

from cantex_sdk import CantexAPIError, CantexTimeoutError, InstrumentId

from .config import FloatRange
from .constants import CC_SYMBOL, TRACKED_SYMBOLS

logger = logging.getLogger("autoswap_bot.fee_oracle")

# All directed pairs we care about (A→B where A != B).
_ALL_PAIRS: list[tuple[str, str]] = [
    (a, b) for a in TRACKED_SYMBOLS for b in TRACKED_SYMBOLS if a != b
]

# Small default amount per symbol used for fee-probing quotes.
_PROBE_AMOUNT: dict[str, Decimal] = {
    "CC": Decimal("10"),
    "USDCx": Decimal("1"),
    "CBTC": Decimal("0.0001"),
}


class SharedFeeOracle:
    """Centralized fee poller shared by all account runners.

    Lifecycle
    ---------
    1. Created in ``AutoswapBot.__init__`` (no SDK yet).
    2. After the *first* account authenticates, call
       :meth:`register_sdk` to hand over an authenticated SDK +
       resolved instruments.
    3. Call :meth:`start` to kick off the background polling task.
    4. Account runners call :meth:`wait_for_stable_fee` instead of
       polling individually.
    5. Call :meth:`stop` during bot shutdown.
    """

    def __init__(
        self,
        *,
        poll_interval_range: FloatRange,
        fee_history_max: int = 50,
        rng: random.Random | None = None,
    ) -> None:
        self._poll_interval_range = poll_interval_range
        self._fee_history_max = max(fee_history_max, 10)
        self._rng = rng or random.Random()

        # SDK + instruments — set via register_sdk()
        self._sdk: Any | None = None
        self._instruments: dict[str, InstrumentId] = {}

        # fee_history[pair_key] = list of (timestamp, fee_cc) most-recent last
        self._fee_history: dict[str, list[tuple[float, Decimal]]] = defaultdict(list)

        # Condition notified every time new fee data arrives
        self._condition = asyncio.Condition()

        # Background task handle
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

        # Track whether SDK is ready
        self._sdk_ready = asyncio.Event()

        # Optional callback called after each poll cycle (for dashboard refresh)
        self._on_poll_callback: Any | None = None

    def set_on_poll_callback(self, callback) -> None:
        """Set a callback to be called after each poll cycle (e.g. dashboard refresh)."""
        self._on_poll_callback = callback

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @staticmethod
    def pair_key(sell_symbol: str, buy_symbol: str) -> str:
        """Canonical key for a directed pair."""
        return f"{sell_symbol}->{buy_symbol}"

    # ------------------------------------------------------------------
    # SDK registration
    # ------------------------------------------------------------------

    def register_sdk(
        self,
        sdk: Any,
        instruments_by_symbol: dict[str, InstrumentId],
    ) -> None:
        """Hand over an authenticated SDK for quote polling.

        Called once after the first account authenticates.  Thread-safe
        because it only *sets* references; the polling loop checks
        ``_sdk_ready``.
        """
        if self._sdk is not None:
            return  # already registered
        self._sdk = sdk
        self._instruments = dict(instruments_by_symbol)
        self._sdk_ready.set()
        logger.info(
            "Fee oracle SDK registered | instruments=%s",
            ", ".join(sorted(self._instruments)),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background polling task (idempotent)."""
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.get_event_loop().create_task(
            self._poll_loop(), name="fee-oracle-poll",
        )
        logger.info("Fee oracle polling task started")

    async def stop(self) -> None:
        """Gracefully stop the polling task."""
        self._stop_event.set()
        # Wake up anyone waiting on the condition so they can exit
        async with self._condition:
            self._condition.notify_all()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        logger.info("Fee oracle stopped")

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ------------------------------------------------------------------
    # Data access (read-only, safe from any coroutine)
    # ------------------------------------------------------------------

    def get_current_fee(self, sell_symbol: str, buy_symbol: str) -> Decimal | None:
        """Return the most recent fee quote (CC) for a pair, or *None*."""
        key = self.pair_key(sell_symbol, buy_symbol)
        history = self._fee_history.get(key)
        if not history:
            return None
        return history[-1][1]

    def get_avg_fee(
        self,
        sell_symbol: str,
        buy_symbol: str,
        samples: int = 3,
    ) -> Decimal | None:
        """Return the average of the last *samples* fee quotes for a pair."""
        key = self.pair_key(sell_symbol, buy_symbol)
        history = self._fee_history.get(key)
        if not history:
            return None
        recent = [fee for _, fee in history[-samples:]]
        if not recent:
            return None
        return sum(recent) / len(recent)

    def get_fee_history(
        self,
        sell_symbol: str,
        buy_symbol: str,
        limit: int | None = None,
    ) -> list[Decimal]:
        """Return fee values (most-recent last) for a pair."""
        key = self.pair_key(sell_symbol, buy_symbol)
        history = self._fee_history.get(key, [])
        fees = [fee for _, fee in history]
        if limit is not None:
            fees = fees[-limit:]
        return fees

    def get_all_pair_fees(self) -> dict[str, Decimal | None]:
        """Return latest fee for every tracked pair (for dashboard)."""
        result: dict[str, Decimal | None] = {}
        for a, b in _ALL_PAIRS:
            key = self.pair_key(a, b)
            history = self._fee_history.get(key)
            result[key] = history[-1][1] if history else None
        return result

    def sample_count(self, sell_symbol: str, buy_symbol: str) -> int:
        """How many fee samples we have for a pair."""
        key = self.pair_key(sell_symbol, buy_symbol)
        return len(self._fee_history.get(key, []))

    # ------------------------------------------------------------------
    # wait_for_stable_fee — replaces per-account polling
    # ------------------------------------------------------------------

    async def wait_for_stable_fee(
        self,
        sell_symbol: str,
        buy_symbol: str,
        fee_cap: Decimal,
        stability_samples: int = 3,
        *,
        stop_event: asyncio.Event | None = None,
        timeout: float | None = None,
    ) -> bool:
        """Block until the average of the last *stability_samples* fee
        quotes for the given pair is ≤ *fee_cap*.

        Returns ``True`` when the fee is stable and below cap, or
        ``False`` if the oracle was stopped / timed out.

        Parameters
        ----------
        stop_event
            Optional per-account stop event.  If set, the wait aborts
            early and returns ``False``.
        timeout
            Maximum seconds to wait.  ``None`` means wait indefinitely.
        """
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        key = self.pair_key(sell_symbol, buy_symbol)

        while True:
            # Check external stop signals
            if self._stop_event.is_set():
                return False
            if stop_event is not None and stop_event.is_set():
                return False

            # Check if we already have enough stable data
            history = self._fee_history.get(key, [])
            if len(history) >= stability_samples:
                recent = [fee for _, fee in history[-stability_samples:]]
                avg = sum(recent) / len(recent)
                if avg <= fee_cap:
                    return True

            # Calculate remaining wait time
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                wait_timeout = min(remaining, 60.0)
            else:
                wait_timeout = 60.0

            # Wait for new data from the polling loop
            try:
                async with self._condition:
                    await asyncio.wait_for(
                        self._condition.wait(),
                        timeout=wait_timeout,
                    )
            except asyncio.TimeoutError:
                # Re-check conditions on next iteration
                continue

    # ------------------------------------------------------------------
    # Background polling loop
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Continuously poll quotes for all pairs."""
        # Wait until an SDK is registered
        logger.info("Fee oracle waiting for SDK registration...")
        try:
            while not self._sdk_ready.is_set():
                if self._stop_event.is_set():
                    return
                try:
                    await asyncio.wait_for(self._sdk_ready.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            return

        logger.info("Fee oracle SDK ready — starting poll cycles")

        while not self._stop_event.is_set():
            try:
                await self._poll_all_pairs()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("Fee oracle poll cycle error: %s", exc)

            # Sleep for a random interval
            interval = max(1.0, self._poll_interval_range.sample(self._rng))
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                return  # stop was requested
            except asyncio.TimeoutError:
                pass  # normal — continue polling

    async def _poll_all_pairs(self) -> None:
        """Poll a quote for every pair and record the CC network fee."""
        sdk = self._sdk
        if sdk is None:
            return

        updated_pairs: list[str] = []

        for sell_symbol, buy_symbol in _ALL_PAIRS:
            if self._stop_event.is_set():
                return

            sell_instrument = self._instruments.get(sell_symbol)
            buy_instrument = self._instruments.get(buy_symbol)
            if sell_instrument is None or buy_instrument is None:
                continue

            probe_amount = _PROBE_AMOUNT.get(sell_symbol, Decimal("10"))
            try:
                quote = await sdk.get_swap_quote(
                    sell_amount=probe_amount,
                    sell_instrument=sell_instrument,
                    buy_instrument=buy_instrument,
                )
            except (CantexAPIError, CantexTimeoutError, Exception) as exc:
                logger.debug(
                    "Fee oracle quote failed %s->%s: %s",
                    sell_symbol, buy_symbol, exc,
                )
                continue

            # Extract network fee
            try:
                network_fee_amount = quote.fees.network_fee.amount
                network_fee_instrument = quote.fees.network_fee.instrument
            except AttributeError:
                continue

            # We only track CC-denominated network fees
            fee_symbol = self._symbol_from_instrument(network_fee_instrument)
            if fee_symbol != CC_SYMBOL:
                continue

            key = self.pair_key(sell_symbol, buy_symbol)
            now = time.monotonic()
            self._fee_history[key].append((now, network_fee_amount))

            # Trim history
            if len(self._fee_history[key]) > self._fee_history_max:
                self._fee_history[key] = self._fee_history[key][-self._fee_history_max:]

            updated_pairs.append(key)

        if updated_pairs:
            # Notify all waiters that new data is available
            async with self._condition:
                self._condition.notify_all()

            if logger.isEnabledFor(logging.DEBUG):
                summaries = []
                for key in updated_pairs:
                    fee = self._fee_history[key][-1][1]
                    summaries.append(f"{key}={fee}")
                logger.debug("Fee oracle updated: %s", " | ".join(summaries))

            # Trigger dashboard refresh callback
            if self._on_poll_callback is not None:
                try:
                    self._on_poll_callback()
                except Exception:
                    pass

    def _symbol_from_instrument(self, instrument: InstrumentId) -> str:
        """Resolve an InstrumentId back to a symbol string."""
        for symbol, candidate in self._instruments.items():
            if candidate == instrument:
                return symbol
        return instrument.id
