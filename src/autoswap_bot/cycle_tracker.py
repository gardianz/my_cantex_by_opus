"""Round-trip cycle spread loss tracker.

Redesigned to handle all conditions:
- USDCx=0 at start (cycle starts when first USDCx→CBTC happens)
- CC→USDCx inflow (top-up, NOT part of USDCx cycle)
- Strategy 4: CC→USDCx→CBTC→USDCx (CC→USDCx is top-up, cycle is USDCx→CBTC→USDCx)
- Restart mid-cycle (graceful reset)

Cycle types:
- USDCx Cycle: USDCx → CBTC → USDCx
  - Start: when USDCx→CBTC swap happens, record sell_amount (USDCx out)
  - End: when CBTC→USDCx swap happens, record buy_amount (USDCx in)
  - Loss = sell_amount_start - buy_amount_end
  - CC→USDCx is IGNORED (it's a top-up, not part of cycle)

- CC Cycle: CC → foreign → CC
  - Start: when CC→USDCx or CC→CBTC swap happens, record CC sell_amount
  - End: when USDCx→CC or CBTC→CC swap happens, record CC buy_amount
  - Loss = CC_out - CC_in (WITHOUT fee, fee is tracked separately)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .constants import CC_SYMBOL

logger = logging.getLogger("autoswap_bot.cycle_tracker")


@dataclass
class CycleResult:
    """Result of a completed cycle."""

    origin_symbol: str
    start_amount: Decimal
    end_amount: Decimal
    spread_loss: Decimal  # start - end (positive = loss)
    cycle_type: str  # "usdcx_cbtc" or "cc_foreign"


@dataclass
class PendingUSDCxCycle:
    """Tracks an in-progress USDCx cycle (USDCx → foreign → USDCx)."""

    sell_amount: Decimal  # USDCx that went out
    target_symbol: str  # foreign token (CBTC, ETH, BTC, etc.)
    active: bool = True


@dataclass
class PendingCCCycle:
    """Tracks an in-progress CC cycle (CC → foreign → CC)."""

    sell_amount: Decimal  # CC that went out
    target_symbol: str  # "USDCx" or "CBTC"
    active: bool = True


@dataclass
class CycleTracker:
    """Tracks round-trip cycle spread losses for a single account.

    Design principles:
    - USDCx Cycle: Tracks USDCx→foreign + foreign→USDCx pairs
      - CC→USDCx is IGNORED (top-up, not cycle start)
      - USDCx→CC is IGNORED (not part of USDCx cycle)
    - CC Cycle: Tracks CC→foreign + foreign→CC pairs
      - Network fee is excluded from loss calculation
    - Handles USDCx=0 start gracefully (no cycle until USDCx→foreign happens)
    - Handles restart mid-cycle (stale pending cycles are overwritten)
    - Mode-aware: only tracks cycles relevant to current refill mode
    """

    # Tracking mode: "CC", "USDCx", or "USDCx_v2"
    mode: str = "CC"

    # Pending USDCx cycle (USDCx → foreign, waiting for foreign → USDCx)
    _pending_usdcx: PendingUSDCxCycle | None = None

    # Pending CC cycle (CC → foreign, waiting for foreign → CC)
    _pending_cc: PendingCCCycle | None = None

    # Accumulated losses
    total_usdcx_spread_loss: Decimal = field(default_factory=lambda: Decimal("0"))
    total_cc_spread_loss: Decimal = field(default_factory=lambda: Decimal("0"))
    cycle_count: int = 0
    cycle_history: list[CycleResult] = field(default_factory=list)

    def __post_init__(self):
        """Validate mode and normalize it."""
        if self.mode in ("USDCx", "USDCx_v2"):
            self.mode = "USDCx"  # Both modes use USDCx cycle tracking
        elif self.mode != "CC":
            self.mode = "CC"  # Default to CC mode

    def record_swap(
        self,
        *,
        sell_symbol: str,
        buy_symbol: str,
        sell_amount: Decimal,
        buy_amount: Decimal,
        network_fee: dict[str, Decimal] | None = None,
        swap_fee: dict[str, Decimal] | None = None,
    ) -> CycleResult | None:
        """Record a completed swap and return CycleResult if a cycle just completed.

        Args:
            sell_symbol: Symbol being sold
            buy_symbol: Symbol being bought
            sell_amount: Amount sold (gross, before fees)
            buy_amount: Amount received (net output from tx)
            network_fee: Network fees paid (by symbol)
            swap_fee: Swap/admin fees paid (by symbol)

        Returns:
            CycleResult if a round-trip cycle just completed, None otherwise.
        """
        # --- USDCx cycle detection (USDCx → foreign → USDCx) ---
        # Only track if mode is USDCx
        if self.mode == "USDCx":
            # Check for foreign symbols (non-CC, non-USDCx tracked symbols)
            foreign_symbols = ("CBTC", "ETH", "BTC")  # Extendable list

            if sell_symbol == "USDCx" and buy_symbol in foreign_symbols:
                # Starting USDCx cycle: USDCx going out to foreign token
                if self._pending_usdcx is not None and self._pending_usdcx.active:
                    logger.debug(
                        "USDCx cycle restarted (previous pending overwritten): "
                        "old_start=%s, new_start=%s",
                        self._pending_usdcx.sell_amount,
                        sell_amount,
                    )
                self._pending_usdcx = PendingUSDCxCycle(
                    sell_amount=sell_amount,
                    target_symbol=buy_symbol,
                )
                logger.debug(
                    "USDCx cycle started: sell_amount=%s USDCx → %s",
                    sell_amount,
                    buy_symbol,
                )
                return None

            if buy_symbol == "USDCx" and self._pending_usdcx is not None:
                # Check if this foreign token matches our pending target
                if sell_symbol == self._pending_usdcx.target_symbol:
                    # Completing USDCx cycle: foreign coming back to USDCx
                    end_amount = buy_amount
                    spread_loss = self._pending_usdcx.sell_amount - end_amount
                    result = CycleResult(
                        origin_symbol="USDCx",
                        start_amount=self._pending_usdcx.sell_amount,
                        end_amount=end_amount,
                        spread_loss=spread_loss,
                        cycle_type="usdcx_foreign",
                    )
                    self.total_usdcx_spread_loss += spread_loss
                    self.cycle_count += 1
                    self._append_history(result)
                    logger.info(
                        "USDCx cycle completed: %s → %s USDCx | spread_loss=%s USDCx | total=%s",
                        self._pending_usdcx.sell_amount,
                        end_amount,
                        spread_loss,
                        self.total_usdcx_spread_loss,
                    )
                    self._pending_usdcx = None
                    return result

        # --- CC cycle detection (CC → foreign → CC) ---
        # Only track if mode is CC
        if self.mode == "CC":
            if sell_symbol == CC_SYMBOL and buy_symbol in ("USDCx", "CBTC"):
                # Starting CC cycle: CC going out to foreign
                # Overwrite any existing pending CC cycle (restart mid-cycle)
                if self._pending_cc is not None and self._pending_cc.active:
                    logger.debug(
                        "CC cycle restarted (previous pending overwritten): "
                        "old_start=%s CC→%s, new_start=%s CC→%s",
                        self._pending_cc.sell_amount,
                        self._pending_cc.target_symbol,
                        sell_amount,
                        buy_symbol,
                    )
                self._pending_cc = PendingCCCycle(
                    sell_amount=sell_amount,
                    target_symbol=buy_symbol,
                )
                logger.debug(
                    "CC cycle started: sell_amount=%s CC → %s",
                    sell_amount,
                    buy_symbol,
                )
                return None

            if sell_symbol in ("USDCx", "CBTC") and buy_symbol == CC_SYMBOL:
                # Completing CC cycle: foreign coming back to CC
                if self._pending_cc is not None and self._pending_cc.active:
                    # For CC cycle, spread loss = CC_start - CC_end
                    # We EXCLUDE network fee from loss calculation because fee is tracked separately.
                    # buy_amount is the net CC received after swap.
                    # Network fee is charged separately on CC, so we add it back to get
                    # the "true" CC output before fee deduction.
                    cc_network_fee = Decimal("0")
                    if network_fee:
                        cc_network_fee = network_fee.get(CC_SYMBOL, Decimal("0"))

                    end_amount = buy_amount + cc_network_fee
                    spread_loss = self._pending_cc.sell_amount - end_amount
                    result = CycleResult(
                        origin_symbol="CC",
                        start_amount=self._pending_cc.sell_amount,
                        end_amount=end_amount,
                        spread_loss=spread_loss,
                        cycle_type="cc_foreign",
                    )
                    self.total_cc_spread_loss += spread_loss
                    self.cycle_count += 1
                    self._append_history(result)
                    logger.info(
                        "CC cycle completed: %s → %s CC (excl fee) | spread_loss=%s CC | total=%s",
                        self._pending_cc.sell_amount,
                        end_amount,
                        spread_loss,
                        self.total_cc_spread_loss,
                    )
                    # Reset pending
                    self._pending_cc = None
                    return result
                else:
                    # foreign → CC without active CC cycle (e.g., recovery swap)
                    logger.debug(
                        "%s → CC swap without active CC cycle, ignoring for cycle tracking",
                        sell_symbol,
                    )
                    return None

        # --- Swaps that are NOT part of any cycle ---
        # CC→USDCx when there's no pending CC cycle: this is a top-up, ignore
        # USDCx→CC: not part of USDCx↔CBTC cycle, ignore
        # Any other pair: ignore
        return None

    def _append_history(self, result: CycleResult) -> None:
        """Append to history with bounded size."""
        self.cycle_history.append(result)
        if len(self.cycle_history) > 100:
            self.cycle_history = self.cycle_history[-50:]

    def get_summary(self) -> dict[str, Any]:
        """Return a summary dict for display purposes."""
        return {
            "total_usdcx_spread_loss": self.total_usdcx_spread_loss,
            "total_cc_spread_loss": self.total_cc_spread_loss,
            "cycle_count": self.cycle_count,
            "usdcx_cycle_pending": self._pending_usdcx is not None and self._pending_usdcx.active,
            "cc_cycle_pending": self._pending_cc is not None and self._pending_cc.active,
        }

    def get_total_spread_loss_map(self) -> dict[str, Decimal]:
        """Return spread loss as a symbol->amount map for display."""
        result: dict[str, Decimal] = {}
        if self.total_usdcx_spread_loss != Decimal("0"):
            result["USDCx"] = self.total_usdcx_spread_loss
        if self.total_cc_spread_loss != Decimal("0"):
            result["CC"] = self.total_cc_spread_loss
        return result

    def reset(self) -> None:
        """Reset all tracking state."""
        self._pending_usdcx = None
        self._pending_cc = None
        self.total_usdcx_spread_loss = Decimal("0")
        self.total_cc_spread_loss = Decimal("0")
        self.cycle_count = 0
        self.cycle_history.clear()

    def get_state_for_persistence(self) -> dict[str, Any]:
        """Return state dict for persistence to runtime_state.

        This captures all accumulated losses and pending cycle state
        so it can be restored after bot restart.
        """
        pending_type = ""
        pending_amount = "0"
        pending_target = ""

        if self.mode == "USDCx" and self._pending_usdcx is not None:
            pending_type = "usdcx"
            pending_amount = str(self._pending_usdcx.sell_amount)
            pending_target = self._pending_usdcx.target_symbol
        elif self.mode == "CC" and self._pending_cc is not None:
            pending_type = "cc"
            pending_amount = str(self._pending_cc.sell_amount)
            pending_target = self._pending_cc.target_symbol

        return {
            "cycle_loss_cc": str(self.total_cc_spread_loss),
            "cycle_loss_usdcx": str(self.total_usdcx_spread_loss),
            "cycle_count": self.cycle_count,
            "cycle_mode": self.mode,
            "cycle_pending_type": pending_type,
            "cycle_pending_amount": pending_amount,
            "cycle_pending_target": pending_target,
        }

    def restore_from_state(self, state: dict[str, Any]) -> None:
        """Restore tracker state from persisted dict.

        Only restores if the mode matches to prevent mixing different
        cycle types.
        """
        restored_mode = state.get("cycle_mode", "")
        if restored_mode != self.mode:
            logger.info(
                "Cycle state mode mismatch (stored=%s, current=%s), starting fresh",
                restored_mode,
                self.mode,
            )
            return

        try:
            self.total_cc_spread_loss = Decimal(state.get("cycle_loss_cc", "0"))
            self.total_usdcx_spread_loss = Decimal(state.get("cycle_loss_usdcx", "0"))
            self.cycle_count = int(state.get("cycle_count", 0))

            # Restore pending cycle if any
            pending_type = state.get("cycle_pending_type", "")
            pending_amount = Decimal(state.get("cycle_pending_amount", "0"))
            pending_target = state.get("cycle_pending_target", "")

            if pending_type == "usdcx" and self.mode == "USDCx" and pending_target:
                self._pending_usdcx = PendingUSDCxCycle(
                    sell_amount=pending_amount,
                    target_symbol=pending_target,
                )
                logger.info(
                    "Restored pending USDCx cycle: %s → %s",
                    pending_amount,
                    pending_target,
                )
            elif pending_type == "cc" and self.mode == "CC" and pending_target:
                self._pending_cc = PendingCCCycle(
                    sell_amount=pending_amount,
                    target_symbol=pending_target,
                )
                logger.info(
                    "Restored pending CC cycle: %s CC → %s",
                    pending_amount,
                    pending_target,
                )

            logger.info(
                "Cycle state restored: mode=%s, cc_loss=%s, usdcx_loss=%s, count=%s",
                self.mode,
                self.total_cc_spread_loss,
                self.total_usdcx_spread_loss,
                self.cycle_count,
            )
        except Exception as exc:
            logger.warning("Failed to restore cycle state: %s", exc)
            # Start fresh if restore fails
            self.reset()
