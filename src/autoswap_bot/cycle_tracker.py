"""Round-trip cycle spread loss tracker.

Tracks spread loss based on full round-trip cycles rather than individual swaps.

Cycle types:
- USDCx cycle: USDCx → CBTC → USDCx  (spread_loss = USDCx_start - USDCx_end)
- CC cycle: CC → foreign → CC  (spread_loss = CC_start - CC_end, excluding fees)

For strategy_4_reserve, the recycle pattern is USDCx ↔ CBTC which forms
1 cycle per 2 swaps.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .constants import CC_SYMBOL

logger = logging.getLogger("autoswap_bot.cycle_tracker")


@dataclass
class CycleState:
    """Tracks a single in-progress cycle."""

    origin_symbol: str  # The symbol that starts and ends the cycle
    start_amount: Decimal  # Amount of origin_symbol when cycle started
    phase: str = "idle"  # "idle", "out", "back"


@dataclass
class CycleResult:
    """Result of a completed cycle."""

    origin_symbol: str
    start_amount: Decimal
    end_amount: Decimal
    spread_loss: Decimal  # start - end (positive = loss)
    cycle_type: str  # "usdcx_cbtc" or "cc_foreign"


@dataclass
class CycleTracker:
    """Tracks round-trip cycle spread losses for a single account.

    USDCx cycle: USDCx → CBTC → USDCx
      - Phase "out": USDCx sold for CBTC, record USDCx start amount
      - Phase "back": CBTC sold for USDCx, compute loss

    CC cycle: CC → foreign → CC
      - Phase "out": CC sold for foreign, record CC start amount
      - Phase "back": foreign sold for CC, compute loss
    """

    # USDCx ↔ CBTC cycle state
    usdcx_cycle: CycleState = field(
        default_factory=lambda: CycleState(origin_symbol="USDCx", start_amount=Decimal("0"))
    )

    # CC ↔ foreign cycle state
    cc_cycle: CycleState = field(
        default_factory=lambda: CycleState(origin_symbol="CC", start_amount=Decimal("0"))
    )

    # Accumulated losses
    total_usdcx_spread_loss: Decimal = field(default_factory=lambda: Decimal("0"))
    total_cc_spread_loss: Decimal = field(default_factory=lambda: Decimal("0"))
    cycle_count: int = 0
    cycle_history: list[CycleResult] = field(default_factory=list)

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
        result: CycleResult | None = None

        # --- USDCx ↔ CBTC cycle detection ---
        if sell_symbol == "USDCx" and buy_symbol == "CBTC":
            # Starting USDCx cycle: USDCx going out to CBTC
            self.usdcx_cycle.start_amount = sell_amount
            self.usdcx_cycle.phase = "out"
            logger.debug(
                "USDCx cycle started: sell_amount=%s USDCx → CBTC",
                sell_amount,
            )

        elif sell_symbol == "CBTC" and buy_symbol == "USDCx":
            # Completing USDCx cycle: CBTC coming back to USDCx
            if self.usdcx_cycle.phase == "out":
                end_amount = buy_amount
                spread_loss = self.usdcx_cycle.start_amount - end_amount
                result = CycleResult(
                    origin_symbol="USDCx",
                    start_amount=self.usdcx_cycle.start_amount,
                    end_amount=end_amount,
                    spread_loss=spread_loss,
                    cycle_type="usdcx_cbtc",
                )
                self.total_usdcx_spread_loss += spread_loss
                self.cycle_count += 1
                self.cycle_history.append(result)
                # Keep history bounded
                if len(self.cycle_history) > 100:
                    self.cycle_history = self.cycle_history[-50:]
                logger.info(
                    "USDCx cycle completed: %s → %s USDCx | spread_loss=%s USDCx | total=%s",
                    self.usdcx_cycle.start_amount,
                    end_amount,
                    spread_loss,
                    self.total_usdcx_spread_loss,
                )
                self.usdcx_cycle.phase = "idle"
                self.usdcx_cycle.start_amount = Decimal("0")
            else:
                # CBTC → USDCx without a prior USDCx → CBTC (e.g., recovery swap)
                logger.debug(
                    "CBTC → USDCx swap without active USDCx cycle (phase=%s), ignoring",
                    self.usdcx_cycle.phase,
                )

        # --- CC ↔ foreign cycle detection ---
        elif sell_symbol == CC_SYMBOL and buy_symbol in ("USDCx", "CBTC"):
            # Starting CC cycle: CC going out to foreign
            self.cc_cycle.start_amount = sell_amount
            self.cc_cycle.phase = "out"
            logger.debug(
                "CC cycle started: sell_amount=%s CC → %s",
                sell_amount,
                buy_symbol,
            )

        elif sell_symbol in ("USDCx", "CBTC") and buy_symbol == CC_SYMBOL:
            # Completing CC cycle: foreign coming back to CC
            if self.cc_cycle.phase == "out":
                # For CC cycle, the spread loss is CC_start - CC_end
                # We need to exclude fees from the calculation.
                # The buy_amount is the net CC received (after swap fee deduction
                # from the swap itself, but network fee is separate).
                # Since network fee is charged separately and we want to exclude it,
                # we add back any CC network fee that was charged on this leg.
                cc_network_fee = Decimal("0")
                if network_fee:
                    cc_network_fee = network_fee.get(CC_SYMBOL, Decimal("0"))

                end_amount = buy_amount + cc_network_fee
                spread_loss = self.cc_cycle.start_amount - end_amount
                result = CycleResult(
                    origin_symbol="CC",
                    start_amount=self.cc_cycle.start_amount,
                    end_amount=end_amount,
                    spread_loss=spread_loss,
                    cycle_type="cc_foreign",
                )
                self.total_cc_spread_loss += spread_loss
                self.cycle_count += 1
                self.cycle_history.append(result)
                if len(self.cycle_history) > 100:
                    self.cycle_history = self.cycle_history[-50:]
                logger.info(
                    "CC cycle completed: %s → %s CC (excl fee) | spread_loss=%s CC | total=%s",
                    self.cc_cycle.start_amount,
                    end_amount,
                    spread_loss,
                    self.total_cc_spread_loss,
                )
                self.cc_cycle.phase = "idle"
                self.cc_cycle.start_amount = Decimal("0")
            else:
                logger.debug(
                    "%s → CC swap without active CC cycle (phase=%s), ignoring",
                    sell_symbol,
                    self.cc_cycle.phase,
                )

        return result

    def get_summary(self) -> dict[str, Any]:
        """Return a summary dict for display purposes."""
        return {
            "total_usdcx_spread_loss": self.total_usdcx_spread_loss,
            "total_cc_spread_loss": self.total_cc_spread_loss,
            "cycle_count": self.cycle_count,
            "usdcx_cycle_phase": self.usdcx_cycle.phase,
            "cc_cycle_phase": self.cc_cycle.phase,
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
        self.usdcx_cycle = CycleState(origin_symbol="USDCx", start_amount=Decimal("0"))
        self.cc_cycle = CycleState(origin_symbol="CC", start_amount=Decimal("0"))
        self.total_usdcx_spread_loss = Decimal("0")
        self.total_cc_spread_loss = Decimal("0")
        self.cycle_count = 0
        self.cycle_history.clear()
