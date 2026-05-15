"""Price Validator — validasi harga quote vs execution price terakhir dari trading history.

Logic:
1. Sebelum swap: bandingkan quote price dengan execution price terakhir dari history
2. Jika selisih > toleransi (default 1%), retry quote sampai 3x
3. Setelah 3x retry masih di luar toleransi: lanjut swap (bypass)
4. Khusus CBTC→USDCx: bypass jika harga lebih menguntungkan dari reverse pair

Sumber execution price:
- Dari trading history API: field `trade_price_raw`
- Disimpan per pair (sell_symbol→buy_symbol)
- Diupdate setelah setiap swap sukses
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

logger = logging.getLogger("autoswap_bot.price_validator")


@dataclass
class ExecutionPriceRecord:
    """Record harga eksekusi aktual dari trading history."""
    sell_symbol: str
    buy_symbol: str
    trade_price: Decimal  # output/input ratio
    amount_in: Decimal
    amount_out: Decimal
    timestamp_utc: str
    update_id: str = ""
    recorded_at: float = field(default_factory=time.monotonic)


class PriceValidator:
    """Validasi harga quote vs execution price terakhir dari trading history.

    Thread-safe untuk penggunaan async (tidak ada shared mutable state antar akun).
    Setiap akun punya instance PriceValidator sendiri.
    """

    def __init__(
        self,
        *,
        tolerance_pct: Decimal = Decimal("0.01"),  # 1%
        max_retries: int = 3,
        retry_delay_seconds: float = 3.0,
    ) -> None:
        self.tolerance_pct = tolerance_pct
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        # Execution price terakhir per pair: key = "sell->buy"
        self._last_execution: dict[str, ExecutionPriceRecord] = {}

    def _pair_key(self, sell_symbol: str, buy_symbol: str) -> str:
        return f"{sell_symbol}->{buy_symbol}"

    def _reverse_pair_key(self, sell_symbol: str, buy_symbol: str) -> str:
        return f"{buy_symbol}->{sell_symbol}"

    def get_last_execution(
        self, sell_symbol: str, buy_symbol: str
    ) -> ExecutionPriceRecord | None:
        """Ambil execution price terakhir untuk pair ini."""
        return self._last_execution.get(self._pair_key(sell_symbol, buy_symbol))

    def update_from_history_item(self, item: dict[str, Any], cc_symbol: str = "CC") -> None:
        """Update execution price dari satu item trading history.

        item: dict dari API /v1/history/trading
        """
        sell_sym_raw = item.get("token_input_instrument_id", "")
        buy_sym_raw = item.get("token_output_instrument_id", "")

        # Normalize: Amulet = CC
        sell_sym = cc_symbol if sell_sym_raw == "Amulet" else sell_sym_raw
        buy_sym = cc_symbol if buy_sym_raw == "Amulet" else buy_sym_raw

        if not sell_sym or not buy_sym:
            return

        try:
            trade_price = Decimal(str(item.get("trade_price_raw") or "0"))
            amount_in = Decimal(str(item.get("amount_input") or "0"))
            amount_out = Decimal(str(item.get("amount_output") or "0"))
        except Exception:
            return

        if trade_price <= Decimal("0") or amount_in <= Decimal("0"):
            return

        record = ExecutionPriceRecord(
            sell_symbol=sell_sym,
            buy_symbol=buy_sym,
            trade_price=trade_price,
            amount_in=amount_in,
            amount_out=amount_out,
            timestamp_utc=str(item.get("timestamp_utc", "")),
            update_id=str(item.get("update_id", "")),
        )
        key = self._pair_key(sell_sym, buy_sym)
        existing = self._last_execution.get(key)
        # Hanya update jika lebih baru (berdasarkan timestamp)
        if existing is None or record.timestamp_utc >= existing.timestamp_utc:
            self._last_execution[key] = record
            logger.debug(
                "Execution price updated | %s->%s | price=%s | ts=%s",
                sell_sym,
                buy_sym,
                trade_price,
                record.timestamp_utc,
            )

    def update_from_history_payload(
        self, history_payload: Any, cc_symbol: str = "CC"
    ) -> int:
        """Update execution prices dari seluruh trading history payload.

        Returns: jumlah item yang diproses.
        """
        if history_payload is None:
            return 0
        items = []
        if isinstance(history_payload, dict):
            items = history_payload.get("history_trading", [])
        elif isinstance(history_payload, list):
            items = history_payload
        count = 0
        for item in items:
            if isinstance(item, dict):
                self.update_from_history_item(item, cc_symbol)
                count += 1
        return count

    def validate_quote_price(
        self,
        sell_symbol: str,
        buy_symbol: str,
        quote_price: Decimal,
    ) -> tuple[bool, str]:
        """Validasi harga quote vs execution price terakhir.

        Returns: (is_valid, reason)
        - is_valid=True: harga dalam toleransi, lanjut swap
        - is_valid=False: harga di luar toleransi, perlu retry
        """
        last = self.get_last_execution(sell_symbol, buy_symbol)
        if last is None:
            # Tidak ada referensi, langsung lanjut
            return True, "no_reference"

        if last.trade_price <= Decimal("0"):
            return True, "reference_zero"

        # Hitung selisih persentase
        diff_pct = abs(quote_price - last.trade_price) / last.trade_price
        is_valid = diff_pct <= self.tolerance_pct

        reason = (
            f"diff={diff_pct:.4%} ref={last.trade_price} quote={quote_price}"
        )
        if not is_valid:
            reason = f"PRICE_DEVIATION {reason}"

        return is_valid, reason

    def is_arbitrage_opportunity(
        self,
        sell_symbol: str,
        buy_symbol: str,
        quote_price: Decimal,
    ) -> tuple[bool, str]:
        """Cek apakah ini peluang arbitrase (harga lebih baik dari reverse pair).

        Khusus untuk CBTC→USDCx: bypass validasi jika harga lebih menguntungkan
        dari USDCx→CBTC sebelumnya.

        Returns: (is_arbitrage, reason)
        """
        if sell_symbol != "CBTC" or buy_symbol != "USDCx":
            return False, "not_cbtc_usdcx"

        # Cek reverse pair: USDCx→CBTC
        reverse = self.get_last_execution("USDCx", "CBTC")
        if reverse is None:
            return False, "no_reverse_reference"

        # Harga CBTC→USDCx saat ini vs harga USDCx→CBTC sebelumnya
        # Jika 1/quote_price > reverse.trade_price, artinya CBTC lebih mahal sekarang
        # = lebih menguntungkan untuk jual CBTC
        if reverse.trade_price <= Decimal("0"):
            return False, "reverse_price_zero"

        # quote_price = USDCx per CBTC
        # reverse.trade_price = CBTC per USDCx
        # Jika quote_price > 1/reverse.trade_price, artinya harga CBTC naik
        implied_cbtc_price = Decimal("1") / reverse.trade_price
        if quote_price > implied_cbtc_price:
            return True, (
                f"arbitrage: CBTC_price={quote_price} > implied={implied_cbtc_price:.6f} "
                f"(from USDCx->CBTC ref={reverse.trade_price})"
            )

        return False, f"no_arbitrage: CBTC_price={quote_price} <= implied={implied_cbtc_price:.6f}"

    def get_summary(self) -> dict[str, Any]:
        """Ringkasan execution prices yang tersimpan."""
        return {
            key: {
                "price": str(rec.trade_price),
                "ts": rec.timestamp_utc,
                "pair": f"{rec.sell_symbol}->{rec.buy_symbol}",
            }
            for key, rec in self._last_execution.items()
        }
