"""Helper untuk menghitung cycle loss dari trading history API.

Berdasarkan struktur API response yang sebenarnya:
- Swap CROSS (USDCx→CBTC via CC) muncul sebagai 2 leg terpisah:
  Leg 1: USDCx → CC  (token_input=USDCx, token_output=Amulet)
  Leg 2: CC → CBTC   (token_input=Amulet, token_output=CBTC)
- Swap CROSS (CBTC→USDCx via CC) muncul sebagai 2 leg terpisah:
  Leg 1: CBTC → CC   (token_input=CBTC, token_output=Amulet)
  Leg 2: CC → USDCx  (token_input=Amulet, token_output=USDCx)

Untuk mode USDCx:
  cycle_loss = USDCx_out (leg 1 USDCx→CC) - USDCx_in (leg 2 CC→USDCx)

Untuk mode CC:
  cycle_loss = CC_out (leg CC→foreign) - CC_in (leg foreign→CC)
"""
from __future__ import annotations
from decimal import Decimal


def compute_cycle_loss_usdcx(today_items: list[dict]) -> tuple[Decimal, int]:
    """Hitung total cycle loss untuk mode USDCx dari list swap hari ini.

    today_items: list of dict dengan keys: sell, buy, amount_in, amount_out, ts
    Returns: (total_loss, cycle_count)
    """
    total_loss = Decimal("0")
    cycle_count = 0
    pending_usdcx_out: Decimal | None = None

    for swap in today_items:
        sell = swap["sell"]
        buy = swap["buy"]
        amount_in = swap["amount_in"]
        amount_out = swap["amount_out"]

        # Open cycle: USDCx keluar ke CC (leg 1 dari swap USDCx→CBTC)
        if sell == "USDCx" and buy == "CC":
            pending_usdcx_out = amount_in  # USDCx yang keluar

        # Close cycle: CC masuk ke USDCx (leg 2 dari swap CBTC→USDCx)
        elif sell == "CC" and buy == "USDCx" and pending_usdcx_out is not None:
            usdcx_in = amount_out  # USDCx yang masuk
            loss = pending_usdcx_out - usdcx_in
            total_loss += loss
            cycle_count += 1
            pending_usdcx_out = None

    return total_loss, cycle_count


def compute_cycle_loss_cc(today_items: list[dict]) -> tuple[Decimal, int]:
    """Hitung total cycle loss untuk mode CC dari list swap hari ini.

    today_items: list of dict dengan keys: sell, buy, amount_in, amount_out, ts
    Returns: (total_loss, cycle_count)
    """
    total_loss = Decimal("0")
    cycle_count = 0
    pending_cc_out: Decimal | None = None
    pending_foreign: str | None = None

    for swap in today_items:
        sell = swap["sell"]
        buy = swap["buy"]
        amount_in = swap["amount_in"]
        amount_out = swap["amount_out"]

        # Open cycle: CC keluar ke foreign
        if sell == "CC" and buy in ("USDCx", "CBTC"):
            pending_cc_out = amount_in
            pending_foreign = buy

        # Close cycle: foreign masuk ke CC
        elif (
            buy == "CC"
            and sell in ("USDCx", "CBTC")
            and pending_cc_out is not None
        ):
            cc_in = amount_out
            loss = pending_cc_out - cc_in
            total_loss += loss
            cycle_count += 1
            pending_cc_out = None
            pending_foreign = None

    return total_loss, cycle_count
