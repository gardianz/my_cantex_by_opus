"""Mode Withdraw — transfer semua CC dari setiap akun ke satu alamat tujuan.

Logic mengikuti wd_cantex/wdcantex.js:
- Login dengan operator key
- Ambil saldo CC unlocked
- Hitung amount = saldo - saldo_sisa - fee_reserve
- Build transfer → sign → submit via SDK

Konfigurasi di [settings] accounts.toml:
    withdraw_target_address = "Cantex::1220..."
    withdraw_saldo_sisa = "0"       # saldo CC yang disisakan
    withdraw_fee_reserve = "0"      # reserve untuk fee
    withdraw_delay_seconds = 10     # delay antar akun
    withdraw_symbols = ["CC"]       # simbol yang di-withdraw (default CC saja)
"""

from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import TYPE_CHECKING

from .constants import CC_SYMBOL, TRACKED_SYMBOLS, dust_for_symbol
from .sdk_ext import ExtendedCantexSDK

if TYPE_CHECKING:
    from .config import AccountConfig, BotConfig

logger = logging.getLogger("autoswap_bot.withdraw")


@dataclass
class WithdrawResult:
    account_name: str
    symbol: str
    balance_before: Decimal
    amount_withdrawn: Decimal
    tx_id: str | None = None
    success: bool = False
    error: str | None = None


@dataclass
class WithdrawSummary:
    results: list[WithdrawResult] = field(default_factory=list)

    @property
    def total_withdrawn(self) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = {}
        for r in self.results:
            if r.success:
                totals[r.symbol] = totals.get(r.symbol, Decimal("0")) + r.amount_withdrawn
        return totals

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.results if not r.success)


class WithdrawBot:
    """Bot untuk withdraw CC (dan token lain opsional) dari semua akun ke satu alamat."""

    def __init__(
        self,
        config: BotConfig,
        *,
        repo_root,
        target_address: str,
        saldo_sisa: Decimal = Decimal("0"),
        fee_reserve: Decimal = Decimal("0"),
        delay_seconds: float = 10.0,
        symbols: list[str] | None = None,
    ) -> None:
        self.config = config
        self.repo_root = repo_root
        self.target_address = target_address
        self.saldo_sisa = saldo_sisa
        self.fee_reserve = fee_reserve
        self.delay_seconds = delay_seconds
        # Default: hanya CC. Bisa ditambah "USDCx", "CBTC" jika perlu.
        self.symbols = symbols or [CC_SYMBOL]
        self.log = logging.getLogger("autoswap_bot.withdraw")

    def _build_sdk(self, account: AccountConfig) -> ExtendedCantexSDK:
        from cantex_sdk import OperatorKeySigner, IntentTradingKeySigner
        api_key_dir = self.repo_root / ".secrets" / "api_keys"
        api_key_dir.mkdir(parents=True, exist_ok=True)
        return ExtendedCantexSDK(
            OperatorKeySigner.from_hex(account.operator_key),
            IntentTradingKeySigner.from_hex(account.trading_key),
            base_url=self.config.runtime.base_url,
            api_key_path=str(api_key_dir / f"{account.key_slug}.txt"),
            max_retries=self.config.runtime.max_retries,
            retry_base_delay=self.config.runtime.retry_base_delay,
        )

    async def _withdraw_symbol(
        self,
        sdk: ExtendedCantexSDK,
        account: AccountConfig,
        symbol: str,
        instruments_by_symbol: dict,
    ) -> WithdrawResult:
        """Withdraw satu simbol dari satu akun."""
        result = WithdrawResult(
            account_name=account.name,
            symbol=symbol,
            balance_before=Decimal("0"),
            amount_withdrawn=Decimal("0"),
        )

        try:
            info = await sdk.get_account_info()
            # Cari balance unlocked untuk simbol ini
            balance = Decimal("0")
            for token in info.tokens:
                if token.instrument_symbol == symbol:
                    balance = token.unlocked_amount
                    break

            result.balance_before = balance
            self.log.info(
                "[%s] Saldo %s: %s",
                account.name,
                symbol,
                balance,
            )

            # Hitung amount yang akan di-withdraw
            # Untuk CC: kurangi saldo_sisa + fee_reserve
            # Untuk token lain: withdraw semua (tidak ada fee reserve)
            if symbol == CC_SYMBOL:
                amount = balance - self.saldo_sisa - self.fee_reserve
            else:
                amount = balance

            # Bulatkan ke 6 desimal ke bawah (hindari precision error)
            amount = amount.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

            dust = dust_for_symbol(symbol)
            if amount <= dust:
                self.log.info(
                    "[%s] %s: saldo tidak cukup untuk WD (amount=%s, dust=%s)",
                    account.name,
                    symbol,
                    amount,
                    dust,
                )
                result.error = f"Saldo tidak cukup (amount={amount})"
                return result

            instrument = instruments_by_symbol.get(symbol)
            if instrument is None:
                result.error = f"Instrument {symbol} tidak ditemukan"
                return result

            self.log.info(
                "[%s] Memproses WD %s sebesar: %s %s → %s...",
                account.name,
                symbol,
                amount,
                symbol,
                self.target_address[:30],
            )

            tx_result = await sdk.transfer(
                amount=amount,
                instrument=instrument,
                receiver=self.target_address,
                memo="",
            )

            tx_id = (
                tx_result.get("id")
                or tx_result.get("transactionId")
                or tx_result.get("contract_id")
                or "-"
            )
            result.amount_withdrawn = amount
            result.tx_id = tx_id
            result.success = True
            self.log.info(
                "[%s] ✅ WD %s berhasil! tx=%s | amount=%s",
                account.name,
                symbol,
                tx_id,
                amount,
            )

        except Exception as exc:
            result.error = str(exc)
            self.log.error("[%s] ❌ WD %s gagal: %s", account.name, symbol, exc)

        return result

    async def _process_account(self, account: AccountConfig) -> list[WithdrawResult]:
        """Proses withdraw untuk satu akun (semua simbol yang dikonfigurasi)."""
        results: list[WithdrawResult] = []
        sdk = self._build_sdk(account)

        try:
            async with sdk:
                self.log.info("[%s] Autentikasi...", account.name)
                await sdk.authenticate(force=True)
                self.log.info("[%s] Login berhasil", account.name)

                # Ambil instruments
                info = await sdk.get_account_info()
                admin = await sdk.get_account_admin()

                instruments_by_symbol: dict = {}
                for instrument in admin.instruments:
                    if instrument.instrument_symbol in TRACKED_SYMBOLS:
                        instruments_by_symbol[instrument.instrument_symbol] = instrument.instrument
                for token in info.tokens:
                    if token.instrument_symbol in TRACKED_SYMBOLS:
                        instruments_by_symbol[token.instrument_symbol] = token.instrument

                for symbol in self.symbols:
                    result = await self._withdraw_symbol(
                        sdk, account, symbol, instruments_by_symbol
                    )
                    results.append(result)

        except Exception as exc:
            self.log.error("[%s] Error saat proses akun: %s", account.name, exc)
            for symbol in self.symbols:
                results.append(WithdrawResult(
                    account_name=account.name,
                    symbol=symbol,
                    balance_before=Decimal("0"),
                    amount_withdrawn=Decimal("0"),
                    success=False,
                    error=str(exc),
                ))

        return results

    async def run(self) -> WithdrawSummary:
        """Jalankan withdraw untuk semua akun yang aktif."""
        summary = WithdrawSummary()
        accounts = [a for a in self.config.accounts if a.enabled]

        print("\n=== AUTO WITHDRAW CANTEX.IO ===", flush=True)
        print(f"Target Address  : {self.target_address}", flush=True)
        print(f"Saldo Sisa      : {self.saldo_sisa} CC", flush=True)
        print(f"Fee Reserve     : {self.fee_reserve} CC", flush=True)
        print(f"Simbol          : {', '.join(self.symbols)}", flush=True)
        print(f"Jumlah Akun     : {len(accounts)}", flush=True)
        print(f"Delay Antar Akun: {self.delay_seconds}s", flush=True)
        print("================================\n", flush=True)

        if not self.target_address or "GANTIKAN" in self.target_address:
            print("⚠️  ERROR: target_address belum dikonfigurasi!", flush=True)
            return summary

        for idx, account in enumerate(accounts):
            self.log.info(
                "Memproses akun %s/%s: %s",
                idx + 1,
                len(accounts),
                account.name,
            )
            results = await self._process_account(account)
            summary.results.extend(results)

            # Print status per akun
            for r in results:
                status = "✅" if r.success else "❌"
                if r.success:
                    print(
                        f"  {status} [{r.account_name}] {r.symbol}: "
                        f"WD {r.amount_withdrawn} | tx={r.tx_id}",
                        flush=True,
                    )
                else:
                    print(
                        f"  {status} [{r.account_name}] {r.symbol}: {r.error}",
                        flush=True,
                    )

            if idx < len(accounts) - 1 and self.delay_seconds > 0:
                print(
                    f"\nMenunggu {self.delay_seconds:.0f}s sebelum akun berikutnya...\n",
                    flush=True,
                )
                await asyncio.sleep(self.delay_seconds)

        # Print ringkasan
        print("\n=== RINGKASAN WITHDRAW ===", flush=True)
        print(f"Berhasil : {summary.success_count} akun", flush=True)
        print(f"Gagal    : {summary.fail_count} akun", flush=True)
        for symbol, total in summary.total_withdrawn.items():
            print(f"Total WD {symbol}: {total}", flush=True)
        print("==========================\n", flush=True)

        return summary
