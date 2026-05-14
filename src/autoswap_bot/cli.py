from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import threading
from decimal import Decimal
from pathlib import Path

from .bot import AutoswapBot, configure_logging, summarize_results
from .estimator import render_required_cc_report
from .config import load_config
from .env_loader import load_dotenv_file
from .withdraw_bot import WithdrawBot


STRATEGY_CHOICES = {
    "1": "strategy_1",
    "2": "strategy_2",
    "3": "strategy_3_cycle",
    "4": "strategy_4_reserve",
}

STARTUP_MODE_CHOICES = {
    "1": "swap_only",
    "2": "free_only",
    "3": "free_then_swap",
    "4": "refill_cc",
    "5": "check_accounts",
    "6": "withdraw",
}

POST_TARGET_REFILL_CHOICES = {
    "1": "CC",
    "2": "USDCx",
    "3": "USDCx_v2",
}


def _default_startup_mode() -> str:
    return "free_then_swap"


def _prompt_strategy() -> str | None:
    """Prompt user to select a strategy. Returns strategy key or None to use config default."""
    if not sys.stdin.isatty():
        return None

    print("\nPilih strategi:", flush=True)
    print("1. Strategi 1: CC → USDCx", flush=True)
    print("2. Strategi 2: CC → CBTC", flush=True)
    print("3. Strategi 3: CC → USDCx → CBTC (Cycle)", flush=True)
    print("4. Strategi 4: CC → USDCx → CBTC (Reserve)", flush=True)
    print("0. Gunakan strategi dari config (default)", flush=True)

    while True:
        print("Masukkan pilihan (0/1/2/3/4): ", end="", flush=True)
        try:
            answer = input().strip()
        except EOFError:
            print("", flush=True)
            return None
        if answer == "0" or answer == "":
            return None
        strategy = STRATEGY_CHOICES.get(answer)
        if strategy is not None:
            return strategy
        print("Pilihan tidak valid.", flush=True)


def _prompt_startup_mode() -> str:
    if not sys.stdin.isatty():
        return _default_startup_mode()

    print("\nPilih mode bot:", flush=True)
    print("1. Mode swap langsung (direct)", flush=True)
    print("2. Mode free swap only", flush=True)
    print("3. Mode free lalu swap", flush=True)
    print("4. Mode refill semua token ke CC lalu berhenti", flush=True)
    print("5. Mode cek akun (lihat balance, reward, fee)", flush=True)
    print("6. Mode withdraw (transfer CC ke alamat tujuan)", flush=True)

    while True:
        print("Masukkan pilihan (1/2/3/4/5/6): ", end="", flush=True)
        try:
            answer = input().strip()
        except EOFError:
            print("", flush=True)
            return _default_startup_mode()
        startup_mode = STARTUP_MODE_CHOICES.get(answer)
        if startup_mode is not None:
            return startup_mode
        print("Pilihan tidak valid.", flush=True)


def _prompt_post_target_refill_symbol(startup_mode: str) -> str:
    """Prompt target refill setelah progress swap tercapai."""
    if startup_mode in {"refill_cc", "check_accounts", "withdraw"}:
        return "CC"
    if not sys.stdin.isatty():
        return "CC"

    print("\nPilih target refill setelah progress swap tercapai:", flush=True)
    print("1. Mode Refill ke CC", flush=True)
    print("2. Mode Refill ke USDCx", flush=True)
    print("3. Mode Refill ke USDCx v2", flush=True)

    while True:
        print("Masukkan pilihan (1/2/3): ", end="", flush=True)
        try:
            answer = input().strip()
        except EOFError:
            print("", flush=True)
            return "CC"
        target = POST_TARGET_REFILL_CHOICES.get(answer)
        if target is not None:
            return target
        print("Pilihan tidak valid.", flush=True)


class InterruptController:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.bot: AutoswapBot | None = None
        self._prompt_active = False
        self._prompt_lock = threading.Lock()

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

    def attach_bot(self, bot: AutoswapBot) -> None:
        self.bot = bot

    def handle_sigint(self) -> None:
        if self.loop is None or self.loop.is_closed():
            return
        with self._prompt_lock:
            if self._prompt_active:
                return
            self._prompt_active = True
        prompt_thread = threading.Thread(
            target=self._confirm_stop_blocking,
            name="interrupt-confirmation",
            daemon=True,
        )
        prompt_thread.start()

    def _confirm_stop_blocking(self) -> None:
        try:
            if self.bot is not None:
                self.bot.monitor.set_terminal_dashboard_paused(True)
            print("\nberhenti? (y/n) ", end="", flush=True)
            answer = input().strip().lower()
            if answer == "y" and self.bot is not None and self.loop is not None and not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.loop.create_task, self.bot.request_stop())
            elif answer == "n":
                print("lanjut.\n", end="", flush=True)
        except (EOFError, KeyboardInterrupt):
            return
        finally:
            if self.bot is not None:
                self.bot.monitor.set_terminal_dashboard_paused(False)
            with self._prompt_lock:
                self._prompt_active = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cantex autoswap bot")
    parser.add_argument(
        "--config",
        default="config/accounts.toml",
        help="Path ke file konfigurasi TOML",
    )
    parser.add_argument(
        "--strategy",
        default=None,
        choices=["1", "2", "3", "4"],
        help="Override strategi (1-4). Jika tidak diset, akan ditanya interaktif.",
    )
    parser.add_argument(
        "--mode",
        default=None,
        choices=["swap_only", "free_only", "free_then_swap", "refill_cc", "check_accounts", "withdraw"],
        help="Override startup mode. Jika tidak diset, akan ditanya interaktif.",
    )
    parser.add_argument(
        "--post-target-refill",
        default=None,
        choices=["CC", "USDCx", "USDCx_v2"],
        help="Target refill setelah progress swap tercapai (CC, USDCx, atau USDCx_v2). Default: CC.",
    )
    return parser


async def _run(
    config_path: str,
    interrupt_controller: InterruptController,
    *,
    cli_strategy: str | None = None,
    cli_mode: str | None = None,
    cli_post_target_refill: str | None = None,
) -> int:
    repo_root = Path(__file__).resolve().parents[2]
    load_dotenv_file(repo_root / ".env")
    config = load_config(config_path)

    # Strategy selection
    selected_strategy = cli_strategy
    if selected_strategy is None:
        selected_strategy = _prompt_strategy()
    # Apply strategy override to all accounts if selected
    if selected_strategy is not None:
        for account in config.accounts:
            object.__setattr__(account, "strategy_name", selected_strategy)

    # Mode selection
    startup_mode = cli_mode
    if startup_mode is None:
        startup_mode = _prompt_startup_mode()
    post_target_refill_symbol = cli_post_target_refill or _prompt_post_target_refill_symbol(startup_mode)

    if startup_mode == "estimate_cc":
        print(render_required_cc_report(config), flush=True)
        return 0

    configure_logging(
        config.runtime.log_level,
        use_utc=True,
        terminal_dashboard_enabled=config.runtime.terminal_dashboard_enabled,
    )

    # Mode withdraw: jalankan WithdrawBot, tidak perlu AutoswapBot
    if startup_mode == "withdraw":
        target_address = config.runtime.withdraw_target_address
        if not target_address:
            # Prompt interaktif jika tidak dikonfigurasi di config
            if sys.stdin.isatty():
                print("\nMasukkan alamat tujuan withdraw (Cantex::1220...): ", end="", flush=True)
                try:
                    target_address = input().strip()
                except EOFError:
                    target_address = ""
            if not target_address:
                print("❌ ERROR: withdraw_target_address tidak dikonfigurasi.", flush=True)
                print("Tambahkan di [settings] accounts.toml:", flush=True)
                print('  withdraw_target_address = "Cantex::1220..."', flush=True)
                return 1

        withdraw_bot = WithdrawBot(
            config,
            repo_root=repo_root,
            target_address=target_address,
            saldo_sisa=config.runtime.withdraw_saldo_sisa,
            fee_reserve=config.runtime.withdraw_fee_reserve,
            delay_seconds=config.runtime.withdraw_delay_seconds,
            symbols=config.runtime.withdraw_symbols,
        )
        await withdraw_bot.run()
        return 0

    bot = AutoswapBot(
        config,
        repo_root=repo_root,
        startup_mode=startup_mode,
        post_target_refill_symbol=post_target_refill_symbol,
    )
    interrupt_controller.attach_bot(bot)
    results = await bot.run()
    print(summarize_results(results))
    return 0 if all(result.ok or result.aborted for result in results) else 1


def main() -> int:
    args = build_parser().parse_args()

    # Resolve CLI overrides
    cli_strategy: str | None = None
    if args.strategy is not None:
        cli_strategy = STRATEGY_CHOICES.get(args.strategy)

    cli_mode: str | None = args.mode
    cli_post_target_refill: str | None = args.post_target_refill

    interrupt_controller = InterruptController()
    loop = asyncio.new_event_loop()
    interrupt_controller.attach_loop(loop)
    previous_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, lambda *_: interrupt_controller.handle_sigint())
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(
            _run(
                args.config,
                interrupt_controller,
                cli_strategy=cli_strategy,
                cli_mode=cli_mode,
                cli_post_target_refill=cli_post_target_refill,
            )
        )
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
