from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import sys
import time
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable

from cantex_sdk import (
    AccountInfo,
    CantexAPIError,
    CantexAuthError,
    CantexTimeoutError,
    InstrumentId,
    IntentTradingKeySigner,
    OperatorKeySigner,
)

from .config import AccountConfig, BotConfig, PreparedAccountRun
from .constants import CC_SYMBOL, MIN_TICKET_SIZE_CC, TRACKED_SYMBOLS, dust_for_symbol
from .models import ActivitySummary, AccountResult, PlanIssue, RouteHop, RoutePlan
from .routing import RouteOptimizer
from .runtime_state import BotRuntimeStateStore, DailyFreeFeeStatus
from .sdk_ext import ExtendedCantexSDK
from .cycle_tracker import CycleTracker, CycleResult
from .fee_scraper import FeeScraper
from .telegram_monitor import TelegramCardState, TelegramCommand, TelegramMonitor


class AccountLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[{self.extra['account']}] {msg}", kwargs


class StopRequested(Exception):
    pass


@dataclass(frozen=True)
class ScheduledRound:
    round_index: int
    execute_at_utc: datetime


@dataclass
class StrategyRuntimeState:
    primary_index: int = 0
    recycle_index: int = 0
    recovery_index: int = 0
    reserve_recovery_active: bool = False
    consecutive_balance_blocked_rounds: int = 0
    strategy_4_topup_pending_recycle: bool = False
    strategy_4_topup_after_foreign_minimum: bool = False
    strategy_4_min_ticket_consecutive: int = 0
    strategy_4_min_ticket_blocked_pairs: set = field(default_factory=set)


@dataclass(frozen=True)
class StrategyAction:
    sell_symbol: str
    buy_symbol: str
    amount_mode: str
    pointer_group: str | None = None
    pointer_next_index: int | None = None
    fraction: Decimal | None = None
    cc_reserve_override: Decimal | None = None
    strict_amount: bool = False


@dataclass(frozen=True)
class RoundExecutionResult:
    completed: bool
    tx_count: int
    stop_reason: str | None = None
    skipped: bool = False


MAX_CONSECUTIVE_BALANCE_BLOCKED_ROUNDS = 5
MAX_CONSECUTIVE_MIN_TICKET_RETRIES = 3


class AutoswapBot:
    def __init__(
        self,
        config: BotConfig,
        *,
        repo_root: Path,
        startup_mode: str,
        post_target_refill_symbol: str = CC_SYMBOL,
    ) -> None:
        self.config = config
        self.repo_root = repo_root
        self.startup_mode = startup_mode
        self.post_target_refill_symbol = post_target_refill_symbol
        self.log = logging.getLogger("autoswap_bot")
        self.startup_utc_date = datetime.now(timezone.utc).date()
        self._prompt_lock = asyncio.Lock()
        self._free_fee_swap_lock = asyncio.Lock()
        self._rng = random.Random(self.config.runtime.random_seed)
        self.monitor = TelegramMonitor(self.config.runtime)
        self.runtime_state = BotRuntimeStateStore(
            self.config.runtime.bot_state_file,
            logging.getLogger("autoswap_bot.state"),
        )
        self._stop_requested = asyncio.Event()
        self._telegram_pause_requested = asyncio.Event()
        self._cycle_trackers: dict[str, CycleTracker] = {}
        self.fee_scraper = FeeScraper()
        self._account_party_ids: dict[str, str] = {}  # account_name -> party_id
        # Map account_name -> TelegramCardState aktif. Dipakai oleh callback
        # FeeScraper.on_result supaya periodic loop bisa merefresh card.
        self._monitor_cards_by_account: dict[str, TelegramCardState] = {}
        self._active_route_optimizers: set[RouteOptimizer] = set()
        self._telegram_command_task: asyncio.Task | None = None
        self._telegram_command_active = False
        # Daftarkan callback ke FeeScraper supaya tiap result.success
        # otomatis update card via Monitor (background task).
        self.fee_scraper.register_on_result(self._on_fee_scrape_result)

    def _get_cycle_tracker(self, account_name: str) -> CycleTracker:
        """Get or create a CycleTracker for the given account.

        Loads persisted state if available and mode matches.
        """
        if account_name not in self._cycle_trackers:
            # Determine mode from post_target_refill_symbol
            mode = self.post_target_refill_symbol
            if mode in ("USDCx", "USDCx_v2"):
                mode = "USDCx"
            else:
                mode = "CC"

            # Create tracker with mode
            tracker = CycleTracker(mode=mode)

            # Load persisted state if available
            try:
                persisted_state = self.runtime_state.get_cycle_state(account_name)
                tracker.restore_from_state(persisted_state)
            except Exception as exc:
                logger.debug("No persisted cycle state for %s: %s", account_name, exc)

            self._cycle_trackers[account_name] = tracker

        return self._cycle_trackers[account_name]

    def _on_fee_scrape_result(self, account_name: str, result) -> None:
        """Callback dipanggil FeeScraper saat ada result.success baru.

        Refresh dashboard card untuk akun terkait. Sengaja non-blocking:
        create background task agar callback tetap sync.
        """
        card = self._monitor_cards_by_account.get(account_name)
        if card is None:
            return

        async def _do_update() -> None:
            try:
                await self.monitor.update_ccview_fee(
                    card,
                    validator_fee_total=result.validator_fee_total,
                    validator_tx_count=result.validator_tx_count,
                    avg_fee_per_swap=result.avg_fee_per_swap,
                )
            except Exception as exc:
                self.log.debug(
                    "FeeScraper on_result update_ccview_fee gagal | %s | %s",
                    account_name,
                    exc,
                )

        try:
            task = asyncio.create_task(_do_update())
            task.add_done_callback(lambda t: None)
        except RuntimeError:
            # Tidak ada running loop (mis. saat shutdown). Abaikan.
            self.log.debug(
                "FeeScraper on_result skip create_task: tidak ada loop aktif | %s",
                account_name,
            )

    def _save_cycle_tracker_state(self, account_name: str) -> None:
        """Save cycle tracker state to runtime_state."""
        tracker = self._cycle_trackers.get(account_name)
        if tracker is None:
            return
        try:
            state = tracker.get_state_for_persistence()
            self.runtime_state.save_cycle_state(
                account_name,
                cycle_loss_cc=state["cycle_loss_cc"],
                cycle_loss_usdcx=state["cycle_loss_usdcx"],
                cycle_count=state["cycle_count"],
                cycle_mode=state["cycle_mode"],
                cycle_pending_type=state["cycle_pending_type"],
                cycle_pending_amount=state["cycle_pending_amount"],
                cycle_pending_target=state["cycle_pending_target"],
            )
        except Exception as exc:
            logger.warning("Failed to save cycle state for %s: %s", account_name, exc)

    async def request_stop(self) -> None:
        self._stop_requested.set()

    async def request_pause(self) -> None:
        self._telegram_pause_requested.set()

    def stop_requested(self) -> bool:
        return self._stop_requested.is_set() or self._telegram_pause_requested.is_set()

    async def request_start(self) -> None:
        self._telegram_pause_requested.clear()

    async def _start_telegram_command_loop(self) -> None:
        if not self.config.runtime.telegram_enabled:
            return
        if self._telegram_command_task is not None and not self._telegram_command_task.done():
            return
        self._telegram_command_active = True
        self._telegram_command_task = asyncio.create_task(
            self._telegram_command_loop(),
            name="telegram-command-loop",
        )
        self._telegram_command_task.add_done_callback(self._log_telegram_command_loop_failure)

    async def _stop_telegram_command_loop(self) -> None:
        self._telegram_command_active = False
        task = self._telegram_command_task
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        self._telegram_command_task = None

    def _log_telegram_command_loop_failure(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc is not None:
            self.log.warning("Telegram command loop berhenti: %s", exc)

    async def _telegram_command_loop(self) -> None:
        while self._telegram_command_active:
            commands = await self.monitor.poll_commands(timeout_seconds=0)
            for command in commands:
                await self._handle_telegram_command(command)
            await self._sleep_for_telegram_command_loop(2.0)

    async def _sleep_for_telegram_command_loop(self, seconds: float) -> None:
        try:
            await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            raise

    async def _handle_telegram_command(self, command: TelegramCommand) -> None:
        text = command.text.strip()
        command_name, _, rest = text.partition(" ")
        command_name = command_name.split("@", 1)[0].lower()
        args = rest.strip()

        if command_name in {"/help", "/menu"}:
            reply = self._telegram_help_text()
        elif command_name in {"/status", "/config"}:
            reply = self._telegram_config_text()
        elif command_name == "/startbot":
            await self.request_start()
            reply = "✅ Bot dilanjutkan. Command /startbot hanya melanjutkan loop yang sedang sleep/paused, bukan restart proses yang sudah selesai."
        elif command_name == "/stopbot":
            await self.request_pause()
            reply = "⏸️ Stop/pause diminta. Bot akan berhenti sementara di titik sleep/poll berikutnya dan bisa dilanjutkan dengan /startbot."
        elif command_name == "/set":
            reply = await self._handle_telegram_set_command(args)
        else:
            reply = "Command tidak dikenal. Kirim /help untuk daftar command."

        await self.monitor.send_command_reply(
            reply,
            reply_to_message_id=command.message_id,
        )

    def _telegram_help_text(self) -> str:
        return "\n".join(
            [
                "<b>Command Cantex Autoswap</b>",
                "/status atau /config - lihat status dan config runtime",
                "/stopbot - pause/stop aman bot",
                "/startbot - lanjutkan bot jika sedang stop/sleep",
                "/set max_network_fee_cc_per_execution 0.36",
                "/set fee_fast_poll_range 0.30 0.40",
                "/set network_fee_poll_seconds 5 10",
                "/set rounds 26",
                "/set rounds 24 28",
                "",
                "Catatan: perubahan berlaku runtime dan tidak menulis ulang file config.",
            ]
        )

    def _telegram_config_text(self) -> str:
        runtime = self.config.runtime
        fee_cap = runtime.max_network_fee_cc_per_execution
        fast_range = runtime.fee_fast_poll_range
        poll_range = runtime.network_fee_poll_seconds_range
        if self._stop_requested.is_set():
            status = "STOPPING"
        elif self._telegram_pause_requested.is_set():
            status = "PAUSED"
        else:
            status = "RUNNING"
        account_rounds = ", ".join(
            f"{account.name}={account.rounds_range.min_value}..{account.rounds_range.max_value}"
            for account in self.config.accounts
        )
        return "\n".join(
            [
                "<b>Status Bot</b>",
                f"State: {status}",
                f"max_network_fee_cc_per_execution: {fee_cap if fee_cap is not None else '-'}",
                (
                    "max_slippage_per_execution: "
                    f"{runtime.max_slippage_per_execution}"
                    if runtime.max_slippage_per_execution is not None
                    else "max_slippage_per_execution: -"
                ),
                (
                    "fee_fast_poll_range: "
                    f"{fast_range[0]}..{fast_range[1]}"
                    if fast_range is not None
                    else "fee_fast_poll_range: -"
                ),
                f"network_fee_poll_seconds: {poll_range.min_value}..{poll_range.max_value}",
                f"rounds: {account_rounds}",
            ]
        )

    async def _handle_telegram_set_command(self, args: str) -> str:
        if not args:
            return "Format: /set key value. Kirim /help untuk contoh."
        parts = args.split()
        key = parts[0].strip().lower()
        values = parts[1:]
        try:
            if key == "max_network_fee_cc_per_execution":
                return self._telegram_set_max_network_fee(values)
            if key == "fee_fast_poll_range":
                return self._telegram_set_fee_fast_poll_range(values)
            if key == "network_fee_poll_seconds":
                return self._telegram_set_network_fee_poll_seconds(values)
            if key == "rounds":
                return self._telegram_set_rounds(values)
        except (ArithmeticError, ValueError) as exc:
            return f"❌ {exc}"
        return "❌ Key tidak didukung. Key: max_network_fee_cc_per_execution, fee_fast_poll_range, network_fee_poll_seconds, rounds."

    def _telegram_set_max_network_fee(self, values: list[str]) -> str:
        if len(values) != 1:
            raise ValueError("Format: /set max_network_fee_cc_per_execution angka|none")
        value = values[0].strip().lower()
        if value in {"none", "off", "null", "-"}:
            object.__setattr__(self.config.runtime, "max_network_fee_cc_per_execution", None)
            self._sync_route_optimizer_fee_cap(None)
            return "✅ max_network_fee_cc_per_execution dinonaktifkan."
        parsed = Decimal(value)
        if parsed <= 0:
            raise ValueError("max_network_fee_cc_per_execution harus > 0")
        object.__setattr__(self.config.runtime, "max_network_fee_cc_per_execution", parsed)
        self._sync_route_optimizer_fee_cap(parsed)
        return f"✅ max_network_fee_cc_per_execution = {parsed} CC"

    def _sync_route_optimizer_fee_cap(self, value: Decimal | None) -> None:
        for router in list(self._active_route_optimizers):
            router.set_max_network_fee_cc(value)

    def _telegram_set_fee_fast_poll_range(self, values: list[str]) -> str:
        if len(values) == 1 and values[0].strip().lower() in {"none", "off", "null", "-"}:
            object.__setattr__(self.config.runtime, "fee_fast_poll_range", None)
            return "✅ fee_fast_poll_range dinonaktifkan."
        min_value, max_value = self._parse_telegram_decimal_range(
            values,
            "fee_fast_poll_range",
        )
        object.__setattr__(self.config.runtime, "fee_fast_poll_range", (min_value, max_value))
        return f"✅ fee_fast_poll_range = {min_value}..{max_value} CC"

    def _telegram_set_network_fee_poll_seconds(self, values: list[str]) -> str:
        min_value, max_value = self._parse_telegram_float_range(
            values,
            "network_fee_poll_seconds",
        )
        range_type = type(self.config.runtime.network_fee_poll_seconds_range)
        object.__setattr__(
            self.config.runtime,
            "network_fee_poll_seconds_range",
            range_type(min_value=min_value, max_value=max_value),
        )
        return f"✅ network_fee_poll_seconds = {min_value}..{max_value} detik"

    def _telegram_set_rounds(self, values: list[str]) -> str:
        min_value, max_value = self._parse_telegram_int_range(values, "rounds")
        for account in self.config.accounts:
            range_type = type(account.rounds_range)
            object.__setattr__(
                account,
                "rounds_range",
                range_type(min_value=min_value, max_value=max_value),
            )
        return f"✅ rounds semua akun = {min_value}..{max_value}. Berlaku untuk sesi baru/perhitungan berikutnya."

    def _parse_telegram_decimal_range(
        self,
        values: list[str],
        field_name: str,
    ) -> tuple[Decimal, Decimal]:
        if len(values) == 1:
            min_value = max_value = Decimal(values[0])
        elif len(values) == 2:
            min_value = Decimal(values[0])
            max_value = Decimal(values[1])
        else:
            raise ValueError(f"Format: /set {field_name} min max")
        if min_value <= 0 or max_value <= 0:
            raise ValueError(f"{field_name} harus > 0")
        if min_value > max_value:
            raise ValueError(f"{field_name}.min tidak boleh lebih besar dari .max")
        return min_value, max_value

    def _parse_telegram_float_range(
        self,
        values: list[str],
        field_name: str,
    ) -> tuple[float, float]:
        if len(values) == 1:
            min_value = max_value = float(values[0])
        elif len(values) == 2:
            min_value = float(values[0])
            max_value = float(values[1])
        else:
            raise ValueError(f"Format: /set {field_name} min max")
        if min_value <= 0 or max_value <= 0:
            raise ValueError(f"{field_name} harus > 0")
        if min_value > max_value:
            raise ValueError(f"{field_name}.min tidak boleh lebih besar dari .max")
        return min_value, max_value

    def _parse_telegram_int_range(
        self,
        values: list[str],
        field_name: str,
    ) -> tuple[int, int]:
        if len(values) == 1:
            min_value = max_value = int(values[0])
        elif len(values) == 2:
            min_value = int(values[0])
            max_value = int(values[1])
        else:
            raise ValueError(f"Format: /set {field_name} min max")
        if min_value < 1 or max_value < 1:
            raise ValueError(f"{field_name} minimal 1")
        if min_value > max_value:
            raise ValueError(f"{field_name}.min tidak boleh lebih besar dari .max")
        return min_value, max_value

    async def run(self) -> list[AccountResult]:
        await self.monitor.start()
        await self._start_telegram_command_loop()
        try:
            if self._startup_mode_is_check_accounts():
                await self._run_check_accounts()
                return [
                    AccountResult(
                        account_name=account.name,
                        strategy_label="check",
                        requested_rounds=0,
                        completed_rounds=0,
                        swap_transactions=0,
                        stop_reason="CHECK_ACCOUNTS_COMPLETE",
                    )
                    for account in self.config.accounts
                ]
            return await asyncio.gather(
                *(self._run_account(account) for account in self.config.accounts)
            )
        finally:
            await self._stop_telegram_command_loop()
            await self.fee_scraper.close()
            await self.monitor.close()

    async def _run_account(self, account: AccountConfig) -> AccountResult:
        logger = AccountLoggerAdapter(self.log, {"account": account.name})
        session_number = 0
        last_result: AccountResult | None = None

        while True:
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            session_number += 1
            last_result = await self._run_account_session(
                account=account,
                logger=logger,
                session_number=session_number,
            )
            if last_result.retry_after_seconds is not None:
                logger.info(
                    "Sesi %s retry dalam %.0f detik",
                    session_number,
                    last_result.retry_after_seconds,
                )
                await self._sleep_or_stop(last_result.retry_after_seconds)
                continue
            if (
                not self.config.runtime.full_24h_mode
                or not self.config.runtime.full_24h_auto_restart
                or last_result.error is not None
                or last_result.aborted
                or last_result.stop_reason is not None
                or self.config.runtime.dry_run
            ):
                return last_result

            logger.info(
                "Sesi 24 jam berikutnya akan dimulai ulang otomatis | sesi sebelumnya=%s selesai",
                session_number,
            )

    async def _run_account_session(
        self,
        *,
        account: AccountConfig,
        logger: AccountLoggerAdapter,
        session_number: int,
    ) -> AccountResult:
        prepared_run = account.prepare_run(self._rng)
        session_progress = self.runtime_state.start_or_resume_round_session(
            account.name,
            strategy_name=prepared_run.strategy_name,
            requested_rounds=prepared_run.rounds,
            prefer_requested_rounds=(
                account.rounds_range.min_value == account.rounds_range.max_value
            ),
        )
        prepared_run = PreparedAccountRun(
            strategy_name=prepared_run.strategy_name,
            rounds=session_progress.requested_rounds,
        )
        result = AccountResult(
            account_name=account.name,
            strategy_label=account.strategy().label,
            requested_rounds=prepared_run.rounds,
            completed_rounds=session_progress.completed_rounds,
            swap_transactions=session_progress.completed_rounds,
        )
        monitor_card = self.monitor.create_card(
            account,
            prepared_run,
            account.strategy().label,
        )
        # Register card supaya FeeScraper.on_result callback bisa
        # me-refresh dashboard saat periodic / background scrape memberi
        # hasil baru tanpa harus melewati card-update task per-hop.
        self._monitor_cards_by_account[account.name] = monitor_card
        self.runtime_state.ensure_account(account.name)
        sdk = self._build_sdk(account)

        try:
            async with sdk:
                await self.monitor.attach_card(monitor_card)
                logger.info("Autentikasi dimulai | sesi=%s", session_number)
                await self.monitor.log_event(
                    monitor_card,
                    f"🚀 Session {session_number} started",
                    force=True,
                )
                await sdk.authenticate(force=True)
                logger.info("Autentikasi sukses | sesi=%s", session_number)
                info = await sdk.get_account_info()
                # Store party_id for ccview fee scraper
                if info.address:
                    self._account_party_ids[account.name] = info.address
                    logger.info(
                        "CCView party_id stored | account=%s | party_id=%s",
                        account.name,
                        info.address,
                    )
                    # Trigger immediate startup scrape (non-blocking)
                    self.fee_scraper.trigger_startup_scrape(
                        party_id=info.address,
                        account_name=account.name,
                    )
                    # Start periodic background scrape as fallback
                    self.fee_scraper.start_periodic_scrape(
                        party_id=info.address,
                        account_name=account.name,
                    )
                    # Schedule delayed card update with startup scrape results
                    self._schedule_startup_ccview_update(
                        account_name=account.name,
                        monitor_card=monitor_card,
                        logger=logger,
                    )
                else:
                    logger.warning(
                        "CCView party_id KOSONG untuk akun %s — Gas tidak akan ter-update. "
                        "info.address='%s'",
                        account.name,
                        info.address,
                    )
                await self.monitor.log_event(
                    monitor_card,
                    f"🗓️ Ready for {prepared_run.rounds} swap rounds",
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"ðŸ§­ Startup mode: {self._startup_mode_label()}",
                )
                await self._sync_daily_free_fee_state_from_history(
                    sdk=sdk,
                    account_name=account.name,
                    logger=logger,
                    monitor_card=monitor_card,
                    force_log=True,
                )
                synced_completed_rounds = await self._wait_for_trading_history_round_progress(
                    sdk=sdk,
                    account=account,
                    prepared_run=prepared_run,
                    logger=logger,
                    monitor_card=monitor_card,
                    previous_completed_rounds=0,
                    force_log=True,
                )
                result.completed_rounds = synced_completed_rounds
                result.swap_transactions = synced_completed_rounds

                # Note: quota check moved to after router creation (need router for refill)
                _startup_quota_reached = (
                    result.completed_rounds >= prepared_run.rounds
                    and not self._startup_mode_is_refill_cc()
                )

                if account.auto_create_intent_account:
                    created = await sdk.ensure_intent_trading_account()
                    if created:
                        logger.info("Intent trading account berhasil dibuat")
                        await self.monitor.log_event(
                            monitor_card,
                            "🧩 Intent account created",
                        )

                intent_mismatch = await sdk.detect_intent_signer_mismatch()
                if intent_mismatch:
                    raise RuntimeError(intent_mismatch)

                admin = await sdk.get_account_admin()
                instruments_by_symbol = self._resolve_instruments(admin.instruments, info)
                router = RouteOptimizer(
                    sdk,
                    instruments_by_symbol,
                    max_network_fee_cc=self.config.runtime.max_network_fee_cc_per_execution,
                    max_slippage=self.config.runtime.max_slippage_per_execution,
                )
                self._active_route_optimizers.add(router)

                logger.info(
                    "Strategi=%s | putaran=%s | nominal-range=%s | delay-range=%s | startup-mode=%s | seed=%s",
                    account.strategy().label,
                    prepared_run.rounds,
                    self._format_text_map(account.describe_amount_ranges()),
                    self.config.runtime.swap_delay_seconds_range.describe(),
                    self._startup_mode_label(),
                    self.config.runtime.random_seed if self.config.runtime.random_seed is not None else "-",
                )
                self._log_balances(logger, info, "Balance awal")
                initial_balances = self._balances_by_symbol(info)
                await self.monitor.update_balances(
                    monitor_card,
                    initial_balances,
                    force=True,
                )
                # Record start-of-day balance untuk daily loss calculation.
                # Pakai simbol target refill yang efektif (CC / USDCx) supaya
                # CyLoss bekerja juga saat target = USDCx atau USDCx_v2.
                loss_target_symbol = self._effective_post_target_refill_symbol()
                start_balance = initial_balances.get(loss_target_symbol, Decimal("0"))
                await self.monitor.set_cc_balance_start_of_day(
                    monitor_card,
                    start_balance,
                    target_symbol=loss_target_symbol,
                )
                logger.info(
                    "Daily-loss start balance recorded: %s=%s",
                    loss_target_symbol,
                    start_balance,
                )
                baseline_activity = await self._fetch_activity_summary(sdk, logger)
                result.activity_summary = baseline_activity
                await self.monitor.update_activity(
                    monitor_card,
                    baseline_activity,
                    force=True,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🗓️ Ready for {prepared_run.rounds} swap rounds",
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🧭 Startup mode: {self._startup_mode_label()}",
                )

                # Startup quota check: jika target sudah tercapai, refill dulu baru sleep
                if _startup_quota_reached:
                    logger.info(
                        "Target round startup sudah tercapai | progress=%s/%s | cek balance untuk refill",
                        result.completed_rounds,
                        prepared_run.rounds,
                    )
                    used_network_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
                    used_swap_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
                    # Refill ke target pilihan sebelum sleep (mematuhi fee cap)
                    await self._refill_after_target(
                        sdk=sdk,
                        router=router,
                        account=account,
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                        result=result,
                    )
                    await self._wait_until_next_utc_day_after_quota(
                        sdk=sdk,
                        account=account,
                        prepared_run=prepared_run,
                        result=result,
                        logger=logger,
                        monitor_card=monitor_card,
                    )

                if self.config.runtime.dry_run:
                    logger.info("Dry-run aktif, tidak ada swap yang dieksekusi")
                    result.final_balances = self._balances_by_symbol(info)
                    result.activity_summary = await self._fetch_activity_summary(sdk, logger)
                    await self.monitor.log_event(
                        monitor_card,
                        "🧪 Dry run only",
                        force=True,
                    )
                    await self.monitor.finalize(monitor_card, phase="DRY-RUN")
                    return result

                used_network_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
                used_swap_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
                strategy_state = StrategyRuntimeState()
                await self._maybe_pre_refill_usdcx_v2(
                    sdk=sdk,
                    router=router,
                    account=account,
                    prepared_run=prepared_run,
                    logger=logger,
                    monitor_card=monitor_card,
                    used_network_fee=used_network_fee,
                    used_swap_fee=used_swap_fee,
                    result=result,
                )
                if result.completed_rounds >= prepared_run.rounds and not self._startup_mode_is_refill_cc():
                    await self._refill_after_target(
                        sdk=sdk,
                        router=router,
                        account=account,
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                        result=result,
                    )
                    await self._wait_until_next_utc_day_after_quota(
                        sdk=sdk,
                        account=account,
                        prepared_run=prepared_run,
                        result=result,
                        logger=logger,
                        monitor_card=monitor_card,
                    )
                if self._startup_mode_is_refill_cc():
                    await self._perform_weekly_refill_to_cc(
                        sdk=sdk,
                        router=router,
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                        result=result,
                    )
                    final_info = await sdk.get_account_info()
                    result.final_balances = self._balances_by_symbol(final_info)
                    result.used_network_fee_by_symbol = dict(used_network_fee)
                    result.used_swap_fee_by_symbol = dict(used_swap_fee)
                    result.activity_summary = await self._fetch_activity_summary(sdk, logger)
                    self._log_balances(logger, final_info, "Balance akhir")
                    await self.monitor.update_balances(
                        monitor_card,
                        result.final_balances,
                        force=True,
                    )
                    await self.monitor.update_activity(
                        monitor_card,
                        result.activity_summary,
                        force=True,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"â›” Session stopped: {self._message_for_stop_reason(result.stop_reason or 'WEEKLY_REFILL_COMPLETE')}",
                        force=True,
                    )
                    await self.monitor.finalize(
                        monitor_card,
                        phase=f"STOPPED_{result.stop_reason or 'WEEKLY_REFILL_COMPLETE'}",
                    )
                    return result
                await self.monitor.log_event(
                    monitor_card,
                    (
                        "🗓️ 24h startup mode: planned"
                        if self._startup_mode_is_planned()
                        else "⚡ 24h startup mode: direct"
                    ),
                    force=True,
                )
                if self._startup_mode_is_planned():
                    while result.completed_rounds < prepared_run.rounds:
                        if self._weekly_stop_due_utc():
                            await self._perform_weekly_stop(
                                logger=logger,
                                monitor_card=monitor_card,
                                result=result,
                            )
                            break
                        session_start_utc = datetime.now(timezone.utc)
                        session_end_utc = self._next_utc_midnight(session_start_utc)
                        remaining_rounds = prepared_run.rounds - result.completed_rounds
                        execution_buffer_seconds = self._estimate_24h_execution_buffer_seconds(remaining_rounds)
                        schedule = self._build_24h_schedule(
                            rounds=remaining_rounds,
                            start_utc=session_start_utc,
                            end_utc=session_end_utc,
                            execution_buffer_seconds=execution_buffer_seconds,
                        )
                        self._log_24h_schedule(
                            logger,
                            remaining_rounds,
                            session_start_utc,
                            session_end_utc,
                            schedule,
                            execution_buffer_seconds,
                            start_round_number=result.completed_rounds + 1,
                        )
                        await self._run_24h_session(
                            sdk=sdk,
                            router=router,
                            account=account,
                            prepared_run=prepared_run,
                            strategy_state=strategy_state,
                            logger=logger,
                            monitor_card=monitor_card,
                            used_network_fee=used_network_fee,
                            used_swap_fee=used_swap_fee,
                            result=result,
                            session_end_utc=session_end_utc,
                            schedule=schedule,
                        )
                        if result.stop_reason:
                            break
                        if result.completed_rounds < prepared_run.rounds:
                            logger.info(
                                "Rounds tersisa %s, lanjut ke sesi UTC berikutnya",
                                prepared_run.rounds - result.completed_rounds,
                            )
                            await self.monitor.log_event(
                                monitor_card,
                                f"⏭️ {prepared_run.rounds - result.completed_rounds} rounds remaining, continue next UTC session",
                                force=True,
                            )
                else:
                    await self._run_24h_direct_session(
                        sdk=sdk,
                        router=router,
                        account=account,
                        prepared_run=prepared_run,
                        strategy_state=strategy_state,
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                        result=result,
                    )

                final_info = await sdk.get_account_info()
                result.final_balances = self._balances_by_symbol(final_info)
                result.used_network_fee_by_symbol = dict(used_network_fee)
                result.used_swap_fee_by_symbol = dict(used_swap_fee)
                result.activity_summary = await self._fetch_activity_summary(sdk, logger)
                self._log_balances(logger, final_info, "Balance akhir")
                await self.monitor.update_balances(
                    monitor_card,
                    result.final_balances,
                    force=True,
                )
                await self.monitor.update_activity(
                    monitor_card,
                    result.activity_summary,
                    force=True,
                )
                if result.error is None and not result.aborted:
                    if result.stop_reason:
                        await self.monitor.log_event(
                            monitor_card,
                            f"⛔ Session stopped: {self._message_for_stop_reason(result.stop_reason)}",
                            force=True,
                        )
                        await self.monitor.finalize(
                            monitor_card,
                            phase=f"STOPPED_{result.stop_reason}",
                        )
                    else:
                        await self.monitor.log_event(
                            monitor_card,
                            "🏁 Session completed",
                            force=True,
                        )
                        await self.monitor.finalize(monitor_card, phase="FINISHED")
        except StopRequested:
            result.aborted = True
            result.stop_reason = "MANUAL_STOP"
            result.error = "Dihentikan user"
            logger.info("Eksekusi dihentikan user")
            await self.monitor.log_event(
                monitor_card,
                "? Stopped by user",
                force=True,
            )
            await self.monitor.finalize(monitor_card, phase="STOPPED_MANUAL")
        except (CantexAuthError, CantexAPIError, CantexTimeoutError) as exc:
            if self._is_retryable_session_error(exc):
                await self._mark_retryable_session_error(
                    result=result,
                    logger=logger,
                    monitor_card=monitor_card,
                    exc=exc,
                    session_number=session_number,
                )
                return result
            result.error = str(exc)
            logger.error("Eksekusi gagal: %s", exc)
            await self.monitor.log_event(
                monitor_card,
                f"❌ Error: {exc}",
                force=True,
            )
            await self.monitor.finalize(monitor_card, phase="FAILED")
        except Exception as exc:  # pragma: no cover - runtime guard
            if self._is_retryable_session_error(exc):
                await self._mark_retryable_session_error(
                    result=result,
                    logger=logger,
                    monitor_card=monitor_card,
                    exc=exc,
                    session_number=session_number,
                )
                return result
            result.error = str(exc)
            logger.exception("Error tak terduga: %s", exc)
            await self.monitor.log_event(
                monitor_card,
                f"❌ Unexpected: {exc}",
                force=True,
            )
            await self.monitor.finalize(monitor_card, phase="FAILED")
        finally:
            if "router" in locals():
                self._active_route_optimizers.discard(router)

        return result

    def _build_sdk(self, account: AccountConfig) -> ExtendedCantexSDK:
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

    def _resolve_instruments(self, admin_instruments, info: AccountInfo) -> dict[str, InstrumentId]:
        resolved: dict[str, InstrumentId] = {}
        for instrument in admin_instruments:
            if instrument.instrument_symbol in TRACKED_SYMBOLS:
                resolved[instrument.instrument_symbol] = instrument.instrument
        for token in info.tokens:
            if token.instrument_symbol in TRACKED_SYMBOLS:
                resolved[token.instrument_symbol] = token.instrument

        missing = [symbol for symbol in TRACKED_SYMBOLS if symbol not in resolved]
        if missing:
            raise RuntimeError(f"Instrument tidak ditemukan untuk simbol: {', '.join(missing)}")
        return resolved

    def _build_round_robin_candidates(
        self,
        actions: tuple[tuple[str, str, str, Decimal | None], ...],
        *,
        start_index: int,
        pointer_group: str,
    ) -> list[StrategyAction]:
        candidates: list[StrategyAction] = []
        action_count = len(actions)
        for offset in range(action_count):
            current_index = (start_index + offset) % action_count
            sell_symbol, buy_symbol, amount_mode, fraction = actions[current_index]
            candidates.append(
                StrategyAction(
                    sell_symbol=sell_symbol,
                    buy_symbol=buy_symbol,
                    amount_mode=amount_mode,
                    pointer_group=pointer_group,
                    pointer_next_index=(current_index + 1) % action_count,
                    fraction=fraction,
                )
            )
        return candidates

    def _strategy_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
        strategy_state: StrategyRuntimeState,
    ) -> tuple[list[StrategyAction], str]:
        strategy_key = account.strategy().key
        if strategy_key == "strategy_1":
            return self._strategy_1_action_candidates(account=account, balances=balances)
        if strategy_key == "strategy_2":
            return self._strategy_2_action_candidates(account=account, balances=balances)
        if strategy_key == "strategy_3_cycle":
            return self._strategy_3_action_candidates(
                account=account,
                balances=balances,
                strategy_state=strategy_state,
            )
        if strategy_key == "strategy_4_reserve":
            return self._strategy_4_action_candidates(
                account=account,
                balances=balances,
                strategy_state=strategy_state,
            )
        raise ValueError(f"Strategi {strategy_key} tidak lagi didukung")

    def _strategy_1_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
    ) -> tuple[list[StrategyAction], str]:
        candidates: list[StrategyAction] = []
        reserve_fee = self._effective_cc_reserve(account)
        foreign_balance = self._spendable_amount(
            "CBTC",
            balances.get("CBTC", Decimal("0")),
            reserve_fee,
        )
        if foreign_balance > dust_for_symbol("CBTC"):
            candidates.append(StrategyAction(sell_symbol="CBTC", buy_symbol="CC", amount_mode="max"))

        cc_amount_range = account.amount_range_for_symbol(CC_SYMBOL)
        spendable_cc = self._spendable_amount(
            CC_SYMBOL,
            balances.get(CC_SYMBOL, Decimal("0")),
            reserve_fee,
        )
        if spendable_cc >= cc_amount_range.min_value:
            candidates.append(StrategyAction(sell_symbol="CC", buy_symbol="USDCx", amount_mode="config"))

        strategy_balance = self._spendable_amount(
            "USDCx",
            balances.get("USDCx", Decimal("0")),
            reserve_fee,
        )
        if strategy_balance > dust_for_symbol("USDCx"):
            candidates.append(StrategyAction(sell_symbol="USDCx", buy_symbol="CC", amount_mode="max"))

        if candidates:
            return candidates, "strategy_1 ready"
        return [], self._cc_source_block_reason(
            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
            spendable_cc=spendable_cc,
            required_min_amount=cc_amount_range.min_value,
            reserve_threshold=reserve_fee,
        )

    def _strategy_2_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
    ) -> tuple[list[StrategyAction], str]:
        candidates: list[StrategyAction] = []
        reserve_fee = self._effective_cc_reserve(account)
        foreign_balance = self._spendable_amount(
            "USDCx",
            balances.get("USDCx", Decimal("0")),
            reserve_fee,
        )
        if foreign_balance > dust_for_symbol("USDCx"):
            candidates.append(StrategyAction(sell_symbol="USDCx", buy_symbol="CC", amount_mode="max"))

        cc_amount_range = account.amount_range_for_symbol(CC_SYMBOL)
        spendable_cc = self._spendable_amount(
            CC_SYMBOL,
            balances.get(CC_SYMBOL, Decimal("0")),
            reserve_fee,
        )
        if spendable_cc >= cc_amount_range.min_value:
            candidates.append(StrategyAction(sell_symbol="CC", buy_symbol="CBTC", amount_mode="config"))

        strategy_balance = self._spendable_amount(
            "CBTC",
            balances.get("CBTC", Decimal("0")),
            reserve_fee,
        )
        if strategy_balance > dust_for_symbol("CBTC"):
            candidates.append(StrategyAction(sell_symbol="CBTC", buy_symbol="CC", amount_mode="max"))

        if candidates:
            return candidates, "strategy_2 ready"
        return [], self._cc_source_block_reason(
            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
            spendable_cc=spendable_cc,
            required_min_amount=cc_amount_range.min_value,
            reserve_threshold=reserve_fee,
        )

    def _strategy_3_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
        strategy_state: StrategyRuntimeState,
    ) -> tuple[list[StrategyAction], str]:
        reserve_fee = self._effective_cc_reserve(account)
        cc_amount_range = account.amount_range_for_symbol(CC_SYMBOL)
        spendable_cc = self._spendable_amount(
            CC_SYMBOL,
            balances.get(CC_SYMBOL, Decimal("0")),
            reserve_fee,
        )
        if spendable_cc >= cc_amount_range.min_value:
            return self._build_round_robin_candidates(
                (
                    ("CC", "USDCx", "config", None),
                    ("CC", "CBTC", "config", None),
                ),
                start_index=strategy_state.primary_index,
                pointer_group="primary",
            ), "strategy_3 spend phase"

        recycle_candidates: list[StrategyAction] = []
        recycle_actions = self._build_round_robin_candidates(
            (
                ("USDCx", "CBTC", "fraction", Decimal("0.5")),
                ("CBTC", "USDCx", "fraction", Decimal("0.5")),
                ("CBTC", "CC", "max", None),
                ("USDCx", "CC", "max", None),
            ),
            start_index=strategy_state.recycle_index,
            pointer_group="recycle",
        )
        for action in recycle_actions:
            spendable_source = self._spendable_amount(
                action.sell_symbol,
                balances.get(action.sell_symbol, Decimal("0")),
                reserve_fee,
            )
            if spendable_source <= dust_for_symbol(action.sell_symbol):
                continue
            recycle_candidates.append(action)
        if recycle_candidates:
            return recycle_candidates, "strategy_3 recycle phase"
        return [], self._cc_source_block_reason(
            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
            spendable_cc=spendable_cc,
            required_min_amount=cc_amount_range.min_value,
            reserve_threshold=reserve_fee,
        )

    def _strategy_4_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
        strategy_state: StrategyRuntimeState,
    ) -> tuple[list[StrategyAction], str]:
        reserve_fee = self._strategy_4_reserve_fee(account)
        reserve_kritis = self._strategy_4_reserve_kritis(account)
        cc_balance = balances.get(CC_SYMBOL, Decimal("0"))

        foreign_available = {
            symbol: self._spendable_amount(
                symbol,
                balances.get(symbol, Decimal("0")),
                reserve_fee,
            )
            for symbol in ("USDCx", "CBTC")
        }
        has_foreign_balance = any(
            amount > dust_for_symbol(symbol)
            for symbol, amount in foreign_available.items()
        )

        if strategy_state.strategy_4_topup_pending_recycle and not has_foreign_balance:
            return [], "strategy_4 waiting top-up balance settlement"

        if strategy_state.reserve_recovery_active and not has_foreign_balance:
            strategy_state.reserve_recovery_active = False

        if has_foreign_balance and (
            strategy_state.reserve_recovery_active or cc_balance <= reserve_kritis
        ):
            strategy_state.reserve_recovery_active = True
            recovery_candidates: list[StrategyAction] = []
            recovery_actions = self._build_round_robin_candidates(
                (
                    ("USDCx", "CC", "max", None),
                    ("CBTC", "CC", "max", None),
                ),
                start_index=strategy_state.recovery_index,
                pointer_group="recovery",
            )
            for action in recovery_actions:
                spendable_source = self._spendable_amount(
                    action.sell_symbol,
                    balances.get(action.sell_symbol, Decimal("0")),
                    reserve_fee,
                )
                if spendable_source <= dust_for_symbol(action.sell_symbol):
                    continue
                recovery_candidates.append(action)
            if recovery_candidates:
                return recovery_candidates, "strategy_4 recovery phase"
            return [], "strategy_4 recovery waiting foreign balance"

        recycle_candidates: list[StrategyAction] = []
        recycle_actions = self._build_round_robin_candidates(
            (
                ("USDCx", "CBTC", "max", None),
                ("CBTC", "USDCx", "max", None),
            ),
            start_index=strategy_state.recycle_index,
            pointer_group="recycle",
        )
        for action in recycle_actions:
            spendable_source = self._spendable_amount(
                action.sell_symbol,
                balances.get(action.sell_symbol, Decimal("0")),
                reserve_fee,
            )
            if spendable_source <= dust_for_symbol(action.sell_symbol):
                continue
            recycle_candidates.append(action)

        if has_foreign_balance and not strategy_state.strategy_4_topup_after_foreign_minimum:
            if recycle_candidates:
                return recycle_candidates, "strategy_4 recycle phase"

        # Jika topup_after_foreign_minimum aktif, HAPUS recycle candidates
        # agar bot langsung ke CC→USDCx topup (tidak retry pair yang kena MIN_TICKET_SIZE)
        if strategy_state.strategy_4_topup_after_foreign_minimum:
            recycle_candidates = []

        spendable_cc = self._spendable_amount(
            CC_SYMBOL,
            cc_balance,
            reserve_fee,
        )
        if spendable_cc > dust_for_symbol(CC_SYMBOL):
            recycle_candidates.append(
                StrategyAction(
                    sell_symbol="CC",
                    buy_symbol="USDCx",
                    amount_mode="config",
                    cc_reserve_override=reserve_fee,
                    strict_amount=True,
                )
            )
        if recycle_candidates:
            return recycle_candidates, (
                "strategy_4 recycle phase"
                if any(action.sell_symbol != CC_SYMBOL for action in recycle_candidates)
                else "strategy_4 spend phase"
            )

        return [], self._cc_source_block_reason(
            balance_cc=cc_balance,
            spendable_cc=spendable_cc,
            required_min_amount=account.amount_range_for_symbol(CC_SYMBOL).min_value,
            reserve_threshold=reserve_fee,
        )

    async def _resolve_strategy_action_amount(
        self,
        *,
        account: AccountConfig,
        action: StrategyAction,
        balances: dict[str, Decimal],
        router: RouteOptimizer,
    ) -> tuple[Decimal | None, Decimal | None, Decimal | None, str | None]:
        cc_reserve = self._effective_cc_reserve(account, action.cc_reserve_override)
        available_amount = self._spendable_amount(
            action.sell_symbol,
            balances.get(action.sell_symbol, Decimal("0")),
            cc_reserve,
        )
        if available_amount <= dust_for_symbol(action.sell_symbol):
            return None, None, None, f"{action.sell_symbol} balance terlalu kecil"

        if action.amount_mode == "config":
            amount_range = account.amount_range_for_symbol(action.sell_symbol)
            max_allowed_amount = min(available_amount, amount_range.max_value)
            if max_allowed_amount < amount_range.min_value:
                reason = (
                    self._cc_source_block_reason(
                        balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
                        spendable_cc=available_amount,
                        required_min_amount=amount_range.min_value,
                        reserve_threshold=cc_reserve,
                    )
                    if action.sell_symbol == CC_SYMBOL
                    else f"{action.sell_symbol} balance below user config min ({available_amount} < {amount_range.min_value})"
                )
                return None, amount_range.min_value, None, reason
            target_amount = self._sample_execution_amount(amount_range, max_allowed_amount)
            actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=action.sell_symbol,
                buy_symbol=action.buy_symbol,
                desired_amount=target_amount,
                max_available_amount=max_allowed_amount,
            )
            if action.strict_amount:
                if actual_amount is None:
                    return None, None, target_amount, min_ticket_reason
                if actual_amount != target_amount:
                    return (
                        None,
                        None,
                        target_amount,
                        (
                            f"exact amount required for {action.sell_symbol}->{action.buy_symbol} "
                            f"({target_amount}), got {actual_amount}"
                        ),
                    )
                return actual_amount, None, target_amount, min_ticket_reason
            return actual_amount, amount_range.min_value, None, min_ticket_reason

        if action.amount_mode == "max":
            if (
                account.strategy().key == "strategy_4_reserve"
                and {action.sell_symbol, action.buy_symbol} == {"USDCx", "CBTC"}
            ):
                return available_amount, None, None, None
            actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=action.sell_symbol,
                buy_symbol=action.buy_symbol,
                desired_amount=available_amount,
                max_available_amount=available_amount,
            )
            return actual_amount, None, None, min_ticket_reason

        if action.amount_mode == "fraction":
            fraction = action.fraction or Decimal("0")
            desired_amount = available_amount * fraction
            if desired_amount <= dust_for_symbol(action.sell_symbol):
                return None, None, None, f"{action.sell_symbol} balance terlalu kecil untuk mode {fraction}"
            actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=action.sell_symbol,
                buy_symbol=action.buy_symbol,
                desired_amount=desired_amount,
                max_available_amount=available_amount,
            )
            return actual_amount, None, None, min_ticket_reason

        raise ValueError(f"Mode amount tidak didukung: {action.amount_mode}")

    def _advance_strategy_state_after_success(
        self,
        *,
        strategy_state: StrategyRuntimeState,
        action: StrategyAction,
    ) -> None:
        if action.pointer_group == "primary" and action.pointer_next_index is not None:
            strategy_state.primary_index = action.pointer_next_index
        if action.pointer_group == "recycle" and action.pointer_next_index is not None:
            strategy_state.recycle_index = action.pointer_next_index
        if action.pointer_group == "recovery" and action.pointer_next_index is not None:
            strategy_state.recovery_index = action.pointer_next_index

    def _update_strategy_4_topup_guard_after_success(
        self,
        *,
        account: AccountConfig,
        strategy_state: StrategyRuntimeState,
        action: StrategyAction,
    ) -> None:
        if account.strategy().key != "strategy_4_reserve":
            return
        # Reset MIN_TICKET_SIZE consecutive counter on any successful swap
        strategy_state.strategy_4_min_ticket_consecutive = 0
        strategy_state.strategy_4_min_ticket_blocked_pairs.clear()
        if action.sell_symbol == CC_SYMBOL and action.buy_symbol == "USDCx":
            strategy_state.strategy_4_topup_pending_recycle = True
            strategy_state.strategy_4_topup_after_foreign_minimum = False
            return
        if action.sell_symbol in {"USDCx", "CBTC"}:
            strategy_state.strategy_4_topup_pending_recycle = False
            strategy_state.strategy_4_topup_after_foreign_minimum = False

    def _is_balance_blocking_stop_reason(self, stop_reason: str | None) -> bool:
        """Check if stop reason indicates a genuine balance shortage.

        NOTE: MIN_TICKET_SIZE is NOT included here because it indicates a
        routing/pair-size issue, not an actual balance shortage. An account
        can have sufficient CC and USDCx but still hit MIN_TICKET_SIZE when
        the routed equivalent is below protocol minimum. Including it here
        caused false-positive INSUFFICIENT_BALANCE stops.
        """
        return stop_reason in {
            "WAIT_SOURCE_BALANCE",
            "SERVER_INSUFFICIENT_BALANCE",
            "USER_CONFIG_MIN_NOT_MET",
        }

    def _is_min_ticket_stop_reason(self, stop_reason: str | None) -> bool:
        """Check if stop reason is MIN_TICKET_SIZE (routing issue, not balance)."""
        return stop_reason == "MIN_TICKET_SIZE"

    def _reset_balance_block_counter(self, strategy_state: StrategyRuntimeState) -> None:
        strategy_state.consecutive_balance_blocked_rounds = 0

    async def _build_skipped_round_result(
        self,
        *,
        account: AccountConfig,
        round_number: int,
        tx_count: int,
        strategy_state: StrategyRuntimeState,
        stop_reason: str,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> RoundExecutionResult:
        # --- MIN_TICKET_SIZE: routing/pair-size issue, NOT a balance shortage ---
        # This must NEVER increment consecutive_balance_blocked_rounds because
        # the account may have plenty of CC/USDCx but the routed equivalent
        # for a specific pair is below protocol minimum.
        if self._is_min_ticket_stop_reason(stop_reason):
            # Always reset the balance block counter — this is not a balance issue
            self._reset_balance_block_counter(strategy_state)

            if self._strategy_4_has_cc_topup_capacity(account=account, monitor_card=monitor_card):
                # Track consecutive MIN_TICKET_SIZE failures to prevent infinite retry loop
                strategy_state.strategy_4_min_ticket_consecutive += 1
                if strategy_state.strategy_4_min_ticket_consecutive >= MAX_CONSECUTIVE_MIN_TICKET_RETRIES:
                    # All foreign pairs are below min ticket size even after topup attempts.
                    # Force a CC->USDCx topup IMMEDIATELY by enabling the topup guard
                    # so the next round skips recycle and goes straight to CC->USDCx.
                    logger.warning(
                        "MIN_TICKET_SIZE berulang %s kali berturut-turut untuk akun %s, "
                        "memaksa CC->USDCx top-up baru SEGERA",
                        strategy_state.strategy_4_min_ticket_consecutive,
                        account.name,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        (
                            f"⚠️ Round {round_number}: MIN_TICKET_SIZE {strategy_state.strategy_4_min_ticket_consecutive}x, "
                            f"forcing IMMEDIATE CC top-up"
                        ),
                        force=True,
                    )
                    strategy_state.strategy_4_min_ticket_consecutive = 0
                    strategy_state.strategy_4_min_ticket_blocked_pairs.clear()
                    # Enable the foreign-minimum guard so next round SKIPS recycle
                    # and goes straight to CC->USDCx topup
                    strategy_state.strategy_4_topup_after_foreign_minimum = True
                    strategy_state.strategy_4_topup_pending_recycle = False
                    return RoundExecutionResult(
                        completed=False,
                        tx_count=tx_count,
                        stop_reason=stop_reason,
                        skipped=True,
                    )

                # Still have topup capacity, enable topup guard for next round
                strategy_state.strategy_4_topup_after_foreign_minimum = True
                logger.info(
                    "MIN_TICKET_SIZE untuk akun %s (bukan saldo kurang) — CC masih cukup untuk top-up, lanjut retry",
                    account.name,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"ℹ️ Round {round_number}: MIN_TICKET_SIZE (routing issue, bukan saldo kurang), retry",
                )
                return RoundExecutionResult(
                    completed=False,
                    tx_count=tx_count,
                    stop_reason=stop_reason,
                    skipped=True,
                )

            # No topup capacity but still NOT a balance block — just skip
            logger.info(
                "MIN_TICKET_SIZE untuk akun %s tanpa topup capacity — skip tanpa naikkan balance counter",
                account.name,
            )
            strategy_state.strategy_4_min_ticket_consecutive = 0
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason=stop_reason,
                skipped=True,
            )

        # --- Non-balance-blocking reasons (e.g. FEE_TOO_HIGH, ROUTE_ERROR) ---
        if not self._is_balance_blocking_stop_reason(stop_reason):
            self._reset_balance_block_counter(strategy_state)
            strategy_state.strategy_4_topup_after_foreign_minimum = False
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason=stop_reason,
                skipped=True,
            )

        # --- Genuine balance-blocking reasons ---
        if self._strategy_4_has_cc_topup_capacity(account=account, monitor_card=monitor_card):
            strategy_state.strategy_4_min_ticket_consecutive = 0
            self._reset_balance_block_counter(strategy_state)
            strategy_state.strategy_4_topup_after_foreign_minimum = True
            logger.info(
                "Saldo akun %s masih cukup untuk top-up CC->USDCx, tidak dihentikan sebagai saldo kurang",
                account.name,
            )
            await self.monitor.log_event(
                monitor_card,
                f"ℹ️ Round {round_number} pending: CC masih cukup untuk top-up, lanjut retry",
                force=True,
            )
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason=stop_reason,
                skipped=True,
            )

        strategy_state.consecutive_balance_blocked_rounds += 1
        blocked_rounds = strategy_state.consecutive_balance_blocked_rounds
        if blocked_rounds <= MAX_CONSECUTIVE_BALANCE_BLOCKED_ROUNDS:
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason=stop_reason,
                skipped=True,
            )

        logger.warning(
            "Saldo akun %s tidak lagi cukup untuk lanjut setelah %s round tertahan berturut-turut",
            account.name,
            blocked_rounds,
        )
        await self.monitor.log_event(
            monitor_card,
            (
                f"⛔ Round {round_number} stopped: saldo kurang "
                f"setelah {blocked_rounds} pending berturut-turut"
            ),
            force=True,
        )
        return RoundExecutionResult(
            completed=False,
            tx_count=tx_count,
            stop_reason="INSUFFICIENT_BALANCE",
            skipped=False,
        )

    def _strategy_4_has_cc_topup_capacity(
        self,
        *,
        account: AccountConfig,
        monitor_card: TelegramCardState | None,
    ) -> bool:
        if account.strategy().key != "strategy_4_reserve" or monitor_card is None:
            return False
        balances = monitor_card.balances
        cc_balance = balances.get(CC_SYMBOL, Decimal("0"))
        reserve_fee = self._strategy_4_reserve_fee(account)
        spendable_cc = self._spendable_amount(CC_SYMBOL, cc_balance, reserve_fee)
        required_cc = max(
            account.amount_range_for_symbol(CC_SYMBOL).min_value,
            MIN_TICKET_SIZE_CC,
        )
        return spendable_cc >= required_cc

    async def _execute_round(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        round_number: int,
        strategy_state: StrategyRuntimeState,
        fee_retry_deadline_utc: datetime | None,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> RoundExecutionResult:
        return await self._execute_round_dynamic(
            sdk=sdk,
            router=router,
            account=account,
            prepared_run=prepared_run,
            round_number=round_number,
            strategy_state=strategy_state,
            fee_retry_deadline_utc=fee_retry_deadline_utc,
            logger=logger,
            monitor_card=monitor_card,
            used_network_fee=used_network_fee,
            used_swap_fee=used_swap_fee,
        )

    async def _execute_round_dynamic(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        round_number: int,
        strategy_state: StrategyRuntimeState,
        fee_retry_deadline_utc: datetime | None,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> RoundExecutionResult:
        tx_count = 0
        sell_symbol = "-"
        buy_symbol = "-"
        pair_key = "-"
        selected_action: StrategyAction | None = None
        daily_free_fee_status: DailyFreeFeeStatus | None = None
        daily_free_fee_consumed = False
        return await self._execute_round_dynamic_v2(
            sdk=sdk,
            router=router,
            account=account,
            prepared_run=prepared_run,
            round_number=round_number,
            strategy_state=strategy_state,
            fee_retry_deadline_utc=fee_retry_deadline_utc,
            logger=logger,
            monitor_card=monitor_card,
            used_network_fee=used_network_fee,
            used_swap_fee=used_swap_fee,
        )
        try:
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            await self.monitor.update_balances(monitor_card, balances)
            amount_range = account.amount_range_for_symbol(sell_symbol)
            available_amount = self._spendable_amount(
                sell_symbol,
                balances.get(sell_symbol, Decimal("0")),
                self._effective_cc_reserve(account),
            )
            max_allowed_amount = min(available_amount, amount_range.max_value)
            if max_allowed_amount < amount_range.min_value or max_allowed_amount <= dust_for_symbol(sell_symbol):
                if sell_symbol == CC_SYMBOL:
                    refill_tx, balances, refill_satisfied = await self._refill_cc_for_source_step(
                        sdk=sdk,
                        router=router,
                        required_amount=amount_range.min_value,
                        cc_reserve=self._effective_cc_reserve(account),
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                    )
                    tx_count += refill_tx
                    available_amount = self._spendable_amount(
                        sell_symbol,
                        balances.get(sell_symbol, Decimal("0")),
                        self._effective_cc_reserve(account),
                    )
                    max_allowed_amount = min(available_amount, amount_range.max_value)
                    if not refill_satisfied and max_allowed_amount < amount_range.min_value:
                        reason = self._cc_source_block_reason(
                            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
                            spendable_cc=available_amount,
                            required_min_amount=amount_range.min_value,
                        )
                        await self.monitor.log_event(
                            monitor_card,
                            f"⏭️ Round {round_number} pending: {reason}",
                            force=True,
                        )
                        return RoundExecutionResult(
                            completed=False,
                            tx_count=tx_count,
                            stop_reason="WAIT_SOURCE_BALANCE",
                            skipped=True,
                        )
                if max_allowed_amount < amount_range.min_value or max_allowed_amount <= dust_for_symbol(sell_symbol):
                    reason = (
                        self._cc_source_block_reason(
                            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
                            spendable_cc=available_amount,
                            required_min_amount=amount_range.min_value,
                        )
                        if sell_symbol == CC_SYMBOL
                        else f"{sell_symbol} balance below user config min ({available_amount} < {amount_range.min_value})"
                    )
                    logger.info(
                        "Putaran %s belum bisa dieksekusi | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        reason,
                    )
                    await self.monitor.update_status(
                        monitor_card,
                        pair_key=self._monitor_pair_key(pair_key),
                        round_number=round_number,
                        phase="PROCESSING",
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Round {round_number} pending: {reason}",
                    )
                    return RoundExecutionResult(
                        completed=False,
                        tx_count=tx_count,
                        stop_reason="WAIT_SOURCE_BALANCE",
                        skipped=True,
                    )

            target_amount = self._sample_execution_amount(amount_range, max_allowed_amount)
            actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                desired_amount=target_amount,
                max_available_amount=max_allowed_amount,
            )
            if actual_amount is None:
                logger.info(
                    "Putaran %s belum valid di protocol | %s -> %s | %s",
                    round_number,
                    sell_symbol,
                    buy_symbol,
                    min_ticket_reason,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {min_ticket_reason}",
                    force=True,
                )
                return RoundExecutionResult(
                    completed=False,
                    tx_count=tx_count,
                    stop_reason="MIN_TICKET_SIZE",
                    skipped=True,
                )
            route, issue = await self._prepare_affordable_route(
                router=router,
                balances=balances,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                proposed_amount=actual_amount,
                round_number=round_number,
                cc_reserve=self._effective_cc_reserve(account),
            )
            if issue is not None:
                if sell_symbol == CC_SYMBOL and issue.sell_symbol == CC_SYMBOL:
                    refill_tx, balances, refill_satisfied = await self._refill_cc_for_source_step(
                        sdk=sdk,
                        router=router,
                        required_amount=amount_range.min_value,
                        cc_reserve=self._effective_cc_reserve(account),
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                    )
                    tx_count += refill_tx
                    if refill_satisfied:
                        route, issue = await self._prepare_affordable_route(
                            router=router,
                            balances=balances,
                            sell_symbol=sell_symbol,
                            buy_symbol=buy_symbol,
                            proposed_amount=min(
                                amount_range.max_value,
                                self._spendable_amount(
                                    sell_symbol,
                                    balances.get(sell_symbol, Decimal("0")),
                                    self._effective_cc_reserve(account),
                                ),
                            ),
                            round_number=round_number,
                            cc_reserve=self._effective_cc_reserve(account),
                        )
                if issue is not None:
                    logger.info(
                        "Putaran %s belum affordable | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        issue.reason,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Round {round_number} pending: {issue.reason}",
                        force=True,
                    )
                    return RoundExecutionResult(
                        completed=False,
                        tx_count=tx_count,
                        stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
                        skipped=True,
                    )
        except (CantexAPIError, CantexTimeoutError) as exc:
            logger.warning(
                "Putaran %s gagal sementara | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient API error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_API_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        except RuntimeError as exc:
            if not self._is_retryable_route_error(exc):
                raise
            logger.warning(
                "Putaran %s gagal sementara saat siapkan route | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_ROUTE_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )

        try:
            route, issue, daily_free_fee_status = await self._wait_for_network_fee_below_cap(
                sdk=sdk,
                router=router,
                balances=balances,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                actual_amount=actual_amount,
                round_number=round_number,
                cc_reserve=self._effective_cc_reserve(account, selected_action.cc_reserve_override),
                fee_retry_deadline_utc=fee_retry_deadline_utc,
                logger=logger,
                monitor_card=monitor_card,
                current_route=route,
                account_name=account.name,
                strict_amount=selected_action.strict_amount,
            )
        except (CantexAPIError, CantexTimeoutError) as exc:
            logger.warning(
                "Putaran %s gagal sementara saat cek fee | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient API error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_API_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        except RuntimeError as exc:
            if not self._is_retryable_route_error(exc):
                raise
            logger.warning(
                "Putaran %s gagal sementara saat tunggu fee | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_ROUTE_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        if issue is not None:
            message = (
                f"⏭️ Round {round_number} slot skipped: {issue.reason}"
                if "30 detik sebelum jadwal berikutnya" in issue.reason
                else f"⏭️ Round {round_number} pending: {issue.reason}"
            )
            await self.monitor.log_event(
                monitor_card,
                message,
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
                logger=logger,
                monitor_card=monitor_card,
            )
        if route.hops and route.hops[0].sell_amount < amount_range.min_value:
            reason = f"route adjusted amount below user config min ({route.hops[0].sell_amount} < {amount_range.min_value})"
            logger.info(
                "Putaran %s belum bisa dieksekusi | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                reason,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: {reason}",
                force=True,
            )
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason="USER_CONFIG_MIN_NOT_MET",
                skipped=True,
            )

        self._schedule_monitor_call(
            self.monitor.update_status(
                monitor_card,
                pair_key=self._monitor_pair_key(pair_key),
                round_number=round_number,
                phase="PROCESSING",
                route_plan=route,
            ),
            logger=logger,
            description="hot-path status update",
        )
        logger.info(
            "Putaran %s | %s -> %s | nominal=%s | route=%s | fee est=%s | network fee est=%s",
            round_number,
            sell_symbol,
            buy_symbol,
            actual_amount,
            route.label,
            self._format_amount_map(route.total_admin_and_liquidity_by_symbol),
            self._format_amount_map(route.total_network_fee_by_symbol),
        )
        self._schedule_monitor_call(
            self.monitor.log_event(
                monitor_card,
                f"🔄 Round {round_number}/{prepared_run.rounds} {self._monitor_pair_key(pair_key)} ({actual_amount})",
            ),
            logger=logger,
            description="hot-path round log",
        )

        for hop_index, hop in enumerate(route.hops, start=1):
            hop_balances_before = dict(balances)
            tx_result, failure_reason = await self._swap_hop_with_retry(
                sdk=sdk,
                hop=hop,
                hop_index=hop_index,
                hop_total=len(route.hops),
                round_number=round_number,
                logger=logger,
                monitor_card=monitor_card,
                free_fee_sequential_account_name=(
                    account.name if daily_free_fee_status is not None and hop_index == 1 else None
                ),
                allow_network_fee_cap_bypass=(
                    daily_free_fee_status is not None and hop_index == 1
                ),
            )
            if tx_result is None:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {failure_reason or 'retry limit reached'}",
                    force=True,
                )
                return await self._build_skipped_round_result(
                    account=account,
                    round_number=round_number,
                    tx_count=tx_count,
                    strategy_state=strategy_state,
                    stop_reason=failure_reason or "SWAP_HOP_FAILED_SKIPPED",
                    logger=logger,
                    monitor_card=monitor_card,
                )

            tx_count += 1
            settled_balances = await self._wait_for_hop_balance_settlement(
                sdk=sdk,
                previous_balances=hop_balances_before,
                hop=hop,
                tx_result=tx_result,
                logger=logger,
                monitor_card=monitor_card,
            )
            actual_network_fee, actual_swap_fee, settled_balances = await self._resolve_actual_successful_hop_fees(
                sdk=sdk,
                hop=hop,
                tx_result=tx_result,
                balances_before=hop_balances_before,
                balances_after=settled_balances,
                logger=logger,
                monitor_card=monitor_card,
            )
            balances = settled_balances
            await self.monitor.update_balances(
                monitor_card,
                balances,
            )
            if daily_free_fee_status is not None and not daily_free_fee_consumed:
                updated_free_fee_status = self._consume_daily_free_fee_swap(account.name)
                daily_free_fee_consumed = True
                for symbol, amount in actual_network_fee.items():
                    round_free_network_fee_credit[symbol] += amount
                await self.monitor.update_free_fee_status(
                    monitor_card,
                    used=updated_free_fee_status.used,
                    limit=3,
                    network_fee_credit=actual_network_fee,
                    force=True,
                )
                logger.info(
                    "Free fee swap harian terpakai | %s | %s/3 | tanggal UTC=%s",
                    account.name,
                    updated_free_fee_status.used,
                    updated_free_fee_status.utc_date,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🎁 Free fee swap used {updated_free_fee_status.used}/3 for {updated_free_fee_status.utc_date} UTC",
                    force=True,
                )
            for symbol, amount in actual_network_fee.items():
                round_network_fee[symbol] += amount
                used_network_fee[symbol] += amount
            for symbol, amount in actual_swap_fee.items():
                round_swap_fee[symbol] += amount
                used_swap_fee[symbol] += amount
            await self.monitor.update_fee_totals(
                monitor_card,
                total_network_fee=dict(used_network_fee),
                total_swap_fee=dict(used_swap_fee),
            )
            tx_identifier = tx_result.get("id") or tx_result.get("transactionId") or tx_result.get("contract_id")
            actual_output_amount, output_warning = self._resolve_actual_output_amount(
                hop=hop,
                tx_result=tx_result,
                balances_before=hop_balances_before,
                balances_after=settled_balances,
            )
            slippage_pct = self._parse_decimal_like(tx_result.get("quote_slippage")) or hop.slippage
            if output_warning is not None:
                logger.warning(output_warning)
                await self.monitor.log_event(
                    monitor_card,
                    f"⚠️ {output_warning}",
                    force=True,
                )
            logger.info(
                "Tx hop %s/%s berhasil | %s -> %s | tx=%s | output=%s %s | "
                "expected=%s | slippage=%s%%",
                hop_index,
                len(route.hops),
                hop.sell_symbol,
                hop.buy_symbol,
                tx_identifier or "-",
                actual_output_amount,
                hop.buy_symbol,
                hop.returned_amount,
                slippage_pct,
            )
            await self.monitor.log_event(
                monitor_card,
                (
                    f"✅ Hop {hop_index}/{len(route.hops)} {hop.sell_symbol}->{hop.buy_symbol} "
                    f"tx={tx_identifier or '-'} | "
                    f"out={actual_output_amount} slip={slippage_pct}%"
                ),
            )
            await self.monitor.log_event(
                monitor_card,
                self._format_fee_log_line(
                    prefix="Fee tx",
                    network_fee=actual_network_fee,
                    swap_fee=actual_swap_fee,
                ),
            )
            await self.monitor.log_event(
                monitor_card,
                self._format_fee_log_line(
                    prefix="Fee total",
                    network_fee=dict(used_network_fee),
                    swap_fee=dict(used_swap_fee),
                ),
            )
            # --- Round-trip cycle spread loss tracking ---
            cycle_tracker = self._get_cycle_tracker(account.name)
            cycle_result = cycle_tracker.record_swap(
                sell_symbol=hop.sell_symbol,
                buy_symbol=hop.buy_symbol,
                sell_amount=hop.sell_amount,
                buy_amount=actual_output_amount,
                network_fee=actual_network_fee,
                swap_fee=actual_swap_fee,
                balances_before=hop_balances_before,
                balances_after=settled_balances,
            )
            if cycle_result is not None:
                await self.monitor.record_cycle_spread_loss(
                    monitor_card,
                    cycle_result=cycle_result,
                )
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"🔁 Cycle #{cycle_tracker.cycle_count} complete "
                        f"({cycle_result.cycle_type}) | "
                        f"{cycle_result.origin_symbol}: {cycle_result.start_amount} → {cycle_result.end_amount} | "
                        f"loss={cycle_result.spread_loss}"
                    ),
                )
                # Save cycle state after completion for persistence across restarts
                self._save_cycle_tracker_state(account.name)
            # Trigger ccview.io fee scrape after each successful hop (non-blocking)
            self._trigger_fee_scrape_if_available(
                account_name=account.name,
                completed_round=round_number,
                monitor_card=monitor_card,
            )
            await self._sleep_between_swaps()
        latest_info = await sdk.get_account_info()
        latest_balances = self._balances_by_symbol(latest_info)
        await self.monitor.update_balances(
            monitor_card,
            latest_balances,
            force=True,
        )
        await self.monitor.record_round_completed(
            monitor_card,
            pair_key=self._monitor_pair_key(pair_key),
            force=True,
        )
        latest_activity = await self._fetch_activity_summary(sdk, logger)
        await self.monitor.update_activity(
            monitor_card,
            latest_activity,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            "🎉 Swap completed!",
            force=True,
        )
        return RoundExecutionResult(
            completed=True,
            tx_count=tx_count,
        )

    async def _execute_round_dynamic_v2(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        round_number: int,
        strategy_state: StrategyRuntimeState,
        fee_retry_deadline_utc: datetime | None,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> RoundExecutionResult:
        tx_count = 0
        sell_symbol = "-"
        buy_symbol = "-"
        pair_key = "-"
        balances: dict[str, Decimal] = {symbol: Decimal("0") for symbol in TRACKED_SYMBOLS}
        balances_before_round: dict[str, Decimal] = {}
        selected_action: StrategyAction | None = None
        daily_free_fee_status: DailyFreeFeeStatus | None = None
        daily_free_fee_consumed = False
        round_network_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
        round_swap_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
        round_free_network_fee_credit: defaultdict[str, Decimal] = defaultdict(Decimal)

        try:
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            balances_before_round = dict(balances)
            await self.monitor.update_balances(monitor_card, balances)
            action_candidates, pending_reason = self._strategy_action_candidates(
                account=account,
                balances=balances,
                strategy_state=strategy_state,
            )
            if not action_candidates:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {pending_reason}",
                    force=True,
                )
                return await self._build_skipped_round_result(
                    account=account,
                    round_number=round_number,
                    tx_count=tx_count,
                    strategy_state=strategy_state,
                    stop_reason="WAIT_SOURCE_BALANCE",
                    logger=logger,
                    monitor_card=monitor_card,
                )

            route: RoutePlan | None = None
            actual_amount: Decimal | None = None
            user_min_amount: Decimal | None = None
            exact_required_amount: Decimal | None = None
            last_invalid_reason = pending_reason
            last_invalid_stop_reason = "WAIT_SOURCE_BALANCE"
            strategy_4_has_foreign_candidates = False
            strategy_4_topup_unlocked = False
            strategy_4_topup_blocked = False

            for action in action_candidates:
                sell_symbol = action.sell_symbol
                buy_symbol = action.buy_symbol
                pair_key = f"{sell_symbol}->{buy_symbol}"
                if account.strategy().key == "strategy_4_reserve" and action.sell_symbol != CC_SYMBOL:
                    strategy_4_has_foreign_candidates = True
                if (
                    account.strategy().key == "strategy_4_reserve"
                    and action.sell_symbol == CC_SYMBOL
                    and strategy_4_has_foreign_candidates
                    and (not strategy_4_topup_unlocked or strategy_4_topup_blocked)
                ):
                    logger.info(
                        "Putaran %s top-up CC->USDCx ditahan karena recycle foreign balance belum benar-benar mentok",
                        round_number,
                    )
                    continue
                actual_amount, user_min_amount, exact_required_amount, amount_reason = await self._resolve_strategy_action_amount(
                    account=account,
                    action=action,
                    balances=balances,
                    router=router,
                )
                if actual_amount is None:
                    if amount_reason:
                        last_invalid_reason = amount_reason
                        last_invalid_stop_reason = (
                            "MIN_TICKET_SIZE"
                            if "minimum ticket size" in amount_reason.lower()
                            else "WAIT_SOURCE_BALANCE"
                        )
                        if account.strategy().key == "strategy_4_reserve" and action.sell_symbol != CC_SYMBOL:
                            if last_invalid_stop_reason in {"MIN_TICKET_SIZE", "WAIT_SOURCE_BALANCE"}:
                                strategy_4_topup_unlocked = True
                            else:
                                strategy_4_topup_blocked = True
                        logger.info(
                            "Putaran %s kandidat belum valid | %s -> %s | %s",
                            round_number,
                            sell_symbol,
                            buy_symbol,
                            amount_reason,
                        )
                    continue

                route, issue = await self._prepare_affordable_route(
                    router=router,
                    balances=balances,
                    sell_symbol=sell_symbol,
                    buy_symbol=buy_symbol,
                    proposed_amount=actual_amount,
                    round_number=round_number,
                    cc_reserve=self._effective_cc_reserve(account, action.cc_reserve_override),
                    strict_amount=action.strict_amount,
                )
                if (
                    issue is not None
                    and account.strategy().key == "strategy_4_reserve"
                    and {sell_symbol, buy_symbol} == {"USDCx", "CBTC"}
                ):
                    logger.info(
                        "Putaran %s submit recycle max tanpa blok preflight lokal | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        issue.reason,
                    )
                    issue = None

                if issue is not None:
                    last_invalid_reason = issue.reason
                    last_invalid_stop_reason = "ROUND_AFFORDABILITY_CHECK_FAILED"
                    if account.strategy().key == "strategy_4_reserve" and action.sell_symbol != CC_SYMBOL:
                        strategy_4_topup_blocked = True
                    logger.info(
                        "Putaran %s kandidat belum affordable | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        issue.reason,
                    )
                    continue

                if exact_required_amount is not None and route.hops and route.hops[0].sell_amount != exact_required_amount:
                    last_invalid_reason = (
                        f"route adjusted amount differs from exact config "
                        f"({route.hops[0].sell_amount} != {exact_required_amount})"
                    )
                    last_invalid_stop_reason = "USER_CONFIG_MIN_NOT_MET"
                    logger.info(
                        "Putaran %s kandidat belum bisa dieksekusi | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        last_invalid_reason,
                    )
                    continue

                if user_min_amount is not None and route.hops and route.hops[0].sell_amount < user_min_amount:
                    last_invalid_reason = (
                        f"route adjusted amount below user config min ({route.hops[0].sell_amount} < {user_min_amount})"
                    )
                    last_invalid_stop_reason = "USER_CONFIG_MIN_NOT_MET"
                    logger.info(
                        "Putaran %s kandidat belum bisa dieksekusi | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        last_invalid_reason,
                    )
                    continue

                selected_action = action
                break

            if selected_action is None or actual_amount is None or route is None:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {last_invalid_reason}",
                    force=True,
                )
                return await self._build_skipped_round_result(
                    account=account,
                    round_number=round_number,
                    tx_count=tx_count,
                    strategy_state=strategy_state,
                    stop_reason=last_invalid_stop_reason,
                    logger=logger,
                    monitor_card=monitor_card,
                )
        except (CantexAPIError, CantexTimeoutError) as exc:
            logger.warning(
                "Putaran %s gagal sementara | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient API error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_API_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        except RuntimeError as exc:
            if not self._is_retryable_route_error(exc):
                raise
            logger.warning(
                "Putaran %s gagal sementara saat siapkan route | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_ROUTE_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )

        try:
            route, issue, daily_free_fee_status = await self._wait_for_network_fee_below_cap(
                sdk=sdk,
                router=router,
                balances=balances,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                actual_amount=actual_amount,
                round_number=round_number,
                cc_reserve=self._effective_cc_reserve(account, selected_action.cc_reserve_override),
                fee_retry_deadline_utc=fee_retry_deadline_utc,
                logger=logger,
                monitor_card=monitor_card,
                current_route=route,
                account_name=account.name,
            )
        except (CantexAPIError, CantexTimeoutError) as exc:
            logger.warning(
                "Putaran %s gagal sementara saat cek fee | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient API error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_API_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        except RuntimeError as exc:
            if not self._is_retryable_route_error(exc):
                raise
            logger.warning(
                "Putaran %s gagal sementara saat tunggu fee | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_ROUTE_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )

        if issue is not None:
            message = (
                f"⏭️ Round {round_number} slot skipped: {issue.reason}"
                if "30 detik sebelum jadwal berikutnya" in issue.reason
                else f"⏭️ Round {round_number} pending: {issue.reason}"
            )
            await self.monitor.log_event(
                monitor_card,
                message,
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
                logger=logger,
                monitor_card=monitor_card,
            )

        self._schedule_monitor_call(
            self.monitor.update_status(
                monitor_card,
                pair_key=self._monitor_pair_key(pair_key),
                round_number=round_number,
                phase="PROCESSING",
                route_plan=route,
            ),
            logger=logger,
            description="hot-path status update",
        )
        logger.info(
            "Putaran %s | %s -> %s | nominal=%s | route=%s | fee est=%s | network fee est=%s",
            round_number,
            sell_symbol,
            buy_symbol,
            actual_amount,
            route.label,
            self._format_amount_map(route.total_admin_and_liquidity_by_symbol),
            self._format_amount_map(route.total_network_fee_by_symbol),
        )
        self._schedule_monitor_call(
            self.monitor.log_event(
                monitor_card,
                f"🔄 Round {round_number}/{prepared_run.rounds} {self._monitor_pair_key(pair_key)} ({actual_amount})",
            ),
            logger=logger,
            description="hot-path round log",
        )

        for hop_index, hop in enumerate(route.hops, start=1):
            hop_balances_before = dict(balances)
            tx_result, failure_reason = await self._swap_hop_with_retry(
                sdk=sdk,
                hop=hop,
                hop_index=hop_index,
                hop_total=len(route.hops),
                round_number=round_number,
                logger=logger,
                monitor_card=monitor_card,
                free_fee_sequential_account_name=(
                    account.name if daily_free_fee_status is not None and hop_index == 1 else None
                ),
                allow_network_fee_cap_bypass=(
                    daily_free_fee_status is not None and hop_index == 1
                ),
            )
            if tx_result is None:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {failure_reason or 'retry limit reached'}",
                    force=True,
                )
                return await self._build_skipped_round_result(
                    account=account,
                    round_number=round_number,
                    tx_count=tx_count,
                    strategy_state=strategy_state,
                    stop_reason=failure_reason or "SWAP_HOP_FAILED_SKIPPED",
                    logger=logger,
                    monitor_card=monitor_card,
                )

            tx_count += 1
            settled_balances = await self._wait_for_hop_balance_settlement(
                sdk=sdk,
                previous_balances=hop_balances_before,
                hop=hop,
                tx_result=tx_result,
                logger=logger,
                monitor_card=monitor_card,
            )
            actual_network_fee, actual_swap_fee, settled_balances = await self._resolve_actual_successful_hop_fees(
                sdk=sdk,
                hop=hop,
                tx_result=tx_result,
                balances_before=hop_balances_before,
                balances_after=settled_balances,
                logger=logger,
                monitor_card=monitor_card,
            )
            balances = settled_balances
            await self.monitor.update_balances(
                monitor_card,
                balances,
            )
            if daily_free_fee_status is not None and not daily_free_fee_consumed:
                updated_free_fee_status = self._consume_daily_free_fee_swap(account.name)
                daily_free_fee_consumed = True
                for symbol, amount in actual_network_fee.items():
                    round_free_network_fee_credit[symbol] += amount
                await self.monitor.update_free_fee_status(
                    monitor_card,
                    used=updated_free_fee_status.used,
                    limit=3,
                    network_fee_credit=actual_network_fee,
                    force=True,
                )
                logger.info(
                    "Free fee swap harian terpakai | %s | %s/3 | tanggal UTC=%s",
                    account.name,
                    updated_free_fee_status.used,
                    updated_free_fee_status.utc_date,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🎁 Free fee swap used {updated_free_fee_status.used}/3 for {updated_free_fee_status.utc_date} UTC",
                    force=True,
                )
            for symbol, amount in actual_network_fee.items():
                round_network_fee[symbol] += amount
                used_network_fee[symbol] += amount
            for symbol, amount in actual_swap_fee.items():
                round_swap_fee[symbol] += amount
                used_swap_fee[symbol] += amount
            await self.monitor.update_fee_totals(
                monitor_card,
                total_network_fee=dict(used_network_fee),
                total_swap_fee=dict(used_swap_fee),
            )
            tx_identifier = tx_result.get("id") or tx_result.get("transactionId") or tx_result.get("contract_id")
            actual_output_amount, output_warning = self._resolve_actual_output_amount(
                hop=hop,
                tx_result=tx_result,
                balances_before=hop_balances_before,
                balances_after=settled_balances,
            )
            slippage_pct = self._parse_decimal_like(tx_result.get("quote_slippage")) or hop.slippage
            if output_warning is not None:
                logger.warning(output_warning)
                await self.monitor.log_event(
                    monitor_card,
                    f"⚠️ {output_warning}",
                    force=True,
                )
            logger.info(
                "Tx hop %s/%s berhasil | %s -> %s | tx=%s | output=%s %s | "
                "expected=%s | slippage=%s%%",
                hop_index,
                len(route.hops),
                hop.sell_symbol,
                hop.buy_symbol,
                tx_identifier or "-",
                actual_output_amount,
                hop.buy_symbol,
                hop.returned_amount,
                slippage_pct,
            )
            await self.monitor.log_event(
                monitor_card,
                (
                    f"✅ Hop {hop_index}/{len(route.hops)} {hop.sell_symbol}->{hop.buy_symbol} "
                    f"tx={tx_identifier or '-'} | "
                    f"out={actual_output_amount} slip={slippage_pct}%"
                ),
            )
            await self.monitor.log_event(
                monitor_card,
                self._format_fee_log_line(
                    prefix="Fee tx",
                    network_fee=actual_network_fee,
                    swap_fee=actual_swap_fee,
                ),
            )
            await self.monitor.log_event(
                monitor_card,
                self._format_fee_log_line(
                    prefix="Fee total",
                    network_fee=dict(used_network_fee),
                    swap_fee=dict(used_swap_fee),
                ),
            )
            # --- Round-trip cycle spread loss tracking ---
            cycle_tracker = self._get_cycle_tracker(account.name)
            cycle_result = cycle_tracker.record_swap(
                sell_symbol=hop.sell_symbol,
                buy_symbol=hop.buy_symbol,
                sell_amount=hop.sell_amount,
                buy_amount=actual_output_amount,
                network_fee=actual_network_fee,
                swap_fee=actual_swap_fee,
                balances_before=hop_balances_before,
                balances_after=settled_balances,
            )
            if cycle_result is not None:
                await self.monitor.record_cycle_spread_loss(
                    monitor_card,
                    cycle_result=cycle_result,
                )
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"🔁 Cycle #{cycle_tracker.cycle_count} complete "
                        f"({cycle_result.cycle_type}) | "
                        f"{cycle_result.origin_symbol}: {cycle_result.start_amount} → {cycle_result.end_amount} | "
                        f"loss={cycle_result.spread_loss}"
                    ),
                )
                # Save cycle state after completion for persistence across restarts
                self._save_cycle_tracker_state(account.name)
            # Trigger ccview.io fee scrape after each successful hop (non-blocking)
            self._trigger_fee_scrape_if_available(
                account_name=account.name,
                completed_round=round_number,
                monitor_card=monitor_card,
            )
            await self._sleep_between_swaps()
        latest_info = await sdk.get_account_info()
        latest_balances = self._balances_by_symbol(latest_info)
        await self.monitor.update_balances(
            monitor_card,
            latest_balances,
            force=True,
        )
        await self.monitor.record_round_completed(
            monitor_card,
            pair_key=self._monitor_pair_key(pair_key),
            force=True,
        )
        latest_activity = await self._fetch_activity_summary(sdk, logger)
        await self.monitor.update_activity(
            monitor_card,
            latest_activity,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            "🎉 Swap completed!",
            force=True,
        )
        self._reset_balance_block_counter(strategy_state)
        self._advance_strategy_state_after_success(
            strategy_state=strategy_state,
            action=selected_action,
        )
        self._update_strategy_4_topup_guard_after_success(
            account=account,
            strategy_state=strategy_state,
            action=selected_action,
        )
        return RoundExecutionResult(
            completed=True,
            tx_count=tx_count,
        )

    async def _recover_until_target_available(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        target_symbol: str,
        required_amount: Decimal,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> tuple[int, dict[str, Decimal], bool]:
        total_tx = 0
        last_balances = self._balances_by_symbol(await sdk.get_account_info())

        while True:
            spendable = self._spendable_amount(
                target_symbol,
                last_balances.get(target_symbol, Decimal("0")),
                cc_reserve,
            )
            if spendable >= required_amount:
                return total_tx, last_balances, True

            recovered_tx = await self._recover_to_symbol(
                sdk=sdk,
                router=router,
                target_symbol=target_symbol,
                cc_reserve=cc_reserve,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            total_tx += recovered_tx
            if recovered_tx <= 0:
                return total_tx, last_balances, False

            updated_balances = await self._wait_for_balance_settlement(
                sdk=sdk,
                target_symbol=target_symbol,
                previous_balances=last_balances,
                required_amount=required_amount,
                cc_reserve=cc_reserve,
                logger=logger,
                monitor_card=monitor_card,
            )
            if updated_balances == last_balances:
                return total_tx, updated_balances, False
            last_balances = updated_balances

    async def _wait_for_balance_settlement(
        self,
        *,
        sdk: ExtendedCantexSDK,
        target_symbol: str,
        previous_balances: dict[str, Decimal],
        required_amount: Decimal,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> dict[str, Decimal]:
        wait_seconds = max(self.config.runtime.retry_base_delay, 2.0)
        max_polls = max(3, self.config.runtime.max_retries * 2)

        for poll_index in range(max_polls):
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            previous_amount = previous_balances.get(target_symbol, Decimal("0"))
            current_amount = balances.get(target_symbol, Decimal("0"))
            spendable = self._spendable_amount(
                target_symbol,
                current_amount,
                cc_reserve,
            )
            if current_amount > previous_amount or spendable >= required_amount:
                return balances

            if poll_index < max_polls - 1:
                logger.info(
                    "Menunggu settlement recovery %s | poll %s/%s | balance=%s",
                    target_symbol,
                    poll_index + 1,
                    max_polls,
                    current_amount,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏳ Waiting recovery settlement {target_symbol} ({poll_index + 1}/{max_polls})",
                )
                await self._sleep_or_stop(wait_seconds)

        return previous_balances

    def _cc_source_block_reason(
        self,
        *,
        balance_cc: Decimal,
        spendable_cc: Decimal,
        required_min_amount: Decimal,
        reserve_threshold: Decimal | None = None,
    ) -> str:
        effective_reserve = reserve_threshold if reserve_threshold is not None else Decimal("0")
        if balance_cc <= effective_reserve or spendable_cc <= dust_for_symbol(CC_SYMBOL):
            return f"CC reserve reached ({balance_cc} <= {effective_reserve})"
        return f"CC spendable below user config min ({spendable_cc} < {required_min_amount})"

    async def _refill_cc_for_source_step(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        required_amount: Decimal,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> tuple[int, dict[str, Decimal], bool]:
        logger.info(
            "Source CC belum cukup, mencoba refill CC hingga spendable >= %s",
            required_amount,
        )
        await self.monitor.log_event(
            monitor_card,
            f"🛟 Refill CC target spendable {required_amount}",
        )
        recovered_tx, balances, refill_satisfied = await self._recover_until_target_available(
            sdk=sdk,
            router=router,
            target_symbol=CC_SYMBOL,
            required_amount=required_amount,
            cc_reserve=cc_reserve,
            logger=logger,
            monitor_card=monitor_card,
            used_network_fee=used_network_fee,
            used_swap_fee=used_swap_fee,
        )
        if refill_satisfied:
            await self.monitor.log_event(
                monitor_card,
                "✅ Refill CC ready",
            )
        else:
            await self.monitor.log_event(
                monitor_card,
                "⏭️ Refill CC belum cukup, lanjut ke step berikutnya",
            )
        return recovered_tx, balances, refill_satisfied

    def _weekly_stop_due_utc(self) -> bool:
        if not self.config.runtime.weekly_stop_on_monday_utc:
            return False
        now_utc = datetime.now(timezone.utc)
        return now_utc.weekday() == 0 and now_utc.date() != self.startup_utc_date

    async def _perform_weekly_stop(
        self,
        *,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        result: AccountResult,
    ) -> None:
        result.stop_reason = "WEEKLY_STOP"
        logger.info("Weekly stop Senin UTC tercapai; sesi dihentikan tanpa refill")
        await self.monitor.update_status(
            monitor_card,
            phase="WEEKLY-STOP",
            clear_route=True,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            "🛑 Weekly stop Monday UTC reached",
            force=True,
        )

    def _non_cc_balances_remaining(self, balances: dict[str, Decimal]) -> dict[str, Decimal]:
        return self._non_target_balances_remaining(balances, CC_SYMBOL)

    def _non_target_balances_remaining(
        self,
        balances: dict[str, Decimal],
        target_symbol: str,
    ) -> dict[str, Decimal]:
        remaining: dict[str, Decimal] = {}
        for symbol in TRACKED_SYMBOLS:
            if symbol == target_symbol:
                continue
            # Untuk target USDCx setelah quota tercapai, hanya rapikan sisa token non-CC
            # (contoh CBTC -> USDCx). Saldo CC tidak boleh ikut dikonversi.
            if target_symbol == "USDCx" and symbol == CC_SYMBOL:
                continue
            amount = balances.get(symbol, Decimal("0"))
            if amount > dust_for_symbol(symbol):
                remaining[symbol] = amount
        return remaining

    async def _perform_weekly_refill_to_cc(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        result: AccountResult,
    ) -> None:
        logger.info("Weekly refill Senin UTC dimulai; semua token akan dikembalikan ke CC")
        await self.monitor.update_status(
            monitor_card,
            phase="WEEKLY-REFILL",
            clear_route=True,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            "🗓️ Weekly refill started: converting all tokens to CC",
            force=True,
        )

        total_tx = 0
        refill_complete = True
        while True:
            balances = self._balances_by_symbol(await sdk.get_account_info())
            remaining = self._non_cc_balances_remaining(balances)
            if not remaining:
                break

            recovered_tx = await self._recover_to_symbol(
                sdk=sdk,
                router=router,
                target_symbol=CC_SYMBOL,
                cc_reserve=Decimal("0"),
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            total_tx += recovered_tx
            if recovered_tx <= 0:
                refill_complete = False
                logger.warning(
                    "Weekly refill belum bisa mengosongkan token non-CC: %s",
                    self._format_amount_map(remaining),
                )
                await self.monitor.log_event(
                    monitor_card,
                    (
                        "⚠️ Weekly refill stopped: token non-CC tersisa "
                        f"{self._format_amount_map(remaining)}"
                    ),
                    force=True,
                )
                break

        result.swap_transactions += total_tx
        result.stop_reason = "WEEKLY_REFILL_COMPLETE" if refill_complete else "WEEKLY_REFILL_INCOMPLETE"
        if refill_complete:
            await self.monitor.log_event(
                monitor_card,
                f"✅ Weekly refill complete: {total_tx} refill swap(s)",
                force=True,
            )

    async def _strategy_4_refill_non_cc_to_cc(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        result: AccountResult,
    ) -> None:
        """Strategy 4: refill semua USDCx dan CBTC kembali ke CC setelah target tercapai."""
        logger.info("Strategy 4 refill: mengembalikan semua non-CC ke CC setelah quota tercapai")
        await self.monitor.log_event(
            monitor_card,
            "🔄 Strategy 4 refill: converting non-CC back to CC",
            force=True,
        )

        total_tx = 0
        max_iterations = 5  # safety limit
        for _ in range(max_iterations):
            balances = self._balances_by_symbol(await sdk.get_account_info())
            remaining = self._non_cc_balances_remaining(balances)
            if not remaining:
                break

            recovered_tx = await self._recover_to_symbol(
                sdk=sdk,
                router=router,
                target_symbol=CC_SYMBOL,
                cc_reserve=Decimal("0"),
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            total_tx += recovered_tx
            if recovered_tx <= 0:
                logger.info(
                    "Strategy 4 refill: tidak bisa mengosongkan sisa token non-CC: %s",
                    self._format_amount_map(remaining),
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⚠️ Strategy 4 refill incomplete: sisa {self._format_amount_map(remaining)}",
                    force=True,
                )
                break

        result.swap_transactions += total_tx
        if total_tx > 0:
            await self.monitor.log_event(
                monitor_card,
                f"✅ Strategy 4 refill done: {total_tx} swap(s)",
                force=True,
            )
            await self.monitor.update_balances(
                monitor_card,
                self._balances_by_symbol(await sdk.get_account_info()),
                force=True,
            )

    async def _maybe_pre_refill_usdcx_v2(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        result: AccountResult,
    ) -> int:
        # Opsi 1 + Opsi 3 (kombinasi):
        # - Hapus gate `result.completed_rounds != 0` yang rapuh terhadap race
        #   dengan trading-history sync setelah daily reset.
        # - Pakai flag harian persisten `last_pre_refill_utc_date` di runtime_state
        #   untuk idempotency: pre-refill maksimal 1x per akun per hari UTC.
        # - Tetap cek balance non-CC > dust supaya tidak swap saat tidak ada sisa.
        if not self._post_target_refill_is_usdcx_v2():
            return 0
        if self._startup_mode_is_refill_cc() or self._startup_mode_is_check_accounts():
            return 0
        if prepared_run.rounds <= 0:
            return 0

        # Idempotency check: kalau sudah dijalankan hari ini, skip diam-diam.
        if self.runtime_state.is_pre_refill_done_today(account.name):
            logger.debug(
                "USDCx v2 pre-refill: sudah dijalankan hari ini untuk %s, skip",
                account.name,
            )
            return 0

        balances = self._balances_by_symbol(await sdk.get_account_info())
        await self.monitor.update_balances(monitor_card, balances, force=True)
        remaining = self._non_cc_balances_remaining(balances)
        if not remaining:
            # Tidak ada sisa non-CC; tetap tandai selesai supaya gate harian
            # konsisten dan tidak dipanggil ulang dari hot-loop hari ini.
            self.runtime_state.mark_pre_refill_done_today(account.name)
            logger.info(
                "USDCx v2 pre-refill: tidak ada sisa non-CC, marker harian dipasang"
            )
            return 0

        logger.info(
            "Refill USDCx v2 pre-step: progress=0 dan saldo non-CC=%s, swap semua ke CC",
            self._format_amount_map(remaining),
        )
        await self.monitor.update_status(
            monitor_card,
            round_number=1,
            phase="PROCESSING",
            clear_route=True,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"USDCx v2 pre-refill: converting {self._format_amount_map(remaining)} to CC",
            force=True,
        )

        recovered_tx = await self._recover_to_symbol(
            sdk=sdk,
            router=router,
            target_symbol=CC_SYMBOL,
            cc_reserve=Decimal("0"),
            logger=logger,
            monitor_card=monitor_card,
            used_network_fee=used_network_fee,
            used_swap_fee=used_swap_fee,
            source_symbols=tuple(remaining.keys()),
        )
        if recovered_tx <= 0:
            # Catatan: TIDAK mark_pre_refill_done_today di sini supaya bot bisa
            # retry pada loop berikutnya saat fee/slippage sudah turun.
            logger.warning(
                "Refill USDCx v2 pre-step gagal: tidak ada swap non-CC->CC yang berhasil "
                "(fee/slippage cap mungkin terlampaui), akan dicoba lagi nanti"
            )
            await self.monitor.log_event(
                monitor_card,
                "⚠️ USDCx v2 pre-refill skipped: non-CC->CC not executed (akan retry)",
                force=True,
            )
            return 0

        counted_rounds = min(recovered_tx, max(prepared_run.rounds - result.completed_rounds, 0))
        for _ in range(counted_rounds):
            await self.monitor.record_round_completed(
                monitor_card,
                pair_key=self._monitor_pair_key("non-CC->CC"),
                force=True,
            )
        if counted_rounds > 0:
            result.completed_rounds += counted_rounds
            result.swap_transactions = result.completed_rounds
            self._persist_round_session_progress(
                account=account,
                prepared_run=prepared_run,
                result=result,
            )
            await self.monitor.sync_round_progress(
                monitor_card,
                completed_rounds=result.completed_rounds,
                force=True,
            )
            self._schedule_ccview_scrape_after_progress(
                account_name=account.name,
                completed_round=result.completed_rounds,
                monitor_card=monitor_card,
                reason="usdcx_v2_pre_refill",
            )

        latest_balances = self._balances_by_symbol(await sdk.get_account_info())
        await self.monitor.update_balances(monitor_card, latest_balances, force=True)
        # Tandai pre-refill harian selesai supaya tidak diulang di hot-loop hari ini.
        # Note: re-check sisa non-CC dilakukan di luar via balance gate, jadi
        # kalau swap berikutnya hasilkan saldo non-CC lagi, yang menanganinya
        # adalah _refill_after_target di akhir hari (target USDCx).
        self.runtime_state.mark_pre_refill_done_today(account.name)
        await self.monitor.log_event(
            monitor_card,
            f"✅ USDCx v2 pre-refill counted as progress: {result.completed_rounds}/{prepared_run.rounds}",
            force=True,
        )
        return recovered_tx

    async def _refill_after_target(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        result: AccountResult,
    ) -> None:
        """Refill semua token selain target ke target setelah target swap tercapai.

        Ini adalah cleanup step: setelah semua round selesai, kembalikan sisa
        token lain ke target pilihan agar saldo bersih untuk hari berikutnya.
        MEMATUHI fee cap — tunggu fee turun sebelum refill.
        """
        target_symbol = self._effective_post_target_refill_symbol()
        balances = self._balances_by_symbol(await sdk.get_account_info())
        remaining = self._non_target_balances_remaining(balances, target_symbol)
        if not remaining:
            logger.debug("Refill after target: tidak ada sisa non-%s untuk di-refill", target_symbol)
            # Tetap hitung daily loss meskipun tidak ada yang perlu di-refill.
            # Ini penting agar CyLoss ter-update saat semua token sudah di target.
            balance_now = balances.get(target_symbol, Decimal("0"))
            logger.info(
                "[CYLOSS DIAG] _refill_after_target early return | target=%s | "
                "start_of_day=%s | balance_now=%s | daily_loss_symbol=%s",
                target_symbol,
                monitor_card.cc_balance_start_of_day if monitor_card is not None else "n/a",
                balance_now,
                monitor_card.daily_loss_symbol if monitor_card is not None else "n/a",
            )
            await self.monitor.update_daily_cc_loss(monitor_card, balance_now)
            return

        logger.info(
            "Refill after target: mengembalikan sisa non-%s ke %s | sisa=%s | strategy=%s",
            target_symbol,
            target_symbol,
            self._format_amount_map(remaining),
            account.strategy().label,
        )
        await self.monitor.log_event(
            monitor_card,
            f"🔄 Refill after target: converting {self._format_amount_map(remaining)} to {target_symbol} (respecting fee cap)",
            force=True,
        )
        await self.monitor.update_status(
            monitor_card,
            phase="PROCESSING",
            clear_route=True,
            force=True,
        )

        total_tx = 0
        max_iterations = 10  # safety limit (lebih banyak karena mungkin perlu tunggu fee)
        for iteration in range(max_iterations):
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            balances = self._balances_by_symbol(await sdk.get_account_info())
            remaining = self._non_target_balances_remaining(balances, target_symbol)
            if not remaining:
                break

            recovered_tx = await self._recover_to_symbol(
                sdk=sdk,
                router=router,
                target_symbol=target_symbol,
                cc_reserve=Decimal("0"),
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
                source_symbols=tuple(remaining.keys()),
            )
            total_tx += recovered_tx
            if recovered_tx <= 0:
                # Fee mungkin terlalu tinggi — tunggu sebentar lalu retry
                if iteration < max_iterations - 1:
                    wait_seconds = self._sample_network_fee_poll_seconds()
                    logger.info(
                        "Refill after target: fee terlalu tinggi atau gagal, tunggu %.0fs lalu retry | sisa=%s",
                        wait_seconds,
                        self._format_amount_map(remaining),
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏳ Refill waiting fee drop ({self._format_amount_map(remaining)})",
                    )
                    await self._sleep_or_stop(wait_seconds)
                else:
                    logger.info(
                        "Refill after target: max iterations tercapai, sisa: %s",
                        self._format_amount_map(remaining),
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⚠️ Refill after target incomplete: sisa {self._format_amount_map(remaining)}",
                        force=True,
                    )

        result.swap_transactions += total_tx

        # Final balance check — pastikan refill berhasil sebelum NEXT-DAY
        final_balances = self._balances_by_symbol(await sdk.get_account_info())
        final_remaining = self._non_target_balances_remaining(final_balances, target_symbol)
        await self.monitor.update_balances(monitor_card, final_balances, force=True)

        if final_remaining:
            logger.warning(
                "Refill after target: MASIH ADA sisa non-%s setelah refill: %s",
                target_symbol,
                self._format_amount_map(final_remaining),
            )
            await self.monitor.log_event(
                monitor_card,
                f"⚠️ Refill incomplete — sisa: {self._format_amount_map(final_remaining)}",
                force=True,
            )
        else:
            logger.info("Refill after target: ✅ semua non-%s berhasil di-refill ke %s", target_symbol, target_symbol)
            await self.monitor.log_event(
                monitor_card,
                f"✅ Refill complete: {total_tx} swap(s) | {target_symbol}={final_balances.get(target_symbol, Decimal('0'))}",
                force=True,
            )

        # --- After refill: scrape ccview for updated gas fee ---
        await self._scrape_ccview_and_update_card(
            account_name=account.name,
            monitor_card=monitor_card,
            force=True,
        )
        self._schedule_ccview_scrape_after_progress(
            account_name=account.name,
            completed_round=result.completed_rounds,
            monitor_card=monitor_card,
            reason="refill",
        )

        # --- After refill: compute daily loss untuk simbol target apa pun ---
        # Loss = balance(target)_start_of_day - balance(target)_after_refill.
        # Bekerja untuk target CC maupun USDCx / USDCx_v2 (target efektif "USDCx").
        balance_after_refill = final_balances.get(target_symbol, Decimal("0"))
        # [DIAG] dipertahankan sementara untuk verifikasi visual di log
        logger.info(
            "[CYLOSS DIAG] _refill_after_target end | target_symbol=%s | "
            "start_of_day=%s | after_refill=%s | daily_loss_symbol=%s",
            target_symbol,
            monitor_card.cc_balance_start_of_day if monitor_card is not None else "n/a",
            balance_after_refill,
            monitor_card.daily_loss_symbol if monitor_card is not None else "n/a",
        )
        await self.monitor.update_daily_cc_loss(
            monitor_card,
            balance_after_refill,
        )
        if monitor_card is not None and monitor_card.cc_balance_start_of_day > Decimal("0"):
            logger.info(
                "Daily %s loss: start=%s | after_refill=%s | loss=%s",
                target_symbol,
                monitor_card.cc_balance_start_of_day,
                balance_after_refill,
                monitor_card.daily_cc_loss,
            )

    def _sample_execution_amount(
        self,
        amount_range,
        max_allowed_amount: Decimal,
    ) -> Decimal:
        if max_allowed_amount <= amount_range.min_value:
            return max_allowed_amount
        fraction = Decimal(str(self._rng.random()))
        return amount_range.min_value + ((max_allowed_amount - amount_range.min_value) * fraction)

    async def _normalize_amount_for_min_ticket(
        self,
        *,
        router: RouteOptimizer,
        sell_symbol: str,
        buy_symbol: str,
        desired_amount: Decimal,
        max_available_amount: Decimal,
    ) -> tuple[Decimal | None, str | None]:
        if sell_symbol == CC_SYMBOL:
            if desired_amount >= MIN_TICKET_SIZE_CC:
                return desired_amount, None
            if max_available_amount >= MIN_TICKET_SIZE_CC:
                return MIN_TICKET_SIZE_CC, "amount adjusted to minimum 10 CC"
            return None, "amount below minimum ticket size (10 CC equivalent)"

        desired_cc_equivalent = await self._estimate_cc_equivalent(
            router=router,
            sell_symbol=sell_symbol,
            amount=desired_amount,
        )
        if desired_cc_equivalent >= MIN_TICKET_SIZE_CC:
            return desired_amount, None

        available_cc_equivalent = await self._estimate_cc_equivalent(
            router=router,
            sell_symbol=sell_symbol,
            amount=max_available_amount,
        )
        if available_cc_equivalent >= MIN_TICKET_SIZE_CC:
            return max_available_amount, "amount raised to available balance to satisfy minimum ticket"

        return None, "amount below minimum ticket size (10 CC equivalent)"

    def _is_min_ticket_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "minimum ticket size" in message
            or "too small amount" in message
            or "10 cc" in message
        )

    def _is_server_insufficient_balance_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "insufficient balance" in message
            or "not enough balance" in message
            or "not enough funds" in message
            or "balance too low" in message
            or "saldo kurang" in message
            or "insufficient funds" in message
        )

    def _is_signature_verification_error(self, exc: Exception) -> bool:
        return "signature verification failed" in str(exc).lower()

    def _is_retryable_route_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "quote gagal" in message
            or "tidak ada route valid" in message
            or "http 500" in message
            or "http 502" in message
            or "http 503" in message
            or "http 504" in message
        )

    def _is_retryable_session_error(self, exc: Exception) -> bool:
        if isinstance(exc, (CantexAPIError, CantexTimeoutError)):
            return True
        if isinstance(exc, CantexAuthError):
            return False
        message = str(exc).lower()
        return (
            self._is_retryable_route_error(exc)
            or "timeout" in message
            or "timed out" in message
            or "temporarily unavailable" in message
            or "connection reset" in message
            or "connection aborted" in message
            or "connection refused" in message
            or "service unavailable" in message
            or "try again later" in message
        )

    def _session_retry_delay_seconds(self, session_number: int) -> float:
        base_delay = max(self.config.runtime.retry_base_delay, 5.0)
        multiplier = min(max(session_number, 1), 6)
        return min(base_delay * multiplier, 120.0)

    async def _mark_retryable_session_error(
        self,
        *,
        result: AccountResult,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        exc: Exception,
        session_number: int,
    ) -> None:
        wait_seconds = self._session_retry_delay_seconds(session_number)
        result.error = None
        result.stop_reason = None
        result.retry_after_seconds = wait_seconds
        logger.warning(
            "Sesi %s gagal sementara, retry dalam %.0f detik: %s",
            session_number,
            wait_seconds,
            exc,
        )
        await self.monitor.update_status(
            monitor_card,
            phase="WAITING",
            next_wait_seconds=wait_seconds,
            clear_route=True,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"⏳ Session transient error: {exc} | retry in {int(wait_seconds)}s",
            force=True,
        )

    def _recovery_source_order(self, target_symbol: str) -> tuple[str, ...]:
        if target_symbol == CC_SYMBOL:
            return tuple(symbol for symbol in TRACKED_SYMBOLS if symbol != target_symbol)
        ordered = [CC_SYMBOL]
        ordered.extend(
            symbol
            for symbol in TRACKED_SYMBOLS
            if symbol not in {target_symbol, CC_SYMBOL}
        )
        return tuple(ordered)

    async def _estimate_cc_equivalent(
        self,
        *,
        router: RouteOptimizer,
        sell_symbol: str,
        amount: Decimal,
    ) -> Decimal:
        if sell_symbol == CC_SYMBOL:
            return amount
        if amount <= 0:
            return Decimal("0")
        try:
            route = await router.choose_best_route(sell_symbol, CC_SYMBOL, amount)
        except Exception:
            return Decimal("0")
        return route.final_amount

    async def _wait_for_hop_balance_settlement(
        self,
        *,
        sdk: ExtendedCantexSDK,
        previous_balances: dict[str, Decimal],
        hop: RouteHop,
        tx_result: dict[str, Any],
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> dict[str, Decimal]:
        wait_seconds = max(self.config.runtime.retry_base_delay, 1.0)
        max_polls = max(10, self.config.runtime.max_retries * 4)
        last_balances = previous_balances
        for poll_index in range(max_polls):
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            last_balances = balances
            if self._hop_execution_observed(
                previous_balances=previous_balances,
                current_balances=balances,
                hop=hop,
            ):
                return balances
            if poll_index < max_polls - 1:
                logger.info(
                    "Menunggu settlement hop %s/%s | poll %s/%s",
                    hop.sell_symbol,
                    hop.buy_symbol,
                    poll_index + 1,
                    max_polls,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"? Waiting hop settlement {hop.sell_symbol}->{hop.buy_symbol} ({poll_index + 1}/{max_polls})",
                )
                await self._sleep_or_stop(wait_seconds)
        return last_balances

    def _hop_execution_observed(
        self,
        *,
        previous_balances: dict[str, Decimal],
        current_balances: dict[str, Decimal],
        hop: RouteHop,
    ) -> bool:
        sell_previous = previous_balances.get(hop.sell_symbol, Decimal("0"))
        sell_current = current_balances.get(hop.sell_symbol, Decimal("0"))
        sell_observed = sell_current < (sell_previous - dust_for_symbol(hop.sell_symbol))

        buy_previous = previous_balances.get(hop.buy_symbol, Decimal("0"))
        buy_current = current_balances.get(hop.buy_symbol, Decimal("0"))
        buy_observed = buy_current > (buy_previous + dust_for_symbol(hop.buy_symbol))

        if not sell_observed or not buy_observed:
            return False

        return True

    def _extract_actual_successful_hop_fees(
        self,
        *,
        hop: RouteHop,
        tx_result: dict[str, Any],
        balances_before: dict[str, Decimal],
        balances_after: dict[str, Decimal],
    ) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
        actual_admin_fee = self._parse_decimal_like(tx_result.get("admin_fee_amount")) or hop.admin_fee_amount
        actual_liquidity_fee = (
            self._parse_decimal_like(tx_result.get("liquidity_fee_amount")) or hop.liquidity_fee_amount
        )

        swap_fee_total = actual_admin_fee + actual_liquidity_fee
        actual_swap_fee: dict[str, Decimal] = {}
        if swap_fee_total > 0:
            actual_swap_fee[hop.fee_symbol] = swap_fee_total

        # --- FIX: Hitung network fee aktual dari selisih balance, bukan dari quote ---
        actual_network_fee: dict[str, Decimal] = {}
        network_fee_symbol = hop.network_fee_symbol

        # Coba hitung dari balance diff untuk mendapatkan fee aktual yang benar-benar dipotong
        balance_derived_network_fee: Decimal | None = None
        if network_fee_symbol == CC_SYMBOL:
            cc_before = balances_before.get(CC_SYMBOL, Decimal("0"))
            cc_after = balances_after.get(CC_SYMBOL, Decimal("0"))
            cc_total_loss = cc_before - cc_after  # Total CC yang hilang

            # Hitung berapa CC yang seharusnya hilang hanya dari swap (tanpa network fee)
            expected_swap_loss = Decimal("0")
            if hop.sell_symbol == CC_SYMBOL:
                expected_swap_loss += hop.sell_amount
            if hop.buy_symbol == CC_SYMBOL:
                # CC yang diterima dari swap mengurangi total loss
                actual_output = self._matching_tx_output_amount(
                    tx_result=tx_result,
                    expected_symbol=CC_SYMBOL,
                )
                if actual_output is not None and hop.buy_symbol == CC_SYMBOL:
                    expected_swap_loss -= actual_output
                else:
                    expected_swap_loss -= hop.returned_amount

            # Network fee = total CC hilang - CC hilang karena swap
            derived_fee = cc_total_loss - expected_swap_loss
            if derived_fee > Decimal("0"):
                balance_derived_network_fee = derived_fee

        if balance_derived_network_fee is not None:
            actual_network_fee[network_fee_symbol] = balance_derived_network_fee
        elif hop.network_fee_amount > 0:
            # Fallback ke quote jika balance diff tidak bisa dihitung
            actual_network_fee[network_fee_symbol] = hop.network_fee_amount

        return actual_network_fee, actual_swap_fee

    async def _resolve_actual_successful_hop_fees(
        self,
        *,
        sdk: ExtendedCantexSDK,
        hop: RouteHop,
        tx_result: dict[str, Any],
        balances_before: dict[str, Decimal],
        balances_after: dict[str, Decimal],
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> tuple[dict[str, Decimal], dict[str, Decimal], dict[str, Decimal]]:
        actual_network_fee, actual_swap_fee = self._extract_actual_successful_hop_fees(
            hop=hop,
            tx_result=tx_result,
            balances_before=balances_before,
            balances_after=balances_after,
        )

        # --- FIX: Log perbandingan fee aktual vs quote dan warn jika melebihi batas ---
        quote_network_fee = hop.network_fee_amount if hop.network_fee_symbol == CC_SYMBOL else Decimal("0")
        actual_cc_fee = actual_network_fee.get(CC_SYMBOL, Decimal("0"))
        fee_cap = self.config.runtime.max_network_fee_cc_per_execution

        if actual_cc_fee > Decimal("0") and actual_cc_fee != quote_network_fee:
            logger.warning(
                "Network fee hop %s -> %s AKTUAL=%s CC berbeda dari QUOTE=%s CC (selisih=%s CC)",
                hop.sell_symbol,
                hop.buy_symbol,
                actual_cc_fee,
                quote_network_fee,
                actual_cc_fee - quote_network_fee,
            )
            await self.monitor.log_event(
                monitor_card,
                (
                    f"⚠️ Fee aktual {actual_cc_fee} CC != quote {quote_network_fee} CC "
                    f"(selisih {actual_cc_fee - quote_network_fee} CC)"
                ),
                force=True,
            )
        else:
            logger.info(
                "Network fee hop %s -> %s aktual (dari balance diff): %s",
                hop.sell_symbol,
                hop.buy_symbol,
                self._format_amount_map(actual_network_fee),
            )

        if fee_cap is not None and actual_cc_fee > fee_cap:
            logger.warning(
                "⚠️ PERINGATAN: Fee aktual %s CC MELEBIHI batas %s CC untuk hop %s -> %s!",
                actual_cc_fee,
                fee_cap,
                hop.sell_symbol,
                hop.buy_symbol,
            )
            await self.monitor.log_event(
                monitor_card,
                (
                    f"🚨 Fee aktual {actual_cc_fee} CC MELEBIHI batas {fee_cap} CC! "
                    f"(hop {hop.sell_symbol}->{hop.buy_symbol})"
                ),
                force=True,
            )

        return actual_network_fee, actual_swap_fee, balances_after

    async def _swap_hop_with_retry(
        self,
        *,
        sdk: ExtendedCantexSDK,
        hop: RouteHop,
        hop_index: int,
        hop_total: int,
        round_number: int,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        free_fee_sequential_account_name: str | None = None,
        allow_network_fee_cap_bypass: bool = False,
    ) -> tuple[dict[str, Any] | None, str | None]:
        # Timeout konfirmasi swap: max(estimated_time * 4, min_timeout_dari_config)
        # Default minimum 90 detik, bisa diubah via swap_confirmation_timeout_seconds di config.
        min_timeout = self.config.runtime.swap_confirmation_timeout_seconds
        confirm_timeout = max(float(hop.estimated_time_seconds) * 4.0, min_timeout)
        lock_acquired = False
        if free_fee_sequential_account_name is not None:
            lock_acquired = await self._acquire_free_fee_sequence_slot(
                account_name=free_fee_sequential_account_name,
                round_number=round_number,
                logger=logger,
                monitor_card=monitor_card,
            )
        try:
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            fee_cap = self.config.runtime.max_network_fee_cc_per_execution
            slippage_cap = self.config.runtime.max_slippage_per_execution
            effective_preflight_fee = (
                hop.network_fee_amount
                if hop.network_fee_symbol == CC_SYMBOL
                else None
            )
            quote_fee_text = (
                f"{hop.network_fee_amount} CC"
                if hop.network_fee_symbol == CC_SYMBOL
                else "-"
            )
            logger.info(
                "Swap hop %s/%s round %s memakai SDK swap_and_confirm | fee quote=%s | slippage quote=%s",
                hop_index,
                hop_total,
                round_number,
                quote_fee_text,
                hop.slippage,
            )
            self._schedule_monitor_call(
                self.monitor.log_event(
                    monitor_card,
                    (
                        f"[preflight] Hop {hop_index}/{hop_total} "
                        f"quote fee={quote_fee_text} | slip={hop.slippage}"
                    ),
                ),
                logger=logger,
                description="hot-path preflight log",
            )

            if (
                fee_cap is not None
                and not allow_network_fee_cap_bypass
                and effective_preflight_fee is not None
                and effective_preflight_fee > fee_cap
            ):
                logger.warning(
                    "Swap hop %s/%s round %s dibatalkan sebelum submit karena fee preflight %s CC > batas %s CC",
                    hop_index,
                    hop_total,
                    round_number,
                    effective_preflight_fee,
                    fee_cap,
                )
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"? Hop {hop_index}/{hop_total} skipped: "
                        f"preflight fee {effective_preflight_fee} CC > limit {fee_cap} CC"
                    ),
                    force=True,
                )
                return None, "NETWORK_FEE_ABOVE_LIMIT"

            if slippage_cap is not None and hop.slippage > slippage_cap:
                logger.warning(
                    "Swap hop %s/%s round %s dibatalkan sebelum submit karena slippage preflight %s > batas %s",
                    hop_index,
                    hop_total,
                    round_number,
                    hop.slippage,
                    slippage_cap,
                )
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"🚫 Hop {hop_index}/{hop_total} DIBATALKAN: "
                        f"preflight slippage {hop.slippage} > limit {slippage_cap}"
                    ),
                    force=True,
                )
                return None, "SLIPPAGE_ABOVE_LIMIT"

            # Re-quote tepat sebelum submit untuk cek fee/slippage terkini.
            fresh_fee_amount = effective_preflight_fee
            fresh_slippage = hop.slippage
            if (
                (
                    fee_cap is not None
                    and not allow_network_fee_cap_bypass
                    and hop.network_fee_symbol == CC_SYMBOL
                )
                or slippage_cap is not None
            ):
                try:
                    fresh_quote = await sdk.get_swap_quote(
                        sell_amount=hop.sell_amount,
                        sell_instrument=hop.raw_quote.sell_instrument,
                        buy_instrument=hop.raw_quote.buy_instrument,
                    )
                    if hop.network_fee_symbol == CC_SYMBOL:
                        fresh_fee_amount = fresh_quote.fees.network_fee.amount
                    fresh_slippage = fresh_quote.prices.slippage
                    logger.info(
                        "Re-quote sebelum submit hop %s/%s round %s: fee=%s CC (was %s) | slippage=%s (was %s)",
                        hop_index,
                        hop_total,
                        round_number,
                        fresh_fee_amount,
                        quote_fee_text,
                        fresh_slippage,
                        hop.slippage,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        (
                            f"[re-quote] Hop {hop_index}/{hop_total} "
                            f"fee={fresh_fee_amount} CC (was {quote_fee_text}) | "
                            f"slip={fresh_slippage} (was {hop.slippage})"
                        ),
                    )
                    if (
                        fee_cap is not None
                        and not allow_network_fee_cap_bypass
                        and hop.network_fee_symbol == CC_SYMBOL
                        and fresh_fee_amount > fee_cap
                    ):
                        logger.warning(
                            "Swap hop %s/%s round %s DIBATALKAN: re-quote fee %s CC > batas %s CC",
                            hop_index,
                            hop_total,
                            round_number,
                            fresh_fee_amount,
                            fee_cap,
                        )
                        await self.monitor.log_event(
                            monitor_card,
                            (
                                f"🚫 Hop {hop_index}/{hop_total} DIBATALKAN: "
                                f"re-quote fee {fresh_fee_amount} CC > batas {fee_cap} CC"
                            ),
                            force=True,
                        )
                        return None, "NETWORK_FEE_ABOVE_LIMIT"
                    if slippage_cap is not None and fresh_slippage > slippage_cap:
                        logger.warning(
                            "Swap hop %s/%s round %s DIBATALKAN: re-quote slippage %s > batas %s",
                            hop_index,
                            hop_total,
                            round_number,
                            fresh_slippage,
                            slippage_cap,
                        )
                        await self.monitor.log_event(
                            monitor_card,
                            (
                                f"🚫 Hop {hop_index}/{hop_total} DIBATALKAN: "
                                f"re-quote slippage {fresh_slippage} > limit {slippage_cap}"
                            ),
                            force=True,
                        )
                        return None, "SLIPPAGE_ABOVE_LIMIT"
                except (CantexAPIError, CantexTimeoutError) as re_quote_exc:
                    logger.warning(
                        "Re-quote gagal untuk hop %s/%s round %s, lanjut dengan quote lama: %s",
                        hop_index,
                        hop_total,
                        round_number,
                        re_quote_exc,
                    )

            event = await sdk.swap_and_confirm(
                sell_amount=hop.sell_amount,
                sell_instrument=hop.raw_quote.sell_instrument,
                buy_instrument=hop.raw_quote.buy_instrument,
                max_network_fee=(
                    fee_cap
                    if fee_cap is not None and not allow_network_fee_cap_bypass
                    else None
                ),
                timeout=confirm_timeout,
            )

            await self.monitor.record_tx_success(monitor_card)
            return (
                {
                    "id": getattr(event, "event_id", ""),
                    "ledger_created_at": getattr(event, "ledger_created_at", ""),
                    "input_amount": getattr(event, "input_amount", None),
                    "output_amount": getattr(event, "output_amount", None),
                    "output_instrument": getattr(getattr(event, "output_instrument", None), "id", ""),
                    "admin_fee_amount": getattr(event, "admin_fee_amount", None),
                    "liquidity_fee_amount": getattr(event, "liquidity_fee_amount", None),
                    "quote_network_fee_amount": fresh_fee_amount if fresh_fee_amount is not None else hop.network_fee_amount,
                    "quote_network_fee_symbol": hop.network_fee_symbol,
                    "quote_slippage": fresh_slippage,
                    "raw": getattr(event, "raw", {}),
                },
                None,
            )
        except Exception as exc:
            if self._is_signature_verification_error(exc):
                await self.monitor.record_tx_failure(monitor_card)
                raise RuntimeError(
                    "Signature verification failed untuk swap intent. "
                    "Kemungkinan CANTEX_TRADING_KEY tidak cocok dengan intent account wallet ini."
                ) from exc
            if isinstance(exc, CantexTimeoutError):
                logger.warning(
                    "Swap hop %s/%s round %s belum terkonfirmasi dalam %.0fs: %s",
                    hop_index,
                    hop_total,
                    round_number,
                    confirm_timeout,
                    exc,
                )
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"⏳ Hop {hop_index}/{hop_total} pending confirmation "
                        f"after {int(confirm_timeout)}s"
                    ),
                    force=True,
                )
                return None, "SWAP_CONFIRMATION_TIMEOUT"
            if self._is_min_ticket_error(exc):
                logger.warning(
                    "Swap hop %s/%s round %s gagal karena minimum ticket size: %s",
                    hop_index,
                    hop_total,
                    round_number,
                    exc,
                )
                await self.monitor.record_tx_failure(monitor_card)
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Hop {hop_index}/{hop_total} skipped: minimum ticket size",
                    force=True,
                )
                return None, "MIN_TICKET_SIZE"
            if self._is_server_insufficient_balance_error(exc):
                logger.warning(
                    "Swap hop %s/%s round %s ditolak server karena balance kurang: %s",
                    hop_index,
                    hop_total,
                    round_number,
                    exc,
                )
                await self.monitor.record_tx_failure(monitor_card)
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Hop {hop_index}/{hop_total} pending: server insufficient balance",
                    force=True,
                )
                return None, "SERVER_INSUFFICIENT_BALANCE"
            logger.warning(
                "Swap hop %s/%s round %s gagal, tidak di-retry: %s",
                hop_index,
                hop_total,
                round_number,
                exc,
            )
            await self.monitor.record_tx_failure(monitor_card)
            await self.monitor.log_event(
                monitor_card,
                f"? Hop {hop_index}/{hop_total} failed without retry: {exc}",
                force=True,
            )
            return None, "SWAP_EXECUTION_FAILED"
        finally:
            await self._release_free_fee_sequence_slot(
                lock_acquired=lock_acquired,
                logger=logger,
                monitor_card=monitor_card,
                apply_delay=free_fee_sequential_account_name is not None,
            )

    async def _wait_for_network_fee_below_cap(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        balances: dict[str, Decimal],
        sell_symbol: str,
        buy_symbol: str,
        actual_amount: Decimal,
        round_number: int,
        cc_reserve: Decimal,
        fee_retry_deadline_utc: datetime | None,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        current_route: RoutePlan,
        account_name: str | None = None,
        strict_amount: bool = False,
    ) -> tuple[RoutePlan, PlanIssue | None, DailyFreeFeeStatus | None]:
        fee_cap = self.config.runtime.max_network_fee_cc_per_execution
        if fee_cap is None:
            return current_route, None, None

        route = current_route
        stability_samples = max(1, self.config.runtime.fee_stability_samples)
        fee_history: list[Decimal] = []

        # Record initial fee from current route into history
        initial_fee = self._route_max_cc_fee(route)
        if initial_fee is not None:
            fee_history.append(initial_fee)
            await self.monitor.update_fee_quote_history(monitor_card, initial_fee)

        while True:
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            violating_hop = self._first_network_fee_cap_violation(
                route,
                fee_cap=fee_cap,
            )
            bypassed_free_fee_status: DailyFreeFeeStatus | None = None
            if (
                violating_hop is not None
                and violating_hop[0] == 1
                and account_name is not None
                and self._startup_mode_uses_free_swap()
            ):
                daily_free_fee_status = await self._sync_daily_free_fee_state_from_history(
                    sdk=sdk,
                    account_name=account_name,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                if daily_free_fee_status.window_open and daily_free_fee_status.remaining > 0:
                    bypass_candidate = self._first_network_fee_cap_violation(
                        route,
                        fee_cap=fee_cap,
                        allow_first_hop_free=True,
                    )
                    if bypass_candidate is None:
                        bypassed_free_fee_status = daily_free_fee_status

            if violating_hop is None:
                # Fee quote saat ini ≤ cap
                if not self.config.runtime.fee_stability_enabled:
                    # Stability check disabled — langsung lolos jika fee ≤ cap
                    return route, None, None

                # Stability check: rata-rata N quote terakhir <= cap?
                if len(fee_history) >= stability_samples:
                    recent = fee_history[-stability_samples:]
                    avg = sum(recent) / len(recent)
                    if avg > fee_cap:
                        # Avg still above cap, keep waiting
                        recent_text = ", ".join(f"{f}" for f in recent)
                        logger.warning(
                            "Round %s fee quote OK tapi avg(%s) = %s CC > batas %s CC [%s], terus tunggu",
                            round_number,
                            stability_samples,
                            avg,
                            fee_cap,
                            recent_text,
                        )
                        await self.monitor.log_event(
                            monitor_card,
                            (
                                f"📊 Fee saat ini OK tapi avg({stability_samples}) = {avg} CC "
                                f"> batas {fee_cap} CC [{recent_text}], tunggu stabil"
                            ),
                        )
                        violating_hop = self._highest_cc_fee_hop(route)
                        if violating_hop is None:
                            return route, None, None
                    else:
                        # Avg OK → stable
                        recent_text = ", ".join(f"{f}" for f in recent)
                        logger.info(
                            "Round %s fee stabil: avg(%s) = %s CC ≤ batas %s CC [%s]",
                            round_number,
                            stability_samples,
                            avg,
                            fee_cap,
                            recent_text,
                        )
                        await self.monitor.log_event(
                            monitor_card,
                            f"✅ Fee stabil: avg({stability_samples}) = {avg} CC ≤ {fee_cap} CC",
                        )
                        return route, None, None
                else:
                    # Not enough samples yet, wait for more
                    collected = len(fee_history)
                    logger.info(
                        "Round %s fee quote OK tapi baru %s/%s samples, tunggu data fee lengkap",
                        round_number,
                        collected,
                        stability_samples,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        (
                            f"📊 Fee OK tapi baru {collected}/{stability_samples} samples, "
                            f"tunggu data fee lengkap sebelum eksekusi"
                        ),
                    )
                    violating_hop = self._highest_cc_fee_hop(route)
                    if violating_hop is None:
                        return route, None, None

            if bypassed_free_fee_status is not None:
                free_swap_number = bypassed_free_fee_status.used + 1
                first_hop = route.hops[0]
                logger.info(
                    "Round %s memakai free fee swap harian %s/3 | hop 1 %s -> %s | fee=%s CC | batas=%s CC",
                    round_number,
                    free_swap_number,
                    first_hop.sell_symbol,
                    first_hop.buy_symbol,
                    first_hop.network_fee_amount,
                    fee_cap,
                )
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"🎁 Free fee swap {free_swap_number}/3 active for "
                        f"{first_hop.sell_symbol}->{first_hop.buy_symbol}, fee cap bypassed "
                        f"({first_hop.network_fee_amount} CC)"
                    ),
                    force=True,
                )
                return route, None, bypassed_free_fee_status

            violating_hop_index, violating_hop_data = violating_hop
            current_fee = violating_hop_data.network_fee_amount

            now_utc = datetime.now(timezone.utc)
            if fee_retry_deadline_utc is not None and now_utc >= fee_retry_deadline_utc:
                return (
                    route,
                    PlanIssue(
                        round_number=round_number,
                        sell_symbol=sell_symbol,
                        requested_amount=actual_amount,
                        available_amount=balances.get(sell_symbol, Decimal("0")),
                        reason="network fee tetap di atas batas sampai 30 detik sebelum jadwal berikutnya",
                    ),
                    None,
                )

            # Calculate poll sleep duration — adaptive based on fee proximity to cap
            fast_range = self.config.runtime.fee_fast_poll_range
            if fast_range is not None and fast_range[0] <= current_fee <= fast_range[1]:
                # Fee is in fast poll range — poll every 1 second
                poll_sleep = 1.0
            else:
                poll_sleep = self.config.runtime.network_fee_poll_seconds_range.sample(self._rng)

            logger.info(
                "Round %s menunggu fee turun | hop=%s/%s | %s -> %s | fee=%s CC | batas=%s CC | poll %.0fs",
                round_number,
                violating_hop_index,
                len(route.hops),
                violating_hop_data.sell_symbol,
                violating_hop_data.buy_symbol,
                current_fee,
                fee_cap,
                poll_sleep,
            )
            await self.monitor.update_status(
                monitor_card,
                round_number=round_number,
                phase="WAITING_FEE",
                next_wait_seconds=poll_sleep,
                route_plan=route,
            )

            # --- Per-account polling: sleep then re-quote ---
            await self._sleep_or_stop(poll_sleep)
            self._raise_if_stop_requested()
            await self._pause_if_requested()

            # Check deadline after sleep
            now_utc = datetime.now(timezone.utc)
            if fee_retry_deadline_utc is not None and now_utc >= fee_retry_deadline_utc:
                return (
                    route,
                    PlanIssue(
                        round_number=round_number,
                        sell_symbol=sell_symbol,
                        requested_amount=actual_amount,
                        available_amount=balances.get(sell_symbol, Decimal("0")),
                        reason="network fee tetap di atas batas sampai 30 detik sebelum jadwal berikutnya",
                    ),
                    None,
                )

            # Re-quote route per-akun
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            try:
                route, issue = await self._prepare_affordable_route(
                    router=router,
                    balances=balances,
                    sell_symbol=sell_symbol,
                    buy_symbol=buy_symbol,
                    proposed_amount=actual_amount,
                    round_number=round_number,
                    cc_reserve=cc_reserve,
                    strict_amount=strict_amount,
                )
            except RuntimeError as exc:
                if not self._is_retryable_route_error(exc):
                    raise
                logger.warning(
                    "Round %s quote gagal sementara saat tunggu fee turun | %s -> %s | %s",
                    round_number,
                    sell_symbol,
                    buy_symbol,
                    exc,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                    force=True,
                )
                continue
            if issue is not None:
                return route, issue, None

            # Record fee to history buffer + monitor card
            new_fee = self._route_max_cc_fee(route)
            if new_fee is not None:
                fee_history.append(new_fee)
                # Keep buffer bounded
                if len(fee_history) > 50:
                    fee_history = fee_history[-50:]
                await self.monitor.update_fee_quote_history(monitor_card, new_fee)

        return route, None, None

    def _route_max_cc_fee(self, route: RoutePlan) -> Decimal | None:
        """Extract the maximum CC network fee from a route's hops."""
        max_fee: Decimal | None = None
        for hop in route.hops:
            if hop.network_fee_symbol == CC_SYMBOL:
                if max_fee is None or hop.network_fee_amount > max_fee:
                    max_fee = hop.network_fee_amount
        return max_fee

    def _first_network_fee_cap_violation(
        self,
        route: RoutePlan,
        *,
        fee_cap: Decimal,
        allow_first_hop_free: bool = False,
    ) -> tuple[int, RouteHop] | None:
        for hop_index, hop in enumerate(route.hops, start=1):
            if hop.network_fee_symbol != CC_SYMBOL:
                continue
            if allow_first_hop_free and hop_index == 1:
                continue
            if hop.network_fee_amount > fee_cap:
                return hop_index, hop
        return None

    def _highest_cc_fee_hop(
        self,
        route: RoutePlan,
    ) -> tuple[int, RouteHop] | None:
        """Return the hop with the highest CC network fee, for use when avg check fails."""
        best: tuple[int, RouteHop] | None = None
        best_fee = Decimal("-1")
        for hop_index, hop in enumerate(route.hops, start=1):
            if hop.network_fee_symbol != CC_SYMBOL:
                continue
            if hop.network_fee_amount > best_fee:
                best = (hop_index, hop)
                best_fee = hop.network_fee_amount
        return best

    async def _acquire_free_fee_sequence_slot(
        self,
        *,
        account_name: str,
        round_number: int,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> bool:
        if self._free_fee_swap_lock.locked():
            logger.info(
                "Round %s menunggu giliran free fee sequential",
                round_number,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏳ Free fee queue: waiting turn for {account_name}",
            )
        await self._free_fee_swap_lock.acquire()
        logger.info(
            "Round %s masuk giliran free fee sequential",
            round_number,
        )
        await self.monitor.log_event(
            monitor_card,
            f"ðŸŽ Free fee sequential turn active for {account_name}",
            force=True,
        )
        return True

    async def _release_free_fee_sequence_slot(
        self,
        *,
        lock_acquired: bool,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        apply_delay: bool,
    ) -> None:
        if not lock_acquired:
            return

        stop_requested: StopRequested | None = None
        try:
            if apply_delay:
                wait_seconds = self._sample_swap_delay_seconds()
                logger.info(
                    "Free fee sequential cooldown %.0f detik sebelum akun berikutnya",
                    wait_seconds,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏳ Free fee queue cooldown {int(wait_seconds)}s",
                )
                try:
                    await self._sleep_or_stop(wait_seconds)
                except StopRequested as exc:
                    stop_requested = exc
        finally:
            if self._free_fee_swap_lock.locked():
                self._free_fee_swap_lock.release()

        if stop_requested is not None:
            raise stop_requested

    async def _wait_for_recovery_route_ready(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        balances: dict[str, Decimal],
        source_symbol: str,
        target_symbol: str,
        recovery_amount: Decimal,
        initial_route: RoutePlan,
        initial_issue: PlanIssue | None,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> tuple[RoutePlan, PlanIssue | None]:
        route = initial_route
        issue = initial_issue
        waiting_logged = False

        while True:
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            if issue is None:
                route, issue, _ = await self._wait_for_network_fee_below_cap(
                    sdk=sdk,
                    router=router,
                    balances=balances,
                    sell_symbol=source_symbol,
                    buy_symbol=target_symbol,
                    actual_amount=route.hops[0].sell_amount if route.hops else recovery_amount,
                    round_number=0,
                    cc_reserve=cc_reserve,
                    fee_retry_deadline_utc=None,
                    logger=logger,
                    monitor_card=monitor_card,
                    current_route=route,
                )
                if issue is None:
                    return route, None

            if issue.reason != "balance fee tidak cukup":
                return route, issue

            current_fee = route.total_network_fee_by_symbol.get(CC_SYMBOL, Decimal("0"))
            current_cc = balances.get(CC_SYMBOL, Decimal("0"))
            if not waiting_logged:
                logger.info(
                    "Recovery %s -> %s menunggu fee turun | fee=%s CC | balance CC=%s",
                    source_symbol,
                    target_symbol,
                    current_fee,
                    current_cc,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Recovery {source_symbol}->{target_symbol} : balance fee tidak cukup, tunggu fee turun",
                    force=True,
                )
                waiting_logged = True

            poll_seconds = self._sample_network_fee_poll_seconds()
            fee_cap = self.config.runtime.max_network_fee_cc_per_execution
            violating_hop = (
                self._first_network_fee_cap_violation(route, fee_cap=fee_cap)
                if fee_cap is not None
                else None
            )
            if violating_hop is not None:
                violating_hop_index, violating_hop_data = violating_hop
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"⏳ Network fee hop {violating_hop_index}/{len(route.hops)} "
                        f"{violating_hop_data.sell_symbol}->{violating_hop_data.buy_symbol} "
                        f"{violating_hop_data.network_fee_amount} CC > limit {fee_cap} CC, "
                        f"waiting {int(poll_seconds)}s"
                    ),
                )

            await self._sleep_or_stop(poll_seconds)
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            available_amount = self._spendable_amount(
                source_symbol,
                balances.get(source_symbol, Decimal("0")),
                cc_reserve,
            )
            if available_amount <= dust_for_symbol(source_symbol):
                return route, PlanIssue(
                    round_number=0,
                    sell_symbol=source_symbol,
                    requested_amount=recovery_amount,
                    available_amount=available_amount,
                    reason=f"{source_symbol} balance tidak cukup untuk recovery",
                )
            recovery_amount = min(recovery_amount, available_amount)
            try:
                route, issue = await self._prepare_affordable_route(
                    router=router,
                    balances=balances,
                    sell_symbol=source_symbol,
                    buy_symbol=target_symbol,
                    proposed_amount=recovery_amount,
                    round_number=0,
                    cc_reserve=cc_reserve,
                )
            except RuntimeError as exc:
                if not self._is_retryable_route_error(exc):
                    raise
                logger.warning(
                    "Recovery %s -> %s quote gagal sementara: %s",
                    source_symbol,
                    target_symbol,
                    exc,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Recovery {source_symbol}->{target_symbol} : transient quote error ({exc})",
                )
                continue

    def _format_fee_log_line(
        self,
        *,
        prefix: str,
        network_fee: dict[str, Decimal],
        swap_fee: dict[str, Decimal],
    ) -> str:
        total_fee = self._merge_amount_maps(network_fee, swap_fee)
        return (
            f"{prefix} | net={self._format_amount_map(network_fee)} | "
            f"swap={self._format_amount_map(swap_fee)} | "
            f"total={self._format_amount_map(total_fee)}"
        )

    def _schedule_monitor_call(
        self,
        awaitable: Awaitable[None],
        *,
        logger: AccountLoggerAdapter,
        description: str,
    ) -> None:
        task = asyncio.create_task(awaitable)

        def _log_background_failure(done_task: asyncio.Task[None]) -> None:
            try:
                done_task.result()
            except Exception:
                logger.debug(
                    "Monitor update background gagal | %s",
                    description,
                    exc_info=True,
                )

        task.add_done_callback(_log_background_failure)

    def _trigger_fee_scrape_if_available(
        self,
        *,
        account_name: str,
        completed_round: int,
        monitor_card: TelegramCardState | None,
    ) -> None:
        """Trigger ccview.io fee scrape in background after swap success.

        Non-blocking: creates a background task that scrapes ccview.io
        and updates the monitor card with actual fee data.
        """
        party_id = self._account_party_ids.get(account_name, "")
        if not party_id:
            self.log.warning(
                "CCView scrape SKIP: no party_id stored for %s "
                "(stored_ids=%s)",
                account_name,
                list(self._account_party_ids.keys()),
            )
            return

        self.log.info(
            "CCView scrape triggered | %s | round=%s | party_id=%s...",
            account_name,
            completed_round,
            party_id[:20],
        )

        self.fee_scraper.trigger_background_scrape(
            party_id=party_id,
            account_name=account_name,
            completed_round=completed_round,
        )

        # Schedule a polling card update.
        # Sebagai safety net selain callback `register_on_result` (yang
        # otomatis refresh card kalau scrape sukses). Polling loop ini
        # menanggulangi situasi: lock antri panjang, callback gagal, dll.
        # Loop sampai dapat result baru atau timeout total 60 detik.
        if monitor_card is not None:
            async def _update_card_with_scrape_result() -> None:
                deadline = time.monotonic() + 60.0
                last_observed_scrape_time: float | None = self.fee_scraper._last_scrape_time.get(
                    account_name
                )
                last_observed_tx: int | None = None
                cached = self.fee_scraper.get_latest_result(account_name)
                if cached is not None and cached.success:
                    last_observed_tx = cached.validator_tx_count

                # Tunggu dulu sekitar indexing delay + slack
                await asyncio.sleep(8)

                while time.monotonic() < deadline:
                    result = self.fee_scraper.get_latest_result(account_name)
                    new_scrape_time = self.fee_scraper._last_scrape_time.get(account_name)

                    if result is not None and result.success:
                        # Anggap "data baru" kalau scrape time atau tx count berubah
                        scrape_advanced = (
                            last_observed_scrape_time is None
                            or (new_scrape_time is not None and new_scrape_time > last_observed_scrape_time)
                        )
                        tx_advanced = (
                            last_observed_tx is None
                            or result.validator_tx_count > last_observed_tx
                        )
                        if scrape_advanced or tx_advanced:
                            self.log.info(
                                "CCView card update OK (poll) | %s | fee=%s | tx=%s | avg=%s",
                                account_name,
                                result.validator_fee_total,
                                result.validator_tx_count,
                                result.avg_fee_per_swap,
                            )
                            await self.monitor.update_ccview_fee(
                                monitor_card,
                                validator_fee_total=result.validator_fee_total,
                                validator_tx_count=result.validator_tx_count,
                                avg_fee_per_swap=result.avg_fee_per_swap,
                            )
                            return

                    await asyncio.sleep(5)

                self.log.warning(
                    "CCView card update timed out 60s | %s | menunggu data baru di _latest_results",
                    account_name,
                )

            task = asyncio.create_task(_update_card_with_scrape_result())

            def _log_card_update_failure(t: asyncio.Task) -> None:
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    self.log.warning(
                        "CCView card update task exception | %s | %s",
                        account_name,
                        exc,
                    )

            task.add_done_callback(_log_card_update_failure)

    async def _scrape_ccview_and_update_card(
        self,
        *,
        account_name: str,
        monitor_card: TelegramCardState | None,
        force: bool = False,
    ) -> None:
        """Synchronous (awaited) ccview scrape + immediate card update.

        Called after swap progress is confirmed and after refill completes.
        Guarantees the gas fee data on the dashboard is up-to-date.
        Non-blocking in the sense that it doesn't block the event loop,
        but it DOES await the result before continuing.
        """
        party_id = self._account_party_ids.get(account_name, "")
        if not party_id:
            self.log.warning(
                "CCView scrape skipped: party_id tidak tersedia untuk %s (available: %s)",
                account_name,
                list(self._account_party_ids.keys())[:5],
            )
            return

        # Small delay for ccview.io indexing (reduced from 5s to 3s for faster updates)
        await asyncio.sleep(3)

        result = await self.fee_scraper.scrape_now(
            party_id=party_id,
            account_name=account_name,
            force=force,
        )

        if result is not None and result.success and monitor_card is not None:
            await self.monitor.update_ccview_fee(
                monitor_card,
                validator_fee_total=result.validator_fee_total,
                validator_tx_count=result.validator_tx_count,
                avg_fee_per_swap=result.avg_fee_per_swap,
            )
            self.log.debug(
                "CCView sync update applied | %s | fee=%s | tx=%s | avg=%s",
                account_name,
                result.validator_fee_total,
                result.validator_tx_count,
                result.avg_fee_per_swap,
            )

    def _schedule_ccview_scrape_after_progress(
        self,
        *,
        account_name: str,
        completed_round: int,
        monitor_card: TelegramCardState | None,
        reason: str,
    ) -> None:
        """Schedule ccview scrape after progress/refill without blocking swap flow."""
        party_id = self._account_party_ids.get(account_name, "")
        if not party_id or monitor_card is None:
            self.log.warning(
                "CCView progress scrape skipped | account=%s | round=%s | reason=%s | party_id_available=%s | card=%s",
                account_name,
                completed_round,
                reason,
                bool(party_id),
                monitor_card is not None,
            )
            return

        async def _runner() -> None:
            # ccview indexing can lag; retry with forced refresh so cached startup data is not reused.
            for attempt, delay_seconds in enumerate((8, 18, 35), start=1):
                try:
                    await asyncio.sleep(delay_seconds)
                    result = await self.fee_scraper.scrape_now(
                        party_id=party_id,
                        account_name=account_name,
                        force=True,
                    )
                    if result is not None and result.success:
                        await self.monitor.update_ccview_fee(
                            monitor_card,
                            validator_fee_total=result.validator_fee_total,
                            validator_tx_count=result.validator_tx_count,
                            avg_fee_per_swap=result.avg_fee_per_swap,
                        )
                        self.log.info(
                            "CCView progress scrape applied | account=%s | round=%s | reason=%s | attempt=%s | fee=%s | tx=%s | avg=%s",
                            account_name,
                            completed_round,
                            reason,
                            attempt,
                            result.validator_fee_total,
                            result.validator_tx_count,
                            result.avg_fee_per_swap,
                        )
                        return
                    self.log.warning(
                        "CCView progress scrape no result | account=%s | round=%s | reason=%s | attempt=%s",
                        account_name,
                        completed_round,
                        reason,
                        attempt,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.log.warning(
                        "CCView progress scrape exception | account=%s | round=%s | reason=%s | attempt=%s | %s",
                        account_name,
                        completed_round,
                        reason,
                        attempt,
                        exc,
                    )

        task = asyncio.create_task(_runner())
        task.add_done_callback(lambda t: None)

    def _schedule_startup_ccview_update(
        self,
        *,
        account_name: str,
        monitor_card: TelegramCardState | None,
        logger: AccountLoggerAdapter,
    ) -> None:
        """Schedule a delayed update to monitor card with startup scrape results.

        Waits for the startup scrape to complete then updates the card.
        Retries up to 3 times with increasing delays if result not ready.
        Non-blocking background task.
        """
        if monitor_card is None:
            logger.debug("CCView startup update skipped: no monitor_card")
            return

        async def _update_card_after_startup_scrape() -> None:
            # Retry up to 3 times with increasing delays
            delays = [15, 30, 60]
            for attempt, delay in enumerate(delays, start=1):
                await asyncio.sleep(delay)
                result = self.fee_scraper.get_latest_result(account_name)
                if result is not None and result.success:
                    logger.info(
                        "CCView startup data applied to dashboard (attempt %s) | "
                        "fee=%s CC | tx=%s | avg=%s CC/swap",
                        attempt,
                        result.validator_fee_total,
                        result.validator_tx_count,
                        result.avg_fee_per_swap,
                    )
                    await self.monitor.update_ccview_fee(
                        monitor_card,
                        validator_fee_total=result.validator_fee_total,
                        validator_tx_count=result.validator_tx_count,
                        avg_fee_per_swap=result.avg_fee_per_swap,
                    )
                    return  # Success, stop retrying
                else:
                    logger.warning(
                        "CCView startup scrape result not available for %s "
                        "(attempt %s/%s, waited %ss) | result=%s",
                        account_name,
                        attempt,
                        len(delays),
                        delay,
                        "no_result" if result is None else f"error:{result.error}",
                    )
            # All retries exhausted
            logger.warning(
                "CCView startup scrape GAGAL setelah %s retry untuk %s — "
                "Gas column akan menunjukkan '-' sampai swap pertama berhasil",
                len(delays),
                account_name,
            )

        task = asyncio.create_task(_update_card_after_startup_scrape())
        task.add_done_callback(lambda t: None)  # Suppress unhandled exception

    def _merge_amount_maps(
        self,
        left: dict[str, Decimal],
        right: dict[str, Decimal],
    ) -> dict[str, Decimal]:
        merged: defaultdict[str, Decimal] = defaultdict(Decimal)
        for symbol, amount in left.items():
            merged[symbol] += amount
        for symbol, amount in right.items():
            merged[symbol] += amount
        return dict(merged)

    async def _recover_to_symbol(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        target_symbol: str,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        source_symbols: tuple[str, ...] | None = None,
    ) -> int:
        total_tx = 0
        info = await sdk.get_account_info()
        balances = self._balances_by_symbol(info)
        for source_symbol in (source_symbols or self._recovery_source_order(target_symbol)):
            available_amount = self._spendable_amount(
                source_symbol,
                balances.get(source_symbol, Decimal("0")),
                cc_reserve,
            )
            if available_amount <= dust_for_symbol(source_symbol):
                continue

            recovery_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=source_symbol,
                buy_symbol=target_symbol,
                desired_amount=available_amount,
                max_available_amount=available_amount,
            )
            if recovery_amount is None:
                logger.info(
                    "Recovery source %s -> %s dilewati: %s | available=%s",
                    source_symbol,
                    target_symbol,
                    min_ticket_reason,
                    available_amount,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Recovery {source_symbol}->{target_symbol} skipped: {min_ticket_reason}",
                )
                continue

            while True:
                try:
                    route, issue = await self._prepare_affordable_route(
                        router=router,
                        balances=balances,
                        sell_symbol=source_symbol,
                        buy_symbol=target_symbol,
                        proposed_amount=recovery_amount,
                        round_number=0,
                        cc_reserve=cc_reserve,
                    )
                    break
                except RuntimeError as exc:
                    if not self._is_retryable_route_error(exc):
                        raise
                    wait_seconds = self._sample_network_fee_poll_seconds()
                    logger.warning(
                        "Recovery %s -> %s quote gagal sementara: %s",
                        source_symbol,
                        target_symbol,
                        exc,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Recovery {source_symbol}->{target_symbol} : transient quote error ({exc}), retry {int(wait_seconds)}s",
                    )
                    await self._sleep_or_stop(wait_seconds)
                    info = await sdk.get_account_info()
                    balances = self._balances_by_symbol(info)
                    available_amount = self._spendable_amount(
                        source_symbol,
                        balances.get(source_symbol, Decimal("0")),
                        cc_reserve,
                    )
                    if available_amount <= dust_for_symbol(source_symbol):
                        route = None
                        issue = PlanIssue(
                            round_number=0,
                            sell_symbol=source_symbol,
                            requested_amount=recovery_amount,
                            available_amount=available_amount,
                            reason=f"{source_symbol} balance tidak cukup untuk recovery",
                        )
                        break
                    recovery_amount = min(recovery_amount, available_amount)
            if route is None:
                if issue is not None:
                    logger.info(
                        "Recovery source %s -> %s dilewati: %s | available=%s",
                        source_symbol,
                        target_symbol,
                        issue.reason,
                        available_amount,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Recovery {source_symbol}->{target_symbol} skipped: {issue.reason}",
                    )
                continue
            route, issue = await self._wait_for_recovery_route_ready(
                sdk=sdk,
                router=router,
                balances=balances,
                source_symbol=source_symbol,
                target_symbol=target_symbol,
                recovery_amount=recovery_amount,
                initial_route=route,
                initial_issue=issue,
                cc_reserve=cc_reserve,
                logger=logger,
                monitor_card=monitor_card,
            )
            if issue is not None:
                logger.info(
                    "Recovery source %s -> %s belum affordable: %s",
                    source_symbol,
                    target_symbol,
                    issue.reason,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Recovery {source_symbol}->{target_symbol} skipped: {issue.reason}",
                )
                continue

            route_amount = route.hops[0].sell_amount if route.hops else recovery_amount
            logger.info(
                "Recovery aset sisa | %s -> %s | nominal=%s | route=%s",
                source_symbol,
                target_symbol,
                route_amount,
                route.label,
            )
            await self.monitor.log_event(
                monitor_card,
                f"🛟 Recovery {source_symbol}->{target_symbol} ({route_amount})",
            )
            recovery_failed = False
            for hop_index, hop in enumerate(route.hops, start=1):
                hop_balances_before = dict(balances)
                tx_result, failure_reason = await self._swap_hop_with_retry(
                    sdk=sdk,
                    hop=hop,
                    hop_index=hop_index,
                    hop_total=len(route.hops),
                    round_number=0,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                if tx_result is None:
                    recovery_failed = True
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Recovery {source_symbol}->{target_symbol} skipped: {failure_reason or 'retry limit reached'}",
                        force=True,
                    )
                    break
                total_tx += 1
                settled_balances = await self._wait_for_hop_balance_settlement(
                    sdk=sdk,
                    previous_balances=hop_balances_before,
                    hop=hop,
                    tx_result=tx_result,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                actual_network_fee, actual_swap_fee, settled_balances = await self._resolve_actual_successful_hop_fees(
                    sdk=sdk,
                    hop=hop,
                    tx_result=tx_result,
                    balances_before=hop_balances_before,
                    balances_after=settled_balances,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                balances = settled_balances
                await self.monitor.update_balances(
                    monitor_card,
                    balances,
                )
                for symbol, amount in actual_network_fee.items():
                    used_network_fee[symbol] += amount
                for symbol, amount in actual_swap_fee.items():
                    used_swap_fee[symbol] += amount
                await self.monitor.update_fee_totals(
                    monitor_card,
                    total_network_fee=dict(used_network_fee),
                    total_swap_fee=dict(used_swap_fee),
                )
                tx_identifier = tx_result.get("id") or tx_result.get("transactionId") or tx_result.get("contract_id")
                logger.info(
                    "Recovery tx | %s -> %s | tx=%s",
                    hop.sell_symbol,
                    hop.buy_symbol,
                    tx_identifier or "-",
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🛟 Recovery tx {hop.sell_symbol}->{hop.buy_symbol} {tx_identifier or '-'}",
                )
                # --- Cycle spread loss tracking utk swap recovery / refill juga ---
                actual_recovery_output = self._resolve_actual_output_amount(
                    hop=hop,
                    tx_result=tx_result,
                    balances_before=hop_balances_before,
                    balances_after=settled_balances,
                )[0]
                cycle_tracker = self._get_cycle_tracker(account.name)
                cycle_result = cycle_tracker.record_swap(
                    sell_symbol=hop.sell_symbol,
                    buy_symbol=hop.buy_symbol,
                    sell_amount=hop.sell_amount,
                    buy_amount=actual_recovery_output,
                    network_fee=actual_network_fee,
                    swap_fee=actual_swap_fee,
                    balances_before=hop_balances_before,
                    balances_after=settled_balances,
                )
                logger.info(
                    "[CYLOSS DIAG] record_swap (recovery path) | mode=%s | %s->%s | "
                    "sell=%s buy=%s | result=%s | total_usdcx=%s | total_cc=%s | count=%s",
                    cycle_tracker.mode,
                    hop.sell_symbol,
                    hop.buy_symbol,
                    hop.sell_amount,
                    actual_recovery_output,
                    "complete" if cycle_result is not None else "none",
                    cycle_tracker.total_usdcx_spread_loss,
                    cycle_tracker.total_cc_spread_loss,
                    cycle_tracker.cycle_count,
                )
                if cycle_result is not None:
                    await self.monitor.record_cycle_spread_loss(
                        monitor_card,
                        cycle_result=cycle_result,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        (
                            f"🔁 Cycle #{cycle_tracker.cycle_count} complete (recovery, "
                            f"{cycle_result.cycle_type}) | "
                            f"{cycle_result.origin_symbol}: {cycle_result.start_amount} -> "
                            f"{cycle_result.end_amount} | loss={cycle_result.spread_loss}"
                        ),
                    )
                    self._save_cycle_tracker_state(account.name)
                await self._sleep_between_swaps()
            if recovery_failed:
                continue
            if target_symbol == CC_SYMBOL:
                await self.monitor.log_event(
                    monitor_card,
                    f"✅ Recovery {source_symbol}->{target_symbol} : refill berhasil",
                    force=True,
                )
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
        return total_tx

    async def _run_24h_direct_session(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        strategy_state: StrategyRuntimeState,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        result: AccountResult,
    ) -> None:
        # Track tanggal UTC saat session dimulai untuk deteksi day change
        _session_utc_date = datetime.now(timezone.utc).date().isoformat()

        while result.completed_rounds < prepared_run.rounds:
            self._raise_if_stop_requested()
            await self._pause_if_requested()

            # === Proactive daily reset: cek apakah hari UTC sudah berganti ===
            was_reset, _session_utc_date = await self._check_and_reset_daily_progress(
                account=account,
                prepared_run=prepared_run,
                result=result,
                monitor_card=monitor_card,
                logger=logger,
                session_utc_date=_session_utc_date,
            )
            if was_reset:
                logger.info(
                    "Daily reset terjadi di awal loop | progress sekarang=%s/%s",
                    result.completed_rounds,
                    prepared_run.rounds,
                )
                # Setelah reset, lanjut loop dari awal dengan progress=0
                continue

            if self._weekly_stop_due_utc():
                await self._perform_weekly_stop(
                    logger=logger,
                    monitor_card=monitor_card,
                    result=result,
                )
                return
            result.completed_rounds = await self._wait_for_trading_history_round_progress(
                sdk=sdk,
                account=account,
                prepared_run=prepared_run,
                logger=logger,
                monitor_card=monitor_card,
                previous_completed_rounds=result.completed_rounds,
            )
            result.swap_transactions = result.completed_rounds
            self._persist_round_session_progress(
                account=account,
                prepared_run=prepared_run,
                result=result,
            )
            await self._maybe_pre_refill_usdcx_v2(
                sdk=sdk,
                router=router,
                account=account,
                prepared_run=prepared_run,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
                result=result,
            )
            if self._weekly_stop_due_utc():
                await self._perform_weekly_stop(
                    logger=logger,
                    monitor_card=monitor_card,
                    result=result,
                )
                return
            if result.completed_rounds >= prepared_run.rounds:
                # Refill semua non-target ke target pilihan sebelum sleep (untuk SEMUA strategi)
                await self._refill_after_target(
                    sdk=sdk,
                    router=router,
                    account=account,
                    logger=logger,
                    monitor_card=monitor_card,
                    used_network_fee=used_network_fee,
                    used_swap_fee=used_swap_fee,
                    result=result,
                )
                await self._wait_until_next_utc_day_after_quota(
                    sdk=sdk,
                    account=account,
                    prepared_run=prepared_run,
                    result=result,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                continue
            current_round_number = result.completed_rounds + 1

            if self._startup_mode_is_free_only():
                daily_free_fee_status = await self._sync_daily_free_fee_state_from_history(
                    sdk=sdk,
                    account_name=account.name,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                if not daily_free_fee_status.window_open:
                    wait_seconds = max(
                        1.0,
                        (daily_free_fee_status.window_opens_at_utc - datetime.now(timezone.utc)).total_seconds(),
                    )
                    logger.info(
                        "Mode free swap menunggu window buka pada %s UTC",
                        self._format_utc(daily_free_fee_status.window_opens_at_utc),
                    )
                    await self.monitor.update_status(
                        monitor_card,
                        round_number=current_round_number,
                        phase="WAITING",
                        next_scheduled_utc=daily_free_fee_status.window_opens_at_utc,
                        next_wait_seconds=wait_seconds,
                        clear_route=True,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"🎁 Free swap window opens in {int(wait_seconds)}s",
                    )
                    await self._sleep_or_stop(wait_seconds)
                    continue
                if daily_free_fee_status.remaining <= 0:
                    if self.config.runtime.full_24h_auto_restart:
                        await self._wait_until_next_utc_day_for_free_fee(
                            logger=logger,
                            monitor_card=monitor_card,
                        )
                    return

            round_result = await self._execute_round(
                sdk=sdk,
                router=router,
                account=account,
                prepared_run=prepared_run,
                round_number=current_round_number,
                strategy_state=strategy_state,
                fee_retry_deadline_utc=None,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            if round_result.completed:
                result.completed_rounds = await self._wait_for_trading_history_round_progress(
                    sdk=sdk,
                    account=account,
                    prepared_run=prepared_run,
                    logger=logger,
                    monitor_card=monitor_card,
                    previous_completed_rounds=result.completed_rounds,
                    require_increment=True,
                    force_log=True,
                )
                result.swap_transactions = result.completed_rounds
                self._persist_round_session_progress(
                    account=account,
                    prepared_run=prepared_run,
                    result=result,
                )
                # Trigger ccview.io fee scrape from confirmed progress (non-blocking)
                self._schedule_ccview_scrape_after_progress(
                    account_name=account.name,
                    completed_round=result.completed_rounds,
                    monitor_card=monitor_card,
                    reason="round_complete",
                )
                if self._weekly_stop_due_utc():
                    await self._perform_weekly_stop(
                        logger=logger,
                        monitor_card=monitor_card,
                        result=result,
                    )
                    return
                if result.completed_rounds >= prepared_run.rounds:
                    # Refill semua non-target ke target pilihan sebelum sleep (untuk SEMUA strategi)
                    await self._refill_after_target(
                        sdk=sdk,
                        router=router,
                        account=account,
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                        result=result,
                    )
                    await self._wait_until_next_utc_day_after_quota(
                        sdk=sdk,
                        account=account,
                        prepared_run=prepared_run,
                        result=result,
                        logger=logger,
                        monitor_card=monitor_card,
                    )
                    continue
                await self._sleep_after_direct_24h_success(
                    logger=logger,
                    monitor_card=monitor_card,
                    next_round_number=result.completed_rounds + 1,
                    pair_key=None,
                )
                continue

            if not round_result.skipped and round_result.stop_reason is not None:
                result.stop_reason = round_result.stop_reason
                return

            if round_result.skipped:
                result.skipped_rounds += 1
            await self._sleep_after_direct_24h_pending(
                logger=logger,
                monitor_card=monitor_card,
                next_round_number=current_round_number,
                pair_key=None,
            )
        return

    async def _run_24h_session(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        strategy_state: StrategyRuntimeState,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        result: AccountResult,
        session_end_utc: datetime,
        schedule: tuple[ScheduledRound, ...],
    ) -> None:
        session_utc_date = datetime.now(timezone.utc).date().isoformat()
        for scheduled_round in schedule:
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            was_reset, session_utc_date = await self._check_and_reset_daily_progress(
                account=account,
                prepared_run=prepared_run,
                result=result,
                monitor_card=monitor_card,
                logger=logger,
                session_utc_date=session_utc_date,
            )
            if was_reset:
                logger.info(
                    "Daily reset terjadi di awal scheduled loop | progress sekarang=%s/%s",
                    result.completed_rounds,
                    prepared_run.rounds,
                )
                return
            if self._weekly_stop_due_utc():
                await self._perform_weekly_stop(
                    logger=logger,
                    monitor_card=monitor_card,
                    result=result,
                )
                return
            if result.completed_rounds >= prepared_run.rounds:
                return
            result.completed_rounds = await self._wait_for_trading_history_round_progress(
                sdk=sdk,
                account=account,
                prepared_run=prepared_run,
                logger=logger,
                monitor_card=monitor_card,
                previous_completed_rounds=result.completed_rounds,
            )
            result.swap_transactions = result.completed_rounds
            self._persist_round_session_progress(
                account=account,
                prepared_run=prepared_run,
                result=result,
            )
            await self._maybe_pre_refill_usdcx_v2(
                sdk=sdk,
                router=router,
                account=account,
                prepared_run=prepared_run,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
                result=result,
            )
            if self._weekly_stop_due_utc():
                await self._perform_weekly_stop(
                    logger=logger,
                    monitor_card=monitor_card,
                    result=result,
                )
                return
            if result.completed_rounds >= prepared_run.rounds:
                # Refill semua non-target ke target pilihan sebelum sleep (untuk SEMUA strategi)
                await self._refill_after_target(
                    sdk=sdk,
                    router=router,
                    account=account,
                    logger=logger,
                    monitor_card=monitor_card,
                    used_network_fee=used_network_fee,
                    used_swap_fee=used_swap_fee,
                    result=result,
                )
                await self._wait_until_next_utc_day_after_quota(
                    sdk=sdk,
                    account=account,
                    prepared_run=prepared_run,
                    result=result,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                return
            now_utc = datetime.now(timezone.utc)
            if now_utc >= session_end_utc:
                logger.info(
                    "Mode 24 jam selesai pada %s UTC",
                    self._format_utc(session_end_utc),
                )
                return

            wait_seconds = (scheduled_round.execute_at_utc - now_utc).total_seconds()
            current_round_number = result.completed_rounds + 1
            next_slot_utc = (
                schedule[scheduled_round.round_index + 1].execute_at_utc
                if scheduled_round.round_index + 1 < len(schedule)
                else session_end_utc
            )
            fee_retry_deadline_utc = next_slot_utc - timedelta(seconds=30)
            if wait_seconds > 0:
                logger.info(
                    "Menunggu round %s sampai %s UTC (%.0f detik lagi)",
                    current_round_number,
                    self._format_utc(scheduled_round.execute_at_utc),
                    wait_seconds,
                )
                await self.monitor.update_status(
                    monitor_card,
                    round_number=current_round_number,
                    phase="WAITING",
                    next_scheduled_utc=scheduled_round.execute_at_utc,
                    next_wait_seconds=wait_seconds,
                    clear_route=True,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏳ Next swap in {int(wait_seconds)}s",
                )
                await self._sleep_or_stop(wait_seconds)
                was_reset, session_utc_date = await self._check_and_reset_daily_progress(
                    account=account,
                    prepared_run=prepared_run,
                    result=result,
                    monitor_card=monitor_card,
                    logger=logger,
                    session_utc_date=session_utc_date,
                )
                if was_reset:
                    logger.info(
                        "Daily reset terjadi setelah wait scheduled round | progress sekarang=%s/%s",
                        result.completed_rounds,
                        prepared_run.rounds,
                    )
                    return
                if self._weekly_stop_due_utc():
                    await self._perform_weekly_stop(
                        logger=logger,
                        monitor_card=monitor_card,
                        result=result,
                    )
                    return
            else:
                logger.info(
                    "Round %s sudah melewati jadwal %.0f detik, dieksekusi sekarang",
                    current_round_number,
                    abs(wait_seconds),
                )

            round_result = await self._execute_round(
                sdk=sdk,
                router=router,
                account=account,
                prepared_run=prepared_run,
                round_number=current_round_number,
                strategy_state=strategy_state,
                fee_retry_deadline_utc=fee_retry_deadline_utc,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            if round_result.completed:
                result.completed_rounds = await self._wait_for_trading_history_round_progress(
                    sdk=sdk,
                    account=account,
                    prepared_run=prepared_run,
                    logger=logger,
                    monitor_card=monitor_card,
                    previous_completed_rounds=result.completed_rounds,
                    require_increment=True,
                    force_log=True,
                )
                result.swap_transactions = result.completed_rounds
                self._persist_round_session_progress(
                    account=account,
                    prepared_run=prepared_run,
                    result=result,
                )
                # Trigger ccview.io fee scrape from confirmed progress (non-blocking)
                self._schedule_ccview_scrape_after_progress(
                    account_name=account.name,
                    completed_round=result.completed_rounds,
                    monitor_card=monitor_card,
                    reason="round_complete",
                )
                if self._weekly_stop_due_utc():
                    await self._perform_weekly_stop(
                        logger=logger,
                        monitor_card=monitor_card,
                        result=result,
                    )
                    return
                if result.completed_rounds >= prepared_run.rounds:
                    # Refill semua non-target ke target pilihan sebelum sleep (untuk SEMUA strategi)
                    await self._refill_after_target(
                        sdk=sdk,
                        router=router,
                        account=account,
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                        result=result,
                    )
                    await self._wait_until_next_utc_day_after_quota(
                        sdk=sdk,
                        account=account,
                        prepared_run=prepared_run,
                        result=result,
                        logger=logger,
                        monitor_card=monitor_card,
                    )
                    return
            elif not round_result.skipped and round_result.stop_reason is not None:
                result.stop_reason = round_result.stop_reason
                return
            elif round_result.skipped:
                result.skipped_rounds += 1
        logger.info("Sesi 24 jam selesai pada %s UTC", self._format_utc(session_end_utc))
        return

    def _build_24h_schedule(
        self,
        *,
        rounds: int,
        start_utc: datetime,
        end_utc: datetime,
        execution_buffer_seconds: float,
    ) -> tuple[ScheduledRound, ...]:
        if rounds < 1:
            return ()

        total_seconds = max(1.0, (end_utc - start_utc).total_seconds())
        reserved_seconds = min(max(0.0, execution_buffer_seconds), max(0.0, total_seconds - 1.0))
        schedulable_seconds = max(1.0, total_seconds - reserved_seconds)

        min_gap_seconds = max(0.0, self.config.runtime.full_24h_min_gap_minutes * 60.0)
        if rounds > 1:
            required_gap_total = min_gap_seconds * (rounds - 1)
            if required_gap_total >= schedulable_seconds:
                min_gap_seconds = max(0.0, (schedulable_seconds * 0.8) / (rounds - 1))
        else:
            min_gap_seconds = 0.0

        remaining_seconds = max(1.0, schedulable_seconds - (min_gap_seconds * max(0, rounds - 1)))
        weights = [self._rng.expovariate(1.0) for _ in range(rounds + 1)]
        weight_total = sum(weights)

        timestamps: list[datetime] = []
        elapsed = 0.0
        for round_index in range(rounds):
            elapsed += (weights[round_index] / weight_total) * remaining_seconds
            scheduled_at = start_utc + timedelta(seconds=elapsed)
            timestamps.append(scheduled_at)
            elapsed += min_gap_seconds

        return tuple(
            ScheduledRound(round_index=index, execute_at_utc=timestamp)
            for index, timestamp in enumerate(timestamps)
        )

    def _log_24h_schedule(
        self,
        logger: AccountLoggerAdapter,
        remaining_rounds: int,
        session_start_utc: datetime,
        session_end_utc: datetime,
        schedule: tuple[ScheduledRound, ...],
        execution_buffer_seconds: float,
        start_round_number: int,
    ) -> None:
        logger.info(
            "Mode 24 jam aktif | mulai=%s UTC | selesai=%s UTC | rounds=%s | buffer-eksekusi=%.0f detik",
            self._format_utc(session_start_utc),
            self._format_utc(session_end_utc),
            remaining_rounds,
            execution_buffer_seconds,
        )
        displayed_schedule = self._compress_schedule_for_logging(schedule)
        for scheduled_round in displayed_schedule:
            if scheduled_round is None:
                logger.info("Jadwal random | ... disingkat ...")
                continue
            logger.info(
                "Jadwal random round %s | waktu=%s UTC",
                start_round_number + scheduled_round.round_index,
                self._format_utc(scheduled_round.execute_at_utc),
            )

    def _compress_schedule_for_logging(
        self,
        schedule: tuple[ScheduledRound, ...],
    ) -> tuple[ScheduledRound | None, ...]:
        limit = self.config.runtime.full_24h_schedule_log_limit
        if len(schedule) <= limit:
            return schedule

        head_count = max(1, limit // 2)
        tail_count = max(1, limit - head_count)
        compressed: list[ScheduledRound | None] = list(schedule[:head_count])
        compressed.append(None)
        compressed.extend(schedule[-tail_count:])
        return tuple(compressed)

    def _estimate_24h_execution_buffer_seconds(self, remaining_rounds: int) -> float:
        return max(300.0, remaining_rounds * 90.0)

    async def _wait_until_next_utc_day_after_quota(
        self,
        *,
        sdk: ExtendedCantexSDK,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        result: AccountResult,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> None:
        """Tunggu sampai UTC midnight lalu reset progress. Tidak polling trading history."""
        if self._weekly_stop_due_utc():
            logger.info("Weekly stop jatuh tempo; tidak menunggu quota harian")
            await self.monitor.log_event(
                monitor_card,
                "Weekly stop due, skip daily quota wait",
                force=True,
            )
            return

        now_utc = datetime.now(timezone.utc)
        next_midnight_utc = self._next_utc_midnight(now_utc)
        wait_seconds = max(0.0, (next_midnight_utc - now_utc).total_seconds())

        logger.info(
            "Quota harian tercapai (%s/%s) | Menunggu sampai %s UTC (%.0f detik)",
            result.completed_rounds,
            prepared_run.rounds,
            self._format_utc(next_midnight_utc),
            wait_seconds,
        )
        await self.monitor.update_status(
            monitor_card,
            phase="WAITING_NEXT_DAY",
            next_scheduled_utc=next_midnight_utc,
            next_wait_seconds=wait_seconds,
            clear_route=True,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            (
                f"Daily quota reached ({result.completed_rounds}/{prepared_run.rounds}), "
                f"sleeping until {self._format_utc(next_midnight_utc)} UTC"
            ),
            force=True,
        )

        while True:
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            if self._weekly_stop_due_utc():
                logger.info("Weekly stop jatuh tempo saat menunggu midnight")
                await self.monitor.log_event(
                    monitor_card,
                    "Weekly stop due during midnight wait",
                    force=True,
                )
                return
            remaining = (next_midnight_utc - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0:
                break
            await self._sleep_or_stop(min(5.0, remaining))

        logger.info("UTC midnight tercapai - reset daily progress")
        self._reset_result_round_progress_for_new_activity_window(
            account=account,
            prepared_run=prepared_run,
            result=result,
        )
        result.skipped_rounds = 0

        # Trigger monitor card rollover
        if monitor_card is not None:
            self.monitor._rollover_card_if_needed(monitor_card)
            # Reset daily loss tracking utk hari baru — pakai simbol target efektif
            loss_target_symbol = self._effective_post_target_refill_symbol()
            current_balance = monitor_card.balances.get(loss_target_symbol, Decimal("0"))
            await self.monitor.set_cc_balance_start_of_day(
                monitor_card,
                current_balance,
                target_symbol=loss_target_symbol,
            )

        await self.monitor.log_event(
            monitor_card,
            f"🌅 Daily reset complete: progress=0/{prepared_run.rounds}",
            force=True,
        )

    async def _wait_until_next_utc_day_for_free_fee(
        self,
        *,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> None:
        if self._weekly_stop_due_utc():
            logger.info("Weekly stop jatuh tempo; tidak menunggu free fee harian")
            await self.monitor.log_event(
                monitor_card,
                "Weekly stop due, skip free fee wait",
                force=True,
            )
            return
        now_utc = datetime.now(timezone.utc)
        next_midnight_utc = self._next_utc_midnight(now_utc)
        wait_seconds = max(0.0, (next_midnight_utc - now_utc).total_seconds())
        if wait_seconds <= 0:
            return
        logger.info(
            "Jatah free fee harian habis, menunggu sampai %s UTC",
            self._format_utc(next_midnight_utc),
        )
        await self.monitor.update_status(
            monitor_card,
            phase="WAITING_NEXT_DAY",
            next_scheduled_utc=next_midnight_utc,
            next_wait_seconds=wait_seconds,
            clear_route=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"Free fee quota used, waiting until {self._format_utc(next_midnight_utc)} UTC",
            force=True,
        )
        await self._sleep_or_stop(wait_seconds)
        return

        if self._weekly_stop_due_utc():
            logger.info("Weekly stop jatuh tempo; tidak menunggu quota harian")
            await self.monitor.log_event(
                monitor_card,
                "🛑 Weekly stop due, skip daily quota wait",
                force=True,
            )
            return
        now_utc = datetime.now(timezone.utc)
        next_midnight_utc = self._next_utc_midnight(now_utc)
        wait_seconds = max(0.0, (next_midnight_utc - now_utc).total_seconds())
        if wait_seconds <= 0:
            return
        logger.info(
            "Quota harian tercapai, menunggu sampai %s UTC untuk sesi berikutnya",
            self._format_utc(next_midnight_utc),
        )
        await self.monitor.update_status(
            monitor_card,
            phase="WAITING_NEXT_DAY",
            next_scheduled_utc=next_midnight_utc,
            next_wait_seconds=wait_seconds,
            clear_route=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"🌙 Daily quota reached, waiting until {self._format_utc(next_midnight_utc)} UTC",
            force=True,
        )
        await self._sleep_or_stop(wait_seconds)

    def _reset_result_round_progress_for_new_activity_window(
        self,
        *,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        result: AccountResult,
    ) -> None:
        result.completed_rounds = 0
        result.swap_transactions = 0
        self.runtime_state.update_round_session_progress(
            account.name,
            strategy_name=prepared_run.strategy_name,
            requested_rounds=prepared_run.rounds,
            completed_rounds=0,
        )

    async def _check_and_reset_daily_progress(
        self,
        *,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        result: AccountResult,
        monitor_card: TelegramCardState | None,
        logger: AccountLoggerAdapter,
        session_utc_date: str,
    ) -> tuple[bool, str]:
        """Cek apakah hari UTC sudah berganti. Jika ya, reset progress ke 0.

        Returns:
            Tuple of (was_reset, current_utc_date_iso).
            was_reset is True if a daily reset was performed.
        """
        current_utc_date = datetime.now(timezone.utc).date().isoformat()
        if current_utc_date == session_utc_date:
            return False, session_utc_date

        # Hari UTC sudah berganti — reset progress
        logger.info(
            "🌅 Hari UTC berganti (%s → %s) | Reset daily progress ke 0",
            session_utc_date,
            current_utc_date,
        )

        # Reset result counters
        result.completed_rounds = 0
        result.swap_transactions = 0
        result.skipped_rounds = 0

        # Persist reset ke runtime state
        self.runtime_state.update_round_session_progress(
            account.name,
            strategy_name=prepared_run.strategy_name,
            requested_rounds=prepared_run.rounds,
            completed_rounds=0,
        )

        # Trigger monitor card rollover
        if monitor_card is not None:
            self.monitor._rollover_card_if_needed(monitor_card)

        # Reset daily loss tracking utk hari baru — pakai simbol target efektif
        # supaya CyLoss bekerja saat target = CC, USDCx, atau USDCx_v2.
        if monitor_card is not None:
            loss_target_symbol = self._effective_post_target_refill_symbol()
            current_balance = monitor_card.balances.get(loss_target_symbol, Decimal("0"))
            await self.monitor.set_cc_balance_start_of_day(
                monitor_card,
                current_balance,
                target_symbol=loss_target_symbol,
            )
            logger.info(
                "Daily reset: %s balance start of new day = %s",
                loss_target_symbol,
                current_balance,
            )

        # Log event to telegram
        await self.monitor.log_event(
            monitor_card,
            f"🌅 Daily reset: {session_utc_date} → {current_utc_date} | progress=0/{prepared_run.rounds}",
            force=True,
        )

        return True, current_utc_date

    async def _sleep_after_direct_24h_success(
        self,
        *,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        next_round_number: int,
        pair_key: str | None,
    ) -> None:
        wait_seconds = self._sample_swap_delay_seconds()
        logger.info(
            "Mode 24 jam direct menunggu %.0f detik sebelum swap berikutnya",
            wait_seconds,
        )
        await self.monitor.update_status(
            monitor_card,
            pair_key=pair_key,
            round_number=next_round_number,
            phase="WAITING",
            next_wait_seconds=wait_seconds,
            clear_route=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"⏳ Next swap in {int(wait_seconds)}s",
        )
        await self._sleep_or_stop(wait_seconds)

    async def _sleep_after_direct_24h_pending(
        self,
        *,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        next_round_number: int,
        pair_key: str | None,
    ) -> None:
        wait_seconds = max(1.0, self.config.runtime.retry_base_delay)
        logger.info(
            "Mode 24 jam direct retry lagi dalam %.0f detik",
            wait_seconds,
        )
        await self.monitor.update_status(
            monitor_card,
            pair_key=pair_key,
            round_number=next_round_number,
            phase="WAITING",
            next_wait_seconds=wait_seconds,
            clear_route=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"⏳ Retry next attempt in {int(wait_seconds)}s",
        )
        await self._sleep_or_stop(wait_seconds)

    def _sample_swap_delay_seconds(self) -> float:
        return self.config.runtime.swap_delay_seconds_range.sample(self._rng)

    async def _sleep_between_swaps(self) -> None:
        if self.config.runtime.full_24h_mode:
            return
        await self._sleep_or_stop(self._sample_swap_delay_seconds())

    def _sample_network_fee_poll_seconds(self) -> float:
        return max(1.0, self.config.runtime.network_fee_poll_seconds_range.sample(self._rng))

    def _startup_mode_label(self) -> str:
        labels = {
            "free_only": "free-only",
            "free_then_swap": "free-then-swap",
            "swap_only": "swap-only",
            "planned_fee": "planned-fee",
            "refill_cc": "refill-cc",
            "check_accounts": "check-accounts",
        }
        return labels.get(self.startup_mode, self.startup_mode)

    def _startup_mode_is_planned(self) -> bool:
        return self.startup_mode == "planned_fee"

    def _startup_mode_is_free_only(self) -> bool:
        return self.startup_mode == "free_only"

    def _startup_mode_is_refill_cc(self) -> bool:
        return self.startup_mode == "refill_cc"

    def _startup_mode_is_check_accounts(self) -> bool:
        return self.startup_mode == "check_accounts"

    def _post_target_refill_is_usdcx_v2(self) -> bool:
        return self.post_target_refill_symbol == "USDCx_v2"

    def _effective_post_target_refill_symbol(self) -> str:
        if self._post_target_refill_is_usdcx_v2():
            return "USDCx"
        return self.post_target_refill_symbol

    async def _run_check_accounts(self) -> None:
        """Mode cek akun: auth semua akun, ambil data, render dashboard 1x, kirim Telegram, berhenti."""
        CHECK_ACCOUNT_TIMEOUT = 60  # detik timeout per akun

        print("\n📊 Mode Cek Akun — memulai...", flush=True)
        print(f"   Jumlah akun: {len(self.config.accounts)}", flush=True)
        print("", flush=True)

        cards: list[TelegramCardState] = []

        for idx, account in enumerate(self.config.accounts, start=1):
            acc_logger = AccountLoggerAdapter(self.log, {"account": account.name})
            print(f"   [{idx}/{len(self.config.accounts)}] {account.name} — auth...", end="", flush=True)

            card = TelegramCardState(
                account_name=account.name,
                display_index=account.display_index,
                proxy_label=account.proxy_label,
                strategy_label="check",
                session_started_utc=datetime.now(timezone.utc),
                total_rounds=0,
                pair_targets={},
                phase="CHECKING",
                balances={symbol: Decimal("0") for symbol in TRACKED_SYMBOLS},
            )

            try:
                await asyncio.wait_for(
                    self._check_single_account(account, card, acc_logger),
                    timeout=CHECK_ACCOUNT_TIMEOUT,
                )
                print(
                    f" OK | CC={card.balances.get('CC', Decimal('0')):.2f}"
                    f" | USDCx={card.balances.get('USDCx', Decimal('0')):.2f}"
                    f" | CBTC={card.balances.get('CBTC', Decimal('0')):.8f}",
                    flush=True,
                )
                card.phase = "FINISHED"
            except asyncio.TimeoutError:
                print(f" TIMEOUT ({CHECK_ACCOUNT_TIMEOUT}s)", flush=True)
                acc_logger.error("Timeout setelah %ss saat cek akun", CHECK_ACCOUNT_TIMEOUT)
                card.phase = "FAILED_TIMEOUT"
            except Exception as exc:
                print(f" ERROR: {exc}", flush=True)
                acc_logger.error("Gagal cek akun: %s", exc)
                card.phase = "FAILED_ERROR"

            cards.append(card)
            self.monitor._cards[account.name] = card

        print("", flush=True)
        print("   Semua akun selesai dicek. Rendering dashboard...", flush=True)
        print("", flush=True)

        # Render dashboard terminal 1x (format sama seperti saat bot running)
        # Bypass terminal_dashboard_enabled dan isatty check — langsung render
        self._render_check_dashboard(cards)

        # Kirim ke Telegram via _publish (format sama seperti dashboard)
        if self.config.runtime.telegram_enabled:
            try:
                # Force new message (reset message_id agar kirim baru, bukan edit)
                old_msg_id = self.monitor._telegram_message_id
                self.monitor._telegram_message_id = None
                self.monitor._telegram_last_render_text = None
                await self.monitor._publish(force=True)
                # Restore jika perlu
                if self.monitor._telegram_message_id is None:
                    self.monitor._telegram_message_id = old_msg_id
                print("   ✅ Laporan terkirim ke Telegram", flush=True)
            except Exception as exc:
                print(f"   ⚠️ Gagal kirim ke Telegram: {exc}", flush=True)

        print("\n📊 Mode cek akun selesai.\n", flush=True)

    async def _check_single_account(
        self,
        account: AccountConfig,
        card: TelegramCardState,
        logger: AccountLoggerAdapter,
    ) -> None:
        """Auth 1 akun, fetch balance/reward/fee, populate card."""
        sdk = self._build_sdk(account)
        async with sdk:
            logger.debug("Authenticating...")
            await sdk.authenticate(force=True)

            logger.debug("Fetching account info...")
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            card.balances = balances

            # Fetch activity summary (reward data)
            logger.debug("Fetching activity summary...")
            summary = await self._fetch_activity_summary(sdk, logger)
            if summary is not None:
                card.activity_summary = summary

            # Scrape ccview fee data
            if info.address:
                logger.debug("Scraping CCView fee data...")
                from datetime import date as _date
                today = _date.today().isoformat()
                try:
                    fee_result = await self.fee_scraper.fetch_actual_fee(info.address, today)
                    if fee_result is not None and fee_result.success:
                        card.ccview_validator_fee_total = fee_result.validator_fee_total
                        card.ccview_validator_tx_count = fee_result.validator_tx_count
                        card.ccview_avg_fee_per_swap = fee_result.avg_fee_per_swap
                except Exception as exc:
                    logger.warning("CCView scrape gagal: %s", exc)

    def _render_check_dashboard(self, cards: list[TelegramCardState]) -> None:
        """Render dashboard terminal 1x untuk mode cek akun (bypass isatty/enabled check)."""
        sorted_cards = sorted(cards, key=lambda c: c.display_index)
        if not sorted_cards:
            print("   (tidak ada data akun)", flush=True)
            return

        col_widths, table_lines = self.monitor._dashboard_table_lines(sorted_cards)
        header_row = table_lines[0]
        row_width = len(header_row) - 2
        border = "+" + ("-" * row_width) + "+"
        strong_border = "+" + ("=" * row_width) + "+"

        lines = [
            strong_border,
            *self.monitor._dashboard_summary_lines(sorted_cards, row_width=row_width),
            border,
            *table_lines,
            border,
        ]

        rendered = "\n".join(lines)
        sys.stdout.write(rendered + "\n")
        sys.stdout.flush()

    def _startup_mode_uses_free_swap(self) -> bool:
        return self.startup_mode in {"free_only", "free_then_swap"}

    def _daily_free_fee_status(self, account_name: str) -> DailyFreeFeeStatus:
        return self.runtime_state.get_daily_free_fee_status(account_name)

    def _available_daily_free_fee_status(self, account_name: str) -> DailyFreeFeeStatus | None:
        status = self._daily_free_fee_status(account_name)
        if status.window_open and status.remaining > 0:
            return status
        return None

    def _consume_daily_free_fee_swap(self, account_name: str) -> DailyFreeFeeStatus:
        return self.runtime_state.consume_daily_free_fee_swap(account_name)

    async def _sync_daily_free_fee_state_from_history(
        self,
        *,
        sdk: ExtendedCantexSDK,
        account_name: str,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        force_log: bool = False,
    ) -> DailyFreeFeeStatus:
        local_status = self._daily_free_fee_status(account_name)
        try:
            source_path, payload = await sdk.get_trading_history_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil trading history untuk free fee sync: %s", exc)
            await self.monitor.update_free_fee_status(
                monitor_card,
                used=local_status.used,
                limit=3,
                force=force_log,
            )
            if force_log:
                await self.monitor.log_event(
                    monitor_card,
                    f"ℹ️ Free fee sync fallback to local state ({local_status.used}/3)",
                )
            return local_status

        if payload is None:
            await self.monitor.update_free_fee_status(
                monitor_card,
                used=local_status.used,
                limit=3,
                force=force_log,
            )
            if force_log:
                await self.monitor.log_event(
                    monitor_card,
                    f"ℹ️ Free fee sync fallback to local state ({local_status.used}/3)",
                )
            return local_status

        history_today_count = self._count_today_trading_history_swaps(payload)
        if history_today_count is None:
            await self.monitor.update_free_fee_status(
                monitor_card,
                used=local_status.used,
                limit=3,
                force=force_log,
            )
            if force_log:
                await self.monitor.log_event(
                    monitor_card,
                    f"ℹ️ Free fee history unavailable, using local state ({local_status.used}/3)",
                )
            return local_status

        synced_status = self.runtime_state.sync_daily_free_fee_swaps(
            account_name,
            min(history_today_count, 3),
            exact=True,
        )
        await self.monitor.update_free_fee_status(
            monitor_card,
            used=synced_status.used,
            limit=3,
            force=force_log,
        )
        if force_log or synced_status.used != local_status.used:
            logger.info(
                "Free fee sync | source=%s | history_swap_hari_ini=%s | used=%s/3 | remaining=%s",
                source_path or "-",
                history_today_count,
                synced_status.used,
                synced_status.remaining,
            )
            logger.info(
                "Free fee startup sync: history=%s -> effective=%s/3",
                history_today_count,
                synced_status.used,
            )
            await self.monitor.log_event(
                monitor_card,
                f"🎁 Free fee sync from history: {synced_status.used}/3 used today",
                force=force_log,
            )
        return synced_status

    async def _wait_for_trading_history_round_progress(
        self,
        *,
        sdk: ExtendedCantexSDK,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        previous_completed_rounds: int,
        require_increment: bool = False,
        force_log: bool = False,
        max_history_polls: int = 3,
    ) -> int:
        target_minimum = max(min(previous_completed_rounds, prepared_run.rounds), 0)
        if require_increment:
            target_minimum = min(previous_completed_rounds + 1, prepared_run.rounds)
        if previous_completed_rounds >= prepared_run.rounds:
            target_minimum = prepared_run.rounds

        poll_count = 0
        while True:
            if self._weekly_stop_due_utc():
                logger.info(
                    "Weekly stop jatuh tempo saat menunggu trading history; keluar dari wait history"
                )
                await self.monitor.log_event(
                    monitor_card,
                    "🛑 Weekly stop due, leaving trading history wait",
                    force=True,
                )
                return max(min(previous_completed_rounds, prepared_run.rounds), 0)

            synced_completed_rounds = await self._sync_round_progress_from_trading_history(
                sdk=sdk,
                account=account,
                prepared_run=prepared_run,
                logger=logger,
                monitor_card=monitor_card,
                previous_completed_rounds=previous_completed_rounds,
                force_log=force_log,
            )
            if synced_completed_rounds is not None:
                # Trigger ccview scrape setiap kali progress naik dari sebelumnya.
                # Ini memastikan Gas fee di-update segera setelah round sync,
                # tanpa menunggu hop swap berikutnya.
                if synced_completed_rounds > previous_completed_rounds:
                    self._schedule_ccview_scrape_after_progress(
                        account_name=account.name,
                        completed_round=synced_completed_rounds,
                        monitor_card=monitor_card,
                        reason="round_sync",
                    )
                if not require_increment:
                    return synced_completed_rounds
                if (
                    synced_completed_rounds >= target_minimum
                    or synced_completed_rounds >= prepared_run.rounds
                ):
                    return synced_completed_rounds

            # Jika sync gagal (None) dan tidak butuh increment, return langsung
            if synced_completed_rounds is None and not require_increment:
                fallback = max(min(previous_completed_rounds, prepared_run.rounds), 0)
                logger.info(
                    "Trading history sync gagal/kosong, lanjut dengan progress=%s (tidak menunggu)",
                    fallback,
                )
                return fallback

            # Jika sudah melebihi max polls, return best known value
            poll_count += 1
            if poll_count >= max_history_polls:
                best = synced_completed_rounds if synced_completed_rounds is not None else previous_completed_rounds
                if require_increment:
                    # Swap sudah berhasil tapi history belum update — increment secara internal
                    best = min(previous_completed_rounds + 1, prepared_run.rounds)
                logger.info(
                    "Trading history polling limit tercapai (%s/%s), lanjut dengan progress=%s",
                    poll_count,
                    max_history_polls,
                    best,
                )
                return max(min(best, prepared_run.rounds), 0)

            wait_seconds = self._sample_network_fee_poll_seconds()
            displayed_rounds = synced_completed_rounds
            if displayed_rounds is None:
                displayed_rounds = max(min(previous_completed_rounds, prepared_run.rounds), 0)
            logger.info(
                "Menunggu trading history harian update | current=%s | target_min=%s | poll=%s/%s | tunggu=%.0fs",
                displayed_rounds,
                target_minimum,
                poll_count,
                max_history_polls,
                wait_seconds,
            )
            await self.monitor.update_status(
                monitor_card,
                phase="WAITING",
                next_wait_seconds=wait_seconds,
                clear_route=True,
            )
            await self.monitor.log_event(
                monitor_card,
                (
                    f"⏳ Waiting trading history swaps update "
                    f"({displayed_rounds}/{prepared_run.rounds})"
                ),
                force=force_log,
            )
            await self._sleep_or_stop(wait_seconds)

    async def _sync_round_progress_from_trading_history(
        self,
        *,
        sdk: ExtendedCantexSDK,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        previous_completed_rounds: int,
        force_log: bool = False,
    ) -> int | None:
        fallback_completed_rounds = min(max(int(previous_completed_rounds), 0), prepared_run.rounds)
        try:
            source_path, history_payload = await sdk.get_trading_history_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil trading history untuk round sync: %s", exc)
            await self.monitor.sync_round_progress(
                monitor_card,
                completed_rounds=fallback_completed_rounds,
                force=force_log,
            )
            return None

        if history_payload is None:
            logger.warning("Trading history belum tersedia untuk round sync")
            await self.monitor.sync_round_progress(
                monitor_card,
                completed_rounds=fallback_completed_rounds,
                force=force_log,
            )
            return None

        history_today_count = self._count_today_trading_history_swaps(history_payload)
        if history_today_count is None:
            logger.warning("Trading history harian tidak bisa diparse untuk round sync")
            await self.monitor.sync_round_progress(
                monitor_card,
                completed_rounds=fallback_completed_rounds,
                force=force_log,
            )
            return None

        cached_history_count: int | None = None
        history_today_trade_keys = self._today_trading_history_trade_keys(history_payload)
        if history_today_trade_keys is not None:
            cached_history_count = self.runtime_state.sync_daily_trading_history_update_ids(
                account.name,
                history_today_trade_keys,
            )
        effective_history_count = max(
            history_today_count,
            cached_history_count if cached_history_count is not None else 0,
        )
        await self.monitor.sync_daily_ok_tx_from_history(
            monitor_card,
            ok_tx_count=effective_history_count,
            force=force_log,
        )
        synced_completed_rounds = min(
            max(effective_history_count, fallback_completed_rounds, 0),
            prepared_run.rounds,
        )
        self.runtime_state.update_round_session_progress(
            account.name,
            strategy_name=prepared_run.strategy_name,
            requested_rounds=prepared_run.rounds,
            completed_rounds=synced_completed_rounds,
        )
        await self.monitor.sync_round_progress(
            monitor_card,
            completed_rounds=synced_completed_rounds,
            force=force_log,
        )
        if force_log or synced_completed_rounds != previous_completed_rounds:
            logger.info(
                "Round sync | source=%s | trading_swap_today=%s | cached_today=%s | progress=%s/%s",
                source_path or "-",
                history_today_count,
                cached_history_count if cached_history_count is not None else "-",
                synced_completed_rounds,
                prepared_run.rounds,
            )
            await self.monitor.log_event(
                monitor_card,
                f"🔁 Round sync from trading history: {synced_completed_rounds}/{prepared_run.rounds}",
                force=force_log,
            )
            # Trigger ccview scrape saat progress swap bertambah (background, non-blocking)
            if synced_completed_rounds > previous_completed_rounds:
                self._schedule_ccview_scrape_after_progress(
                    account_name=account.name,
                    completed_round=synced_completed_rounds,
                    monitor_card=monitor_card,
                    reason="progress",
                )
        return synced_completed_rounds

    async def _sleep_or_stop(self, seconds: float) -> None:
        if seconds <= 0:
            self._raise_if_stop_requested()
            await self._pause_if_requested()
            return
        sleep_task = asyncio.create_task(asyncio.sleep(seconds))
        stop_task = asyncio.create_task(self._stop_requested.wait())
        pause_task = asyncio.create_task(self._telegram_pause_requested.wait())
        try:
            done, pending = await asyncio.wait(
                {sleep_task, stop_task, pause_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if sleep_task in done:
                return
            await self._wait_until_started()
        finally:
            for task in (sleep_task, stop_task, pause_task):
                if not task.done():
                    task.cancel()

    def _raise_if_stop_requested(self) -> None:
        if self._stop_requested.is_set():
            raise StopRequested()

    async def _pause_if_requested(self) -> None:
        if self._telegram_pause_requested.is_set():
            await self._wait_until_started()

    async def _wait_until_started(self) -> None:
        while self._telegram_pause_requested.is_set():
            if self._stop_requested.is_set():
                raise StopRequested()
            await asyncio.sleep(1.0)

    def _persist_round_session_progress(
        self,
        *,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        result: AccountResult,
    ) -> None:
        self.runtime_state.update_round_session_progress(
            account.name,
            strategy_name=prepared_run.strategy_name,
            requested_rounds=prepared_run.rounds,
            completed_rounds=result.completed_rounds,
        )

    def _format_utc(self, dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _monitor_pair_key(self, pair_key: str) -> str:
        return pair_key

    def _next_utc_midnight(self, dt: datetime) -> datetime:
        normalized = dt.astimezone(timezone.utc)
        next_day = normalized.date() + timedelta(days=1)
        return datetime.combine(next_day, datetime.min.time(), tzinfo=timezone.utc)

    def _count_activity_payload_swaps_24h(self, payload: Any) -> int | None:
        if payload is None:
            return None
        raw_count = self._find_value(
            payload,
            {
                "count_24h",
                "swaps_24h",
                "swaps24h",
                "24h_swaps",
                "twenty_four_hour_swaps",
            },
        )
        count = self._parse_decimal_like(raw_count)
        if count is None:
            return None
        return max(int(count), 0)

    def _count_today_trading_history_swaps(self, payload: Any) -> int | None:
        identified_trade_keys = self._today_trading_history_trade_keys(payload)
        if identified_trade_keys is not None:
            return len(identified_trade_keys)

        items = self._extract_trading_history_items(payload)
        if not items:
            if self._payload_has_empty_collection(payload):
                return 0
            return None
        today_utc = datetime.now(timezone.utc).date()
        fallback_count = 0
        for item in items:
            timestamp = self._extract_item_timestamp(item)
            if timestamp is None or timestamp.date() != today_utc:
                continue
            if self._is_failed_history_item(item):
                continue
            trade_key = self._history_trade_key(item)
            if trade_key is None:
                fallback_count += 1
        return fallback_count

    def _today_trading_history_trade_keys(self, payload: Any) -> set[str] | None:
        items = self._extract_trading_history_items(payload)
        if not items:
            if self._payload_has_empty_collection(payload):
                return set()
            return None
        today_utc = datetime.now(timezone.utc).date()
        identified_trade_keys: set[str] = set()
        for item in items:
            timestamp = self._extract_item_timestamp(item)
            if timestamp is None or timestamp.date() != today_utc:
                continue
            if self._is_failed_history_item(item):
                continue
            trade_key = self._history_trade_key(item)
            if trade_key is not None:
                identified_trade_keys.add(trade_key)
        return identified_trade_keys if identified_trade_keys else None

    def _payload_has_empty_collection(self, payload: Any) -> bool:
        if payload in (None, ""):
            return False
        if isinstance(payload, (list, tuple)):
            return len(payload) == 0
        if isinstance(payload, dict):
            for key in (
                "data",
                "items",
                "results",
                "rows",
                "history",
                "trades",
                "transactions",
            ):
                value = payload.get(key)
                if isinstance(value, (list, tuple)) and len(value) == 0:
                    return True
        return False

    def _history_trade_key(self, item: dict[str, Any]) -> str | None:
        for key in (
            "update_id",
            "updateId",
            "updateID",
            "trade_id",
            "tradeId",
            "event_id",
            "eventId",
            "transaction_id",
            "transactionId",
            "id",
        ):
            raw_value = item.get(key)
            if raw_value not in {None, ""}:
                return f"{key}:{str(raw_value).strip()}"
        return None

    def _extract_trading_history_items(self, payload: Any) -> list[dict[str, Any]]:
        return self._extract_timestamped_history_items(payload)

    def _extract_funding_history_items(self, payload: Any) -> list[dict[str, Any]]:
        return self._extract_timestamped_history_items(payload)

    def _extract_timestamped_history_items(self, payload: Any) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []

        def visit(node: Any) -> None:
            if isinstance(node, list):
                if node and all(isinstance(entry, dict) for entry in node):
                    timestamped = [
                        entry
                        for entry in node
                        if self._extract_item_timestamp(entry) is not None
                    ]
                    if timestamped:
                        items.extend(timestamped)
                        return
                for child in node:
                    visit(child)
                return

            if isinstance(node, dict):
                for value in node.values():
                    visit(value)

        visit(payload)
        return items

    def _extract_distributed_reward_from_funding_history(
        self,
        payload: Any,
    ) -> tuple[str | None, str | None, str | None]:
        if payload is None:
            return None, None, None

        matched_item: dict[str, Any] | None = None
        matched_timestamp: datetime | None = None
        for item in self._extract_funding_history_items(payload):
            if not self._is_distributed_reward_funding_item(item):
                continue
            item_timestamp = self._extract_item_timestamp(item)
            if matched_item is None:
                matched_item = item
                matched_timestamp = item_timestamp
                continue
            if item_timestamp is not None and (
                matched_timestamp is None or item_timestamp > matched_timestamp
            ):
                matched_item = item
                matched_timestamp = item_timestamp

        if matched_item is None:
            return None, None, None

        distributed_amount = self._extract_funding_amount_text(matched_item)
        distributed_update_id = self._extract_history_identity(
            matched_item,
            keys=("update_id", "updateId", "updateID", "id", "transaction_id", "transactionId"),
        )
        distributed_timestamp = (
            matched_timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
            if matched_timestamp is not None
            else None
        )
        return distributed_amount, distributed_update_id, distributed_timestamp

    def _is_distributed_reward_funding_item(self, item: dict[str, Any]) -> bool:
        if self._is_failed_history_item(item):
            return False

        type_text = str(
            self._find_value(item, {"type", "transaction_type", "kind"}) or ""
        ).strip().lower()
        if type_text and "deposit" not in type_text:
            return False

        status_text = str(
            self._find_value(item, {"status", "state", "result"}) or ""
        ).strip().lower()
        if status_text and not any(
            token in status_text for token in ("complete", "completed", "confirm", "success", "paid")
        ):
            return False

        counterparty_text = str(
            self._find_value(item, {"counterparty", "sender", "from", "source"}) or ""
        ).strip().lower()
        message_text = str(
            self._find_value(item, {"message", "memo", "description", "note"}) or ""
        ).strip().lower()
        return (
            "cantex-rewards" in counterparty_text
            or "cantex app rebate" in message_text
            or "rebate" in counterparty_text
            or "rebate" in message_text
            or "reward" in counterparty_text
            or "reward" in message_text
        )

    def _extract_external_funding_total_from_funding_history(self, payload: Any) -> str | None:
        if payload is None:
            return None

        total = Decimal("0")
        found = False
        for item in self._extract_funding_history_items(payload):
            if not self._is_external_cc_funding_item(item):
                continue
            amount = self._extract_funding_amount_decimal(item)
            if amount is None:
                continue
            total += abs(amount)
            found = True

        if not found:
            return None
        return f"{self._compact_decimal_text(total, places=4)} CC"

    def _is_external_cc_funding_item(self, item: dict[str, Any]) -> bool:
        if self._is_failed_history_item(item):
            return False
        if self._is_distributed_reward_funding_item(item):
            return False

        type_text = str(
            self._find_value(item, {"type", "transaction_type", "kind"}) or ""
        ).strip().lower()
        if type_text and "deposit" not in type_text:
            return False

        status_text = str(
            self._find_value(item, {"status", "state", "result"}) or ""
        ).strip().lower()
        if status_text and not any(
            token in status_text for token in ("complete", "completed", "confirm", "success", "paid")
        ):
            return False

        symbol = self._extract_funding_symbol(item)
        if symbol is not None and symbol != CC_SYMBOL:
            return False

        amount = self._extract_funding_amount_decimal(item)
        return amount is not None and amount > Decimal("0")

    def _extract_funding_symbol(self, item: dict[str, Any]) -> str | None:
        raw_symbol = self._find_value(
            item,
            {
                "instrument_symbol",
                "instrumentSymbol",
                "token_symbol",
                "tokenSymbol",
                "symbol",
                "token",
                "instrument",
                "token_instrument_id",
                "tokenInstrumentId",
            },
        )
        if isinstance(raw_symbol, dict):
            raw_symbol = self._find_value(
                raw_symbol,
                {
                    "instrument_symbol",
                    "instrumentSymbol",
                    "token_symbol",
                    "tokenSymbol",
                    "symbol",
                    "instrument_id",
                    "instrumentId",
                },
        )
        symbol = self._history_symbol(raw_symbol)
        normalized_symbol = str(symbol or "").strip().lower()
        if normalized_symbol in {"cc", "amulet", "canton coin"} or "canton coin" in normalized_symbol:
            return CC_SYMBOL
        return symbol

    def _extract_funding_amount_decimal(self, item: dict[str, Any]) -> Decimal | None:
        raw_amount = self._find_value(
            item,
            {"amount", "display_amount", "cc_amount", "amount_cc", "quantity"},
        )
        return self._parse_decimal_like(raw_amount)

    def _extract_funding_amount_text(self, item: dict[str, Any]) -> str | None:
        raw_amount = self._find_value(
            item,
            {"amount", "display_amount", "cc_amount", "amount_cc", "quantity"},
        )
        if raw_amount in {None, ""}:
            return None
        parsed_amount = self._parse_decimal_like(raw_amount)
        if parsed_amount is None:
            return self._stringify_optional(raw_amount)
        return f"{self._compact_decimal_text(abs(parsed_amount), places=4)} CC"

    def _extract_history_identity(
        self,
        item: dict[str, Any],
        *,
        keys: tuple[str, ...],
    ) -> str | None:
        for key in keys:
            raw_value = self._find_value(item, {key})
            if raw_value not in {None, ""}:
                return str(raw_value).strip()
        return None

    def _extract_item_timestamp(self, item: dict[str, Any]) -> datetime | None:
        for key in (
            "createdAt",
            "created_at",
            "timestamp",
            "timestamp_utc",
            "time",
            "updatedAt",
            "updated_at",
            "executedAt",
            "executed_at",
        ):
            raw_value = item.get(key)
            parsed = self._parse_datetime_like(raw_value)
            if parsed is not None:
                return parsed
        return None

    def _parse_datetime_like(self, value: Any) -> datetime | None:
        if value in {None, ""}:
            return None
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000.0
            try:
                return datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except (ValueError, OSError):
                return None

        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _parse_decimal_like(self, value: Any) -> Decimal | None:
        if value in {None, ""}:
            return None
        cleaned = re.sub(r"[^0-9eE+.\-]+", "", str(value))
        if cleaned in {"", "-", ".", "-."}:
            return None
        try:
            return Decimal(cleaned)
        except Exception:
            return None

    def _tx_output_symbol_matches(self, tx_result: dict[str, Any], expected_symbol: str) -> bool:
        actual_symbol = str(tx_result.get("output_instrument", "")).strip()
        if not actual_symbol:
            return False
        return actual_symbol.lower() == expected_symbol.strip().lower()

    def _matching_tx_output_amount(
        self,
        *,
        tx_result: dict[str, Any],
        expected_symbol: str,
    ) -> Decimal | None:
        amount = self._parse_decimal_like(tx_result.get("output_amount"))
        if amount is None:
            return None
        if not self._tx_output_symbol_matches(tx_result, expected_symbol):
            return None
        return amount

    def _resolve_actual_output_amount(
        self,
        *,
        hop: RouteHop,
        tx_result: dict[str, Any],
        balances_before: dict[str, Decimal],
        balances_after: dict[str, Decimal],
    ) -> tuple[Decimal, str | None]:
        matched_event_output = self._matching_tx_output_amount(
            tx_result=tx_result,
            expected_symbol=hop.buy_symbol,
        )
        if matched_event_output is not None:
            return matched_event_output, None

        raw_event_output = self._parse_decimal_like(tx_result.get("output_amount"))
        balance_delta = balances_after.get(hop.buy_symbol, Decimal("0")) - balances_before.get(
            hop.buy_symbol,
            Decimal("0"),
        )
        if balance_delta > dust_for_symbol(hop.buy_symbol):
            output_symbol = str(tx_result.get("output_instrument", "")).strip() or "?"
            warning = None
            if raw_event_output is not None:
                warning = (
                    f"Tx output mismatch {hop.sell_symbol}->{hop.buy_symbol}: "
                    f"event={output_symbol} {raw_event_output}, balance={balance_delta} {hop.buy_symbol}"
                )
            return balance_delta, warning

        return raw_event_output or hop.returned_amount, None

    def _is_failed_history_item(self, item: dict[str, Any]) -> bool:
        status_parts = [
            str(item.get(key, "")).strip().lower()
            for key in ("status", "state", "result")
            if item.get(key) not in {None, ""}
        ]
        if not status_parts:
            return False
        failed_tokens = {"failed", "error", "rejected", "cancelled", "canceled"}
        return any(token in failed_tokens for token in status_parts)

    async def _prepare_affordable_route(
        self,
        *,
        router: RouteOptimizer,
        balances: dict[str, Decimal],
        sell_symbol: str,
        buy_symbol: str,
        proposed_amount: Decimal,
        round_number: int,
        cc_reserve: Decimal,
        strict_amount: bool = False,
    ) -> tuple[RoutePlan, PlanIssue | None]:
        effective_cc_reserve = cc_reserve
        route = await router.choose_best_route(sell_symbol, buy_symbol, proposed_amount)
        issue = self._check_route_affordability(
            balances=balances,
            route=route,
            cc_reserve=effective_cc_reserve,
            round_number=round_number,
        )
        if issue is None:
            return route, None

        if sell_symbol == CC_SYMBOL and not strict_amount:
            fee_buffer = route.total_network_fee_by_symbol.get(CC_SYMBOL, Decimal("0"))
            adjusted_amount = max(
                Decimal("0"),
                self._spendable_amount(
                    CC_SYMBOL,
                    balances.get(CC_SYMBOL, Decimal("0")),
                    effective_cc_reserve,
                )
                - fee_buffer,
            )
            if adjusted_amount > dust_for_symbol(CC_SYMBOL) and adjusted_amount < proposed_amount:
                route = await router.choose_best_route(sell_symbol, buy_symbol, adjusted_amount)
                issue = self._check_route_affordability(
                    balances=balances,
                    route=route,
                    cc_reserve=effective_cc_reserve,
                    round_number=round_number,
                )
        return route, issue

    def _check_route_affordability(
        self,
        *,
        balances: dict[str, Decimal],
        route: RoutePlan,
        cc_reserve: Decimal,
        round_number: int,
    ) -> PlanIssue | None:
        simulated = deepcopy(balances)
        for hop in route.hops:
            current_sell = simulated.get(hop.sell_symbol, Decimal("0"))
            spendable_sell = self._source_spendable_amount(
                sell_symbol=hop.sell_symbol,
                buy_symbol=hop.buy_symbol,
                balance=current_sell,
                cc_reserve=cc_reserve,
            )
            combined_spend = hop.sell_amount
            if hop.sell_symbol == hop.network_fee_symbol:
                combined_spend += hop.network_fee_amount

            if spendable_sell < combined_spend:
                if (
                    hop.sell_symbol != CC_SYMBOL
                    and spendable_sell + dust_for_symbol(hop.sell_symbol) >= hop.sell_amount
                ):
                    pass
                else:
                    return PlanIssue(
                        round_number=round_number,
                        sell_symbol=hop.sell_symbol,
                        requested_amount=combined_spend,
                        available_amount=spendable_sell,
                        reason="balance sell token tidak cukup setelah fee",
                    )

            if hop.sell_symbol != hop.network_fee_symbol:
                network_fee_balance = simulated.get(hop.network_fee_symbol, Decimal("0"))
                spendable_fee = self._fee_spendable_amount(
                    symbol=hop.network_fee_symbol,
                    balance=network_fee_balance,
                )
                if spendable_fee < hop.network_fee_amount:
                    return PlanIssue(
                        round_number=round_number,
                        sell_symbol=hop.network_fee_symbol,
                        requested_amount=hop.network_fee_amount,
                        available_amount=spendable_fee,
                        reason="balance fee tidak cukup",
                    )

            simulated[hop.sell_symbol] = current_sell - hop.sell_amount
            simulated[hop.network_fee_symbol] = (
                simulated.get(hop.network_fee_symbol, Decimal("0")) - hop.network_fee_amount
            )
            simulated[hop.buy_symbol] = simulated.get(hop.buy_symbol, Decimal("0")) + hop.returned_amount
        return None

    def _apply_route_to_balances(self, balances: dict[str, Decimal], route: RoutePlan) -> None:
        for hop in route.hops:
            balances[hop.sell_symbol] = balances.get(hop.sell_symbol, Decimal("0")) - hop.sell_amount
            balances[hop.network_fee_symbol] = (
                balances.get(hop.network_fee_symbol, Decimal("0")) - hop.network_fee_amount
            )
            balances[hop.buy_symbol] = balances.get(hop.buy_symbol, Decimal("0")) + hop.returned_amount

    async def _fetch_activity_summary(
        self,
        sdk: ExtendedCantexSDK,
        logger: AccountLoggerAdapter,
    ) -> ActivitySummary | None:
        if not self.config.runtime.activity_enabled:
            return None

        activity_source_path: str | None = None
        activity_payload: Any | None = None
        history_source_path: str | None = None
        history_payload: Any | None = None
        funding_source_path: str | None = None
        funding_payload: Any | None = None

        try:
            activity_source_path, activity_payload = await sdk.get_activity_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil activity: %s", exc)
        try:
            history_source_path, history_payload = await sdk.get_trading_history_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil trading history: %s", exc)
        try:
            funding_source_path, funding_payload = await sdk.get_funding_history_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil funding history: %s", exc)

        if activity_payload is None and history_payload is None and funding_payload is None:
            logger.info("Activity user tidak tersedia dari endpoint yang dicoba")
            return None

        summary = self._normalize_activity_payload(
            source_path=activity_source_path,
            payload=activity_payload,
            history_source_path=history_source_path,
            history_payload=history_payload,
            funding_source_path=funding_source_path,
            funding_payload=funding_payload,
        )
        self._log_activity_summary(logger, summary)
        return summary

    def _normalize_activity_payload(
        self,
        *,
        source_path: str | None,
        payload: Any | None,
        history_source_path: str | None,
        history_payload: Any | None,
        funding_source_path: str | None,
        funding_payload: Any | None,
    ) -> ActivitySummary:
        if (
            isinstance(payload, dict)
            and isinstance(payload.get("stats"), dict)
            and isinstance(payload.get("rebates"), dict)
        ):
            return self._normalize_reward_activity_payload(
                source_path=source_path,
                payload=payload,
                history_source_path=history_source_path,
                history_payload=history_payload,
                funding_source_path=funding_source_path,
                funding_payload=funding_payload,
            )

        effective_payload = payload if payload is not None else history_payload
        if effective_payload is None:
            effective_payload = funding_payload
        if effective_payload is None:
            return ActivitySummary()

        swaps_7d = self._find_value(effective_payload, {"swaps7d", "swaps_7d", "seven_day_swaps", "sevenDaySwaps"})
        volume_7d = self._find_value(effective_payload, {"volume7d", "volume_7d", "seven_day_volume", "sevenDayVolume"})
        total_swaps = self._find_value(effective_payload, {"total_swaps", "totalSwaps", "all_time_swaps"})
        total_volume = self._find_value(effective_payload, {"total_volume", "totalVolume", "all_time_volume"})
        reward_total = self._find_value(effective_payload, {"reward", "rewards", "total_reward", "totalReward"})
        tx_count = self._find_value(
            effective_payload,
            {"tx", "tx_count", "transactions", "transactionCount", "total_tx"},
        )
        rank = self._find_value(effective_payload, {"rank", "leaderboard_rank", "leaderboardRank"})
        volume_usd = self._find_value(effective_payload, {"volume_usd", "volumeUsd", "usd_volume", "usdVolume"})
        rebates: dict[str, str] = {}

        rebate_payload = self._find_container(effective_payload, "rebate")
        if isinstance(rebate_payload, dict):
            for label, value in rebate_payload.items():
                rebates[str(label)] = self._stringify_value(value)

        recent_items = self._extract_recent_items(history_payload if history_payload is not None else effective_payload)
        distributed_reward, distributed_update_id, distributed_timestamp = (
            self._extract_distributed_reward_from_funding_history(funding_payload)
        )
        funding_total = self._extract_external_funding_total_from_funding_history(funding_payload)
        raw_preview = json.dumps(effective_payload, default=str)[:400]
        return ActivitySummary(
            source_path=source_path,
            history_source_path=history_source_path,
            funding_source_path=funding_source_path,
            swaps_7d=self._stringify_optional(swaps_7d),
            volume_7d=self._stringify_optional(volume_7d),
            total_swaps=self._stringify_optional(total_swaps),
            total_volume=self._stringify_optional(total_volume),
            reward_total=self._stringify_optional(reward_total),
            tx_count=self._stringify_optional(tx_count),
            rank=self._stringify_optional(rank),
            volume_usd=self._stringify_optional(volume_usd),
            distributed_reward=distributed_reward,
            distributed_update_id=distributed_update_id,
            distributed_timestamp=distributed_timestamp,
            funding_total=funding_total,
            rebates=rebates,
            recent_items=tuple(recent_items[: self.config.runtime.activity_items_limit]),
            raw_preview=raw_preview,
        )

    def _normalize_reward_activity_payload(
        self,
        *,
        source_path: str | None,
        payload: dict[str, Any],
        history_source_path: str | None,
        history_payload: Any | None,
        funding_source_path: str | None,
        funding_payload: Any | None,
    ) -> ActivitySummary:
        stats = payload.get("stats") or {}
        rebates_payload = payload.get("rebates") or {}
        rebates: dict[str, str] = {}
        for label, entry in rebates_payload.items():
            if isinstance(entry, dict):
                amount = entry.get("cc_amount")
                status = str(entry.get("status", "")).strip()
                parts: list[str] = []
                if amount not in {None, ""}:
                    parts.append(f"{amount} CC")
                if status:
                    parts.append(status)
                rebates[str(label)] = " | ".join(parts) if parts else self._stringify_value(entry)
            else:
                rebates[str(label)] = self._stringify_value(entry)

        recent_items = self._extract_recent_trading_items(history_payload)
        history_preview = self._extract_trading_history_items(history_payload)[:2] if history_payload is not None else []
        funding_preview = self._extract_funding_history_items(funding_payload)[:2] if funding_payload is not None else []
        raw_preview = json.dumps(
            {
                "reward_activity": payload,
                "history_trading": history_preview,
                "history_funding": funding_preview,
            },
            default=str,
        )[:400]
        this_week_rebate = rebates_payload.get("this_week") if isinstance(rebates_payload, dict) else None
        distributed_reward, distributed_update_id, distributed_timestamp = (
            self._extract_distributed_reward_from_funding_history(funding_payload)
        )
        funding_total = self._extract_external_funding_total_from_funding_history(funding_payload)

        return ActivitySummary(
            source_path=source_path,
            history_source_path=history_source_path,
            funding_source_path=funding_source_path,
            swaps_24h=self._stringify_optional(stats.get("count_24h")),
            volume_24h=self._stringify_optional(stats.get("cc_volume_24h")),
            volume_24h_usd=self._stringify_optional(
                stats.get("usd_volume_24h")
                or stats.get("volume_usd_24h")
                or stats.get("cc_volume_24h_usd")
            ),
            swaps_7d=self._stringify_optional(stats.get("count_7d")),
            volume_7d=self._stringify_optional(stats.get("cc_volume_7d")),
            swaps_30d=self._stringify_optional(stats.get("count_30d")),
            volume_30d=self._stringify_optional(stats.get("cc_volume_30d")),
            total_swaps=self._stringify_optional(stats.get("count_alltime")),
            total_volume=self._stringify_optional(stats.get("cc_volume_alltime")),
            reward_total=self._extract_rebate_cc_amount(this_week_rebate),
            tx_count=self._stringify_optional(stats.get("count_alltime")),
            distributed_reward=distributed_reward,
            distributed_update_id=distributed_update_id,
            distributed_timestamp=distributed_timestamp,
            funding_total=funding_total,
            rebates=rebates,
            recent_items=tuple(recent_items[: self.config.runtime.activity_items_limit]),
            raw_preview=raw_preview,
        )

    def _extract_recent_items(self, payload: Any) -> list[str]:
        trading_items = self._extract_recent_trading_items(payload)
        if trading_items:
            return trading_items

        items: list[str] = []
        if isinstance(payload, list):
            iterable = payload
        elif isinstance(payload, dict):
            iterable = []
            for value in payload.values():
                if isinstance(value, list):
                    iterable.extend(value[: self.config.runtime.activity_items_limit])
        else:
            iterable = []

        for item in iterable[: self.config.runtime.activity_items_limit]:
            if isinstance(item, dict):
                parts = []
                for key in (
                    "type",
                    "status",
                    "instrument",
                    "instrumentSymbol",
                    "amount",
                    "createdAt",
                    "timestamp",
                    "timestamp_utc",
                ):
                    if key in item:
                        parts.append(f"{key}={item[key]}")
                if parts:
                    items.append(", ".join(parts))
        return items

    def _extract_recent_trading_items(self, payload: Any) -> list[str]:
        history_items = self._extract_trading_history_items(payload)
        formatted: list[str] = []
        for item in history_items[: self.config.runtime.activity_items_limit]:
            rendered = self._format_trading_history_item(item)
            if rendered:
                formatted.append(rendered)
        return formatted

    def _format_trading_history_item(self, item: dict[str, Any]) -> str | None:
        sell_symbol = self._history_symbol(item.get("token_input_instrument_id"))
        buy_symbol = self._history_symbol(item.get("token_output_instrument_id"))
        amount_input = self._compact_decimal_text(item.get("amount_input"))
        amount_output = self._compact_decimal_text(item.get("amount_output"))
        timestamp = self._extract_item_timestamp(item)
        timestamp_text = (
            timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
            if timestamp is not None
            else self._stringify_optional(item.get("timestamp_utc"))
        )
        if sell_symbol is None or buy_symbol is None:
            return None

        parts = [f"{sell_symbol}->{buy_symbol}"]
        if amount_input is not None and amount_output is not None:
            parts.append(f"in {amount_input}")
            parts.append(f"out {amount_output}")
        price = str(item.get("trade_prices_display", "")).strip()
        if price:
            parts.append(f"price {price}")
        if timestamp_text:
            parts.insert(0, timestamp_text)
        return " | ".join(parts)

    def _history_symbol(self, value: Any) -> str | None:
        if value in {None, ""}:
            return None
        symbol = str(value).strip()
        if symbol == "Amulet":
            return CC_SYMBOL
        return symbol

    def _compact_decimal_text(self, value: Any, *, places: int = 4) -> str | None:
        if value in {None, ""}:
            return None
        try:
            decimal_value = Decimal(str(value))
        except Exception:
            return str(value)
        if decimal_value == 0:
            return "0"
        quantizer = Decimal("1").scaleb(-places)
        rounded = decimal_value.quantize(quantizer)
        rendered = format(rounded, "f")
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
        return rendered

    def _extract_rebate_cc_amount(self, rebate_entry: Any) -> str | None:
        if not isinstance(rebate_entry, dict):
            return None
        amount = rebate_entry.get("cc_amount")
        if amount in {None, ""}:
            return None
        return f"{amount} CC"

    def _find_value(self, payload: Any, candidates: set[str]) -> Any | None:
        normalized_candidates = {candidate.lower() for candidate in candidates}
        if isinstance(payload, dict):
            for key, value in payload.items():
                normalized_key = str(key).lower()
                if normalized_key in normalized_candidates:
                    return value
                nested = self._find_value(value, candidates)
                if nested is not None:
                    return nested
        elif isinstance(payload, list):
            for item in payload:
                nested = self._find_value(item, candidates)
                if nested is not None:
                    return nested
        return None

    def _find_container(self, payload: Any, substring: str) -> Any | None:
        if isinstance(payload, dict):
            for key, value in payload.items():
                if substring.lower() in str(key).lower():
                    return value
                nested = self._find_container(value, substring)
                if nested is not None:
                    return nested
        elif isinstance(payload, list):
            for item in payload:
                nested = self._find_container(item, substring)
                if nested is not None:
                    return nested
        return None

    def _log_activity_summary(
        self,
        logger: AccountLoggerAdapter,
        summary: ActivitySummary,
    ) -> None:
        logger.info(
            (
                "Activity | source=%s | 24h swaps=%s | 24h volume=%s | "
                "7d swaps=%s | 7d volume=%s | 30d swaps=%s | 30d volume=%s | "
                "all-time swaps=%s | all-time volume=%s"
            ),
            summary.source_path or "-",
            summary.swaps_24h or "-",
            summary.volume_24h or "-",
            summary.swaps_7d or "-",
            summary.volume_7d or "-",
            summary.swaps_30d or "-",
            summary.volume_30d or "-",
            summary.total_swaps or "-",
            summary.total_volume or "-",
        )
        if summary.history_source_path:
            logger.info("Trading history | source=%s", summary.history_source_path)
        if summary.funding_source_path:
            logger.info("Funding history | source=%s", summary.funding_source_path)
        if summary.rebates:
            logger.info("Activity rebates | %s", self._format_text_map(summary.rebates))
        if summary.distributed_reward is not None:
            logger.info(
                "Distributed reward | amount=%s | update_id=%s | timestamp=%s",
                summary.distributed_reward,
                summary.distributed_update_id or "-",
                summary.distributed_timestamp or "-",
            )
        if summary.funding_total is not None:
            logger.info("External funding total | amount=%s", summary.funding_total)
        for item in summary.recent_items:
            logger.info("Trading recent | %s", item)

    def _log_balances(self, logger: AccountLoggerAdapter, info: AccountInfo, title: str) -> None:
        logger.info("%s | %s", title, self._format_amount_map(self._balances_by_symbol(info)))

    def _balances_by_symbol(self, info: AccountInfo) -> dict[str, Decimal]:
        balances = {symbol: Decimal("0") for symbol in TRACKED_SYMBOLS}
        for token in info.tokens:
            if token.instrument_symbol in balances:
                balances[token.instrument_symbol] = token.unlocked_amount
        return balances

    def _spendable_amount(
        self,
        symbol: str,
        balance: Decimal,
        cc_reserve: Decimal,
    ) -> Decimal:
        if symbol == CC_SYMBOL:
            return max(Decimal("0"), balance - cc_reserve)
        return max(Decimal("0"), balance)

    def _effective_cc_reserve(
        self,
        account: AccountConfig,
        reserve_override: Decimal | None = None,
    ) -> Decimal:
        if reserve_override is None:
            return account.reserve_fee
        return max(account.reserve_fee, reserve_override)

    def _strategy_4_reserve_fee(self, account: AccountConfig) -> Decimal:
        return account.reserve_fee

    def _strategy_4_reserve_kritis(self, account: AccountConfig) -> Decimal:
        if account.reserve_kritis is None:
            raise RuntimeError("Strategi 4 membutuhkan reserve_kritis")
        return account.reserve_kritis

    def _source_spendable_amount(
        self,
        *,
        sell_symbol: str,
        buy_symbol: str,
        balance: Decimal,
        cc_reserve: Decimal,
    ) -> Decimal:
        if sell_symbol == CC_SYMBOL and buy_symbol != CC_SYMBOL:
            return self._spendable_amount(sell_symbol, balance, cc_reserve)
        return max(Decimal("0"), balance)

    def _fee_spendable_amount(
        self,
        *,
        symbol: str,
        balance: Decimal,
    ) -> Decimal:
        return max(Decimal("0"), balance)

    def _format_amount_map(self, values: dict[str, Decimal]) -> str:
        if not values:
            return "-"
        return ", ".join(f"{symbol}={amount}" for symbol, amount in sorted(values.items()))

    def _format_text_map(self, values: dict[str, str]) -> str:
        if not values:
            return "-"
        return ", ".join(f"{key}={value}" for key, value in values.items())

    def _stringify_optional(self, value: Any) -> str | None:
        if value is None:
            return None
        return self._stringify_value(value)

    def _stringify_value(self, value: Any) -> str:
        if isinstance(value, dict):
            return json.dumps(value, default=str)
        if isinstance(value, list):
            return json.dumps(value[:3], default=str)
        return str(value)

    def _message_for_stop_reason(self, stop_reason: str) -> str:
        mapping = {
            "USER_ABORT_LOW_BALANCE_PROMPT": "Dihentikan user karena balance tidak cukup",
            "MANUAL_STOP": "Dihentikan user secara manual",
            "LOW_BALANCE_MODE_I": "Eksekusi berhenti karena balance kurang dan mode 'i' aktif",
            "INSUFFICIENT_BALANCE": "saldo kurang",
            "RECOVERY_NOT_ENOUGH": "Round di-skip karena recovery belum menghasilkan balance yang cukup",
            "ROUND_AFFORDABILITY_CHECK_FAILED": "Eksekusi berhenti karena route tidak lagi affordable",
            "SWAP_HOP_FAILED": "Eksekusi berhenti karena transaksi swap gagal",
            "SWAP_HOP_FAILED_SKIPPED": "Round di-skip karena swap gagal setelah retry limit",
            "SWAP_RETRY_EXHAUSTED": "Round di-skip karena swap gagal setelah retry limit",
            "MIN_TICKET_SIZE": "Round di-skip karena nominal di bawah minimum ticket size",
            "USER_CONFIG_MIN_NOT_MET": "Round di-skip karena nominal belum memenuhi amount minimum",
            "ROUND_STOPPED": "Eksekusi berhenti di tengah sesi",
            "WEEKLY_STOP": "Weekly stop Senin UTC tercapai; bot berhenti tanpa refill",
            "WEEKLY_REFILL_COMPLETE": "Weekly refill Senin UTC selesai; semua akun berhenti",
            "WEEKLY_REFILL_INCOMPLETE": "Weekly refill Senin UTC berhenti dengan sisa token non-CC",
        }
        return mapping.get(stop_reason, f"Eksekusi berhenti: {stop_reason}")


def configure_logging(
    level: str,
    *,
    use_utc: bool = False,
    terminal_dashboard_enabled: bool = False,
) -> None:
    if use_utc:
        logging.Formatter.converter = time.gmtime
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not (terminal_dashboard_enabled and sys.stdout.isatty()):
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
        )
        root_logger.addHandler(handler)
    else:
        root_logger.addHandler(logging.NullHandler())
    sdk_log_level = logging.DEBUG if level.upper() == "DEBUG" else logging.WARNING
    logging.getLogger("cantex_sdk").setLevel(sdk_log_level)
    logging.getLogger("cantex_sdk._sdk").setLevel(sdk_log_level)


def summarize_results(results: list[AccountResult]) -> str:
    lines = ["Ringkasan hasil:"]
    for result in results:
        status = "OK" if result.ok else "FAIL"
        lines.append(
            (
                f"- {result.account_name} | {status} | strategi={result.strategy_label} | "
                f"putaran={result.completed_rounds}/{result.requested_rounds} | "
                f"skipped_rounds={result.skipped_rounds} | "
                f"swap_tx={result.swap_transactions} | "
                f"estimasi_network_fee={_format_summary_map(result.estimated_network_fee_by_symbol)} | "
                f"network_fee_terpakai={_format_summary_map(result.used_network_fee_by_symbol)} | "
                f"swap_fee_terpakai={_format_summary_map(result.used_swap_fee_by_symbol)} | "
                f"balance={_format_summary_map(result.final_balances)} | "
                f"stop_reason={result.stop_reason or '-'}"
            )
        )
        if result.error:
            lines.append(f"  error={result.error}")
    return "\n".join(lines)


def _format_summary_map(values: dict[str, Decimal]) -> str:
    if not values:
        return "-"
    return ", ".join(f"{symbol}={amount}" for symbol, amount in sorted(values.items()))
