"""CCView.io Fee Scraper — menggunakan API internal ccview.io.

Mengambil data fee harian, balance, reward, dan estimasi dari counterparties API.

API Endpoints:
    GET https://ccview.io/api/v1/internal/api/v1/parties/counterparties
        ?party_id={party_id}&limit=50&offset=0&start={YYYY-MM-DD}&end={YYYY-MM-DD}
    GET https://ccview.io/api/v1/internal/api/v1/parties/{party_id}
        (untuk balance)

Usage:
    python scraper.py                    # Scrape hari ini
    python scraper.py 2026-04-29         # Scrape tanggal tertentu
    python scraper.py 2026-04-28 2026-04-29  # Scrape range tanggal
    python scraper.py daemon             # Mode daemon (24 jam)
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import httpx
from dotenv import load_dotenv

# Load .env from script directory
SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR / ".env")

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_SCRAPER", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_SCRAPER", "")
PARTYID_FILE = SCRIPT_DIR / "partyid.txt"
COUNTERPARTIES_URL = "https://ccview.io/api/v1/internal/api/v1/parties/counterparties"
PARTY_INFO_URL = "https://ccview.io/api/v1/internal/api/v1/parties"
MAX_RETRIES = 3
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "1.5"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))

# Reward constants
REWARD_FULL = Decimal("14.345")  # Reward jika swap volume >= 50 tx
REWARD_PER_SWAP = Decimal("0.28")  # Reward per swap jika < 50 tx
REWARD_THRESHOLD_TX = 50  # Minimum tx untuk reward penuh

# Validator identifier patterns
VALIDATOR_PATTERNS = ("cantex-validator", "Cantex-validator")


@dataclass
class CounterpartyData:
    counterparty_id: str
    display_name: str
    total_transfers_count: int
    total_transfers_volume: Decimal
    transfers_in_count: int
    transfers_out_count: int
    transfers_in_volume: Decimal
    transfers_out_volume: Decimal
    last_transfer_time: str


@dataclass
class FeeReport:
    party_id: str
    party_short: str
    date_start: str
    date_end: str
    # Fee data
    validator_fee_total: Decimal
    validator_tx_count: int
    avg_fee_per_swap: Decimal
    swap_volume: Decimal
    swap_tx_count: int
    total_tx: int
    # Balance
    balance: Decimal
    balance_unlocked: Decimal
    balance_locked: Decimal
    # Reward
    reward: Decimal
    reward_net: Decimal  # reward - validator fee
    # Estimasi
    days_remaining: int  # balance / validator fee per hari
    # Raw
    counterparties: list[CounterpartyData]
    error: str | None = None


def load_party_ids(path: Path) -> list[str]:
    """Load party IDs from file, one per line. Skip empty lines and comments."""
    if not path.exists():
        print(f"ERROR: File {path} tidak ditemukan")
        sys.exit(1)
    ids = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            ids.append(stripped)
    if not ids:
        print(f"ERROR: File {path} kosong")
        sys.exit(1)
    return ids


def shorten_party_id(party_id: str, max_len: int = 20) -> str:
    """Shorten party ID for display."""
    if len(party_id) <= max_len:
        return party_id
    parts = party_id.split("::")
    if len(parts) == 2:
        prefix = parts[0]
        suffix = parts[1]
        return f"{prefix}::{suffix[:6]}...{suffix[-4:]}"
    return f"{party_id[:max_len]}..."


def calculate_reward(swap_tx_count: int) -> Decimal:
    """Calculate reward based on swap transaction count.

    >= 50 tx: full reward (14.345 CC)
    < 50 tx: swap_count * 0.28 CC
    """
    if swap_tx_count >= REWARD_THRESHOLD_TX:
        return REWARD_FULL
    return REWARD_PER_SWAP * swap_tx_count


def calculate_days_remaining(balance: Decimal, daily_fee: Decimal) -> int:
    """Calculate how many days the account can sustain with current balance.

    days = floor(balance / daily_fee)
    """
    if daily_fee <= 0:
        return 999  # No fee = infinite
    return int(balance / daily_fee)


async def init_session(client: httpx.AsyncClient) -> bool:
    """Initialize session by visiting ccview.io homepage + session endpoint."""
    try:
        print("  Initializing session...")
        resp = await client.get("https://ccview.io/", timeout=REQUEST_TIMEOUT)
        resp = await client.get("https://ccview.io/api/v1/session", timeout=REQUEST_TIMEOUT)
        print(f"  Session: HTTP {resp.status_code}, cookies: {len(client.cookies)}")
        return resp.status_code == 200
    except Exception as exc:
        print(f"  Session init failed: {exc}")
        return False


async def fetch_counterparties(
    client: httpx.AsyncClient,
    party_id: str,
    start_date: str,
    end_date: str,
) -> dict | None:
    """Fetch counterparties data from ccview.io API."""
    params = {
        "party_id": party_id,
        "limit": "50",
        "offset": "0",
        "start": start_date,
        "end": end_date,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.get(COUNTERPARTIES_URL, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response.json()
            if response.status_code == 404:
                return None
            print(f"  [attempt {attempt}/{MAX_RETRIES}] counterparties HTTP {response.status_code}: {response.text[:200]}")
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
            print(f"  [attempt {attempt}/{MAX_RETRIES}] counterparties error: {exc}")
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)
    return None


async def fetch_party_info(
    client: httpx.AsyncClient,
    party_id: str,
) -> dict | None:
    """Fetch party info (balance) from ccview.io API."""
    url = f"{PARTY_INFO_URL}/{party_id}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response.json()
            if response.status_code == 404:
                return None
            print(f"  [attempt {attempt}/{MAX_RETRIES}] party info HTTP {response.status_code}: {response.text[:200]}")
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
            print(f"  [attempt {attempt}/{MAX_RETRIES}] party info error: {exc}")
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)
    return None


def parse_balance(party_info: dict | None) -> tuple[Decimal, Decimal, Decimal]:
    """Parse balance from party info response.

    Returns: (total_balance, unlocked, locked)
    """
    if party_info is None:
        return Decimal("0"), Decimal("0"), Decimal("0")

    # Try different response structures
    balance_data = party_info.get("balance", party_info)
    if isinstance(balance_data, dict):
        total = Decimal(str(balance_data.get("total", balance_data.get("unlocked_amount", "0"))))
        unlocked = Decimal(str(balance_data.get("unlocked", balance_data.get("unlocked_amount", "0"))))
        locked = Decimal(str(balance_data.get("locked", balance_data.get("locked_amount", "0"))))
        return total, unlocked, locked

    # Fallback: try top-level fields
    unlocked = Decimal(str(party_info.get("unlocked_amount", party_info.get("balance", "0"))))
    locked = Decimal(str(party_info.get("locked_amount", "0")))
    return unlocked + locked, unlocked, locked


def parse_counterparties(data: dict) -> list[CounterpartyData]:
    """Parse API response into CounterpartyData list."""
    items = data.get("data", [])
    ans_binding = data.get("ans_binding", {})

    result = []
    for item in items:
        cid = item.get("counterparty_id", "")
        display_name = cid
        if cid in ans_binding:
            bindings = ans_binding[cid]
            if bindings and isinstance(bindings, list):
                display_name = bindings[0].get("ans_name", cid)

        result.append(CounterpartyData(
            counterparty_id=cid,
            display_name=display_name,
            total_transfers_count=int(item.get("total_transfers_count", 0)),
            total_transfers_volume=Decimal(item.get("total_transfers_volume", "0")),
            transfers_in_count=int(item.get("transfers_in_count", 0)),
            transfers_out_count=int(item.get("transfers_out_count", 0)),
            transfers_in_volume=Decimal(item.get("transfers_in_volume", "0")),
            transfers_out_volume=Decimal(item.get("transfers_out_volume", "0")),
            last_transfer_time=item.get("last_transfer_record_time", ""),
        ))
    return result


def build_fee_report(
    party_id: str,
    start_date: str,
    end_date: str,
    counterparties: list[CounterpartyData],
    balance: Decimal,
    balance_unlocked: Decimal,
    balance_locked: Decimal,
) -> FeeReport:
    """Build fee report from counterparties data + balance."""
    party_short = shorten_party_id(party_id)

    # Find validator (fee) and pool-custodian
    validator = None
    pool_custodian = None
    for cp in counterparties:
        cid_lower = cp.counterparty_id.lower()
        if any(pattern.lower() in cid_lower for pattern in VALIDATOR_PATTERNS):
            validator = cp
        elif "pool-custodian" in cid_lower:
            pool_custodian = cp

    validator_fee_total = Decimal("0")
    validator_tx_count = 0
    if validator is not None:
        validator_fee_total = validator.transfers_out_volume
        validator_tx_count = validator.transfers_out_count

    swap_volume = Decimal("0")
    swap_tx_count = 0
    if pool_custodian is not None:
        swap_volume = pool_custodian.total_transfers_volume
        swap_tx_count = pool_custodian.total_transfers_count

    avg_fee = (
        validator_fee_total / validator_tx_count
        if validator_tx_count > 0
        else Decimal("0")
    )
    total_tx = sum(cp.total_transfers_count for cp in counterparties)

    # Reward calculation
    reward = calculate_reward(swap_tx_count)
    reward_net = reward - validator_fee_total

    # Estimasi hari bertahan
    days_remaining = calculate_days_remaining(balance, validator_fee_total)

    return FeeReport(
        party_id=party_id,
        party_short=party_short,
        date_start=start_date,
        date_end=end_date,
        validator_fee_total=validator_fee_total,
        validator_tx_count=validator_tx_count,
        avg_fee_per_swap=avg_fee,
        swap_volume=swap_volume,
        swap_tx_count=swap_tx_count,
        total_tx=total_tx,
        balance=balance,
        balance_unlocked=balance_unlocked,
        balance_locked=balance_locked,
        reward=reward,
        reward_net=reward_net,
        days_remaining=days_remaining,
        counterparties=counterparties,
    )


def format_decimal(value: Decimal, places: int = 4) -> str:
    """Format decimal for display."""
    text = format(value, f".{places}f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def format_report_telegram(report: FeeReport) -> str:
    """Format fee report for Telegram message."""
    if report.error:
        return (
            f"❌ <b>{report.party_short}</b>\n"
            f"Error: {report.error}"
        )

    date_display = (
        report.date_start
        if report.date_start == report.date_end
        else f"{report.date_start} → {report.date_end}"
    )

    # Reward status
    reward_status = "✅" if report.reward_net > 0 else "⚠️"
    days_emoji = "🟢" if report.days_remaining >= 7 else ("🟡" if report.days_remaining >= 3 else "🔴")

    lines = [
        f"📊 <b>CCView Report — {date_display}</b>",
        "",
        f"🔑 <code>{report.party_short}</code>",
        "",
        f"💰 <b>Balance:</b> {format_decimal(report.balance, 2)} CC",
        "",
        f"📋 <b>Fee:</b>",
        f"├─ Validator Fee: {format_decimal(report.validator_fee_total)} CC ({report.validator_tx_count} tx)",
        f"├─ Avg Fee/Swap: {format_decimal(report.avg_fee_per_swap)} CC",
        f"└─ Swap Volume: {format_decimal(report.swap_volume, 2)} CC ({report.swap_tx_count} tx)",
        "",
        f"🎁 <b>Reward:</b>",
        f"├─ Reward: {format_decimal(report.reward)} CC",
        f"├─ Fee: -{format_decimal(report.validator_fee_total)} CC",
        f"└─ {reward_status} Bersih: <b>{format_decimal(report.reward_net)} CC</b>",
        "",
        f"{days_emoji} <b>Estimasi:</b> ~{report.days_remaining} hari tersisa",
        f"   ({format_decimal(report.balance, 2)} / {format_decimal(report.validator_fee_total)} = {report.days_remaining} hari)",
    ]

    return "\n".join(lines)


def format_report_console(report: FeeReport) -> str:
    """Format fee report for console output."""
    if report.error:
        return f"  ERROR: {report.party_short} — {report.error}"

    reward_sign = "+" if report.reward_net >= 0 else ""
    return (
        f"  {report.party_short}\n"
        f"    Balance: {format_decimal(report.balance, 2)} CC\n"
        f"    Fee: {format_decimal(report.validator_fee_total)} CC ({report.validator_tx_count} tx) | "
        f"Avg: {format_decimal(report.avg_fee_per_swap)} CC/swap\n"
        f"    Swap: {report.swap_tx_count} tx | Volume: {format_decimal(report.swap_volume, 2)} CC\n"
        f"    Reward: {format_decimal(report.reward)} CC | "
        f"Bersih: {reward_sign}{format_decimal(report.reward_net)} CC\n"
        f"    Estimasi: ~{report.days_remaining} hari tersisa"
    )


async def send_telegram(text: str) -> bool:
    """Send message to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, timeout=15)
            if response.status_code == 200:
                return True
            print(f"  Telegram error: {response.status_code} {response.text[:200]}")
    except Exception as exc:
        print(f"  Telegram error: {exc}")
    return False


def _build_client() -> httpx.AsyncClient:
    """Build httpx client with proper headers for ccview.io."""
    return httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://ccview.io",
            "Referer": "https://ccview.io/",
        },
        follow_redirects=True,
    )


async def run_scrape(
    party_ids: list[str],
    start_date: str,
    end_date: str,
    *,
    send_to_telegram: bool = True,
) -> list[FeeReport]:
    """Scrape fee data for all party IDs and optionally send to Telegram."""
    print(f"CCView Fee Scraper — {start_date} to {end_date}")
    print(f"{'=' * 50}")
    print(f"Scraping {len(party_ids)} party IDs...")
    print()

    reports: list[FeeReport] = []
    async with _build_client() as client:
        session_ok = await init_session(client)
        if not session_ok:
            print("WARNING: Session initialization failed, API calls may fail")
        print()

        for i, party_id in enumerate(party_ids, 1):
            print(f"[{i}/{len(party_ids)}] {shorten_party_id(party_id)}...")

            # Fetch counterparties + balance in parallel
            cp_data, party_info = await asyncio.gather(
                fetch_counterparties(client, party_id, start_date, end_date),
                fetch_party_info(client, party_id),
            )

            if cp_data is None:
                report = FeeReport(
                    party_id=party_id,
                    party_short=shorten_party_id(party_id),
                    date_start=start_date,
                    date_end=end_date,
                    validator_fee_total=Decimal("0"),
                    validator_tx_count=0,
                    avg_fee_per_swap=Decimal("0"),
                    swap_volume=Decimal("0"),
                    swap_tx_count=0,
                    total_tx=0,
                    balance=Decimal("0"),
                    balance_unlocked=Decimal("0"),
                    balance_locked=Decimal("0"),
                    reward=Decimal("0"),
                    reward_net=Decimal("0"),
                    days_remaining=0,
                    counterparties=[],
                    error="Failed to fetch data from ccview.io API",
                )
            else:
                counterparties = parse_counterparties(cp_data)
                balance, balance_unlocked, balance_locked = parse_balance(party_info)
                report = build_fee_report(
                    party_id, start_date, end_date, counterparties,
                    balance, balance_unlocked, balance_locked,
                )

            reports.append(report)
            print(format_report_console(report))

            if send_to_telegram:
                telegram_text = format_report_telegram(report)
                sent = await send_telegram(telegram_text)
                if sent:
                    print("    → Sent to Telegram ✓")
                elif TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                    print("    → Telegram send failed ✗")

            if i < len(party_ids):
                await asyncio.sleep(REQUEST_DELAY_SECONDS)

    # Summary
    print()
    print(f"{'=' * 50}")
    print("SUMMARY")
    print(f"{'=' * 50}")
    ok_reports = [r for r in reports if not r.error]
    total_fee = sum(r.validator_fee_total for r in ok_reports)
    total_swaps = sum(r.validator_tx_count for r in ok_reports)
    total_volume = sum(r.swap_volume for r in ok_reports)
    total_reward = sum(r.reward for r in ok_reports)
    total_reward_net = sum(r.reward_net for r in ok_reports)
    total_balance = sum(r.balance for r in ok_reports)
    avg_fee_all = total_fee / total_swaps if total_swaps > 0 else Decimal("0")
    avg_days = calculate_days_remaining(total_balance, total_fee) if total_fee > 0 else 999

    print(f"  Accounts: {len(reports)} ({len(ok_reports)} OK, {sum(1 for r in reports if r.error)} errors)")
    print(f"  Total Balance: {format_decimal(total_balance, 2)} CC")
    print(f"  Total Fee: {format_decimal(total_fee)} CC ({total_swaps} swaps)")
    print(f"  Avg Fee/Swap: {format_decimal(avg_fee_all)} CC")
    print(f"  Total Volume: {format_decimal(total_volume, 2)} CC")
    print(f"  Total Reward: {format_decimal(total_reward)} CC")
    print(f"  Total Reward Bersih: {format_decimal(total_reward_net)} CC")
    print(f"  Avg Days Remaining: ~{avg_days} hari")

    if send_to_telegram and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        date_display = start_date if start_date == end_date else f"{start_date} → {end_date}"
        reward_sign = "+" if total_reward_net >= 0 else ""
        summary_text = (
            f"📈 <b>Fee Summary — {date_display}</b>\n"
            f"\n"
            f"👥 Accounts: {len(ok_reports)}/{len(reports)}\n"
            f"💰 Total Balance: <b>{format_decimal(total_balance, 2)} CC</b>\n"
            f"🔥 Total Fee: {format_decimal(total_fee)} CC ({total_swaps} swaps)\n"
            f"📊 Avg Fee/Swap: {format_decimal(avg_fee_all)} CC\n"
            f"📦 Total Volume: {format_decimal(total_volume, 2)} CC\n"
            f"\n"
            f"🎁 Total Reward: {format_decimal(total_reward)} CC\n"
            f"💵 Reward Bersih: <b>{reward_sign}{format_decimal(total_reward_net)} CC</b>\n"
            f"⏳ Avg Estimasi: ~{avg_days} hari tersisa"
        )
        await send_telegram(summary_text)
        print("  → Summary sent to Telegram ✓")

    return reports


async def run_once():
    """Run scraper once (CLI mode)."""
    today = datetime.now(timezone.utc).date().isoformat()
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if arg == "daemon":
            await run_daemon()
            return
        start_date = end_date = arg
    else:
        start_date = end_date = today

    party_ids = load_party_ids(PARTYID_FILE)
    await run_scrape(party_ids, start_date, end_date)


async def run_daemon():
    """Run as daemon — automatically scrape yesterday's data when a new UTC day starts."""
    MIDNIGHT_DELAY = float(os.getenv("MIDNIGHT_DELAY_SECONDS", "300"))

    party_ids = load_party_ids(PARTYID_FILE)
    print(f"CCView Fee Scraper — DAEMON MODE")
    print(f"{'=' * 50}")
    print(f"Loaded {len(party_ids)} party IDs")
    print(f"Midnight delay: {int(MIDNIGHT_DELAY)}s")
    print(f"Reward: >= {REWARD_THRESHOLD_TX} tx = {REWARD_FULL} CC, < {REWARD_THRESHOLD_TX} tx = count × {REWARD_PER_SWAP} CC")
    print(f"Watching for UTC day changes...")
    print()

    last_scraped_date: str | None = None
    current_utc_date = datetime.now(timezone.utc).date()

    while True:
        now_utc = datetime.now(timezone.utc)
        new_utc_date = now_utc.date()

        if new_utc_date != current_utc_date:
            yesterday = current_utc_date.isoformat()
            current_utc_date = new_utc_date

            if last_scraped_date == yesterday:
                print(f"[{now_utc.strftime('%H:%M:%S')} UTC] Already scraped {yesterday}, skipping")
            else:
                print(f"\n[{now_utc.strftime('%H:%M:%S')} UTC] 🌅 New day! Waiting {int(MIDNIGHT_DELAY)}s before scraping {yesterday}...")
                await send_telegram(
                    f"🌅 <b>New UTC day</b> — waiting {int(MIDNIGHT_DELAY)}s before scraping {yesterday}"
                )
                await asyncio.sleep(MIDNIGHT_DELAY)

                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC] Starting scrape for {yesterday}...")
                try:
                    await run_scrape(party_ids, yesterday, yesterday)
                    last_scraped_date = yesterday
                    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC] ✅ Scrape complete for {yesterday}")
                except Exception as exc:
                    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC] ❌ Scrape failed: {exc}")
                    await send_telegram(f"❌ <b>Scrape failed for {yesterday}</b>\n{exc}")

        await asyncio.sleep(30)


async def main():
    await run_once()


if __name__ == "__main__":
    asyncio.run(main())
