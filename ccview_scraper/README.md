# CCView.io Fee Scraper

Bot terpisah untuk mengambil data fee harian dari ccview.io dan mengirim laporan ke Telegram.

## Cara Kerja

Bot menggunakan **API internal ccview.io** (bukan scraping HTML) untuk mengambil data counterparties per wallet/party ID. Dari data ini, bot menghitung:

- **Validator Fee** — total fee yang dibayar ke validator (cantex.unverified.cns)
- **Avg Fee/Swap** — rata-rata fee per swap
- **Swap Volume** — total volume swap via pool-custodian

## Setup

```bash
cd ccview_scraper
pip install -r requirements.txt
cp .env.example .env
# Edit .env → isi TELEGRAM_BOT_TOKEN_SCRAPER dan TELEGRAM_CHAT_ID_SCRAPER
```

## Input

Edit `partyid.txt` — satu party ID per baris:
```
Cantex::12205af96d5d0461caf37bddec46963c74b10c46f8590fd8fdcff7bcd8b21228f50e
Cantex::aeb377...
```

Baris kosong dan baris yang dimulai dengan `#` akan di-skip.

## Usage

```bash
# Scrape hari ini (UTC)
python scraper.py

# Scrape tanggal tertentu
python scraper.py 2026-04-29

# Scrape range tanggal
python scraper.py 2026-04-28 2026-04-29
```

## Output

### Console:
```
[1/3] Cantex::12205a...f50e...
  Cantex::12205a...f50e
    Fee: 8.891 CC (26 tx) | Avg: 0.342 CC/swap
    Volume: 1257.92 CC (50 tx) | Total: 77 tx
    → Sent to Telegram ✓
```

### Telegram:
```
📊 CCView Fee Report — 2026-04-29

🔑 Cantex::12205a...f50e
├─ Validator Fee: 8.891 CC (26 tx)
├─ Avg Fee/Swap: 0.342 CC
├─ Swap Volume: 1257.92 CC (50 tx)
└─ Total Tx: 77

📋 Counterparties:
  • pool-custodian::122038...909b8c: 50 tx, 1257.92 CC
  • cantex.unverified.cns: 26 tx, 8.89 CC
  • walley-gosjavar::122071...d5a0e6: 1 tx, 80 CC
```

## API Endpoint

```
GET https://ccview.io/api/v1/internal/api/v1/parties/counterparties
    ?party_id={party_id}
    &limit=50
    &offset=0
    &start={YYYY-MM-DD}
    &end={YYYY-MM-DD}
```

Tidak memerlukan autentikasi.
