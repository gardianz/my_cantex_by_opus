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
cp partyid.example.txt partyid.txt
# Edit partyid.txt → masukkan party ID (1 per baris)
```

## Input

Edit `partyid.txt` — satu party ID per baris:
```
Cantex::12205af96d5d0461caf37bddec46963c74b10c46f8590fd8fdcff7bcd8b21228f50e
Cantex::aeb377...
```

Baris kosong dan baris yang dimulai dengan `#` akan di-skip.

## Usage

### Mode Sekali Jalan

```bash
# Scrape hari ini (UTC)
python scraper.py

# Scrape tanggal tertentu
python scraper.py 2026-04-29

# Scrape range tanggal
python scraper.py 2026-04-28 2026-04-29
```

### Mode Daemon (24 Jam)

Bot berjalan terus-menerus dan **otomatis mengirim laporan hari sebelumnya** saat hari berganti (UTC midnight).

```bash
python scraper.py daemon
```

**Alur daemon:**
1. Bot start → catat tanggal UTC saat ini
2. Cek setiap 30 detik apakah hari sudah berganti
3. Saat hari berganti (UTC midnight):
   - Tunggu 5 menit (configurable) agar data ccview.io settle
   - Scrape data **hari sebelumnya**
   - Kirim laporan per party ID + summary ke Telegram
4. Kembali menunggu hari berikutnya

**Contoh:**
- Bot dijalankan tanggal 29 April
- Masuk 30 April 00:00 UTC → tunggu 5 menit → scrape data 29 April → kirim ke Telegram
- Masuk 1 Mei 00:00 UTC → tunggu 5 menit → scrape data 30 April → kirim ke Telegram

### Menjalankan Daemon di Background (VPS)

**Dengan screen:**
```bash
screen -S ccview
cd ccview_scraper
python scraper.py daemon
# Ctrl+A, D untuk detach
# screen -r ccview untuk kembali
```

**Dengan tmux:**
```bash
tmux new -s ccview
cd ccview_scraper
python scraper.py daemon
# Ctrl+B, D untuk detach
# tmux attach -t ccview untuk kembali
```

**Dengan nohup:**
```bash
cd ccview_scraper
nohup python scraper.py daemon > scraper.log 2>&1 &
```

**Dengan systemd (recommended):**
```bash
sudo nano /etc/systemd/system/ccview-scraper.service
```

```ini
[Unit]
Description=CCView Fee Scraper Daemon
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/path/to/ccview_scraper
ExecStart=/usr/bin/python3 scraper.py daemon
Restart=always
RestartSec=30
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable ccview-scraper
sudo systemctl start ccview-scraper
sudo systemctl status ccview-scraper
# Log: journalctl -u ccview-scraper -f
```

## Konfigurasi (.env)

```env
# Telegram
TELEGRAM_BOT_TOKEN_SCRAPER=your_bot_token_here
TELEGRAM_CHAT_ID_SCRAPER=your_chat_id_here

# Request settings
REQUEST_DELAY_SECONDS=1.5      # Jeda antar request (detik)
REQUEST_TIMEOUT=30              # Timeout per request (detik)

# Daemon settings
MIDNIGHT_DELAY_SECONDS=300      # Delay setelah UTC midnight sebelum scraping (default 5 menit)
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

### Summary Telegram:
```
📈 Fee Summary — 2026-04-29

👥 Accounts: 3 (3 OK)
💰 Total Fee: 25.67 CC (78 swaps)
📊 Avg Fee/Swap: 0.329 CC
📦 Total Volume: 3,773.77 CC
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

Memerlukan session cookie (bot handle otomatis).
