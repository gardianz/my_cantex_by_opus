# Cantex Autoswap Bot — Developer Notes

Dokumen ini merangkum logic penting yang perlu dijaga saat mengembangkan bot.

## 1. Peta file inti

- `run_bot.py`
  - entry point CLI
- `src/autoswap_bot/bot.py`
  - orchestration utama: planning route, preflight, submit swap, settlement, monitoring
- `src/autoswap_bot/cycle_tracker.py`
  - state machine cycle loss per account
- `src/autoswap_bot/routing.py`
  - pilih route terbaik berdasarkan quote SDK
- `src/autoswap_bot/config.py`
  - parsing config runtime dan account
- `src/autoswap_bot/runtime_state.py`
  - state persisten harian, free-fee, dan cache runtime
- `src/autoswap_bot/telegram_monitor.py`
  - dashboard terminal + Telegram
- `src/autoswap_bot/sdk_ext.py`
  - wrapper tambahan di atas `cantex_sdk`
- `cantex_sdk/src/cantex_sdk/_sdk.py`
  - SDK lokal, termasuk `swap_and_confirm`

## 2. Cycle loss: invariants penting

Cycle loss **bukan** total rugi fee. Cycle loss hanya dimaksudkan untuk mengukur rugi spread / hasil round-trip.

### Mode `USDCx`

Cycle yang dihitung:

- start: `USDCx -> foreign`
- selesai: `foreign -> USDCx`

Rumus:

- `start_amount = sell_amount USDCx`
- `end_amount = actual output USDCx`
- `spread_loss = start_amount - end_amount`

Catatan:

- `CC -> USDCx` dianggap top-up / refill, **bukan** start cycle USDCx
- `USDCx -> CC` juga **bukan** bagian cycle USDCx
- target foreign saat close harus sama dengan foreign saat open cycle

Implementasi utama ada di:

- `src/autoswap_bot/cycle_tracker.py`

### Mode `CC`

Cycle yang dihitung:

- start: `CC -> foreign`
- selesai: `foreign -> CC`

Rumus:

- `start_amount = sell_amount CC`
- `end_amount = actual CC received + CC network fee`
- `spread_loss = start_amount - end_amount`

Kenapa network fee ditambah balik?

- karena network fee CC sudah dicatat terpisah di statistik fee
- cycle loss ingin merepresentasikan spread / harga swap, bukan double-count fee

### Aturan maintenance

Kalau menambah pair / token baru:

1. cek apakah pair itu harus ikut cycle `USDCx` atau cycle `CC`
2. update simbol foreign yang valid di `CycleTracker`
3. pastikan token top-up / refill tidak salah dianggap sebagai close cycle

## 3. Guard output mismatch: perubahan penting

Masalah historis:

- event `swap_and_confirm` bisa menerima event swap lain yang bukan milik hop aktif
- akibatnya `tx_result.output_amount` bisa berisi amount token yang salah, misalnya `CC` terbaca saat hop seharusnya `CBTC -> USDCx`
- ini bisa merusak log hop, fee derivation, dan cycle loss

### Guard level SDK

`cantex_sdk.swap_and_confirm()` sekarang hanya menerima event WebSocket yang cocok dengan request swap aktif:

- input instrument harus cocok
- output instrument harus cocok

Jika tidak cocok, event di-ignore dan loop tetap menunggu event yang benar.

### Guard level bot

`AutoswapBot` tidak langsung percaya `tx_result.output_amount`.

Urutan pengambilan actual output:

1. pakai `output_amount` jika `output_instrument` cocok dengan `hop.buy_symbol`
2. jika mismatch, hitung dari delta balance hasil settlement
3. jika delta balance juga tidak memberi sinyal kuat, fallback ke raw output / `hop.returned_amount`

Tujuan:

- cycle loss tidak salah membaca token lain
- log hop lebih konsisten dengan saldo account
- fee aktual berbasis balance diff tetap aman

Fungsi yang perlu dijaga:

- `CantexSDK.swap_and_confirm`
- `AutoswapBot._resolve_actual_output_amount`
- `AutoswapBot._matching_tx_output_amount`

## 4. Max fee vs max slippage

Bot sekarang punya dua guard harga sebelum submit:

- `max_network_fee_cc_per_execution`
- `max_slippage_per_execution`

### `max_network_fee_cc_per_execution`

Membatasi network fee per hop dalam satuan `CC`.

### `max_slippage_per_execution`

Membatasi slippage quote per hop.

Nilainya memakai angka mentah dari SDK:

- `0.001 = 0.1%`
- `0.01 = 1%`

### Urutan cek sebelum swap

1. Router quote semua candidate route
2. route dengan fee/slippage di atas cap dihindari jika ada alternatif
3. saat preflight sebelum submit, hop dicek lagi terhadap fee/slippage dari quote yang sedang dibawa
4. jika `pre_submit_requote_enabled = true`, bot re-quote tepat sebelum submit
5. jika fee atau slippage terbaru melewati cap, hop dibatalkan

Artinya guard dilakukan dua kali:

- saat memilih route
- saat preflight final sebelum submit

Kalau `pre_submit_requote_enabled = false`:

- bot tetap memakai cek fee/slippage dari quote awal
- bot tidak refresh quote final sebelum submit
- polling fee route-level dan quote retry lain tetap berjalan

### Batasan saat ini

- `max_slippage_per_execution` bisa di-set dari file config
- belum ada command Telegram `/set` khusus slippage

Kalau nanti ingin menambah runtime override slippage:

1. tambahkan parser di `bot.py` untuk command `/set`
2. sinkronkan ke `RouteOptimizer`
3. update `README.md` dan dokumen ini

## 5. Saat debug cycle loss yang terlihat salah

Checklist cepat:

1. lihat log hop sukses: apakah `output_symbol` sesuai `hop.buy_symbol`
2. cek apakah muncul warning `Tx output mismatch ...`
3. bandingkan delta balance settlement dengan amount di event
4. cek apakah mode account sedang `CC` atau `USDCx`
5. cek apakah swap yang terlihat sebenarnya top-up / refill, bukan cycle close

Jika log menunjukkan angka besar yang tampak berasal dari token lain, curigai mismatch output terlebih dulu.

## 6. Verifikasi cepat setelah ubahan

Minimal lakukan:

```bash
python -m compileall src cantex_sdk/src
```

Kalau environment punya `pytest`, jalankan test SDK yang relevan:

```bash
python -m pytest cantex_sdk/tests/test_cantex_sdk.py -k "swap_and_confirm or unrelated_swap_events"
```

Untuk perubahan di bot:

- verifikasi log hop sukses tetap menampilkan symbol output yang benar
- verifikasi cycle loss mode `CC` dan `USDCx` dengan contoh round-trip kecil
- verifikasi hop dibatalkan saat fee / slippage melebihi cap

## 7. Aturan update dokumentasi

Jika ada perubahan perilaku bot, update minimal:

1. `README.md` jika perubahan berdampak ke user / config
2. `AGENTS.md` ini jika perubahan menyentuh logic engineering / maintenance

Contoh perubahan yang wajib didokumentasikan:

- state machine strategy / refill
- perhitungan fee atau cycle loss
- rule penentuan route
- sumber data monitoring / history
- perubahan semantics config

## 8. Daily loss simple multi-target

Sebelumnya `daily_cc_loss` hanya dihitung saat `target_symbol == CC_SYMBOL` di `_refill_after_target`. Akibatnya saat user pilih target refill `USDCx` atau `USDCx_v2`, kolom `CyLoss` selalu kosong.

Sekarang daily loss generic untuk SEMUA target:

- `TelegramCardState` punya field `daily_loss_symbol` yang menyimpan simbol target loss harian (`"CC"` / `"USDCx"`).
- `set_cc_balance_start_of_day(card, balance, target_symbol=...)` menerima parameter `target_symbol` opsional. Default `"CC"` agar backward-compatible. Bot memanggilnya dengan `_effective_post_target_refill_symbol()` di tiga titik: startup, daily reset di `_check_and_reset_daily_progress`, dan daily-quota midnight wait.
- `_refill_after_target` selalu memanggil `update_daily_cc_loss(balance(target)_after_refill)` tanpa lagi mensyaratkan target = CC.
- Render `_format_cycle_spread_loss_compact` membaca `card.daily_loss_symbol` dan menampilkan label sesuai (mis. `0.50 U` untuk USDCx, `0.50 CC` untuk CC).
- Field disk `cc_balance_start_of_day` dan `daily_cc_loss` namanya tetap untuk backward-compat dengan state file lama; isinya kini bisa simbol apa pun sesuai `daily_loss_symbol`.

Saat menambah target refill baru:

1. tambahkan opsi di `cli.py POST_TARGET_REFILL_CHOICES`
2. petakan `_effective_post_target_refill_symbol` ke simbol kanonik
3. pastikan `daily_loss_symbol` di card konsisten dengan target

## 9. Cycle tracker juga merekam recovery / refill hop

`cycle_tracker.record_swap` sebelumnya hanya dipanggil dari `_execute_round_dynamic` dan `_execute_round_dynamic_v2`. Cycle yang dimulai di round normal lalu ditutup lewat `_recover_to_symbol` (mis. `_refill_after_target` cleanup akhir hari) tidak pernah tertutup di tracker, akibatnya pending cycle bisa "menggantung" sampai bot restart.

Sekarang `_recover_to_symbol` ikut memanggil `cycle_tracker.record_swap` setiap hop sukses dengan `actual_recovery_output` dari `_resolve_actual_output_amount`. Bila record mengembalikan `CycleResult`, bot juga `record_cycle_spread_loss` dan persist state via `_save_cycle_tracker_state`.

Aturan: setiap hop yang menyebabkan perpindahan saldo nyata di akun WAJIB melewati `cycle_tracker.record_swap` agar invariant cycle (start-end balanced) tetap terjaga.

## 10. USDCx_v2 pre-refill: idempotency harian

Pre-refill `_maybe_pre_refill_usdcx_v2` sebelumnya memakai gate `result.completed_rounds != 0`. Gate ini rapuh setelah `_check_and_reset_daily_progress` melakukan reset progress, karena `_wait_for_trading_history_round_progress` akan langsung men-sync ulang dari API trading history dan bisa membuat `completed_rounds > 0` di hari baru. Hasilnya: pre-refill di-skip diam-diam untuk akun yang kebetulan sudah punya progress di hari baru, sementara akun lain tetap di-refill ke CC. Tidak konsisten.

Pendekatan baru:

- Tambah field persisten `last_pre_refill_utc_date` di `runtime_state.AccountRuntimeState`.
- API baru di `BotRuntimeStateStore`: `is_pre_refill_done_today(account_name)` dan `mark_pre_refill_done_today(account_name)`. Direset otomatis tiap pergantian hari UTC oleh `_normalized_state`.
- `_maybe_pre_refill_usdcx_v2` sekarang:
  - HAPUS gate `result.completed_rounds != 0`.
  - Cek `is_pre_refill_done_today` lebih awal; kalau sudah ditandai, return tanpa I/O.
  - Bila tidak ada saldo non-CC > dust, langsung `mark_pre_refill_done_today` + return supaya tidak loop pemeriksaan setiap iterasi.
  - Bila scrape sukses dan ada swap dilakukan, `mark_pre_refill_done_today` setelah selesai.
  - Bila `_recover_to_symbol` mengembalikan `0` (mis. fee terlalu tinggi), TIDAK mark — biar trigger berikutnya bisa retry.

Saat menambah mode refill baru yang butuh pre-step di awal hari, ikuti pola idempotency harian yang sama: simpan tanggal UTC eksekusi terakhir di state, reset saat pergantian hari, panggil dari titik yang sama dengan pre-refill USDCx v2.

## 11. CCView fee scraper: cooldown completion + callback registry

Masalah historis:

- `MIN_SCRAPE_COOLDOWN_SECONDS = 5` di-set sebelum scrape dimulai. Bila scrape gagal/timeout, slot itu hangus — trigger dari hop berikutnya yang datang dalam < 5s langsung di-skip. Akibatnya kolom `Gas` macet di angka lama.
- `_scrape_lock` bersifat global untuk 20 akun; lock antri panjang kalau ccview.io lambat. Card-update task per-trigger sebelumnya rigid 8 + 5 detik (max 13 detik), gagal langsung diam saja tanpa retry.
- `_periodic_scrape_loop` (fallback 90 detik) hanya update `_latest_results`, tidak pernah refresh `monitor` card.

Perbaikan:

- `MIN_SCRAPE_COOLDOWN_SECONDS` diturunkan dari 5 → 2.
- `trigger_background_scrape` TIDAK lagi men-set `_last_scrape_time`. Hanya cek cooldown.
- `_background_scrape` set `_last_scrape_time[account_name] = time.monotonic()` HANYA setelah `result.success`. Trigger gagal tidak menelan slot.
- `FeeScraper.register_on_result(callback)` + `_notify_result(account_name, result)`. Semua jalur scrape (`_background_scrape`, `_startup_scrape`, `_periodic_scrape_loop`, `scrape_now`) memanggil `_notify_result` saat sukses.
- `AutoswapBot._on_fee_scrape_result` register di `__init__`, cari card di `_monitor_cards_by_account` (registry baru), lalu `create_task(monitor.update_ccview_fee(...))`. Aman dipanggil dari callback sync.
- Card-update task per-trigger dirombak jadi polling loop: snapshot `_last_scrape_time` & `validator_tx_count` di awal, sleep 8s lalu loop tiap 5s sampai dapat data baru atau timeout 60s.

Saat menambah jalur scrape baru atau merubah strategi rate-limit:

1. SELALU set `_last_scrape_time` setelah scrape sukses, jangan saat trigger.
2. SELALU panggil `_notify_result` saat ada `result.success` di path manapun.
3. Pastikan `_monitor_cards_by_account` ter-isi sebelum bisa dipanggil callback (saat `monitor.create_card`).

## 12. Format dashboard & balance

- Saldo `CC` dan `USDCx` di terminal dashboard maupun di Telegram combined card sekarang ditampilkan dengan 2 desimal (`14.32`, `11.53`). `CBTC` tetap 8 desimal.
- Lebar kolom dashboard di `TelegramMonitor._dashboard_col_widths` di-tune ulang untuk memuat: nama akun 10 char (mis. `wallet-15`), `Plan` 22 char, `Gas` 10 char, `CyLoss` 14 char, plus margin.
- Saat menambah/mengubah kolom dashboard, jaga konsistensi tuple `_dashboard_col_widths` dan urutan tuple di `_dashboard_table_lines`.

## 13. Re-quote final sebelum submit bisa dimatikan

Re-quote final sebelum `swap_and_confirm` dikontrol oleh `settings.pre_submit_requote_enabled`.

Tujuannya:

- `true`: user ingin proteksi tambahan dengan refresh quote terakhir sebelum submit
- `false`: user ingin submit lebih cepat dan konsisten memakai quote awal

Perilaku:

- `true`
  - bot ambil `fresh_quote` terakhir sebelum submit
  - fee/slippage final memakai nilai fresh quote itu
- `false`
  - bot lewati langkah `fresh_quote`
  - fee/slippage final tetap memakai quote awal dari `hop`

Catatan maintenance:

- setting ini hanya berlaku di `_swap_hop_with_retry`
- jangan dipakai untuk mematikan polling fee route-level atau retry quote untuk validasi harga

## 14. Strategy 4 top-up settlement tidak boleh deadlock

Masalah historis:

- setelah swap top-up `CC -> USDCx` sukses, `strategy_4_topup_pending_recycle` di-set `True`
- jika balance foreign belum muncul di `get_account_info()`, `_strategy_4_action_candidates` mengembalikan `strategy_4 waiting top-up balance settlement`
- loop 24h lalu hanya tidur 5 detik dan retry
- tanpa escape hatch, akun bisa stuck cooldown selamanya

Perilaku yang harus dijaga sekarang:

- `StrategyRuntimeState` punya counter `strategy_4_topup_settlement_wait_retries`
- selama `strategy_4_topup_pending_recycle=True` dan foreign balance belum terlihat, counter naik
- selama counter masih `<= MAX_STRATEGY_4_TOPUP_SETTLEMENT_WAIT_RETRIES`, bot tetap menunggu settlement
- jika limit terlampaui, bot clear `strategy_4_topup_pending_recycle` dan reset counter agar state machine bisa lanjut lagi
- saat masih pending settlement, `_build_skipped_round_result` TIDAK boleh mengaktifkan `strategy_4_topup_after_foreign_minimum`; kalau tidak, foreign balance yang baru muncul bisa salah dilewati dan recycle tidak jalan
- setiap sukses swap `CC->USDCx` atau sukses foreign sell (`USDCx/CBTC`), counter settlement wait harus di-reset ke `0`

Saat debug akun strategy 4 yang stuck cooldown:

1. cek apakah log berulang `strategy_4 waiting top-up balance settlement`
2. cek apakah setelah beberapa retry state pending akhirnya clear
3. pastikan saat foreign balance akhirnya muncul, bot kembali memilih recycle candidate, bukan langsung top-up lagi

## 15. CCView jadi sumber fee/gas rendered untuk today/week

Masalah historis:

- terminal row `Gas`, terminal summary `Fee paid`, Telegram combined `Gas`, dan Telegram summary tidak memakai sumber data yang sama
- sebagian memakai accounting internal bot, sebagian memakai scrape ccview

Perilaku yang harus dijaga sekarang:

- `FeeScraper` punya fetch rentang tanggal:
  - harian: `today -> today`
  - mingguan: `Monday UTC -> today UTC`
- hasil scrape sukses membawa dua aggregate sekaligus:
  - `validator_fee_total` / `validator_tx_count` / `avg_fee_per_swap` untuk hari ini
  - `week_validator_fee_total` / `week_validator_tx_count` / `week_avg_fee_per_swap` untuk minggu berjalan
- `TelegramMonitor` menyimpan field ccview today + week di state persisten card/account
- render `today/week fee paid` di terminal dashboard dan Telegram summary HARUS lewat `_current_day_total_fee()` / `_current_week_total_fee()` yang sekarang memantulkan aggregate ccview, bukan accounting internal bot
- row/account-level display boleh tetap menampilkan metrik tambahan internal lain, tapi angka `Gas` / `Fee paid today/week` harus konsisten lintas terminal + Telegram

Saat mengubah lagi dashboard fee:

1. jangan campur sumber internal bot dan ccview untuk label yang sama
2. kalau menambah label `today/week`, pakai aggregate ccview yang sudah ada
3. kalau scrape week gagal tapi scrape today sukses, jangan mengosongkan state week yang terakhir valid tanpa sengaja

## 16. Weekly refill harus retry terus selama blocker masih transien

Masalah historis:

- `_perform_weekly_refill_to_cc` berhenti pada kegagalan recovery pertama
- akibatnya weekly refill bisa selesai dengan status incomplete meski penyebabnya cuma fee/slippage/cap sementara

Perilaku yang harus dijaga sekarang:

- weekly refill tetap MEMATUHI:
  - `max_network_fee_cc_per_execution`
  - `max_slippage_per_execution`
- kalau `_recover_to_symbol()` tidak menghasilkan tx, bot HARUS klasifikasikan blocker:
  - `wait`: fee/slippage/quote masih mungkin membaik, maka tidur sebentar lalu retry lagi
  - `insufficient_cc_gas`: semua source yang masih layak hanya terblokir `balance fee tidak cukup`, maka boleh stop incomplete
  - `terminal`: kasus non-transien seperti semua source tersisa di bawah min-ticket / issue balance terminal, maka boleh stop incomplete
- selama blocker = `wait`, bot tidak boleh declare weekly refill selesai ataupun incomplete; loop harus lanjut sampai saldo non-CC habis atau benar-benar mentok

Saat debug weekly refill yang tampak macet:

1. cek log `Weekly refill waiting retry`
2. lihat `reason=` dari `_classify_recovery_blocker`
3. pastikan stop final hanya terjadi pada `insufficient_cc_gas` atau alasan terminal nyata, bukan sekadar fee/slippage tinggi sesaat
