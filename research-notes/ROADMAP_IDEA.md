# ROADMAP IDEA: Fitur Data Engineering ala Snowflake & Databricks untuk StarRocks

> Tanggal: 2026-11-22 · Basis kode: fork `dwickyfp/starrocks` (branch `main`, commit terakhir 2026-11-21)
> Dokumen pendamping: `RISET_FITUR_SNOWFLAKE.md` (desain detail Time Travel, Task, Stored Procedure) dan `ANALISA_STARROCKS.md` (peta codebase).

Dokumen ini mendaftar fitur Snowflake dan Databricks yang paling membantu kerja data engineer, menjelaskan cara kerjanya, lalu mencocokkannya dengan kondisi StarRocks saat ini dan memberi sketsa implementasi serta prioritas.

**Cara membaca status di StarRocks:**

| Simbol | Arti |
|---|---|
| ✅ | Sudah ada di upstream StarRocks (dicek di grammar `StarRocks.g4` / kode FE) |
| 🟡 | Ada sebagian / terbatas |
| ❌ | Belum ada |
| 🔨 | Sudah dikerjakan di fork ini (branch `feature/*`) |

**Skala effort:** S = < 1 minggu, M = 1–3 minggu, L = 1–2 bulan, XL = > 2 bulan (perkiraan untuk satu orang yang sudah paham codebase).

---

## Ringkasan Eksekutif

Temuan penting dari pengecekan codebase:

- Beberapa fitur yang disangka belum ada ternyata **sudah ada di upstream**: `SELECT * EXCLUDE/EXCEPT`, `GROUP BY ALL`, `PIVOT`, `ASOF JOIN`, `QUALIFY` (bentuk terbatas), SQL UDF (`CREATE FUNCTION ... RETURNS expr`), `SAMPLE`, `BEGIN WORK` (transaksi), `SWAP WITH`, `RECOVER` (recycle bin), `PIPE` + `FILES()`, temporary table, generated column, async MV dengan `refresh_mode` incremental.
- **`MERGE INTO` sudah ada, tetapi hanya untuk tabel Iceberg V2** (`MergeIntoAnalyzer.java:74` → `"MERGE INTO is only supported for Iceberg tables"`). Membawanya ke tabel native Primary Key adalah *quick win* bernilai tinggi.
- **`QUALIFY` hanya mendukung bentuk `QUALIFY <window_fn> <op> <INTEGER>`** (`StarRocks.g4:2565`), belum ekspresi bebas.
- Gap terbesar untuk data engineering: **Stream / Change Data Feed**, **zero-copy clone**, **Dynamic Table (target lag)**, **AUTO CDC / SCD2**, **expectations / data quality**, dan **governance (masking & row access policy)**.

### Top 10 rekomendasi (urut prioritas)

| # | Fitur | Asal | Status SR | Effort | Alasan |
|---|---|---|---|---|---|
| 1 | `MERGE INTO` untuk tabel native PK | Snowflake/Databricks | 🟡 (Iceberg saja) | M | Pola upsert paling umum; parser & planner sudah ada |
| 2 | Stream standard (insert/update/delete) pada tabel | Snowflake | ❌ | L–XL | Fondasi CDC incremental; dibutuhkan task trigger |
| 3 | Triggered task `WHEN SYSTEM$STREAM_HAS_DATA` | Snowflake | ❌ (task graph 🔨) | S–M | Melanjutkan branch `feature/snowflake-task` |
| 4 | Zero-copy clone (`CREATE TABLE ... CLONE`) | Snowflake/Databricks | ❌ | L | Dev/test environment, backup instan |
| 5 | `CHANGES` clause + `DESCRIBE HISTORY` + `RESTORE TABLE` | Snowflake/Databricks | ❌ (time travel 🔨) | M | Memanfaatkan kerja time travel |
| 6 | Dynamic Table (`TARGET_LAG`) | Snowflake | 🟡 (async MV) | M–L | Pipeline deklaratif di atas MV yang sudah ada |
| 7 | `QUALIFY` penuh, `SELECT * REPLACE/RENAME/ILIKE`, `ORDER BY ALL`, `UNPIVOT` | Snowflake/Databricks | 🟡/❌ | S each | Dialect quick wins, risiko rendah |
| 8 | `AUTO CDC ... INTO` (SCD1/SCD2) | Databricks | ❌ | L | CDC deklaratif; PK table + `merge_condition` cocok |
| 9 | Expectations / `CHECK` constraint | Databricks/Snowflake | ❌ | M | Data quality di jalur ingest |
| 10 | Masking policy & row access policy | Snowflake/Databricks | ❌ | L | Governance, syarat enterprise |

---

## Daftar Isi

1. [Change Data Capture & Pipeline](#1-change-data-capture--pipeline)
2. [Manajemen Tabel & Storage](#2-manajemen-tabel--storage)
3. [Dialect SQL](#3-dialect-sql)
4. [Data Quality](#4-data-quality)
5. [Governance & Security](#5-governance--security)
6. [Orkestrasi & Observability](#6-orkestrasi--observability)
7. [Sudah Ada di StarRocks (tidak perlu dibuat)](#7-sudah-ada-di-starrocks-tidak-perlu-dibuat)
8. [Roadmap Bertahap](#8-roadmap-bertahap)
9. [Ketergantungan Antar Fitur](#9-ketergantungan-antar-fitur)
10. [Referensi](#10-referensi)
11. [Lampiran A. Spesifikasi Desain Stream](#lampiran-a-spesifikasi-desain-stream-standard--append-only)

---

## 1. Change Data Capture & Pipeline

### 1.1 Stream (Snowflake) / Change Data Feed (Delta Lake) — ❌ · L–XL · Prioritas TINGGI

> **Keputusan desain:** stream di StarRocks wajib mendukung **standard stream** (INSERT, UPDATE, DELETE) sebagai mode utama, sama seperti default di Snowflake. `APPEND_ONLY` hanya jalur cepat (opsi), bukan batasan.
>
> **Prinsip kompatibilitas:** syntax, opsi, kolom output, aturan offset, dan kolom `SHOW STREAMS` **sama dengan Snowflake**, supaya user yang migrasi tidak perlu belajar ulang. Ekstensi StarRocks bersifat opt-in, dan setiap perbedaan yang tidak bisa dihindari dicatat di [Lampiran A.17](#a17-matriks-kompatibilitas-dengan-snowflake).

#### Cara kerja di Snowflake

```sql
CREATE STREAM orders_s ON TABLE orders;                    -- standard: insert + update + delete (default)
CREATE STREAM orders_ins ON TABLE orders APPEND_ONLY = TRUE;
CREATE STREAM s0 ON TABLE orders SHOW_INITIAL_ROWS = TRUE;  -- konsumsi pertama = hanya baris saat stream dibuat
CREATE STREAM s1 ON TABLE orders AT (TIMESTAMP => '2026-11-20 00:00'::timestamp);  -- offset awal dari time travel

SELECT * FROM orders_s;
-- kolom tabel + METADATA$ACTION ('INSERT'/'DELETE'), METADATA$ISUPDATE (BOOLEAN), METADATA$ROW_ID (stabil)

MERGE INTO orders_dim d USING orders_s s ON d.id = s.id ...;   -- DML yang membaca stream → offset maju saat commit
```

- Stream **tidak menyimpan data**, hanya **offset** (versi tabel). Saat dibaca, Snowflake menghitung selisih antara snapshot di offset dan snapshot sekarang dengan metadata micro-partition + kolom change-tracking tersembunyi (aktif otomatis lewat `CHANGE_TRACKING = TRUE` saat stream pertama dibuat).
- Hasilnya **net change** antara dua snapshot:
  - baris baru → `INSERT`;
  - baris hilang → `DELETE`;
  - baris berubah → **pasangan** `DELETE` (nilai lama) + `INSERT` (nilai baru), keduanya `METADATA$ISUPDATE = TRUE`;
  - baris yang di-insert lalu di-delete di antara dua offset → **tidak muncul**; baris yang di-insert lalu di-update → satu `INSERT` (`ISUPDATE = FALSE`); baris yang di-update berkali-kali → hanya nilai di offset vs nilai akhir.
- **Append-only** hanya mencatat insert, termasuk baris yang kemudian dihapus (contoh resmi: insert 10 baris, delete 5 → stream mengembalikan 10). UPDATE, DELETE, dan TRUNCATE tidak dicatat.
- **Offset hanya maju jika stream dikonsumsi di dalam DML yang commit** (termasuk CTAS dan `COPY INTO <location>`). `SELECT` biasa tidak memajukan offset, bahkan di dalam transaksi eksplisit. Di dalam transaksi, stream dibaca pada **waktu awal transaksi** (*repeatable read*), DML yang memakai stream **mengunci** stream sampai transaksi selesai, dan offset maju ke waktu awal transaksi saat commit.
- Stream menahan data history. Jika offset lebih tua dari retensi time travel, stream menjadi **stale**. Snowflake memperpanjang retensi otomatis sampai 14 hari (`MAX_DATA_EXTENSION_TIME_IN_DAYS`).

#### Cara kerja di Databricks (Change Data Feed)

```sql
ALTER TABLE orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
SELECT * FROM table_changes('orders', 5, 10);
-- _change_type: insert | update_preimage | update_postimage | delete; _commit_version; _commit_timestamp
```

- Delta menulis file `_change_data/` berisi pre/post image untuk operasi update/delete/merge. Insert murni dibaca langsung dari file data baru. Hasilnya **per commit** (bukan net), konsumen menyimpan checkpoint versi sendiri.

#### Bagaimana StarRocks menyimpan perubahan (yang jadi bahan baku stream)

Dicek di `gensrc/proto/olap_file.proto` dan `gensrc/proto/lake_types.proto`:

| Tipe tabel | INSERT | UPDATE | DELETE | Artefak per versi |
|---|---|---|---|---|
| **Primary Key** | rowset baru | rowset baru berisi image baru (row lama ditandai di DelVec); partial update *column mode* → delta column group (`num_update_files`, `dcg_lists`) | del file berisi key yang dihapus (`num_delete_files`; lake: `del_files`) | rowset + del file + DCG per versi |
| **Unique Key** (merge-on-read) | rowset baru | rowset baru dengan key sama, versi terbaru menang saat baca | rowset dengan `delete_predicate` | rowset per versi |
| **Aggregate Key** | rowset baru | rowset baru, nilai di-agregasi saat baca | `delete_predicate` | rowset per versi |
| **Duplicate Key** | rowset baru | tidak didukung | `delete_predicate` (berbasis kondisi) | rowset per versi |
| Semua | `INSERT OVERWRITE` membuat **temporary partition baru lalu swap** (partition id berubah), `TRUNCATE`/`DROP PARTITION` menghapus partisi utuh | | | metadata FE |

Kesimpulan penting: StarRocks **tidak menyimpan before-image**, tetapi menyimpan **artefak per versi** yang cukup untuk mengetahui **key mana yang tersentuh** di rentang versi. Ditambah time travel (`feature/time-travel`, yang sudah mencatat kapan tiap versi partisi visible dan menahan versi lama di BE), before-image bisa dibaca ulang dari snapshot lama.

#### Desain inti: *key-scoped snapshot diff*

Membaca stream dengan offset `v1` (per partisi) terhadap snapshot `v2` (dikunci saat statement mulai):

1. **Kumpulkan himpunan key yang tersentuh `K`** dari artefak versi di rentang `(v1, v2]`, hanya membaca kolom key:
   - PK: key dari rowset baru + key di del file + key dari baris yang disentuh DCG (rowid → key).
   - Unique/Aggregate: key dari rowset baru; untuk `delete_predicate` di rentang, key = baris di snapshot `v1` yang memenuhi predikat.
2. **Baca dua snapshot, dibatasi `K`:** `before = snapshot@v1 ⋉ K`, `after = snapshot@v2 ⋉ K` (scan time travel; `K` dikirim sebagai runtime filter / IN-filter sehingga PK index dan zone map memotong scan menjadi lookup).
3. **`FULL OUTER JOIN before, after ON key`**, ditambah penanda apakah key itu di-delete di rentang (`deleted_in_range`):

   | before | after | Output |
   |---|---|---|
   | – | ada | `INSERT`, `ISUPDATE = FALSE` (termasuk insert lalu update) |
   | ada | – | `DELETE` nilai saat offset |
   | ada | ada, tidak di-delete di rentang | `DELETE`(lama) + `INSERT`(baru), `ISUPDATE = TRUE`, termasuk jika nilainya sama (baris ditulis ulang) |
   | ada | ada, di-delete lalu di-insert lagi | `DELETE`(lama) + `INSERT`(baru), `ISUPDATE = FALSE` (dua baris berbeda, seperti di Snowflake) |
   | – | – | tidak ada (insert lalu delete di rentang yang sama) |

   Semantik **net change** Snowflake didapat otomatis. Aturan lengkap per tipe tabel ada di Lampiran A.7.
4. **Biaya sebanding jumlah perubahan, bukan ukuran tabel.** Kalau `K` sangat besar (misal `UPDATE` seluruh tabel), plan yang sama berubah menjadi full diff dua snapshot. Tetap benar, hanya lebih mahal.

Semua ini direncanakan sebagai **query biasa** di optimizer (scan khusus key + 2 scan time travel + join + proyeksi), jadi memakai join, runtime filter, dan paralelisme BE yang sudah ada. Komponen BE baru hanya satu mode scan: **`ChangeKeyScan`** (baca rowset/del file/DCG dengan rentang versi `(v1, v2]` dan keluarkan key saja).

#### Penanganan khusus

- **Duplicate Key (tanpa key).** `INSERT` = baris dari rowset di `(v1, v2]` yang tidak terkena `delete_predicate` yang commit setelahnya di rentang yang sama. `DELETE` = baris di snapshot `v1` yang terkena `delete_predicate` di rentang. Untuk `METADATA$ROW_ID` yang stabil, tabel bisa mengaktifkan (opt-in) property `change_tracking_row_id = true` yang menambah **kolom tersembunyi `__row_id`** (diisi saat load dari `txn_id` + urutan baris, mirip *row tracking* Delta). Tanpa itu, ROW_ID = posisi fisik yang berubah setelah compaction.
- **Aggregate Key.** Diff membandingkan nilai **setelah agregasi**. Load yang menambah `SUM` muncul sebagai `UPDATE` (nilai lama → nilai baru). Ini sesuai dengan yang dilihat user di query.
- **`INSERT OVERWRITE` / `TRUNCATE` / `DROP PARTITION`.** Offset disimpan per **partisi logis** (nama/rentang), bukan partition id. Seperti di Snowflake, `TRUNCATE` dan `INSERT OVERWRITE` = hapus semua baris lama lalu insert baris baru: stream mengeluarkan `DELETE` semua baris lama + `INSERT` semua baris baru (`ISUPDATE = FALSE`), untuk semua tipe tabel. Partisi lama harus tetap terbaca selama stream membutuhkannya: tahan di recycle bin sampai offset semua stream melewatinya.
- **Partisi baru** setelah offset → `v1 = 0`, semua baris jadi `INSERT`.
- **Schema change.** Fast schema evolution (tambah/hapus kolom) aman: output mengikuti skema saat ini, kolom baru bernilai `NULL` di before-image. Schema change yang menulis ulang tablet (ubah sort key/tipe key) membuat stream **stale** untuk offset sebelum perubahan. Ini batasan yang terdokumentasi.
- **Shared-data (lake).** Artefak per versi ada di tablet metadata + txn log per versi; vacuum harus mempertahankan versi `≥ min(offset)` semua stream.

#### Retensi, compaction, dan stale

- Property tabel **`CHANGE_TRACKING = TRUE`** (nama Snowflake), otomatis aktif saat `CREATE STREAM` pertama atau lewat `ALTER TABLE t SET CHANGE_TRACKING = TRUE`.
- **Pin versi:** FE menghitung `min_offset` per partisi dari semua stream, lalu BE tidak membuang rowset, del file, dan DCG versi `> min_offset`. Di S1 cukup memakai mekanisme time travel yang sudah ada (`TimeTravelBeConfigSyncer` menaikkan `tablet_rowset_stale_sweep_time_sec` sesuai umur offset tertua). Di S4 diganti pin presisi per tablet (`min_readable_version` yang dikirim FE ke BE).
- **Compaction tetap jalan.** Rowset input yang dibutuhkan stream disimpan sebagai *stale rowset* sampai tidak lagi di-pin. Masalahnya: kalau compaction menggabungkan rowset **melintasi batas `v1`**, rowset hasil tidak bisa dipakai untuk menghitung `K`, jadi tetap perlu rowset lama.
- **Optimasi (fase lanjut): change-key log.** Saat commit tabel ber-change-tracking, BE menulis file kecil berisi key yang tersentuh per rowset. Untuk PK, key ini sudah ada di tangan saat apply di `RowsetUpdateState`. Dengan ini `K` dihitung tanpa membaca rowset lama dan tidak bergantung pada compaction; yang perlu ditahan hanya snapshot `v1` untuk before-image.
- **Stale (sama dengan Snowflake):** `stale_after = waktu baca terakhir + max(DATA_RETENTION_TIME_IN_DAYS, MAX_DATA_EXTENSION_TIME_IN_DAYS)`. Retensi diperpanjang otomatis sampai offset stream, maksimal `MAX_DATA_EXTENSION_TIME_IN_DAYS` (default 14). `SYSTEM$STREAM_HAS_DATA` yang mengembalikan `FALSE` ikut memperbarui `stale_after`. Jika data benar-benar sudah dibuang, pembacaan gagal dan stream harus dibuat ulang.

#### Konsumsi transaksional (exactly-once), sama dengan Snowflake

- DML yang membaca stream **mengunci** stream sampai transaksinya selesai; konsumen lain menunggu (batas waktu `lock_wait_timeout`). Stream dibaca pada **waktu awal transaksi**, jadi semua statement dalam satu `BEGIN WORK ... COMMIT` melihat data yang sama.
- Offset baru dicatat di **`TxnCommitAttachment`** baru (`StreamTxnCommitAttachment`) dan diterapkan atomik saat commit di `DatabaseTransactionMgr`. Polanya sama seperti Routine Load menyimpan offset Kafka di `RLTaskTxnCommitAttachment`. Rollback tidak memajukan offset.
- `SELECT` biasa tidak memajukan offset dan tidak mengambil lock.

#### Objek & syntax target (ringkas; lengkap di Lampiran A.3)

```sql
-- Sama dengan Snowflake
CREATE [OR REPLACE] STREAM [IF NOT EXISTS] s [COPY GRANTS] ON TABLE t
  [{AT | BEFORE} (TIMESTAMP => ... | OFFSET => ... | STATEMENT => ... | STREAM => '...')]
  [APPEND_ONLY = TRUE | FALSE]
  [SHOW_INITIAL_ROWS = TRUE | FALSE]
  [COMMENT = '...'];
CREATE STREAM s2 CLONE s;   CREATE OR ALTER STREAM s ON TABLE t COMMENT = '...';
ALTER STREAM s SET COMMENT = '...';   DROP STREAM s;   SHOW [TERSE] STREAMS [LIKE ...] [IN ...];   DESC STREAM s;
ALTER TABLE t SET CHANGE_TRACKING = TRUE;
SELECT * FROM s;                        -- kolom tabel + METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID
SELECT SYSTEM$STREAM_HAS_DATA('s');     -- FE-only, tanpa query BE
SELECT * FROM t CHANGES (INFORMATION => DEFAULT) AT (TIMESTAMP => ...) END (TIMESTAMP => ...);

-- Ekstensi StarRocks (opt-in)
CREATE STREAM s ON TABLE t UPSERT_ONLY = TRUE;             -- PK/Unique/Agg: baris yang ditulis & masih ada
CREATE STREAM s ON TABLE t IGNORE_PARTITION_DROP = TRUE;   -- drop partisi oleh TTL tidak menghasilkan DELETE
SHOW STREAM OFFSETS s;
SELECT * FROM table_changes('t', 5, 10);                   -- gaya Databricks: per-commit, bukan net
```

> Spesifikasi lengkap (syntax, contoh perilaku untuk test, metadata, rencana eksekusi per tipe tabel, perubahan BE, transaksi, retensi, interaksi DDL, rencana pengujian, peta file) ada di **[Lampiran A](#lampiran-a-spesifikasi-desain-stream-standard--append-only)**.

#### Tahapan implementasi stream

| Tahap | Cakupan | Catatan |
|---|---|---|
| **S1** | Objek stream + offset + konsumsi transaksional; **standard stream untuk PK table** (key-scoped diff, termasuk del file & partial update); `APPEND_ONLY` untuk semua tipe; `UPSERT_ONLY` untuk PK; `IGNORE_PARTITION_DROP`; pin versi lewat config global | Nilai terbesar: PK table adalah tempat UPDATE/DELETE terjadi |
| **S2** | Standard stream untuk Unique & Aggregate Key; `INSERT OVERWRITE`/`TRUNCATE` per partisi logis | |
| **S3** | Duplicate Key dengan `delete_predicate` + kolom tersembunyi `__row_id` (opt-in) | |
| **S4** | Pin versi presisi (`min_readable_version`), change-key log, shared-data, `table_changes()` per-commit, stream on view | Optimasi & cakupan |


---

### 1.2 `CHANGES` clause (Snowflake) — ❌ · M · Prioritas SEDANG

```sql
SELECT * FROM orders
  CHANGES (INFORMATION => DEFAULT)          -- atau APPEND_ONLY
  AT (TIMESTAMP => '2026-11-01 00:00') END (TIMESTAMP => '2026-11-02 00:00');
```

- Sama seperti stream, tetapi **tanpa objek dan tanpa offset**: rentang versi diberikan langsung di query. Berguna untuk audit dan backfill.
- Di StarRocks dibangun di atas mesin *key-scoped snapshot diff* stream (1.1) + `queryPeriod` time travel. `INFORMATION => DEFAULT` (insert/update/delete) dan `APPEND_ONLY` didukung (grammar sudah punya `queryPeriod` di `StarRocks.g4:2613`). Implementasinya hampir gratis setelah 1.1 selesai.

---

### 1.3 Triggered Task dari Stream — ❌ (task graph 🔨) · S–M · Prioritas TINGGI

**Cara kerja di Snowflake**

```sql
CREATE TASK load_hist
  WAREHOUSE = wh
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('orders_s')
AS
  INSERT INTO orders_hist SELECT * FROM orders_s;

-- Triggered task (tanpa SCHEDULE): jalan otomatis begitu stream ada data
CREATE TASK t2 WHEN SYSTEM$STREAM_HAS_DATA('orders_s') AS ...;
```

- Kondisi `WHEN` dievaluasi di **cloud services layer tanpa menyalakan warehouse**; jika false, run dilewati (status `SKIPPED`) dan tidak ada biaya compute.
- Triggered task (GA 2025) tidak butuh `SCHEDULE`: Snowflake memantau stream dan menjalankan task maksimal sekali per ~30 detik (`USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS`). Bisa di-trigger oleh beberapa stream (OR).
- `EXECUTE TASK t USING CONFIG = '...'` (Jan 2026) untuk config dinamis per run; `SYSTEM$GET_TASK_GRAPH_CONFIG`.

**Sketsa implementasi di StarRocks**

- Branch `feature/snowflake-task` sudah memiliki task graph (`AFTER`, kondisi, finalizer). Tambahkan:
  - Branch `feature/snowflake-task` sudah punya `WHEN SYSTEM$PREDECESSORS_STATE(...)` untuk child task. Perluas kondisi `WHEN` agar menerima `SYSTEM$STREAM_HAS_DATA(...)` (digabung dengan `AND`/`OR`) juga untuk root task. Evaluasi di FE: jika ekspresi hanya berisi `SYSTEM$STREAM_HAS_DATA(...)`, cukup cek metadata (offset stream vs visible version partisi tabel). Murah, tanpa query BE.
  - Fungsi FE-only `SYSTEM$STREAM_HAS_DATA` (constant-folding di analyzer).
  - Mode **triggered**: `TaskManager` mendaftarkan listener pada commit transaksi tabel dasar stream (hook di `DatabaseTransactionMgr.finishTransaction`) → enqueue task dengan debounce minimal N detik.
  - Status run `SKIPPED` di `TaskRunStatus`.

---

### 1.4 Dynamic Table (Snowflake) — 🟡 · M–L · Prioritas SEDANG-TINGGI

**Cara kerja di Snowflake**

```sql
CREATE OR REPLACE DYNAMIC TABLE daily_sales
  TARGET_LAG = '10 minutes'            -- atau DOWNSTREAM
  WAREHOUSE = wh
  REFRESH_MODE = AUTO                  -- AUTO | FULL | INCREMENTAL | ADAPTIVE (2026)
  INITIALIZE = ON_CREATE
AS
  SELECT d, region, SUM(amount) FROM orders GROUP BY ALL;
```

- **Deklaratif**: user menulis query hasil akhir, Snowflake mengurus refresh. `TARGET_LAG` = batas maksimal tabel boleh tertinggal dari sumber. Scheduler memilih kapan refresh agar lag terpenuhi.
- `TARGET_LAG = DOWNSTREAM`: tabel hanya di-refresh jika ada dynamic table di hilirnya yang butuh. Seluruh DAG di-refresh secara **konsisten pada snapshot yang sama**.
- Incremental refresh memanfaatkan change tracking (mesin yang sama dengan stream). 2026 menambah: `ADAPTIVE` mode, primary key, frozen regions (bagian yang tidak di-refresh ulang), `CREATE OR ALTER DYNAMIC TABLE`, `EXPLAIN CHANGES` untuk memprediksi apakah refresh akan incremental.
- Snowflake merekomendasikan migrasi dari pola stream+task ke dynamic table untuk transformasi sederhana; stream+task tetap untuk logika prosedural.

**Kondisi StarRocks**

- Async MV sudah kuat: refresh per partisi, `REFRESH ASYNC EVERY (INTERVAL ...)`, nested MV, `refresh_mode` termasuk incremental (IVM, terutama untuk sumber append-only seperti Iceberg), query rewrite.
- Yang kurang: **semantik target lag** (scheduler berbasis lag, bukan interval tetap), **refresh DAG yang konsisten** (upstream dulu, satu snapshot), `DOWNSTREAM`, dan observability (lag aktual, alasan tidak incremental).

**Sketsa implementasi**

- `CREATE DYNAMIC TABLE` = sugar di atas `CREATE MATERIALIZED VIEW` dengan property `target_lag`. Hindari engine baru.
- Scheduler baru di `MVActiveChecker`/`TaskManager`: hitung `lag = now - lastRefreshSnapshotTime`; jadwalkan refresh saat `lag > target_lag - estimasiDurasiRefresh`.
- Saat refresh node, telusuri upstream MV/DT (graph sudah ada di `MvId` relations), refresh mereka dulu dengan snapshot timestamp yang sama.
- `SHOW DYNAMIC TABLES` dengan kolom `target_lag`, `actual_lag`, `refresh_mode_resolved`, `last_refresh_reason`.
- Setelah stream (1.1) ada, IVM untuk tabel native (termasuk sumber yang di-update/delete) bisa memakai change set standard stream, sehingga incremental refresh tidak terbatas pada sumber append-only.

---

### 1.5 Streaming Table + `AUTO CDC ... INTO` (Databricks Lakeflow) — ❌ · L · Prioritas SEDANG

**Cara kerja di Databricks**

```sql
CREATE OR REFRESH STREAMING TABLE customers;

CREATE FLOW customers_cdc AS AUTO CDC INTO customers
  FROM STREAM(cdc_raw)
  KEYS (customer_id)
  APPLY AS DELETE WHEN op = 'D'
  SEQUENCE BY event_ts                 -- menangani event out-of-order
  COLUMNS * EXCEPT (op, event_ts)
  STORED AS SCD TYPE 2;                -- atau SCD TYPE 1
```

- **Streaming table** memproses setiap record sumber tepat sekali (checkpoint). **AUTO CDC** (dulu `APPLY CHANGES INTO`) mengubah aliran event CDC menjadi tabel target dengan menangani urutan, deduplikasi, delete, dan SCD Type 2 (`__START_AT`, `__END_AT`) otomatis.
- 2026: SCD1 lebih sederhana, bitemporal tracking, `IGNORE NULL UPDATES` per kolom, `AUTO CDC FROM SNAPSHOT` (membandingkan snapshot berurutan).

**Pemetaan ke StarRocks**

- **SCD Type 1 hampir sudah ada**: PK table + partial update + `merge_condition` (update hanya jika kolom sequence lebih baru) sudah tersedia di Stream Load / Routine Load. Yang kurang hanya SQL deklaratif.
- **SCD Type 2** perlu logika: tutup versi lama (`__END_AT = seq`) dan insert versi baru. Bisa diimplementasikan sebagai rewrite ke `MERGE INTO` (item 3.1) yang dijalankan oleh task/pipe.

**Sketsa**

- Statement baru `CREATE CDC FLOW f INTO target FROM source KEYS(...) SEQUENCE BY ... [APPLY AS DELETE WHEN ...] STORED AS SCD TYPE {1|2}`.
- Sumber: stream (1.1), Routine Load (Kafka), atau `PIPE`. Eksekusi = task terjadwal/triggered yang membaca batch baru, deduplikasi per key dengan `ROW_NUMBER() OVER (PARTITION BY key ORDER BY seq DESC)`, lalu `MERGE`.

---

### 1.6 `REPLACE WHERE` (Databricks) — ❌ · M · Prioritas SEDANG

```sql
INSERT INTO events REPLACE WHERE event_date >= '2026-11-01'
SELECT * FROM staging_events;
```

- Atomically menghapus baris yang memenuhi predikat lalu insert hasil query (selective overwrite). Jauh lebih fleksibel dari overwrite per partisi.
- StarRocks punya `INSERT OVERWRITE` (partisi, termasuk dynamic overwrite). Untuk predikat sembarang: implementasikan sebagai `DELETE WHERE pred` + `INSERT` dalam **satu transaksi** (memakai infrastruktur `BEGIN WORK` yang sudah ada) untuk PK table; untuk Duplicate table, batasi pada predikat yang sejajar dengan partisi.

---

### 1.7 Snowpipe / Auto Loader — 🟡 · Prioritas RENDAH (sudah sebagian)

- Snowflake Snowpipe: `CREATE PIPE ... AUTO_INGEST = TRUE AS COPY INTO t FROM @stage` dipicu notifikasi object storage (S3 event). Databricks Auto Loader (`cloudFiles`) menambah **schema inference & evolution** dan kolom `_rescued_data` untuk data yang tidak cocok skema.
- StarRocks sudah punya `CREATE PIPE ... AS INSERT INTO t SELECT * FROM FILES(...)` dengan polling. Gap: event notification (SQS/PubSub), **schema evolution otomatis** (tambah kolom baru), dan **rescued data column**. Kandidat peningkatan kecil, bukan fitur baru.

---

## 2. Manajemen Tabel & Storage

### 2.1 Zero-Copy Clone — ❌ · L · Prioritas TINGGI

**Cara kerja di Snowflake**

```sql
CREATE TABLE orders_dev CLONE orders;
CREATE TABLE orders_snap CLONE orders AT (TIMESTAMP => '2026-11-20 08:00'::timestamp);
CREATE SCHEMA analytics_dev CLONE analytics;
CREATE DATABASE prod_copy CLONE prod;
```

- Data Snowflake tersimpan di **micro-partition immutable**. Clone hanya menyalin **metadata** (daftar micro-partition) ke objek baru, sehingga instan dan tanpa biaya storage awal.
- Setelah clone, kedua tabel independen: perubahan di salah satu menulis micro-partition baru (copy-on-write); micro-partition bersama tetap dipakai keduanya. Storage dihitung per micro-partition yang dimiliki.
- Bisa digabung dengan time travel (`AT`/`BEFORE`). Clone schema/database ikut mengkloning task (dalam keadaan suspended), stream, dll.

**Cara kerja di Databricks**

- `CREATE TABLE t2 SHALLOW CLONE t1 [VERSION AS OF n]`: menyalin transaction log saja, file data direferensikan. `DEEP CLONE`: menyalin file data + metadata, bisa incremental (sync ulang hanya file baru).

**Kondisi StarRocks**

- `CREATE TABLE LIKE` hanya menyalin skema. Tidak ada clone data.
- Segment/rowset file di BE **immutable**, jadi secara konsep clone metadata memungkinkan.

**Sketsa implementasi**

- **Shared-data (lake) dulu: lebih mudah.**
  - Tablet metadata (`TabletMetadataPB`) mereferensikan file segment di object storage. Clone = buat tabel baru dengan layout partisi/tablet identik, lalu tulis tablet metadata baru yang **menunjuk ke file yang sama**.
  - Masalah utama: **GC/vacuum**. Vacuum saat ini menghapus file yang tidak direferensikan oleh metadata tabletnya sendiri. Perlu reference counting / "shared file registry" sehingga file tidak dihapus selama masih dipakai clone. Opsi lebih sederhana: tandai file clone dengan prefix tabel sumber dan vacuum memeriksa semua tabel yang mereferensikan prefix tersebut.
- **Shared-nothing: hard link.**
  - FE membuat tabel baru (tablet id baru, mapping 1:1 ke tablet sumber pada BE yang sama untuk setiap replika).
  - RPC BE baru `clone_tablet_by_link(src_tablet, dst_tablet, version)`: hard link file segment ke direktori tablet baru dengan rowset id baru, tulis rowset meta baru. Mekanisme mirip *linked schema change* / snapshot pada backup yang sudah memakai hard link (`SnapshotManager`).
  - PK table: index primary juga perlu dibangun ulang atau disalin (persistent index file juga bisa di-link lalu di-copy-on-write).
- `AT (TIMESTAMP => ...)` memakai versi dari time travel.
- Clone database/schema = iterasi tabel + view + MV (MV di-clone dalam keadaan inactive).

---

### 2.2 `UNDROP` — ✅ (sebagai `RECOVER`) · S · Prioritas RENDAH

- Snowflake: `UNDROP TABLE/SCHEMA/DATABASE`. StarRocks sudah punya `RECOVER TABLE/DATABASE/PARTITION` dari recycle bin. Cukup tambahkan **alias `UNDROP`** di grammar untuk kompatibilitas dialek dan `SHOW TABLES HISTORY` (Snowflake) → tampilkan isi recycle bin.

---

### 2.3 `DESCRIBE HISTORY` & `RESTORE TABLE TO VERSION` (Databricks) — ❌ · M · Prioritas SEDANG

```sql
DESCRIBE HISTORY orders;                          -- versi, timestamp, operasi, user, metrics
RESTORE TABLE orders TO TIMESTAMP AS OF '2026-11-20';
RESTORE TABLE orders TO VERSION AS OF 42;
```

- Delta menyimpan setiap commit di `_delta_log` sehingga history dan restore murah. Iceberg punya `rollback_to_snapshot`.
- Di StarRocks: riset time travel (`RISET_FITUR_SNOWFLAKE.md` §1.2.g) menyebut **histori versi↔waktu belum ada**. Buat tabel sistem `information_schema.table_history` (atau `_statistics_`) yang mencatat `(table_id, partition_id, version, commit_time, txn_id, label, operation, user, rows)` dari `finishTransaction`. `DESCRIBE HISTORY` membaca tabel itu.
- `RESTORE` = buat versi baru yang isinya sama dengan versi lama (clone in-place per partisi), bukan memundurkan versi, sehingga tetap bisa di-undo.

---

### 2.4 Clustering otomatis / Liquid Clustering — 🟡 · L · Prioritas RENDAH-SEDANG

- Snowflake: `ALTER TABLE t CLUSTER BY (a, b)` + Automatic Clustering service yang me-recluster di background berdasarkan *clustering depth*. Databricks: **Liquid Clustering** `CLUSTER BY (a,b)` / `CLUSTER BY AUTO` (memilih kunci berdasarkan pola query), `OPTIMIZE` incremental, dan Z-ORDER.
- StarRocks: sort key (`ORDER BY` di DDL), compaction, dan bisa mengubah sort key. Belum ada: **clustering berdasarkan kurva Z-order/Hilbert multi-kolom**, metrik *clustering depth*, dan recluster otomatis.
- Ide: `OPTIMIZE t ZORDER BY (a, b)` sebagai job compaction khusus; fungsi `SYSTEM$CLUSTERING_INFORMATION('t')` dari zone map. `CLUSTER BY AUTO` butuh statistik predikat dari query history; jadikan item jangka panjang.

---

### 2.5 `CREATE OR ALTER TABLE` (Snowflake, DDL deklaratif) — ❌ · M · Prioritas SEDANG

```sql
CREATE OR ALTER TABLE orders (id INT, amount DECIMAL(18,2), note STRING);
```

- Jika tabel belum ada → create; jika ada → Snowflake menghitung diff terhadap definisi dan menjalankan `ALTER` (add/drop column, ubah komentar, default) **tanpa kehilangan data**. Sangat membantu untuk CI/CD dan dbt (idempoten).
- Di StarRocks: di analyzer, bandingkan `CreateTableStmt` dengan skema yang ada, hasilkan daftar `AlterClause` (memakai fast schema evolution yang sudah ada). Tolak perubahan yang tidak bisa dilakukan (ubah key column, distribusi) dengan pesan jelas.

---

### 2.6 Sequence object — ❌ · S–M · Prioritas RENDAH

- Snowflake: `CREATE SEQUENCE s START 1 INCREMENT 1; SELECT s.NEXTVAL;`. StarRocks sudah punya `AUTO_INCREMENT` per kolom; sequence global berguna untuk surrogate key lintas tabel. Implementasi FE: generator ID dengan alokasi blok (mirip `AutoIncrementInfo`).

---

### 2.7 Schema evolution saat load — 🟡 · M · Prioritas SEDANG

- Snowflake: `ALTER TABLE t SET ENABLE_SCHEMA_EVOLUTION = TRUE` → `COPY INTO` menambah kolom baru otomatis; `INFER_SCHEMA` + `CREATE TABLE ... USING TEMPLATE`. Databricks: `MERGE WITH SCHEMA EVOLUTION`, `mergeSchema`.
- StarRocks: `FILES()` sudah meng-infer skema, `INSERT ... BY NAME` sudah ada. Tambahkan property tabel `enable_schema_evolution` yang membuat `INSERT ... BY NAME`/PIPE menambah kolom yang belum ada (lewat fast schema evolution) alih-alih error.

---

## 3. Dialect SQL

### 3.1 `MERGE INTO` untuk tabel native (PK) — 🟡 · M · Prioritas TERTINGGI

**Semantik Snowflake/Databricks**

```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.updated_at = s.ts
WHEN NOT MATCHED THEN INSERT (id, amount, updated_at) VALUES (s.id, s.amount, s.ts)
-- Databricks tambahan:
WHEN NOT MATCHED BY SOURCE THEN DELETE;
-- Databricks: UPDATE SET * / INSERT *; MERGE WITH SCHEMA EVOLUTION
```

- Klausa dievaluasi berurutan; baris pertama yang cocok menang. Jika satu baris target cocok dengan banyak baris sumber → error (Snowflake: `ERROR_ON_NONDETERMINISTIC_MERGE = TRUE`, Databricks: error `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`).

**Kondisi StarRocks**

- Grammar `mergeIntoStatement` sudah lengkap untuk `WHEN MATCHED [AND] UPDATE/DELETE` dan `WHEN NOT MATCHED [AND] INSERT * / INSERT VALUES` (`StarRocks.g4:1386-1406`).
- `MergeIntoAnalyzer` menolak non-Iceberg; `MergeIntoPlanner` menghasilkan `IcebergRowDeltaSink`.

**Sketsa implementasi untuk PK table**

1. Rewrite: `source LEFT/FULL OUTER JOIN target ON cond` → proyeksi `CASE` per klausa yang menghasilkan kolom tujuan + kolom `__op` (0 = upsert, 1 = delete) + penanda "tidak ada aksi" yang di-filter.
2. Sink: `OlapTableSink` ke PK table dengan kolom `__op` (mekanisme yang sama dengan Stream Load upsert/delete). UPDATE parsial → gunakan **partial update** (column mode) jika hanya sebagian kolom di-SET.
3. Deteksi multi-match: tambahkan agregasi `COUNT(*) OVER (PARTITION BY target_row_id)` > 1 → raise error (opsional lewat session variable).
4. Tambahkan `WHEN NOT MATCHED BY SOURCE` (butuh FULL/RIGHT join) dan `UPDATE SET *`.
5. Duplicate/Aggregate key table: tolak (tidak punya semantik update), atau dukung hanya `INSERT`.

Effort M karena parser, AST, dan banyak analisis sudah ada; risiko utama di partial update dan konsistensi transaksi.

---

### 3.2 `QUALIFY` penuh — 🟡 · S · Prioritas TINGGI

```sql
SELECT * FROM events
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) = 1
    AND amount > 0;
```

- Snowflake/Databricks: `QUALIFY` adalah filter setelah window function, menerima **ekspresi boolean bebas** dan bisa merujuk alias select.
- StarRocks: hanya `QUALIFY <selectItem> <cmp> <INTEGER>` (`StarRocks.g4:2565`), biasanya untuk top-N.
- Implementasi: ubah grammar menjadi `QUALIFY expression`; di analyzer, kumpulkan window function di ekspresi QUALIFY, tambahkan ke projection tersembunyi, lalu pasang `Filter` di atas `Window` (sama seperti `HAVING` di atas agregasi). Pola top-N lama tetap dioptimasi oleh rule `PartitionTopN` yang sudah ada.

---

### 3.3 `SELECT *` modifiers: `REPLACE`, `RENAME`, `ILIKE` — 🟡 · S · Prioritas TINGGI

```sql
SELECT * EXCLUDE (password) RENAME (id AS user_id) FROM users;        -- Snowflake
SELECT * REPLACE (UPPER(name) AS name) FROM users;                     -- Snowflake/BigQuery
SELECT * ILIKE '%_id' FROM users;                                      -- Snowflake
```

- StarRocks sudah punya `EXCLUDE/EXCEPT` (`excludeClause`, `StarRocks.g4:2599`). Tambahkan `replaceClause`, `renameClause`, `ILIKE 'pattern'` di grammar dan ekspansi di `SelectAnalyzer` saat `*` di-expand. Murah dan sangat disukai user.

---

### 3.4 `ORDER BY ALL` — ❌ · S · Prioritas SEDANG

- Snowflake & Databricks: urutkan berdasarkan semua kolom select dari kiri ke kanan. `GROUP BY ALL` sudah ada di StarRocks; `ORDER BY ALL` tinggal ekspansi serupa.

---

### 3.5 `UNPIVOT` (dan `PIVOT` dinamis) — 🟡 · S–M · Prioritas SEDANG

```sql
SELECT * FROM monthly_sales
  UNPIVOT [INCLUDE NULLS] (sales FOR month IN (jan, feb, mar));

SELECT * FROM quarterly PIVOT (SUM(amount) FOR quarter IN (ANY ORDER BY quarter));   -- Snowflake dynamic pivot
```

- StarRocks sudah punya `PIVOT` (`pivotClause`), belum `UNPIVOT` dan pivot dinamis (`ANY` / subquery).
- `UNPIVOT` = rewrite ke `CROSS JOIN LATERAL (VALUES ...)` atau `unnest` dari array struct. Pivot dinamis butuh eksekusi dua fase (cari nilai distinct dulu); lebih kompleks.

---

### 3.6 Multi-table insert: `INSERT ALL` / `INSERT FIRST` (Snowflake) — ❌ · M · Prioritas SEDANG

```sql
INSERT ALL
  WHEN amount > 1000 THEN INTO big_orders
  WHEN amount <= 1000 THEN INTO small_orders
  ELSE INTO other_orders
SELECT * FROM staging;
```

- Satu scan sumber, banyak target. Sangat umum di pipeline bronze→silver.
- StarRocks: planner perlu fragment dengan **multiple sinks** (`MultiCastPlanFragment` sudah ada untuk CTE reuse). Bungkus semua sink dalam satu transaksi multi-tabel (infrastruktur `BEGIN WORK` sudah mendukung beberapa tabel).

---

### 3.7 Variabel, `IDENTIFIER()`, `EXECUTE IMMEDIATE` — 🟡 · S–M · Prioritas SEDANG

```sql
SET tbl = 'orders';                              -- Snowflake session variable
SELECT * FROM IDENTIFIER($tbl);
EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || $tbl;

DECLARE VARIABLE cutoff DATE DEFAULT current_date() - 7;   -- Databricks
SELECT * FROM IDENTIFIER(:tbl_name);             -- parameter marker
```

- StarRocks punya user variable `@x` dan `PREPARE/EXECUTE`. Tambahkan `IDENTIFIER(expr)` (evaluasi konstan di parser/analyzer untuk nama tabel/kolom) dan `EXECUTE IMMEDIATE <string>`. Berguna untuk script/task yang diparameterisasi dan melengkapi stored procedure di `feature/stored-procedure`.

---

### 3.8 SQL Scripting (anonymous block) — 🔨 sebagian · M · Prioritas SEDANG

- Snowflake Scripting (`BEGIN ... END`, `LET`, `IF`, `FOR`, `CURSOR`, `EXCEPTION`, `RETURN TABLE(...)`) dan Databricks SQL Scripting (`BEGIN ... END` compound statement, `BEGIN ATOMIC`).
- Sebagian sudah dikerjakan di `feature/stored-procedure` (lihat `RISET_FITUR_SNOWFLAKE.md` §3). Lanjutan: anonymous block di luar procedure (`EXECUTE IMMEDIATE $$ BEGIN ... END $$`), dan `PROCEDURE SCOPED TEMP TABLE` (Snowflake Mei 2026).

---

### 3.9 Multi-statement transaction `BEGIN ATOMIC` (Databricks) — 🟡 · M · Prioritas SEDANG

- Databricks (GA 2026): beberapa INSERT/UPDATE/DELETE/MERGE lintas tabel di-commit atomik.
- StarRocks: `BEGIN WORK ... COMMIT` sudah ada (`StarRocks.g4:2467`), tapi dukungannya terbatas (terutama INSERT; cek UPDATE/DELETE/visibility read-your-writes). Roadmap: lengkapi DML yang didukung, dukung `MERGE` di dalam transaksi, dan tambahkan sintaks `BEGIN ATOMIC ... END` sebagai alias.

---

### 3.10 Pipe syntax `|>` (Databricks/BigQuery) dan pipe operator `->>` (Snowflake) — ❌ · M · Prioritas RENDAH-SEDANG

```sql
FROM orders
|> WHERE status = 'paid'
|> AGGREGATE SUM(amount) AS total GROUP BY customer_id
|> ORDER BY total DESC
|> LIMIT 10;
```

- Query dibaca dari atas ke bawah sesuai urutan logis; mudah dikembangkan bertahap dan disukai untuk eksplorasi.
- Implementasi: aturan grammar baru yang setiap operator membungkus relasi sebelumnya sebagai subquery di `AstBuilder`. Tidak menyentuh optimizer/BE.
- Snowflake `->>` (2025) berbeda: merangkai **statement** dan merujuk hasil sebelumnya dengan `$1`; cocok dipadukan dengan scripting.

---

### 3.11 `MATCH_RECOGNIZE` — ❌ · XL · Prioritas RENDAH

- Pattern matching baris berurutan (sesi user, deteksi fraud: `PATTERN (A B+ C)`, `DEFINE`, `MEASURES`). Ada di Snowflake (Databricks tidak).
- Butuh operator BE baru (NFA per partisi terurut). Nilai tinggi untuk analitik event tetapi effort besar; tunda.

---

### 3.12 Semi-structured: path `:`, `FLATTEN`, VARIANT — 🟡 · M · Prioritas SEDANG

```sql
SELECT raw:customer.name::string, f.value:sku
FROM events, LATERAL FLATTEN(input => raw:items) f;      -- Snowflake
SELECT raw:customer.name FROM events;                     -- Databricks VARIANT juga pakai ':'
```

- StarRocks: tipe JSON, operator `->`, `json_query`, `unnest`, dan VARIANT untuk Iceberg. Gap: **sintaks path `:`** dan **`FLATTEN` table function** dengan kolom `SEQ, KEY, PATH, INDEX, VALUE, THIS`. Keduanya bisa di-rewrite ke fungsi JSON yang sudah ada (perhatian: `:` bentrok dengan parameter/variabel; aktifkan lewat `sql_dialect`).

---

### 3.13 Lain-lain dialect (cepat)

| Fitur | Asal | Status SR | Catatan |
|---|---|---|---|
| `MINUS` sebagai alias `EXCEPT` | Snowflake | ✅ (keyword ada) | Verifikasi semantik |
| `ILIKE ANY / LIKE ALL` | Snowflake | ❌ | Rewrite ke OR/AND dari LIKE |
| `IFF`, `NVL2`, `ZEROIFNULL`, `DIV0`, `EQUAL_NULL` | Snowflake | 🟡 | Fungsi skalar, mudah ditambah |
| `ARRAY_AGG ... WITHIN GROUP (ORDER BY)` | Snowflake | 🟡 | StarRocks punya `array_agg(x ORDER BY)` |
| `TRY_CAST`, `TRY_TO_NUMBER`, `TRY_TO_DATE` | Snowflake/Databricks | 🟡 | Penting untuk cleansing; banyak data engineer mengandalkan |
| `GENERATOR(ROWCOUNT => n)`, `SEQ4()` | Snowflake | 🟡 | `generate_series` sudah ada; tambah alias |
| `CREATE OR REPLACE` di semua objek | Snowflake | 🟡 | Ada untuk view/function/pipe; tambah untuk table, MV |
| Lateral column alias (pakai alias di SELECT yang sama) | Databricks/Snowflake | 🟡 | `enable_groupby_use_output_alias` hanya untuk GROUP BY |
| `COMMENT ON` / `ALTER ... SET COMMENT` | keduanya | 🟡 | Verifikasi cakupan kolom |

---

## 4. Data Quality

### 4.1 Expectations (Databricks) & `CHECK` constraint — ❌ · M · Prioritas TINGGI

```sql
-- Databricks pipelines (2026: juga untuk standalone MV)
CREATE OR REFRESH MATERIALIZED VIEW clean_orders (
  CONSTRAINT valid_id     EXPECT (id IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT pos_amount   EXPECT (amount >= 0)           ON VIOLATION FAIL UPDATE,
  CONSTRAINT recent       EXPECT (ts > '2020-01-01')     -- default: hanya dicatat (warn)
) AS SELECT * FROM raw_orders;

-- Delta / Snowflake (2026: CHECK enforced di COPY INTO & Snowpipe)
ALTER TABLE orders ADD CONSTRAINT pos_amount CHECK (amount >= 0);
```

- Expectation dievaluasi per baris saat refresh; aksi: **warn** (tetap ditulis, dihitung), **drop row**, atau **fail**. Metrik lolos/gagal per constraint dicatat ke event log pipeline sehingga bisa dibuat dashboard kualitas data.
- StarRocks saat ini hanya punya `strict_mode` dan `max_filter_ratio` (untuk error tipe/format), tanpa aturan bisnis.

**Sketsa**

- Simpan constraint di `OlapTable` (property atau objek baru) dan di definisi MV/Dynamic Table.
- Di planner INSERT/load, tambahkan `ProjectNode` yang menghitung `CASE WHEN NOT (expr) ...` per constraint:
  - `FAIL` → fungsi `raise_error()` (bisa dibuat di BE) membuat load gagal;
  - `DROP ROW` → filter;
  - `WARN` → kolom hitung yang diagregasi ke `TaskRun`/load statistics.
- Metrik ke tabel sistem `information_schema.data_quality_metrics (table, constraint, txn_id, passed, failed, ts)`.

### 4.2 Data Metric Functions (Snowflake) — ❌ · M · Prioritas RENDAH-SEDANG

- `ALTER TABLE t ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (email)` + `DATA_METRIC_SCHEDULE = '60 MINUTE'` → hasil disimpan di tabel event. Fungsi bawaan: `NULL_COUNT`, `DUPLICATE_COUNT`, `FRESHNESS`, `ROW_COUNT`, `UNIQUE_COUNT`, plus custom DMF.
- Di StarRocks dapat dibangun di atas **Task** (sudah ada): DMF = task terjadwal internal yang menjalankan query metrik dan menulis ke tabel `_statistics_.data_metrics`. Ringan karena hanya orkestrasi.

### 4.3 DML error logging (Snowflake, Mar 2026) — 🟡 · M · Prioritas SEDANG

- Baris yang gagal (tipe, constraint) pada INSERT/COPY/Snowpipe dicatat ke error table alih-alih menggagalkan seluruh statement.
- StarRocks sudah mencatat sebagian rejected record di load (`log_rejected_record_num`) dan `SHOW LOAD` error URL. Kembangkan menjadi **error table yang bisa di-query SQL** (`INSERT ... LOG ERRORS INTO err_tbl`).

---

## 5. Governance & Security

### 5.1 Masking policy & Row access policy — ❌ · L · Prioritas SEDANG-TINGGI

```sql
-- Snowflake
CREATE MASKING POLICY mask_email AS (v STRING) RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ('PII_READER') THEN v ELSE REGEXP_REPLACE(v, '.+@', '***@') END;
ALTER TABLE users MODIFY COLUMN email SET MASKING POLICY mask_email;

CREATE ROW ACCESS POLICY region_rap AS (region STRING) RETURNS BOOLEAN ->
  EXISTS (SELECT 1 FROM region_map m WHERE m.role = CURRENT_ROLE() AND m.region = region);
ALTER TABLE sales ADD ROW ACCESS POLICY region_rap ON (region);

-- Databricks Unity Catalog
ALTER TABLE sales SET ROW FILTER region_filter ON (region);
ALTER TABLE users ALTER COLUMN email SET MASK mask_email;
```

- Policy adalah objek schema yang di-attach ke kolom/tabel. Saat query, engine **menyuntikkan** ekspresi masking di setiap referensi kolom dan predikat row access di atas scan, berlaku untuk semua tool. Tag-based masking: policy di-attach ke tag, otomatis berlaku untuk semua kolom bertag.
- StarRocks: keyword `MASKING` / `POLICY` ada di daftar nonReserved, **tidak ada statement** di upstream open-source.

**Sketsa**

- Objek katalog `Policy { name, type: MASKING|ROW_ACCESS, signature, bodyExpr }` dan binding ke `(tableId, columnId)`.
- Di `QueryAnalyzer`/`RelationTransformer`, saat membentuk `TableRelation`, bungkus dengan projection yang mengganti kolom bermasker dan filter untuk row access. Penting: lakukan **sebelum** MV rewrite dan pastikan MV rewrite tidak bocorkan data (MV di atas tabel berpolicy harus mewarisi policy atau ditolak untuk rewrite).
- Fungsi konteks: `CURRENT_ROLE()`, `IS_ROLE_IN_SESSION()`, `CURRENT_USER()`.

### 5.2 Object tagging — ❌ · M · Prioritas RENDAH-SEDANG

- `CREATE TAG pii ALLOWED_VALUES 'email','phone'; ALTER TABLE users MODIFY COLUMN email SET TAG pii = 'email';` plus `TAG_REFERENCES()` untuk mencari objek bertag. Fondasi untuk tag-based masking dan klasifikasi otomatis.
- Catatan: keyword `TAG` di StarRocks sekarang dipakai untuk **Iceberg branch/tag** (`StarRocks.g4:1239`); gunakan nama statement yang tidak bentrok, mis. `CREATE OBJECT TAG` atau konteks grammar terpisah.

### 5.3 Lineage & Access History — ❌ · M · Prioritas SEDANG

- Snowflake `ACCOUNT_USAGE.ACCESS_HISTORY` (kolom yang dibaca/ditulis per query, termasuk lineage kolom ke kolom) dan Unity Catalog lineage (tabel & kolom, divisualisasikan).
- StarRocks punya audit log (statement + tabel). Tambahkan: ekstraksi **column-level lineage** dari plan yang sudah dianalisis (untuk INSERT/CTAS/MERGE/MV) dan tulis ke tabel sistem `_statistics_.lineage (query_id, src_table, src_column, dst_table, dst_column, ts)`. Sangat berguna untuk impact analysis.

---

## 6. Orkestrasi & Observability

### 6.1 Task graph & serverless task — 🔨 · selesai sebagian

- Sudah ada di branch `feature/snowflake-task`: `AFTER`, kondisi sukses/gagal, finalizer, body multi-statement. Lanjutan: `WHEN` condition (1.3), `EXECUTE TASK ... USING CONFIG`, `SUSPEND_TASK_AFTER_NUM_FAILURES`, `TASK_AUTO_RETRY_ATTEMPTS`, `TASK_HISTORY()` table function.

### 6.2 Alert & Notification Integration (Snowflake) — ❌ · M · Prioritas SEDANG

```sql
CREATE ALERT late_data
  WAREHOUSE = wh  SCHEDULE = '5 MINUTE'
  IF (EXISTS (SELECT 1 FROM ingest_log WHERE ts < CURRENT_TIMESTAMP - INTERVAL '30 minute'))
  THEN CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(... 'webhook_int' ...);
```

- Alert = task yang menjalankan query kondisi; jika mengembalikan baris, aksi dijalankan. Notification integration mendefinisikan tujuan (email, Slack/Teams webhook, SNS/PubSub).
- Di StarRocks: implementasi di atas Task (task dengan `condition query`), ditambah objek `NOTIFICATION INTEGRATION` (URL webhook + secret) dan fungsi `SYSTEM$SEND_NOTIFICATION(integration, message)` yang dieksekusi FE (HTTP POST). Juga bisa dipakai oleh task gagal (`ERROR_INTEGRATION`).

### 6.3 Query tag & resource monitor — 🟡 · S · Prioritas SEDANG

- Snowflake `ALTER SESSION SET QUERY_TAG = 'dbt:model_x'` → muncul di `QUERY_HISTORY` untuk atribusi biaya. Resource monitor: kuota kredit per warehouse dengan aksi notify/suspend.
- StarRocks: resource group dan audit log sudah ada; **belum ada session variable `query_tag`** (dicek di `SessionVariable.java`). Tambahkan variabel dan kolom di audit log / `information_schema.loads` / `task_runs`. Effort sangat kecil, dampak besar untuk observability dbt/Airflow.

### 6.4 Persisted query result cache (Snowflake) — 🟡 · M · Prioritas RENDAH-SEDANG

- Snowflake menyimpan hasil query 24 jam; query identik pada data yang tidak berubah dijawab tanpa compute. `RESULT_SCAN(LAST_QUERY_ID())` membaca hasil query sebelumnya sebagai tabel, sangat berguna di scripting (mis. memproses output `SHOW`).
- StarRocks punya query cache tingkat tablet untuk agregasi, bukan cache hasil akhir. `RESULT_SCAN` bisa diimplementasikan lebih dulu (simpan hasil terakhir per sesi di FE dengan batas ukuran) karena membantu scripting/procedure.

### 6.5 Event table / logging dari procedure — ❌ · S–M · Prioritas RENDAH

- Snowflake event table menerima log/trace dari procedure, UDF, task. Untuk StarRocks: `SYSTEM$LOG(level, msg)` di procedure → tulis ke tabel sistem; bergantung pada `feature/stored-procedure`.

---

## 7. Sudah Ada di StarRocks (tidak perlu dibuat)

Hasil pengecekan grammar `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4` dan kode FE:

| Fitur Snowflake/Databricks | Padanan StarRocks | Lokasi bukti |
|---|---|---|
| `SELECT * EXCLUDE (...)` / `* EXCEPT (...)` | ✅ sama | `StarRocks.g4:2595-2600` |
| `GROUP BY ALL` | ✅ sama | `StarRocks.g4:2564` |
| `PIVOT` | ✅ (statis) | `StarRocks.g4:2627` |
| `ASOF JOIN` | ✅ | `StarRocks.g4:2676` |
| `QUALIFY` (top-N) | 🟡 terbatas | `StarRocks.g4:2565` |
| `MERGE INTO` | 🟡 Iceberg V2 saja | `MergeIntoAnalyzer.java:74` |
| SQL UDF | ✅ `CREATE FUNCTION f(args) RETURNS expr` | `StarRocks.g4:1665` |
| Java/Python UDF, UDTF, UDAF | ✅ | `createUdfFunctionStmt` |
| `TABLESAMPLE` | ✅ `SAMPLE (...)` | `StarRocks.g4:2641` |
| Transaksi multi-statement | 🟡 `BEGIN WORK ... COMMIT` | `StarRocks.g4:2467` |
| `ALTER TABLE ... SWAP WITH` | ✅ | `StarRocks.g4:1146` |
| `UNDROP` | ✅ sebagai `RECOVER` | grammar `RECOVER` |
| Snowpipe / `COPY INTO` dari stage | ✅ `CREATE PIPE` + `FILES()` | `StarRocks.g4:2276` |
| `COPY INTO <location>` (unload) | ✅ `INSERT INTO FILES(...)` | — |
| Temporary table | ✅ | — |
| Generated column / `AUTOINCREMENT` | ✅ | — |
| `INSERT ... BY NAME` | ✅ | grammar `BY NAME` |
| Dynamic Table (dasar) | 🟡 async MV + `refresh_mode` | `PropertyAnalyzer.java:233` |
| Branch/Tag tabel (Iceberg) | ✅ untuk Iceberg | `StarRocks.g4:1239-1253` |
| Lambda / higher-order function | ✅ | — |
| Time travel `AT/BEFORE` | 🔨 `feature/time-travel` | `queryPeriod` |
| Task graph | 🔨 `feature/snowflake-task` | — |
| Stored procedure (Snowflake Scripting) | 🔨 `feature/stored-procedure` | — |

---

## 8. Roadmap Bertahap

### Fase 0 — Selesai/berjalan di fork
- Time travel (`feature/time-travel`), task graph (`feature/snowflake-task`), stored procedure (`feature/stored-procedure`).

### Fase 1 — Dialect quick wins (± 3–5 minggu total)
Risiko rendah, sebagian besar hanya grammar + analyzer, mudah di-upstream-kan.
1. `QUALIFY` ekspresi penuh (3.2)
2. `SELECT * REPLACE / RENAME / ILIKE` (3.3)
3. `ORDER BY ALL` (3.4)
4. `UNPIVOT` (3.5)
5. Alias `UNDROP` + `SHOW TABLES HISTORY` (2.2)
6. Session variable `query_tag` (6.3)
7. `IDENTIFIER()` + `EXECUTE IMMEDIATE` (3.7)
8. Fungsi kompatibilitas: `IFF`, `TRY_*`, `DIV0`, `ILIKE ANY` (3.13)

### Fase 2 — Core data engineering (± 2–3 bulan)
1. **`MERGE INTO` untuk PK table** (3.1)
2. **Standard stream untuk PK table + `APPEND_ONLY` untuk semua tipe** (1.1 tahap S1) + `SYSTEM$STREAM_HAS_DATA`
3. **Task `WHEN` + triggered task** (1.3)
4. Riwayat versi tabel: `DESCRIBE HISTORY` (2.3)
5. `CHANGES` clause (1.2)
6. `INSERT ALL / FIRST` (3.6)
7. `REPLACE WHERE` (1.6)

### Fase 3 — Fitur besar (± 3–4 bulan)
1. **Zero-copy clone**: shared-data dulu, lalu shared-nothing hard link (2.1)
2. Standard stream untuk Unique/Aggregate/Duplicate Key, partisi overwrite, change-key log (1.1 tahap S2–S4)
3. **Dynamic Table** dengan `TARGET_LAG` (1.4)
4. `RESTORE TABLE TO VERSION/TIMESTAMP` (2.3)
5. **Expectations + `CHECK` constraint** (4.1)
6. `AUTO CDC` SCD1/SCD2 (1.5)
7. `CREATE OR ALTER TABLE` (2.5)

### Fase 4 — Governance & lanjutan
1. Masking & row access policy (5.1), tagging (5.2)
2. Column-level lineage (5.3)
3. Alert + notification integration (6.2)
4. Data Metric Functions (4.2), DML error table (4.3)
5. Semi-structured `:` path + `FLATTEN` (3.12)
6. Pipe syntax `|>` (3.10), `RESULT_SCAN` (6.4)
7. Jangka panjang: `MATCH_RECOGNIZE` (3.11), clustering otomatis / Z-order (2.4)

---

## 9. Ketergantungan Antar Fitur

```
Time travel (🔨) ──┬──> CHANGES clause ──┐
                   ├──> DESCRIBE HISTORY ─┼──> RESTORE TABLE
                   ├──> Clone ... AT(...) │
                   └──> Stream standard (before-image dari snapshot lama; offset mem-pin versi)
                                 │
            Stream ──┬──> Triggered task / WHEN (task graph 🔨)
                     ├──> Dynamic Table incremental untuk tabel native
                     └──> AUTO CDC (sumber)
MERGE INTO (PK) ─────┬──> AUTO CDC SCD2
                     └──> Stream consumer pattern (INSERT/MERGE FROM stream)
BEGIN WORK (transaksi) ──> INSERT ALL, REPLACE WHERE, konsumsi offset stream atomik
Task ──> Alert, Data Metric Function, Dynamic Table scheduler
Stored procedure (🔨) ──> EXECUTE IMMEDIATE, anonymous block, event logging
Policy objects ──> tag-based masking; harus dipertimbangkan di MV rewrite
```

Urutan kritis: **Time travel → Stream → Triggered task** dan **MERGE INTO → AUTO CDC**. Zero-copy clone dan dialect quick wins bisa dikerjakan paralel karena tidak saling bergantung.

**Catatan strategi fork** (lihat juga `RISET_FITUR_SNOWFLAKE.md` §4.2): fitur dialect Fase 1 dan `MERGE INTO` PK berpotensi diterima upstream, jadi tulis sebagai perubahan kecil yang terisolasi. Fitur besar (stream, clone) taruh di package/kelas baru dan minimalkan perubahan pada file inti (`StmtExecutor`, `GlobalStateMgr`) agar sinkronisasi dengan upstream tetap mudah.

---

## 10. Referensi

**Snowflake**
- Streams: https://docs.snowflake.com/en/user-guide/streams-intro
- CHANGES clause: https://docs.snowflake.com/en/sql-reference/constructs/changes
- Tasks & triggered tasks: https://docs.snowflake.com/en/user-guide/tasks-intro , https://docs.snowflake.com/en/user-guide/tasks-triggered
- Dynamic tables: https://docs.snowflake.com/en/user-guide/dynamic-tables/overview (migrasi dari stream/task: https://docs.snowflake.com/user-guide/dynamic-tables/migrate-streams-tasks)
- Release notes 2026 (adaptive refresh, CREATE OR ALTER DYNAMIC TABLE, DML error logging, EXPLAIN CHANGES): https://docs.snowflake.com/en/release-notes/new-features-2026
- Cloning: https://docs.snowflake.com/en/user-guide/object-clone
- MERGE: https://docs.snowflake.com/en/sql-reference/sql/merge
- SELECT (EXCLUDE/RENAME/REPLACE/ILIKE): https://docs.snowflake.com/en/sql-reference/sql/select
- QUALIFY: https://docs.snowflake.com/en/sql-reference/constructs/qualify
- UNPIVOT / PIVOT: https://docs.snowflake.com/en/sql-reference/constructs/unpivot
- INSERT multi-table: https://docs.snowflake.com/en/sql-reference/sql/insert-multi-table
- MATCH_RECOGNIZE: https://docs.snowflake.com/en/sql-reference/constructs/match_recognize
- CREATE OR ALTER TABLE: https://docs.snowflake.com/en/sql-reference/sql/create-table#create-or-alter-table
- Masking policy: https://docs.snowflake.com/en/user-guide/security-column-ddm-intro ; Row access policy: https://docs.snowflake.com/en/user-guide/security-row-intro
- Object tagging: https://docs.snowflake.com/en/user-guide/object-tagging
- Access history: https://docs.snowflake.com/en/user-guide/access-history
- Alerts: https://docs.snowflake.com/en/user-guide/alerts
- Data metric functions: https://docs.snowflake.com/en/user-guide/data-quality-intro
- IDENTIFIER / variables: https://docs.snowflake.com/en/sql-reference/identifier-literal

**Databricks**
- Lakeflow pipelines concepts: https://docs.databricks.com/aws/en/ldp/concepts/
- AUTO CDC: https://docs.databricks.com/aws/en/ldp/cdc , lanjutan: https://docs.databricks.com/aws/en/ldp/cdc-advanced
- Release notes Lakeflow 2026: https://docs.databricks.com/aws/en/release-notes/dlt/2026
- Expectations: https://docs.databricks.com/aws/en/ldp/expectations
- Change Data Feed: https://docs.databricks.com/aws/en/delta/delta-change-data-feed
- MERGE INTO: https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into
- REPLACE WHERE: https://docs.databricks.com/aws/en/ldp/flows-replace-where
- Clone (shallow/deep): https://docs.databricks.com/aws/en/delta/clone
- DESCRIBE HISTORY / RESTORE: https://docs.databricks.com/aws/en/delta/history
- Liquid clustering: https://docs.databricks.com/aws/en/delta/clustering
- Pipe syntax: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-pipeline
- SQL scripting & multi-statement transactions: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting
- Row filters & column masks: https://docs.databricks.com/aws/en/tables/row-and-column-filters
- Lineage: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage

---

## Lampiran A. Spesifikasi Desain Stream (Standard + Append-only)

Lampiran ini adalah spesifikasi lengkap untuk item 1.1, cukup detail untuk mulai implementasi. Lokasi kode dicek di worktree `.worktrees/time-travel` (berisi time travel, task graph, dan stored procedure) dan `main`. Perilaku Snowflake dicek ulang (2026-11-22) di dokumentasi resmi: *Introduction to streams*, *CREATE STREAM*, *SHOW STREAMS* (tautan di A.18).

### A.0 Prinsip kompatibilitas dengan Snowflake

Tujuannya: user yang pindah dari Snowflake bisa memakai SQL, pola pipeline, dan intuisi yang sama tanpa belajar ulang.

1. **Default = perilaku Snowflake.** Setiap statement, opsi, kolom output, dan aturan offset yang ada di Snowflake berperilaku sama. Jika StarRocks tidak bisa meniru persis, perbedaannya dicatat di **A.17** beserta alasannya, dan hasil akhirnya tetap ekuivalen bagi konsumen (menerapkan output stream ke tabel replika menghasilkan isi yang sama).
2. **Ekstensi StarRocks bersifat opt-in** dan tidak mengubah perilaku default: `UPSERT_ONLY`, `IGNORE_PARTITION_DROP`, `table_changes()` (gaya Databricks), `SHOW STREAM OFFSETS`. Ditandai **[ekstensi]**.
3. **Nama objek dan kolom mengikuti Snowflake**: `CHANGE_TRACKING`, `DATA_RETENTION_TIME_IN_DAYS`, `MAX_DATA_EXTENSION_TIME_IN_DAYS`, `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`, `SYSTEM$STREAM_HAS_DATA`, kolom `SHOW STREAMS`. Lexer StarRocks sudah menerima `$` di identifier (`fragment LETTER : [a-zA-Z_$...]` di `StarRocksLex.g4`), jadi `METADATA$ACTION` dan `SYSTEM$...` bisa dipakai tanpa quote.
4. **Pemetaan namespace:** Snowflake `database.schema.object` ↔ StarRocks `catalog.database.object`. Di output yang meniru Snowflake (`SHOW STREAMS`, `DESC STREAM`), kolom `database_name` = catalog (`default_catalog`), `schema_name` = database StarRocks. Tool yang dibuat untuk Snowflake jadi bisa membaca output tanpa diubah.
5. **Jika ragu karena docs Snowflake tidak menjelaskan**, pilih perilaku yang membuat pipeline Snowflake yang sudah ada tetap benar, lalu tandai "perlu verifikasi di akun Snowflake" di A.17.

### A.1 Tujuan dan non-tujuan

**Tujuan**
1. `CREATE STREAM ... ON TABLE` pada tabel native StarRocks dengan mode **standard** (default, `mode = DEFAULT`) dan **`APPEND_ONLY`**, semantik sama dengan Snowflake.
2. Exactly-once: offset maju hanya saat DML yang mengonsumsi stream commit, dengan isolasi *repeatable read* dan penguncian stream seperti Snowflake.
3. Biaya baca sebanding jumlah perubahan, bukan ukuran tabel.
4. Dipakai oleh: task `WHEN SYSTEM$STREAM_HAS_DATA`, `CHANGES` clause, Dynamic Table incremental, AUTO CDC.

**Non-tujuan (versi pertama)**
- Stream pada view, external table, directory table, dynamic table/MV, event table (`ON VIEW`, `ON EXTERNAL TABLE`, `INSERT_ONLY`, dll.). Parser menerima syntax-nya lalu mengembalikan error "not supported yet" agar script Snowflake gagal dengan jelas. Direncanakan di S4.
- Shared-data (lake). Time travel saat ini juga hanya shared-nothing (`TimeTravelResolver`: "not supported ... in shared-data mode").
- Object tagging (`WITH TAG`): diterima parser, error sampai fitur tag (5.2) ada.

### A.2 Terminologi

| Istilah | Arti |
|---|---|
| **Offset** | Titik transaksional di riwayat versi tabel yang sudah dikonsumsi stream. Disimpan sebagai map `partisi logis → versi` + waktu. |
| **v1 / v2** | Versi awal (offset) dan versi akhir per partisi. Stream mengeluarkan perubahan transaksi di `(v1, v2]`. `v2` = versi visible pada **awal transaksi** (A.6). |
| **Partisi logis** | Identitas partisi yang stabil walau partition id berubah (nama partisi). Perlu karena `INSERT OVERWRITE` membuat temporary partition lalu swap. |
| **K (changed key set)** | Himpunan key baris yang tersentuh oleh versi di `(v1, v2]`, beserta penanda apakah key tersebut dihapus di rentang itu. |
| **Before / After** | Baris di snapshot `v1` / `v2` yang key-nya ada di `K`. |
| **Stale** | Perubahan sejak offset tidak lagi bisa dibaca. Stream harus dibuat ulang. |
| **Pin** | Penahanan versi oleh stream agar GC BE tidak membuangnya (Snowflake: *data retention extension*). |

### A.3 Syntax dan semantik

Urutan klausa mengikuti Snowflake. Parser menerima opsi dalam urutan bebas.

```sql
-- Sama dengan Snowflake
CREATE [ OR REPLACE ] STREAM [ IF NOT EXISTS ] <name>
  [ [ WITH ] TAG ( ... ) ]                                  -- diterima, error sampai fitur tag ada
  [ COPY GRANTS ]
  ON TABLE <table_name>
  [ { AT | BEFORE } ( { TIMESTAMP => <ts> | OFFSET => <detik_negatif> | STATEMENT => '<query_id>' | STREAM => '<name>' } ) ]
  [ APPEND_ONLY = TRUE | FALSE ]
  [ SHOW_INITIAL_ROWS = TRUE | FALSE ]
  [ COMMENT = '<string>' ]
  [ COPY = ( GRANTS ) ]
  -- ekstensi StarRocks (opt-in):
  [ UPSERT_ONLY = TRUE | FALSE ]
  [ IGNORE_PARTITION_DROP = TRUE | FALSE ];

CREATE [ OR REPLACE ] STREAM <name> CLONE <source_stream> [ COPY GRANTS ];
CREATE OR ALTER STREAM <name> ON TABLE <table_name> [ APPEND_ONLY = ... ] [ SHOW_INITIAL_ROWS = ... ] [ COMMENT = '...' ];

ALTER STREAM [ IF EXISTS ] <name> SET COMMENT = '<string>';
ALTER STREAM [ IF EXISTS ] <name> UNSET COMMENT;
DROP STREAM [ IF EXISTS ] <name>;
SHOW [ TERSE ] STREAMS [ LIKE '<pattern>' ] [ IN { ACCOUNT | DATABASE [<catalog>] | [SCHEMA] [<db>] } ]
                       [ STARTS WITH '<prefix>' ] [ LIMIT <n> [ FROM '<name>' ] ];
DESC[RIBE] STREAM <name>;

-- Tabel sumber (sama dengan Snowflake)
ALTER TABLE <t> SET CHANGE_TRACKING = TRUE | FALSE;
ALTER TABLE <t> SET DATA_RETENTION_TIME_IN_DAYS = <n>;          -- sudah ada di branch time travel (sebagai property)
ALTER TABLE <t> SET MAX_DATA_EXTENSION_TIME_IN_DAYS = <n>;

-- Membaca
SELECT ... FROM <stream> ...;                                    -- di mana pun relasi tabel boleh dipakai
SELECT SYSTEM$STREAM_HAS_DATA('<name>');
SELECT ... FROM <t> CHANGES ( INFORMATION => { DEFAULT | APPEND_ONLY } )
               { AT | BEFORE } ( ... ) [ END ( ... ) ];

-- Ekstensi StarRocks
SHOW STREAM OFFSETS <name>;                                      -- offset per partisi
SELECT ... FROM table_changes('<db.t>', <start> [, <end>]);      -- gaya Databricks, per-commit
```

Gaya StarRocks juga diterima sebagai alias: `COMMENT 'x'` tanpa `=`, `ALTER TABLE t SET ("change_tracking" = "true")`, `SHOW STREAMS FROM db`.

**Opsi**

| Opsi | Semantik (sama dengan Snowflake kecuali ditandai) |
|---|---|
| *(default)* | Standard stream: INSERT, UPDATE (pasangan DELETE+INSERT), DELETE termasuk `TRUNCATE`, sebagai net change antara offset dan `v2`. `mode` di `SHOW STREAMS` = `DEFAULT`. |
| `APPEND_ONLY = TRUE` | Hanya **insert**. UPDATE, DELETE, dan TRUNCATE **tidak** dicatat. Baris yang di-insert lalu di-delete sebelum offset maju **tetap dikembalikan** (contoh resmi Snowflake: insert 10 baris, delete 5 → stream mengembalikan 10). Lihat A.7.6 untuk arti "insert" pada PK table. |
| `SHOW_INITIAL_ROWS = TRUE` | Konsumsi **pertama** mengembalikan **hanya** baris yang ada saat stream dibuat (`ISUPDATE = FALSE`). Setelah itu perilaku normal: perubahan sejak offset terakhir. |
| `AT / BEFORE (...)` | Offset awal di titik masa lalu (time travel). `AT` inklusif, `BEFORE` tepat sebelum titik itu. `OFFSET` dalam detik negatif (`AT(OFFSET => -60*5)`). `STATEMENT => '<query_id>'` (S2): versi hasil commit query itu, dicari dari riwayat transaksi. `STREAM => 'name'`: salin offset stream lain; `CREATE OR REPLACE STREAM s ... AT(STREAM => 's')` membuat ulang stream sambil mempertahankan offset. Jika titik waktu di luar data yang masih tersimpan → `CREATE STREAM` gagal. |
| `COMMENT = '...'` | Komentar. |
| `COPY GRANTS` / `COPY = (GRANTS)` | Saat `OR REPLACE` atau `CLONE`, salin grant (kecuali ownership) dari stream lama. |
| `CLONE <s>` | Stream baru dengan definisi dan **offset saat ini** milik `<s>`. |
| `CREATE OR ALTER STREAM` | Buat jika belum ada; jika ada, hanya `COMMENT` yang boleh berubah. Mengubah tabel sumber atau `APPEND_ONLY` → error (sama dengan Snowflake). |
| `OR REPLACE` + `IF NOT EXISTS` | Tidak boleh bersamaan (error, sama dengan Snowflake). `OR REPLACE` tanpa `AT(STREAM => ...)` **mereset offset** ke sekarang. |
| `UPSERT_ONLY = TRUE` **[ekstensi]** | Hanya PK/Unique/Aggregate. Semua baris yang **ditulis** (insert atau upsert/update) di `(v1, v2]` dan masih ada di `v2`, dengan nilai di `v2`. DELETE diabaikan. Tidak membaca snapshot lama, jadi paling murah. `METADATA$ACTION = 'INSERT'`, `METADATA$ISUPDATE = NULL`. `mode` = `UPSERT_ONLY`. Tidak boleh bersama `APPEND_ONLY`. |
| `IGNORE_PARTITION_DROP = TRUE` **[ekstensi]** | Partisi yang di-drop **otomatis oleh TTL** (dynamic partition, `partition_live_number`, `partition_retention_condition`) tidak menghasilkan DELETE dan tidak perlu ditahan di recycle bin. Drop manual, `TRUNCATE`, `INSERT OVERWRITE` tetap dicatat. |

**Kolom output stream** (sama dengan Snowflake): semua kolom tabel sumber dengan **skema saat ini**, urutan sama, lalu

| Kolom | Tipe | Isi |
|---|---|---|
| `METADATA$ACTION` | VARCHAR | `'INSERT'` atau `'DELETE'` |
| `METADATA$ISUPDATE` | BOOLEAN | `TRUE` jika baris adalah bagian dari UPDATE (pasangan DELETE+INSERT) |
| `METADATA$ROW_ID` | VARCHAR | ID baris yang unik dan stabil (A.7.4) |

Kolom metadata ikut dalam `SELECT *`, sama seperti Snowflake. Pola umum: `SELECT * EXCLUDE (METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID) FROM s` (`EXCLUDE` sudah ada di StarRocks).

**Statement yang memajukan offset** (sama dengan Snowflake): setiap DML yang membaca stream, dalam transaksi autocommit maupun eksplisit, jika transaksinya **commit**: `INSERT`, `INSERT OVERWRITE`, `UPDATE`, `DELETE`, `MERGE`, `CREATE TABLE ... AS SELECT`, dan `INSERT INTO FILES(...)` (padanan `COPY INTO <location>`). `SELECT` biasa **tidak** memajukan offset, termasuk di dalam transaksi eksplisit.

**Memajukan offset tanpa memproses data** (resep Snowflake, berlaku sama):
- `CREATE OR REPLACE STREAM s ON TABLE t;`
- `INSERT INTO tmp SELECT * FROM s WHERE 0 = 1;`

### A.4 Contoh perilaku (juga spesifikasi test)

Tabel PK `t(id INT PRIMARY KEY, v VARCHAR)` berisi `(1,'a'), (2,'b')` saat stream `s` (standard) dibuat. Kolom "Snowflake" menunjukkan dasar perilakunya.

| # | Operasi setelah offset | Output `SELECT * FROM s` (`id, v, ACTION, ISUPDATE`) | Dasar |
|---|---|---|---|
| 1 | `INSERT (3,'c')` | `3,c,INSERT,FALSE` | Docs |
| 2 | `UPDATE SET v='B' WHERE id=2` | `2,b,DELETE,TRUE` + `2,B,INSERT,TRUE` | Docs |
| 3 | `DELETE WHERE id=1` | `1,a,DELETE,FALSE` | Docs |
| 4 | `INSERT (4,'d')` lalu `DELETE WHERE id=4` | *(kosong)* | Docs ("inserted and then deleted ... is removed in the delta") |
| 5 | `UPDATE v='x'` lalu `UPDATE v='y'` untuk id=2 | `2,b,DELETE,TRUE` + `2,y,INSERT,TRUE` | Docs (net change) |
| 6 | `INSERT (4,'d')` lalu `UPDATE v='D' WHERE id=4` | `4,D,INSERT,FALSE` | Docs ("added and then updated ... a new row ... FALSE") |
| 7 | `UPDATE v='z'` lalu `DELETE` untuk id=2 | `2,b,DELETE,FALSE` | Net change (nilai saat offset) |
| 8 | `UPDATE SET v='b' WHERE id=2` (nilai sama) | `2,b,DELETE,TRUE` + `2,b,INSERT,TRUE` | Tidak didokumentasikan; baris ditulis ulang → dicatat sebagai update. **Perlu verifikasi** |
| 9 | `DELETE WHERE id=1` lalu `INSERT (1,'z')` | `1,a,DELETE,FALSE` + `1,z,INSERT,FALSE` | Di Snowflake ini dua baris berbeda (ROW_ID beda), bukan update. Lihat A.17 untuk ROW_ID |
| 10 | `DELETE WHERE id=1` lalu `INSERT (1,'a')` | `1,a,DELETE,FALSE` + `1,a,INSERT,FALSE` | Sama dengan #9 |
| 11 | Upsert (Stream Load / `INSERT`) `(2,'q'),(5,'e')` | `2,b,DELETE,TRUE` + `2,q,INSERT,TRUE` + `5,e,INSERT,FALSE` | Upsert key yang ada = UPDATE (A.7.6) |
| 12 | Partial update kolom `v` untuk id=2 | sama seperti #2 | |
| 13 | `TRUNCATE TABLE t` | `1,a,DELETE,FALSE` + `2,b,DELETE,FALSE` | Docs (standard mencatat truncate) |
| 14 | `INSERT OVERWRITE t VALUES (2,'b'),(6,'f')` | `1,a,DELETE,FALSE` + `2,b,DELETE,FALSE` + `2,b,INSERT,FALSE` + `6,f,INSERT,FALSE` | Overwrite = truncate + insert |
| 15 | **APPEND_ONLY**: #1 + #2 + #3 | `3,c,INSERT,FALSE` | Docs |
| 16 | **APPEND_ONLY**: `INSERT (4,'d')` lalu `DELETE WHERE id=4` | `4,d,INSERT,FALSE` | Docs (contoh 10 insert, 5 delete → 10) |
| 17 | **APPEND_ONLY**: `INSERT (4,'d')` lalu `UPDATE v='D' WHERE id=4` | `4,D,INSERT,FALSE` | Sumber pihak ketiga; **perlu verifikasi** |
| 18 | **APPEND_ONLY**: `TRUNCATE` lalu `INSERT (7,'g')` | `7,g,INSERT,FALSE` | Docs |
| 19 | **APPEND_ONLY**: upsert key yang sudah ada `(2,'q')` | *(kosong)* | Upsert key lama = update, tidak dicatat (A.7.6) |
| 20 | **SHOW_INITIAL_ROWS**: konsumsi pertama, sementara sesudah create ada `INSERT (3,'c')` | Konsumsi 1: `1,a,INSERT,FALSE` + `2,b,INSERT,FALSE`. Konsumsi 2: `3,c,INSERT,FALSE` | Docs |
| 21 | **[ekstensi] UPSERT_ONLY**: #1 + #2 + #3 | `3,c,INSERT,NULL` + `2,B,INSERT,NULL` | |
| 22 | Partisi tertua di-drop oleh TTL; stream standard | DELETE semua baris partisi itu | Konsisten dengan truncate |
| 23 | Sama #22, **[ekstensi]** `IGNORE_PARTITION_DROP = TRUE` | *(kosong)*; `ALTER TABLE t DROP PARTITION p` manual tetap DELETE | |

Konsumsi dan transaksi (semua sesuai docs Snowflake):

| # | Langkah | Hasil |
|---|---|---|
| C1 | `INSERT INTO h SELECT * FROM s` commit | `h` berisi output; `SELECT * FROM s` setelahnya kosong |
| C2 | Statement C1 gagal / rollback | Offset tidak berubah; output tetap ada |
| C3 | Dua sesi menjalankan `INSERT ... FROM s` bersamaan | Sesi kedua **menunggu** lock stream sampai transaksi pertama selesai, lalu membaca perubahan sesudah offset baru (tidak ada data ganda) |
| C4 | `BEGIN; INSERT INTO a SELECT * FROM s; INSERT INTO b SELECT * FROM s; COMMIT;` | `a` dan `b` berisi data **sama**; offset maju sekali saat COMMIT |
| C5 | `BEGIN` (t1); sesi lain menulis ke `t` (t2); `INSERT INTO a SELECT * FROM s` (t3) | Perubahan t2 **tidak** ikut, karena stream dibaca pada waktu mulai transaksi (t1) |
| C6 | `SELECT * FROM s` dua kali tanpa DML | Hasil sama; offset tidak maju |
| C7 | `BEGIN; SELECT * FROM s; COMMIT;` | Offset tidak maju (hanya query) |
| C8 | `INSERT INTO t SELECT ... FROM s` (menulis ke tabel sumber sendiri) | Boleh; perubahan ini muncul pada pembacaan berikutnya |
| C9 | `INSERT INTO tmp SELECT * FROM s WHERE 0 = 1` | Offset maju; stream kosong |

### A.5 Model metadata dan persistensi

Ikuti pola `Pipe` (`com.starrocks.load.pipe.Pipe`, `PipeManager`, `PipeRepo`, op `OP_PIPE = 12200`).

```java
package com.starrocks.stream;

public class Stream implements Writable {
    @SerializedName("id")       long id;
    @SerializedName("name")     String name;
    @SerializedName("dbId")     long dbId;
    @SerializedName("tableId")  long baseTableId;
    @SerializedName("mode")     StreamMode mode;            // DEFAULT | APPEND_ONLY | UPSERT_ONLY
    @SerializedName("offsets")  Map<String, PartitionOffset> offsets;  // key = partisi logis
    @SerializedName("init")     InitialRows initialRows;    // null, atau {version per partisi} saat SHOW_INITIAL_ROWS belum dikonsumsi
    @SerializedName("schemaV")  long baseSchemaVersion;
    @SerializedName("ct")       long createdOn;
    @SerializedName("lr")       long lastReadTime;          // dasar STALE_AFTER (konsumsi atau HAS_DATA = FALSE)
    @SerializedName("lcTxn")    long lastConsumeTxnId;
    @SerializedName("owner")    UserIdentity owner;
    @SerializedName("comment")  String comment;
    @SerializedName("ignTtl")   boolean ignorePartitionDrop;
    @SerializedName("invalid")  String invalidReason;       // null = valid; contoh 'base table dropped'
}

public class PartitionOffset {
    @SerializedName("pid")  long partitionId;   // id fisik saat offset dicatat
    @SerializedName("v")    long version;
    @SerializedName("t")    long visibleTime;
}
```

- **`StreamManager`** (singleton di `GlobalStateMgr`): CRUD, index `tableId → List<Stream>`, `minOffset(tableId, partition)`, lock stream (A.9), image (`SRMetaBlockID` baru), replay EditLog.
- **OperationType baru:** `OP_CREATE_STREAM`, `OP_DROP_STREAM`, `OP_ALTER_STREAM`, `OP_UPDATE_STREAM_READ` (lastReadTime). Pemajuan offset ikut di replay commit transaksi (A.9). Nomor baru, **tidak pernah dipakai ulang**.
- **Privilege** (sama dengan Snowflake, dipetakan ke model StarRocks):

  | Aksi | Butuh |
  |---|---|
  | `CREATE STREAM` | `CREATE STREAM` pada database (padanan schema Snowflake) + `SELECT` pada tabel |
  | Stream pertama pada tabel yang `CHANGE_TRACKING` belum aktif | Pemilik tabel, di StarRocks: privilege `ALTER` pada tabel (karena mengaktifkan change tracking) |
  | Membaca stream | `SELECT` pada stream **dan** `SELECT` pada tabel sumber |
  | `ALTER` / `DROP STREAM` | `ALTER` / `DROP` pada stream (atau ownership) |
  | `CREATE OR ALTER` pada stream yang ada | ownership |

  `ObjectType.STREAM` baru (pola `ObjectType.PIPE = 13`).
- **`SHOW STREAMS` / `DESC STREAM`**: kolom **persis** Snowflake, dengan urutan yang sama:

  | Kolom | Nilai di StarRocks |
  |---|---|
  | `created_on` | waktu create |
  | `name` | nama stream |
  | `database_name` | nama catalog (`default_catalog`) |
  | `schema_name` | nama database StarRocks |
  | `owner` | user/role pemilik |
  | `comment` | komentar |
  | `table_name` | `catalog.db.table` sumber |
  | `source_type` | `Table` |
  | `base_tables` | kosong (untuk stream on view nanti) |
  | `type` | `DELTA` |
  | `stale` | `true` jika `now > stale_after` (stream *mungkin* stale) |
  | `mode` | `DEFAULT` / `APPEND_ONLY` / `UPSERT_ONLY` |
  | `stale_after` | `lastReadTime + max(DATA_RETENTION_TIME_IN_DAYS, MAX_DATA_EXTENSION_TIME_IN_DAYS)` |
  | `invalid_reason` | `N/A`, atau alasan (`base table dropped`, `incompatible schema change`) |
  | `owner_role_type` | `ROLE` atau `USER` |

  `SHOW TERSE STREAMS`: `created_on, name, kind, database_name, schema_name, tableOn`, sama dengan Snowflake.
- **`information_schema.streams`** dengan kolom yang sama.
- **`SHOW STREAM OFFSETS s`** **[ekstensi]**: `partition, partition_id, offset_version, offset_time, visible_version, pending_versions`.

### A.6 Resolusi offset, snapshot, dan transaksi

Aturan Snowflake yang diikuti: stream mengembalikan perubahan dari offset sampai **awal transaksi**; semua statement di transaksi yang sama melihat data yang sama; jika transaksi commit, offset maju ke waktu awal transaksi.

1. **Menetapkan `v2`.**
   - Autocommit: `v2` = visible version setiap partisi saat statement mulai di-analyze.
   - Transaksi eksplisit (`BEGIN WORK`): `v2` = versi partisi yang visible pada **waktu `BEGIN`**, dicari dengan `VersionHistory.versionAt(beginTime)` (struktur dari branch time travel). Karena itu tabel dengan `CHANGE_TRACKING = TRUE` wajib mencatat `VersionHistory`, walaupun time travel tidak diaktifkan.
   - Semua referensi ke stream yang sama dalam satu statement atau satu transaksi memakai `v2` yang sama (disimpan di `ConnectContext`, mirip `addTimeTravelScanVersions`).
2. **Rentang per partisi logis:**
   - Ada di offset dan `pid` sama → `(offset.v, v2]`.
   - Ada di offset, `pid` berbeda → overwrite/swap (A.7.5).
   - Tidak ada di offset → partisi baru, `(0, v2]`, semua baris INSERT.
   - Ada di offset tetapi partisinya sudah tidak ada → DELETE semua baris snapshot `offset.v` partisi lama (masih ada di recycle bin dan di-pin). Pengecualian **[ekstensi]**: `IGNORE_PARTITION_DROP = TRUE` dan drop oleh TTL → lewati tanpa output. Untuk itu operasi drop partisi di EditLog mencatat penyebab (`MANUAL` / `TTL`), diisi `DynamicPartitionScheduler` dan scheduler TTL partisi.
   - `offset.v == v2` → dilewati (tidak ada perubahan), jadi tabel besar yang hanya berubah di satu partisi tetap murah.
3. **`SHOW_INITIAL_ROWS`:** selama `initialRows != null`, pembacaan mengembalikan snapshot pada versi saat create (`InitialRows`) sebagai INSERT. Saat dikonsumsi, offset diset ke versi saat create (bukan `v2`), sehingga perubahan setelah create muncul pada pembacaan berikutnya (A.4 #20).
4. **Cek stale** sebelum planning: jika versi yang dibutuhkan sudah tidak bisa dibaca (di-GC, schema change berat, replika hilang), statement error:
   `Stream s is stale: changes since 2026-11-20 08:00:00 are no longer available. Recreate the stream.`
5. **`SYSTEM$STREAM_HAS_DATA('s')`** dihitung di FE saja dari langkah 1–2: `TRUE` jika ada partisi dengan rentang tidak kosong (atau initial rows belum dikonsumsi). Sama seperti Snowflake:
   - `TRUE` berarti ada versi baru, bukan pasti ada baris net change (A.4 #4 bisa `TRUE` tapi kosong).
   - Jika hasilnya `FALSE`, `lastReadTime` diperbarui sehingga stream tidak menjadi stale ("calling SYSTEM$STREAM_HAS_DATA ... prevents it from becoming stale"). Penulisan EditLog dibatasi maksimal sekali per menit per stream.

### A.7 Rencana eksekusi

Analyzer mengganti `TableRelation` yang menunjuk stream dengan **`StreamRelation`** (AST baru: tabel sumber, mode, rentang versi per partisi, kolom output). `RelationTransformer` mengubahnya menjadi sub-plan logis biasa, sehingga optimizer, runtime filter, dan eksekusi BE yang ada langsung dipakai. Seperti time travel, query yang mengandung `StreamRelation` melewati MV rewrite, meta scan rewrite, query cache, dan short-circuit read.

#### A.7.1 Primary Key table, mode standard

```
                     Project(kolom, METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID)
                                         │
                     Expand: 1 baris join → 0, 1, atau 2 baris output (tabel aturan di bawah)
                                         │
                       HashJoin FULL OUTER ON before.k <=> after.k
                      ┌──────────────────┴───────────────────┐
           OlapScan t @ v1                               OlapScan t @ v2
           (time travel, IN-filter K)                    (IN-filter K)
                      ▲                                       ▲
                      └──────── runtime filter dari K ────────┘
                                         │
                   Aggregate per k: deleted_in_range = OR(dari del file / truncate)
                                         │
                   ChangeKeyScan t (v1, v2]  →  (k, is_delete_marker)
```

**Aturan output** (sesuai A.4):

| before | after | `deleted_in_range` | Output |
|---|---|---|---|
| – | ada | – | `INSERT`, `ISUPDATE = FALSE` (termasuk insert lalu update, A.4 #6) |
| ada | – | – | `DELETE` nilai saat offset, `ISUPDATE = FALSE` (A.4 #3, #7) |
| ada | ada | `FALSE` | `DELETE`(lama) + `INSERT`(baru), `ISUPDATE = TRUE`, **termasuk jika nilainya sama** (A.4 #2, #5, #8, #11) |
| ada | ada | `TRUE` | `DELETE`(lama) + `INSERT`(baru), `ISUPDATE = FALSE`: baris lama dihapus lalu baris baru dibuat (A.4 #9, #10) |
| – | – | – | tidak ada (A.4 #4) |

- **`ChangeKeyScan`** (BE, A.8) membaca per tablet hanya artefak versi di `(v1, v2]`: kolom key dari segment rowset baru (insert/upsert, partial update row mode), key di del file (`num_delete_files`) dengan `is_delete_marker = TRUE`, dan untuk partial update column mode (delta column group, `num_update_files`) rowid yang tersentuh → key.
- Karena aturan "ada di kedua sisi → selalu pasangan update", tidak perlu membandingkan nilai kolom. Lebih murah dan lebih dekat ke Snowflake yang mencatat setiap penulisan ulang baris.
- `deleted_in_range` hanya `TRUE` jika DELETE terjadi **sebelum** penulisan terakhir key itu di rentang. Urutan diketahui dari versi artefak: `ChangeKeyScan` mengeluarkan `(k, version, kind)`, dan agregasi mengambil `max(version of delete) < max(version of write)`.
- `K` dipakai dua kali (multicast fragment / CTE reuse yang sudah ada) lalu dikirim sebagai runtime filter IN ke dua scan snapshot. Pada PK table, scan dengan IN-filter key memakai primary index / zone map sehingga menjadi lookup.
- **Ambang full diff:** jika estimasi `|K|` > `stream_full_diff_ratio` (session variable, default 0.3) × jumlah baris, IN-filter dilewati dan dua snapshot di-join penuh. Hasil sama.

#### A.7.2 Unique Key dan Aggregate Key, mode standard

Sama dengan A.7.1, dengan `ChangeKeyScan` yang:
- mengambil key dari rowset baru di `(v1, v2]`;
- untuk rowset dengan `delete_predicate` di rentang: key = baris di snapshot sebelum versi delete yang memenuhi predikat, dengan `is_delete_marker = TRUE`. Diimplementasikan sebagai cabang plan `OlapScan t @ (versi_delete - 1) WHERE pred` (proyeksi key) yang di-`UNION` ke `K`. Predikat diambil FE dari metadata delete job.
- Aggregate Key membandingkan nilai **setelah agregasi** (yang dilihat user). Load yang menambah `SUM` → pasangan `ISUPDATE = TRUE`.

#### A.7.3 Duplicate Key, mode standard

Tidak ada key. Identitas baris = baris fisik, sama seperti tabel Snowflake biasa, jadi perilakunya paling mirip Snowflake:

```
UNION ALL
├── INSERT : baris dari rowset (v1, v2] yang tidak terkena delete_predicate dengan versi lebih tinggi di rentang
│            → ACTION='INSERT', ISUPDATE=FALSE
└── DELETE : baris di snapshot v1 yang terkena salah satu delete_predicate di rentang
             → ACTION='DELETE', ISUPDATE=FALSE
```

- Duplicate Key tidak mendukung UPDATE, jadi `ISUPDATE` selalu `FALSE`.
- Baris yang di-insert lalu di-delete di rentang tidak muncul (net change, sama dengan Snowflake).
- Predikat delete diterapkan sesuai urutan versi: baris dari rowset versi `x` hanya terkena predikat dengan versi `> x`. BE sudah menerapkan delete predicate berdasarkan versi saat membaca biasa; mode scan `ROWS_IN_RANGE` memakai logika yang sama.

#### A.7.4 `METADATA$ROW_ID`

Snowflake menjamin: unik, tidak berubah, sama untuk semua stream pada tabel yang sama, dan sama untuk stream pada clone tabel (untuk baris yang ada saat clone).

| Tipe tabel | Nilai |
|---|---|
| PK / Unique / Aggregate | `hex(murmur_hash3_128(key columns))`. Sama di semua stream pada tabel itu dan pada clone. Partition column selalu bagian dari key di StarRocks, jadi tidak perlu nama partisi. |
| Duplicate dengan `change_tracking_row_id = true` (**opt-in**) | Kolom tersembunyi `__row_id BIGINT` yang diisi saat load: `(txn_id << 20) \| urutan baris dalam tablet`. Bisa diset saat `CREATE TABLE` atau `ALTER TABLE ... SET` (fast schema evolution). Baris yang ditulis sebelum kolom ada bernilai NULL → fallback ke posisi fisik. |
| Duplicate tanpa `__row_id` | `tablet_id:rowset_id:segment_id:rowid` di snapshot asal. **Tidak stabil** setelah compaction. `CREATE STREAM` memberi WARNING. |

#### A.7.5 `INSERT OVERWRITE`, `TRUNCATE`, swap partisi

- `TRUNCATE` dan `INSERT OVERWRITE` diperlakukan seperti di Snowflake: **hapus semua baris lama, lalu insert baris baru**. Untuk PK/Unique/Agg ini menghasilkan `deleted_in_range = TRUE` untuk semua key lama, jadi key yang muncul lagi keluar sebagai `DELETE` + `INSERT` dengan `ISUPDATE = FALSE` (A.4 #14), bukan net change per key.
- Jika partisi logis `P` punya `offset.pid ≠ pid sekarang`: before = partisi lama `offset.pid @ offset.v` (semua baris → `K` dengan `is_delete_marker = TRUE`), after = partisi baru `@ v2`.
- Partisi lama harus tetap terbaca: `InsertOverwriteJobRunner` dan `TRUNCATE` memindahkan partisi lama ke recycle bin, dan recycle bin **tidak meng-erase** partisi yang masih di-pin stream (`StreamManager.isPinned(partitionId)`).
- `ALTER TABLE ... SWAP WITH` seluruh tabel: tableId berubah. Stream mengikuti objek tabel (bukan nama), seperti Snowflake ("if a source object is dropped and a new object is created with the same name, any streams linked to the original object are not linked to the new object").

#### A.7.6 Mode `APPEND_ONLY` dan arti "insert" pada PK table

Snowflake tidak punya upsert: setiap `INSERT` membuat baris baru. Di PK table StarRocks, `INSERT`/Stream Load dengan key yang sudah ada adalah **update**. Pemetaannya:

| Operasi di PK table | Dianggap Snowflake sebagai | Masuk `APPEND_ONLY`? |
|---|---|---|
| Insert/load key yang **belum ada** saat ditulis | INSERT | Ya |
| Insert/load key yang **sudah ada** (upsert) | UPDATE | Tidak (A.4 #19) |
| `UPDATE`, partial update | UPDATE | Tidak |
| `DELETE`, `TRUNCATE` | DELETE | Tidak |
| Insert key yang sebelumnya di-delete di rentang yang sama | INSERT (baris baru) | Ya |

Rencana:
- **Duplicate Key:** `OlapScan rowset (v1, v2]` mode `ROWS_IN_RANGE` **tanpa** menerapkan delete predicate. Baris yang kemudian di-delete tetap dikembalikan (A.4 #16, sesuai contoh resmi Snowflake). Nilai = nilai saat di-insert.
- **PK/Unique/Agg:** `ChangeKeyScan` mode `INSERTED_KEYS` mengeluarkan key yang ditulis di rentang **dan** tidak ada di snapshot tepat sebelum versi penulisannya (dicek dengan primary index pada versi itu; untuk Unique/Agg dengan anti-join ke snapshot `v1` ditambah urutan versi di rentang). Nilai output:
  - jika key masih ada di `v2` → nilai di `v2` (A.4 #17: insert lalu update → nilai terbaru);
  - jika key sudah dihapus → nilai saat di-insert, dibaca dari rowset versi insert-nya tanpa delvec (A.4 #16).
- Tidak membaca del file untuk menghasilkan DELETE, dan TRUNCATE setelah konsumsi tidak menambah biaya (sama dengan Snowflake).

**`UPSERT_ONLY` [ekstensi]:** `OlapScan t @ v2 ⋉ (ChangeKeyScan (v1, v2] mode WRITTEN_KEYS)`. `WRITTEN_KEYS` = key dari segment rowset baru dan DCG; del file diabaikan. Tidak ada scan `v1`.

### A.8 Perubahan BE

**Thrift (`gensrc/thrift/PlanNodes.thrift`, `TInternalScanRange`)**: field opsional baru dengan nomor yang belum pernah dipakai:

```thrift
  N1: optional i64 start_version                  // baca artefak versi (start_version, version]
  N2: optional TChangeScanMode change_scan_mode   // NONE | CHANGED_KEYS | INSERTED_KEYS | WRITTEN_KEYS | ROWS_IN_RANGE
```

**PK table (`be/src/storage/tablet_updates.*`)**
- Tablet PK menyimpan riwayat `EditVersion` (`_edit_version_infos`). `CHANGED_KEYS` iterasi edit version di `(v1, v2]`, ambil rowset delta tiap versi, lalu keluarkan `(key, version, kind)` dari segment (write), del file (delete), dan delta column group (write, rowid → key).
- `INSERTED_KEYS` menambah cek: key tidak ada di primary index pada versi sebelum penulisan (pakai snapshot index pada versi itu atau lookup ke snapshot `version - 1`).
- Edit version lama dibuang oleh `remove_expired_versions(expire_time)`. Time travel sudah membuat FE menaikkan `tablet_rowset_stale_sweep_time_sec` (`TimeTravelBeConfigSyncer`); stream ikut mekanisme ini (A.10).
- Snapshot `@ v1` memakai jalur time travel (`get_applied_rowsets(v1)` + delvec di v1).

**Non-PK (`be/src/storage/tablet.*`)**
- Dukung `capture_consistent_rowsets(Version(v1 + 1, v2))` untuk mode rentang.
- Jika compaction sudah menggabungkan rowset melintasi batas `v1`, cari path versi lewat `_stale_rs_version_map` / `TimestampedVersionTracker`. Jika tidak ada path → error `STREAM_VERSION_NOT_AVAILABLE` → FE menandai stale.

**Scanner**: `OlapScanner` / `TabletReader` menerima `start_version` dan mode. Mode key hanya membaca kolom key dan tidak menerapkan delvec.

**Replika:** tablet yang di-repair dengan full clone bisa kehilangan versi lama. BE melaporkan versi tertua yang masih terbaca per tablet di tablet report; FE memilih replika yang masih punya `v1`. Jika tidak ada → stale.

### A.9 Penguncian dan pemajuan offset

Snowflake: DML yang memakai stream **mengunci** stream sampai transaksi selesai; konsumen lain menunggu. StarRocks mengikuti:

```
analyze   : StreamRelation menetapkan v2 (A.6). Jika statement adalah DML:
              StreamManager.lock(streamId, txn)   -- tunggu jika dipegang transaksi lain,
                                                  -- batas waktu = session variable lock_wait_timeout
            simpan {streamId → (expectedOffset, newOffset = v2)} di transaksi
exec      : seperti DML biasa
commit    : TransactionState.txnCommitAttachment = StreamTxnCommitAttachment
            DatabaseTransactionMgr.commitTransaction:
              cek stream.offsets == expectedOffset (pengaman tambahan)
              offset := newOffset, lastReadTime := now, tercatat di EditLog bersama commit
abort     : offset tidak berubah
akhir txn : StreamManager.unlock(streamId)
```

- Pola attachment sama dengan Routine Load yang menyimpan offset Kafka (`RLTaskTxnCommitAttachment`, diproses di `DatabaseTransactionMgr` sekitar baris 1284).
- **Setelah menunggu lock**, `v2` tetap waktu awal transaksi penunggu. Jika transaksi sebelumnya sudah memajukan offset melewati `v2`, rentangnya kosong. Tidak ada data ganda (A.4 C3).
- Statement yang membaca stream tapi menghasilkan 0 baris tetap membuat transaksi dan tetap memajukan offset (A.4 C9).
- `SELECT` (bukan DML) tidak mengambil lock dan tidak memajukan offset.
- Lock hanya di memori leader FE. Saat leader berganti, transaksi yang sedang berjalan di-abort (perilaku StarRocks yang sudah ada), jadi lock tidak perlu dipersistenkan.

### A.10 Retensi, pin, dan stale

Snowflake: jika stream belum dikonsumsi dan retensi tabel kurang dari 14 hari, retensi diperpanjang sampai offset stream, maksimal `MAX_DATA_EXTENSION_TIME_IN_DAYS` (default 14). `STALE_AFTER = last read + max(DATA_RETENTION_TIME_IN_DAYS, MAX_DATA_EXTENSION_TIME_IN_DAYS)`.

| Aspek | Desain |
|---|---|
| `CHANGE_TRACKING` | Property tabel. Diaktifkan otomatis oleh `CREATE STREAM` pertama, atau manual `ALTER TABLE t SET CHANGE_TRACKING = TRUE`. Mengaktifkan pencatatan `VersionHistory` dan pin. Tidak bisa dimatikan selama ada stream. |
| Retensi efektif | `max(DATA_RETENTION_TIME_IN_DAYS, umur offset tertua)`, dibatasi `max(DATA_RETENTION_TIME_IN_DAYS, MAX_DATA_EXTENSION_TIME_IN_DAYS)`. Sama dengan Snowflake. |
| `MAX_DATA_EXTENSION_TIME_IN_DAYS` | Property tabel + default global (14), sama dengan nama Snowflake. |
| Pin di BE (S1) | Mekanisme yang sudah ada: `TimeTravelBeConfigSyncer` menaikkan `tablet_rowset_stale_sweep_time_sec` sesuai retensi efektif terbesar. Konsekuensi yang diterima: semua tablet di BE menahan versi lama selama itu. Mitigasi: metrik `stream_lag_seconds`, kolom `stale_after`, dokumentasi agar stream yang tidak dipakai di-drop. |
| Pin di BE (S4) | Pin presisi per tablet (`min_readable_version`) agar hanya tablet yang dibutuhkan yang menahan versi. |
| Recycle bin | Partisi lama dan tabel ter-drop tidak di-erase selama di-pin stream. |
| Compaction | Tidak diblokir; rowset input disimpan sebagai stale sampai tidak di-pin. |
| Change-key log (S4) | Sidecar `<rowset_id>.ck` berisi key yang tersentuh, ditulis saat commit tabel ber-change-tracking (untuk PK key sudah ada di `RowsetUpdateState`). `ChangeKeyScan` tidak perlu rowset lama. |
| Stale | `stale` = `now > stale_after` (mungkin stale, sama dengan Snowflake). Pembacaan baru gagal jika data benar-benar sudah dibuang. Stream stale harus dibuat ulang. Stream tidak punya time travel/undrop, sama dengan Snowflake. |

### A.11 Interaksi dengan DDL

| DDL pada tabel sumber | Perilaku stream | Snowflake |
|---|---|---|
| `ADD COLUMN` (fast schema evolution) | Valid. Output memakai skema saat ini; kolom baru di sisi before bernilai default/NULL | Sama ("streams use the current table schema") |
| `DROP COLUMN`, `RENAME COLUMN` (fast) | Valid; output mengikuti skema saat ini | Sama |
| Mengubah kolom jadi `NOT NULL` | Query stream bisa gagal jika data lama berisi NULL | Sama (didokumentasikan sebagai batasan Snowflake) |
| Schema change berat (ubah key, sort key, tipe, distribusi) yang menulis ulang tablet | Offset sebelum perubahan → stale, `invalid_reason = incompatible schema change` | Snowflake: "incompatible schema changes ... can cause query failures" |
| `ADD PARTITION` | Partisi baru → semua baris INSERT | – |
| `DROP PARTITION` manual | DELETE semua baris partisi | Mirip DELETE |
| Drop partisi oleh TTL | Default DELETE; **[ekstensi]** `IGNORE_PARTITION_DROP` untuk mengabaikan | – |
| `TRUNCATE` | DELETE semua baris lama (standard); tidak dicatat (append-only) | Sama |
| `INSERT OVERWRITE` | DELETE semua lama + INSERT semua baru | Sama |
| `RENAME TABLE` | Valid (stream mengikuti tableId) | Sama ("renaming a source object does not break a stream") |
| `DROP TABLE` | `invalid_reason = base table dropped`; `RECOVER`/`UNDROP` mengembalikan jika versi masih di-pin | Snowflake: drop lalu buat tabel baru dengan nama sama tidak terhubung. Sama |
| `CREATE OR REPLACE TABLE` | Stale (riwayat hilang) | Sama |
| `SWAP WITH` | Stream mengikuti objek tabel asal | Sesuai prinsip "stream mengikuti objek" |
| `RESTORE` dari backup | Stale | – |
| `CREATE TABLE ... CLONE` (2.1) | Stream tidak ikut; stream baru pada clone menghasilkan `METADATA$ROW_ID` yang sama untuk baris yang ada saat clone | Sama |
| Clone database/schema yang berisi stream | Stream hasil clone tidak bisa membaca record yang belum dikonsumsi | Sama |

### A.12 Observability

- `SHOW STREAMS` / `DESC STREAM` / `information_schema.streams` dengan kolom Snowflake (A.5).
- **[ekstensi]** `SHOW STREAM OFFSETS s` untuk offset per partisi.
- Profil query: node `ChangeKeyScan` mencatat `RowsetsRead`, `DelFilesRead`, `DcgRead`, `ChangedKeys`.
- Metrik FE: `stream_total`, `stream_stale_total`, `stream_lag_seconds{db,stream}` (now − waktu offset), `stream_lock_wait_ms`.
- Audit log: statement konsumsi mencatat `stream_id` dan offset lama/baru.

### A.13 Kinerja dan batas

| Skenario | Perkiraan biaya |
|---|---|
| Sedikit perubahan pada tabel PK besar | `ChangeKeyScan` sebanding ukuran rowset baru + 2× lookup key di snapshot. Murah. |
| Perubahan masif (UPDATE seluruh tabel) | Full outer join dua snapshot (≈ 2× full scan + hash join) |
| `APPEND_ONLY` pada Duplicate | Hanya scan rowset baru. Paling murah |
| Banyak stream pada satu tabel | Biaya baca independen per stream; pin mengikuti offset tertua |
| Partisi tanpa perubahan | Dilewati di FE |
| Duplicate dengan banyak delete predicate | Scan `v1` dengan OR predikat; bisa mahal. Didokumentasikan sebagai anti-pattern |

Session variable: `stream_full_diff_ratio` (0.3). Config FE: `max_streams_per_table` (100). Property/global: `max_data_extension_time_in_days` (14).

### A.14 Rencana pengujian

1. **Unit test FE (`fe-core`):** parser untuk semua bentuk A.3 (termasuk syntax Snowflake yang ditolak dengan pesan jelas); analyzer (privilege, resolusi offset, partisi baru/hilang/overwrite, stale); persistensi (image + replay, restart FE, leader switch); lock dan attachment (C2, C3, C4, C5). Pola: `UserTaskTest`, `TimeTravelTest` di branch fork.
2. **Unit test BE:** `ChangeKeyScan` semua mode untuk PK (rowset + del file + DCG), non-PK rentang versi, path lewat stale rowset setelah compaction, error saat versi hilang.
3. **SQL test (`test/sql/test_stream/`):** semua skenario A.4 (#1–#23, C1–C9) untuk PK, Unique, Aggregate, Duplicate, dengan dan tanpa partisi. Output `SHOW STREAMS` dibandingkan kolom per kolom dengan daftar Snowflake.
4. **Uji kesetaraan dengan Snowflake:** script yang sama (DDL + urutan DML + `SELECT * FROM s ORDER BY ...`) dijalankan di akun Snowflake dan di StarRocks, lalu hasilnya dibandingkan. Prioritas untuk baris bertanda "perlu verifikasi" (A.4 #8, #17) dan semua perbedaan di A.17.
5. **E2E** `e2e/stream_e2e.sh` (pola `e2e/time_travel_e2e.sh`, `e2e/task_graph_e2e.sh`): stream + task `WHEN SYSTEM$STREAM_HAS_DATA` + `MERGE` ke tabel SCD, beberapa siklus.
6. **Ketahanan:** compaction paksa di antara konsumsi; restart BE; tablet repair/clone; `INSERT OVERWRITE`/`TRUNCATE` di tengah; DML konkuren selama konsumsi.
7. **Property test acak:** urutan DML acak, snapshot sebelum/sesudah disimpan, output stream dibandingkan dengan diff yang dihitung dari snapshot.

### A.15 Peta file (perkiraan)

**FE baru**
- `fe/fe-core/src/main/java/com/starrocks/stream/`: `Stream`, `StreamMode`, `PartitionOffset`, `InitialRows`, `StreamManager`, `StreamLock`, `StreamTxnCommitAttachment`, `StreamOffsetResolver`.
- `fe/fe-core/src/main/java/com/starrocks/sql/ast/stream/`: `CreateStreamStmt`, `DropStreamStmt`, `AlterStreamStmt`, `ShowStreamsStmt`, `DescStreamStmt`, `ShowStreamOffsetsStmt`, `StreamRelation`.
- `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/StreamAnalyzer.java`; `StreamRelationTransformer` (di `sql/optimizer/transformer`).
- `information_schema.streams`.

**FE diubah**
- `StarRocks.g4`, `StarRocksLex.g4`: statement stream, `CHANGES`, `table_changes`, `CHANGE_TRACKING` / `MAX_DATA_EXTENSION_TIME_IN_DAYS` di `ALTER TABLE ... SET`, `SYSTEM$STREAM_HAS_DATA`.
- `AstBuilder`, `Analyzer`, `AuthorizerStmtVisitor`, `ObjectType` (+`STREAM`), `OperationType`, `EditLog`, `GlobalStateMgr` (image block), `DatabaseTransactionMgr`, `TransactionState`, `OlapScanNode` (start_version, mode), `ConnectContext` (v2 dan stream yang dibaca transaksi), `CatalogRecycleBin` (pin), `InsertOverwriteJobRunner`, `DynamicPartitionScheduler` (penyebab drop), `VersionHistory` (dicatat juga untuk tabel ber-change-tracking), task: `WHEN` menerima `SYSTEM$STREAM_HAS_DATA` (saat ini `taskCondition` hanya `identifier '(' string ')'`, cukup diperluas ke ekspresi boolean dari fungsi `SYSTEM$...`).

**BE diubah**
- `gensrc/thrift/PlanNodes.thrift`.
- `be/src/exec/olap_scan_node.cpp`, `be/src/exec/pipeline/scan/olap_*`, `be/src/storage/tablet_reader.*`.
- `be/src/storage/tablet_updates.*`, `be/src/storage/tablet.*`.
- (S4) `be/src/storage/rowset_update_state.*` untuk change-key log.

### A.16 Keputusan desain

| # | Pertanyaan | Keputusan (2026-11-22) | Dampak |
|---|---|---|---|
| 1 | Drop partisi karena TTL | **DELETE secara default**, opsi **[ekstensi]** `IGNORE_PARTITION_DROP = TRUE` untuk mengabaikan drop oleh TTL. Drop manual, `TRUNCATE`, `INSERT OVERWRITE` selalu dicatat. | A.3, A.4 #22–23, A.5, A.6, A.11. Drop partisi di EditLog mencatat penyebab. |
| 2 | Arti `APPEND_ONLY` pada PK | **Semantik Snowflake** (hanya insert baru; baris yang kemudian di-delete tetap dikembalikan; upsert key lama tidak masuk) **dan** mode **[ekstensi]** `UPSERT_ONLY`. | A.3, A.4 #15–21, A.7.6, mode scan `INSERTED_KEYS` dan `WRITTEN_KEYS`. |
| 3 | Kapan pin presisi | **S1 memakai config global** time travel; pin presisi di **S4**. | A.10. |
| 4 | `__row_id` untuk Duplicate | **Opt-in** (`change_tracking_row_id = true`). | A.7.4. |
| 5 | Shared-data | **Ditunda** sampai time travel mendukung shared-data. | A.1, S4. |
| 6 | MV rewrite | **Dilewati** untuk query dengan `StreamRelation`, sama dengan time travel. | A.7. |
| 7 | Kompatibilitas Snowflake (2026-11-22) | **Default = perilaku Snowflake**; ekstensi opt-in; perbedaan dicatat di A.17. | Seluruh lampiran. |

Koreksi setelah pengecekan ulang docs Snowflake (2026-11-22):
- `APPEND_ONLY` **mengembalikan** baris yang di-insert lalu di-delete (sebelumnya ditulis "masih ada di v2").
- Update ke nilai yang sama **dicatat** sebagai pasangan update (sebelumnya dibuang). Snowflake tidak mendokumentasikan kasus ini; dipilih perilaku yang cocok dengan model Snowflake yang menulis ulang baris.
- Delete lalu insert key yang sama menjadi `DELETE` + `INSERT` dengan `ISUPDATE = FALSE` (sebelumnya pasangan update atau kosong).
- `TRUNCATE` / `INSERT OVERWRITE` = hapus semua + insert, bukan net change per key.
- Konsumen paralel **menunggu lock** (sebelumnya langsung gagal dengan error konflik).
- Dalam transaksi eksplisit, stream dibaca pada **waktu `BEGIN`** (sebelumnya pada pembacaan pertama).
- `SHOW_INITIAL_ROWS`: konsumsi pertama **hanya** snapshot saat create, perubahan sesudahnya di konsumsi berikutnya (sebelumnya digabung).
- `SYSTEM$STREAM_HAS_DATA` yang mengembalikan `FALSE` memperbarui `stale_after`.
- Kolom `SHOW STREAMS`, nama parameter (`CHANGE_TRACKING`, `MAX_DATA_EXTENSION_TIME_IN_DAYS`), `COMMENT = '...'`, `CLONE`, `CREATE OR ALTER`, `COPY GRANTS`, `AT(STATEMENT => ...)` disamakan dengan Snowflake.

### A.17 Matriks kompatibilitas dengan Snowflake

**Sama dengan Snowflake**

| Area | Catatan |
|---|---|
| `CREATE [OR REPLACE] STREAM [IF NOT EXISTS] ... ON TABLE`, `AT/BEFORE` (TIMESTAMP, OFFSET, STREAM), `APPEND_ONLY`, `SHOW_INITIAL_ROWS`, `COMMENT =`, `COPY GRANTS`, `CLONE`, `CREATE OR ALTER STREAM` | `AT(STATEMENT => ...)` di S2 |
| `ALTER STREAM SET/UNSET COMMENT`, `DROP STREAM`, `SHOW [TERSE] STREAMS` (kolom & filter), `DESC STREAM` | `IN ACCOUNT` = semua catalog internal |
| Kolom output + `METADATA$ACTION/ISUPDATE/ROW_ID`, skema mengikuti tabel saat ini | |
| Net change: insert→delete hilang; insert→update = INSERT `FALSE`; update berulang = satu pasangan; update→delete = DELETE nilai lama | |
| `TRUNCATE` tercatat di standard, tidak di append-only | |
| Append-only mengembalikan baris yang kemudian dihapus | |
| Offset maju hanya oleh DML yang commit (termasuk CTAS dan unload); `SELECT` tidak memajukan | |
| Repeatable read: stream dibaca pada waktu awal transaksi; offset maju ke waktu itu | |
| Lock stream untuk konsumen paralel | Batas tunggu mengikuti `lock_wait_timeout` StarRocks (Snowflake: `LOCK_TIMEOUT`) |
| `SYSTEM$STREAM_HAS_DATA`, termasuk mencegah stale saat `FALSE` | |
| Stale, `stale_after`, perpanjangan retensi sampai `MAX_DATA_EXTENSION_TIME_IN_DAYS` | |
| `CHANGE_TRACKING = TRUE`, stream pertama mengaktifkannya | |
| Privilege: `CREATE STREAM` di schema/database, `SELECT` pada stream + tabel | Dipetakan ke model privilege StarRocks |
| Rename tabel tidak merusak stream; drop + create ulang tidak terhubung; `CREATE OR REPLACE TABLE` membuat stale | |
| Stream tidak punya time travel / undrop | |
| Resep memajukan offset (`OR REPLACE`, `WHERE 0 = 1`) | |
| `CHANGES (INFORMATION => DEFAULT \| APPEND_ONLY) AT/BEFORE ... END` | |

**Berbeda (karena model data StarRocks)**

| Area | Snowflake | StarRocks | Dampak ke user |
|---|---|---|---|
| Identitas baris pada PK/Unique/Agg table | ROW_ID internal per baris; delete lalu insert = baris baru dengan ROW_ID baru | ROW_ID = hash primary key. Delete lalu insert key yang sama memakai **ROW_ID yang sama** (output `DELETE`+`INSERT` dengan `ISUPDATE = FALSE` tetap sama dengan Snowflake) | Kecil. Hanya berpengaruh jika konsumen memakai ROW_ID untuk membedakan "baris lama" dan "baris baru" dengan key yang sama |
| Upsert | Tidak ada (MERGE eksplisit) | `INSERT`/load ke key yang ada = UPDATE (pasangan `ISUPDATE = TRUE`); tidak masuk `APPEND_ONLY` | Sesuai intuisi PK table. Didokumentasikan |
| Aggregate Key table | Tidak ada padanan | Nilai dibandingkan setelah agregasi | – |
| ROW_ID Duplicate tanpa `__row_id` | Selalu stabil | Tidak stabil setelah compaction (WARNING saat create) | Aktifkan `change_tracking_row_id` jika ROW_ID dipakai |
| `CHANGES` sebelum change tracking aktif | Error | Boleh jika versinya masih tersimpan (time travel) | Lebih longgar; query Snowflake yang valid tetap valid |
| Namespace | `database.schema.stream` | `catalog.db.stream`, ditampilkan sebagai `database_name = catalog`, `schema_name = db` | Ubah qualified name di script; nama tanpa qualifier tetap sama |
| Sumber stream | Table, view, external, directory, dynamic, event, Iceberg | Tabel native (S1). Lainnya ditolak dengan pesan jelas | Direncanakan S4 |
| Shared-data | – | Belum didukung (S1) | – |
| `WITH TAG` | Didukung | Error sampai fitur tag (5.2) ada | – |
| Lock stream | Lock persisten di layanan Snowflake | Lock di memori leader FE; transaksi di-abort jika leader berganti | Konsumen cukup retry, seperti DML lain |

**Ekstensi StarRocks (opt-in, tidak mengubah default)**: `UPSERT_ONLY`, `IGNORE_PARTITION_DROP`, `SHOW STREAM OFFSETS`, `table_changes()` (gaya Databricks), alias syntax StarRocks (`COMMENT 'x'`, `SET ("change_tracking"="true")`, `SHOW STREAMS FROM db`).

**Perlu verifikasi di akun Snowflake** (uji A.14 #4): A.4 #8 (update ke nilai yang sama), A.4 #17 (append-only: insert lalu update), urutan baris output (Snowflake tidak menjamin urutan; test harus memakai `ORDER BY`).

### A.18 Referensi Snowflake untuk lampiran ini

- Introduction to streams: https://docs.snowflake.com/en/user-guide/streams-intro
- CREATE STREAM: https://docs.snowflake.com/en/sql-reference/sql/create-stream
- SHOW STREAMS: https://docs.snowflake.com/en/sql-reference/sql/show-streams
- ALTER STREAM: https://docs.snowflake.com/en/sql-reference/sql/alter-stream
- CHANGES: https://docs.snowflake.com/en/sql-reference/constructs/changes
- SYSTEM$STREAM_HAS_DATA: https://docs.snowflake.com/en/sql-reference/functions/system_stream_has_data
- Parameter `MAX_DATA_EXTENSION_TIME_IN_DAYS`: https://docs.snowflake.com/en/sql-reference/parameters#max-data-extension-time-in-days
- Tasks triggered by streams: https://docs.snowflake.com/en/user-guide/tasks-triggered
