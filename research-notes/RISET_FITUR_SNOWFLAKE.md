# Riset: Time Travel, Task, dan Stored Procedure ala Snowflake untuk StarRocks

- **Basis kode:** fork `dwickyfp/starrocks`, setara upstream `main` per 21 Nov 2026 (commit `a9a9fd73`)
- **Metode:** penelusuran kode FE (Java) dan BE (C++) oleh tiga agen paralel. Setiap klaim kunci diverifikasi ulang langsung di kode (path dan nomor baris di bawah).
- **Status:** ketiga fitur sudah diimplementasikan di fork: Task + task graph (`feature/snowflake-task`), Stored Procedure (`feature/stored-procedure`), dan Time Travel shared-nothing (`feature/time-travel`). Lihat bagian 1.4, 2.5, dan 3.4 untuk desain yang dipilih dan batasannya.

> **Konvensi path:**
> - `G/` = `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/`
> - `C/` = `fe/fe-core/src/main/java/com/starrocks/`
> - `P/` = `fe/fe-parser/src/main/java/com/starrocks/sql/ast/`
> - `BE/` = `be/src/`

---

## Ringkasan eksekutif

| Fitur | Yang sudah ada di StarRocks | Celah utama | Kesulitan | Estimasi kasar* |
|---|---|---|---|---|
| **1. Time Travel** | Klausa tersembunyi `BEFORE '<ts>'` untuk tabel OLAP shared-nothing (berbasis GTID = timestamp commit). BE bisa membaca versi lama. Lake menyimpan metadata per versi. `RECOVER` ≈ UNDROP. | FE tidak punya **histori versi↔waktu**, layout partisi lama tidak disimpan, GC versi hanya ±30 menit dan global, dan **lake mengabaikan `BEFORE` diam-diam** | **Tinggi** (FE+BE, storage) | MVP lake: 6–10 minggu. Penuh: 4–6 bulan |
| **2. Task ala Snowflake** | `SUBMIT TASK`, `ALTER TASK RESUME/SUSPEND/SET`, `DROP TASK`, scheduler interval, histori run | Tidak ada `CREATE TASK`, `EXECUTE TASK`, `SHOW/DESC TASKS`, cron/zona waktu, DAG (`AFTER`), `WHEN`, retry, privilege task | **Sedang** (FE saja) | MVP: 3–5 minggu. DAG+WHEN: +3–4 minggu |
| **3. Stored Procedure** | Grammar `CALL` (dipakai Iceberg), token `$$…$$`, framework UDF (metadata, overloading, persistensi), `SimpleExecutor` untuk SQL bersarang, transaksi eksplisit | Tidak ada objek procedure user, **bahasa scripting**, interpreter, cursor, exception, EXECUTE AS OWNER | **Tinggi** (FE saja, tapi besar) | MVP: 5–8 minggu. Lengkap: +4–6 minggu |

\* Estimasi untuk 1 engineer berpengalaman yang sudah paham codebase. Angka ini perkiraan kasar dari hasil membaca kode, bukan dari prototipe.

**Urutan yang disarankan:**
1. **Task.** Risiko paling rendah, murni FE, langsung terasa manfaatnya. Sekalian menutup celah keamanan yang ditemukan.
2. **Stored Procedure.** Murni FE. Setelah ada, Task bisa menjalankan `CALL proc()` untuk alur multi-statement.
3. **Time Travel.** Paling berisiko karena menyentuh storage dan GC. Mulai dari mode **shared-data (lake)**, yang paling dekat secara arsitektur.

**Temuan sampingan penting:**
- 🔴 **Celah keamanan:** `DROP TASK` dan `ALTER TASK` tidak punya pengecekan privilege sama sekali. `C/sql/analyzer/AuthorizerStmtVisitor.java` hanya meng-override `visitSubmitTaskStatement` (baris 2387). Akibatnya user mana pun bisa men-drop atau men-suspend task milik orang lain, termasuk task refresh MV dengan `FORCE`.
- 🟠 **Hasil salah diam-diam:** `SELECT … FROM lake_table BEFORE '<ts>'` mengembalikan data **terkini** tanpa error, karena konektor lake (`BE/connector/lake/lake_connector.cpp`) tidak membaca field `gtid` sama sekali.

Keduanya layak diperbaiki lebih dulu sebagai bug-fix kecil.

---

# 1. Time Travel ala Snowflake

## 1.1 Perilaku Snowflake yang ditiru

| Snowflake | Fungsi |
|---|---|
| `SELECT … FROM t AT(TIMESTAMP => ts)` | Data per waktu tertentu |
| `AT(OFFSET => -3600)` | Data N detik yang lalu |
| `AT(STATEMENT => '<query_id>')` / `BEFORE(STATEMENT => …)` | Data saat / sebelum query tertentu dijalankan |
| `CREATE TABLE t2 CLONE t [AT(…)]` | Salinan (zero-copy) tabel per waktu |
| `UNDROP TABLE/SCHEMA/DATABASE` | Kembalikan objek yang di-drop |
| `DATA_RETENTION_TIME_IN_DAYS` (per objek + minimum akun) | Lama versi lama disimpan; GC mengikuti nilai ini |

## 1.2 Kondisi di StarRocks saat ini

### a. Syntax yang sudah ada

**`queryPeriod`** (`G/StarRocks.g4:2516`):
- Bentuknya `FOR? (SYSTEM_TIME|TIMESTAMP|VERSION) AS OF expr`.
- AST-nya `P/QueryPeriod.java`.
- Analyzer menolaknya untuk tabel internal: `C/sql/analyzer/QueryAnalyzer.java` (~l.949) melempar *"Unsupported table type for temporal clauses"* kecuali `table.isTemporal()`.
- `isTemporal()` hanya `true` di `IcebergTable`, `PaimonTable`, dan `MysqlTable`.

**Klausa tersembunyi `BEFORE`** (`G/StarRocks.g4:2614`, `#tableAtom`): `… bracketHint? (BEFORE ts=string)?`
- Tidak terdokumentasi.
- `C/sql/parser/AstBuilder.java` (~l.6745) mengubah `'yyyy-MM-dd HH:mm:ss'` menjadi GTID lewat `GtidGenerator.getGtid(ms)`.
- GTID mengalir ke `OlapScanNode` lalu ke `TInternalScanRange.gtid` (`gensrc/thrift/PlanNodes.thrift` field 16).

### b. GTID = timestamp commit

`C/transaction/GtidGenerator.java` meng-encode `(ms − EPOCH 2020-01-01) << 21 | cluster | seq`:
- GTID dibuat saat COMMIT (`DatabaseTransactionMgr`, ~l.1761).
- Lalu ikut publish ke BE dan disimpan di `RowsetMetaPB.gtid` (`gensrc/proto/olap_file.proto`).

### c. BE shared-nothing sudah bisa membaca versi lama

- `BE/storage/tablet.cpp` menyimpan `_gtid_to_version_map`.
- `Tablet::capture_consistent_rowsets(gtid)` (~l.906) mencari versi lewat *version graph*.
- Tabel Primary Key ditangani `tablet_updates.cpp:3945` (`get_applied_rowsets_by_gtid`), yang menelusuri `_edit_version_infos`.
- Membaca dengan **nomor versi eksplisit** (`Version(0, v)`) juga sudah didukung.

### d. Batas utamanya: GC

- Shared-nothing: `tablet_rowset_stale_sweep_time_sec = 1800` (`BE/common/config.h:413`). Riwayat hanya bertahan ±30 menit, berlaku global, dan tidak bisa diatur per tabel.
- Clone, repair replika, dan schema change berat juga mereset riwayat.

### e. Shared-data (lake)

- Setiap versi punya file `TabletMetadataPB` sendiri yang immutable, berisi `commit_time` (13), `gtid` (19), `historical_schemas` (17), dan `rowset_to_schema` (18) (`gensrc/proto/lake_types.proto`).
- Membaca versi lama cukup dengan mengubah `scan_range.version` (`BE/connector/lake/lake_connector.cpp:397/713/2199`), **asal file-nya belum di-vacuum**.
- **Vacuum** (`C/lake/vacuum/AutovacuumDaemon.java`) mengirim:
  - `minRetainVersion`
  - `graceTimestamp = now − lake_autovacuum_grace_period_minutes`, default 30 (`C/common/Config.java:4406`)
- Jadi retensi berbasis waktu **sudah ada**, hanya saja global dan dalam satuan menit.
- `C/lake/snapshot/ClusterSnapshotMgr.java` sudah punya mekanisme "lindungi versi tertentu dari vacuum", yang bisa dipakai ulang.

### f. Recycle bin sebagai padanan UNDROP

- Syntax yang ada: `RECOVER DATABASE|TABLE|PARTITION` (`C/catalog/CatalogRecycleBin.java`).
- Retensi diatur `catalog_trash_expire_second = 86400` (global).
- Keterbatasan:
  - Map-nya di-key oleh **nama**, jadi hanya objek *terakhir* dengan nama itu yang bisa dipulihkan. Snowflake menyimpan beberapa.
  - Objek yang di-drop dengan `DROP … FORCE` tidak bisa dipulihkan.

### g. Histori versi↔waktu: belum ada

- `PhysicalPartition` hanya menyimpan `visibleVersion` dan `visibleVersionTime` **terakhir**.
- `PartitionCommitInfo` (versi + waktu) memang ada di edit log transaksi, tapi histori transaksinya dipangkas.

### h. Layout fisik berubah seiring waktu

`INSERT OVERWRITE` menukar partisi (`OlapTable.replaceTempPartitionsWithoutCheck`, ~l.2856), sehingga partisi lama punya **ID partisi dan ID tablet yang berbeda**. Hal serupa terjadi pada drop partisi, truncate, dan reshard tablet di lake.

Inilah alasan klausa `BEFORE` yang ada sekarang **salah secara konsep**: ia memakai daftar partisi *saat ini*, lalu meminta versi lama per tablet.

## 1.3 Desain yang diusulkan

### Syntax target

```sql
-- Query
SELECT * FROM orders AT(TIMESTAMP => '2026-11-20 10:00:00'::DATETIME);
SELECT * FROM orders AT(OFFSET => -3600);            -- 1 jam lalu
SELECT * FROM orders AT(STATEMENT => '8f3c…-query-id');
SELECT * FROM orders BEFORE(STATEMENT => '8f3c…');
SELECT * FROM orders FOR TIMESTAMP AS OF '2026-11-20 10:00:00';   -- tetap didukung (kompatibel Iceberg)

-- Retensi
CREATE TABLE orders (...) PROPERTIES ("data_retention_time" = "7d");
ALTER TABLE orders SET ("data_retention_time" = "1d");

-- Pemulihan & salinan
UNDROP TABLE orders;          -- alias RECOVER TABLE, dengan semantik Snowflake
UNDROP DATABASE sales;
CREATE TABLE orders_backup CLONE orders AT(TIMESTAMP => '2026-11-20 10:00:00');

-- Juga boleh sebagai sumber DML (seperti Snowflake)
INSERT INTO orders_fix SELECT * FROM orders AT(OFFSET => -600);
```

### Komponen yang perlu dibuat

| # | Komponen | Apa yang dikerjakan | File utama | Kesulitan |
|---|---|---|---|---|
| T1 | Grammar & AST | Rule `atBeforeClause : (AT \| BEFORE) '(' (TIMESTAMP\|OFFSET\|STATEMENT\|VERSION) '=>' expression ')'` di `relationPrimary`. `UNDROP`, `CLONE … [AT]`. Token `=>` sudah dipakai `argumentList` | `G/StarRocks.g4`, `G/StarRocksLex.g4` (tambah `UNDROP`, `OFFSET` sudah ada?), `P/QueryPeriod.java` (tambah jenis OFFSET/STATEMENT), `C/sql/parser/AstBuilder.java` | Mudah |
| T2 | Analyzer | `OlapTable`/`LakeTable` jadi temporal **hanya bila** properti retensi > 0. Ubah OFFSET → waktu absolut (pakai waktu mulai query agar konsisten di satu statement). STATEMENT → waktu mulai query (lihat T7). Tolak di view/MV definition (sudah ada `AnalyzerUtils.prohibitTimeTravelQuery`). Matikan MV rewrite, query cache, dan short-circuit untuk relasi ber-AT | `C/sql/analyzer/QueryAnalyzer.java`, `QueryPeriodResolver.java` | Mudah–Sedang |
| T3 | **Ledger histori versi (inti)** | Struktur per tabel: `TableVersionHistory = List<Entry{commitTsMs, gtid, txnId, Map<physicalPartitionId, version>}>` + `List<LayoutEpoch{effectiveTs, partitions→indexes→tablets, schemaId}>`. Diisi saat publish (`PublishVersionDaemon` → `updateVisibleVersion`) dan saat DDL yang mengubah layout. Dipangkas oleh retensi | Kelas baru `C/catalog/history/TableVersionHistory.java`; hook di `C/transaction/*TxnLogApplier.java` | **Sulit** |
| T4 | Persistensi ledger | Entri versi bisa **direkonstruksi dari replay edit log transaksi** yang sudah ada (`PartitionCommitInfo` berisi versi + `versionTime`), jadi tidak perlu op baru untuk setiap commit. Image butuh `SRMetaBlock` baru. Perubahan layout dicatat dengan op baru (ordinal baru, jangan dipakai ulang) | `C/persist/OperationType.java`, `C/persist/metablock/` | Sedang |
| T5 | Planner: pin versi & layout | Resolve T → layout epoch → daftar partisi/tablet historis + versi per partisi. Pakai mekanisme `ConnectContext.getScanVersionOverride()` (sudah ada, `C/qe/ConnectContext.java:346`, dipakai `OnlineOptimizeJobV2`). Partition pruning memakai *range info historis*. Replika dipilih yang masih memegang versi itu | `C/planner/OlapScanNode.java` (`chooseScanVersion`, ~l.673), `C/sql/optimizer/transformer/RelationTransformer.java` | Sedang |
| T6 | BE: baca per versi | Kirim **nomor versi**, bukan GTID. Ini sudah jalan di kedua mode dan sekaligus menutup bug lake. Untuk shared-nothing: replika tanpa versi lama (habis clone/repair) harus menghasilkan error jelas, bukan data salah | `BE/exec/pipeline/scan/olap_scan_context.cpp`, `BE/connector/lake/lake_connector.cpp` | Mudah |
| T7 | `STATEMENT =>` | Butuh peta `query_id → waktu mulai` yang persisten (tabel audit internal dengan TTL = retensi maks) | Hook audit di `C/qe/StmtExecutor.java` / `C/plugin/AuditEvent` | Sedang |
| T8 | **Retensi & GC** | Properti tabel `data_retention_time` + `Config.max_data_retention_time` dan minimum global. **Lake:** `AutovacuumDaemon` mengirim `graceTimestamp` per partisi = now − retensi tabel. **Shared-nothing:** sweep stale rowset harus per-tablet (properti di tablet meta), plus base compaction yang tidak boleh membuang input yang masih dalam retensi. **Recycle bin:** partisi yang diganti/di-drop disimpan selama retensi | `C/lake/vacuum/AutovacuumDaemon.java`, `BE/storage/tablet.cpp`, `BE/storage/tablet_updates.cpp` (PK: delvec + persistent index), `C/catalog/CatalogRecycleBin.java` | **Sulit** (terutama shared-nothing & PK) |
| T9 | Schema historis | Fase 1: izinkan hanya bila skema kolom yang diquery tidak berubah sejak T (cek lewat schemaId per epoch). Fase 2: simpan skema per `schemaId` selama retensi (lake sudah punya `historical_schemas`) | `C/alter/OlapTableHistorySchema.java` (precedent) | Sedang |
| T10 | UNDROP | Alias `RECOVER` + semantik Snowflake. Ubah `CatalogRecycleBin` agar menyimpan **beberapa** versi drop per nama (key `name → stack<id>`) dan memakai kedaluwarsa berbasis retensi objek | `C/catalog/CatalogRecycleBin.java` | Mudah–Sedang |
| T11 | CLONE … AT | **Fase 1:** CTAS dari `SELECT * FROM t AT(…)` (mudah, tapi menyalin data). **Fase 2 (zero-copy):** lake memakai metadata-only clone dengan file bersama (`shared_file` di `lake_types.proto` sudah ada) + vacuum ber-referensi; shared-nothing memakai hard-link snapshot per versi (`BE/storage/snapshot_manager.cpp`, seperti BACKUP) | `C/backup/*`, `BE/storage/snapshot_manager.cpp` | Sedang–Sulit |

### Strategi bertahap (disarankan)

1. **Fase 0, perbaikan cepat (≈1 minggu):**
   - Klausa `BEFORE '<ts>'` pada tabel lake harus **error**, bukan diam-diam mengembalikan data terkini.
   - Dokumentasikan batasan klausa itu di shared-nothing: ±30 menit, dan layout harus sama.
2. **Fase 1, Time Travel untuk tabel lake (≈6–10 minggu).** Mulai dari lake karena:
   - metadata per versi immutable dan sudah menyimpan `commit_time`,
   - pembacaan per versi sudah bisa,
   - retensi tinggal diubah dari global menjadi per tabel di `AutovacuumDaemon`.

   Cakupannya T1–T6, T8 (lake), dan T9 tahap 1.
3. **Fase 2, UNDROP + CLONE (CTAS) (≈3–4 minggu).** T10, T11 tahap 1.
4. **Fase 3, shared-nothing (≈6–10 minggu).** Retensi per tablet di BE, interaksi dengan compaction dan PK, serta penanganan clone/repair replika.
5. **Fase 4:** `STATEMENT =>`, zero-copy CLONE, schema historis penuh.

### Risiko

- **Ledakan storage.** Versi lama tidak bisa dibersihkan selama masa retensi, dan base compaction menggandakan data sementara. Tabel PK paling rapuh, karena delete vector dan persistent index per versi ikut harus disimpan.
- **Clock skew.** GTID dan `commit_time` berasal dari jam FE leader. Gunakan waktu commit FE secara konsisten, jangan jam BE.
- **Memori FE.** Ledger berukuran tabel × partisi × commit. Perlu kompaksi: cukup satu entri per commit, dan hanya untuk partisi yang berubah.
- **Kompatibilitas upgrade.** Op edit log baru harus memakai ordinal baru. Field Gson baru harus aman bila tidak ada.


## 1.4 Yang diimplementasikan (branch `feature/time-travel`)

Keputusan utama: **shared-nothing dulu, murni FE, tanpa perubahan BE.** BE shared-nothing sudah bisa membaca versi lama selama rowset-nya belum disapu, jadi yang kurang hanya (1) peta waktu↔versi di FE dan (2) GC yang cukup lama. Ini jauh lebih kecil risikonya daripada retensi per tablet di BE. Shared-data ditolak dengan error yang jelas, bukan diam-diam membaca data terkini.

**Retensi (sesuai permintaan: disimpan di global env):**
- Global variable `data_retention_time_in_days`, hanya `SET GLOBAL`, dipersist lewat edit log global variable. Default 0 (mati) agar disk tidak membengkak tanpa sengaja.
- Table property `data_retention_time_in_days` (CREATE TABLE / ALTER TABLE SET, tampil di SHOW CREATE TABLE) meng-override global, termasuk nilai 0 untuk mematikan per tabel.
- Rentang 0..`time_travel_max_retention_days` (FE config, default 90, seperti batas Snowflake Enterprise).

**Histori versi:** setiap `PhysicalPartition` tabel ber-retensi menyimpan `VersionHistory` (array `version`, `time`, di-persist Gson). Titik dicatat di `OlapTableTxnLogApplier.applyVisibleLog` dengan `versionTime` dari edit log, jadi leader dan follower (replay) membangun histori yang sama. Titik pertama juga mencatat versi yang terlihat sebelumnya, sehingga histori mencakup waktu sejak load terakhir sebelum retensi dinyalakan. Pemangkasan: titik yang lebih tua dari retensi dibuang kecuali satu yang terbaru (penanda versi di awal periode), dibatasi `time_travel_max_versions_per_partition` (1000).

**GC di BE:** daemon leader `TimeTravelBeConfigSyncer` menaikkan config BE `tablet_rowset_stale_sweep_time_sec` ke `retensi_terpanjang × 86400 + 3600` lewat `POST /api/update_config` (respon `status: OK` dicek). Config ini berlaku untuk stale rowset hasil compaction maupun versi PK yang kedaluwarsa. Diterapkan ulang bila BE restart (deteksi lewat `lastStartTime`), dan dikembalikan ke default 1800 bila tidak ada tabel yang butuh. Konsekuensi: kenaikan disk berlaku untuk semua tabel di BE, karena config ini bukan per tabel.

**Syntax:**
```sql
SELECT * FROM t AT(TIMESTAMP => '2026-11-21 08:00:00');
SELECT * FROM t AT(OFFSET => -3600);                 -- seconds_add(now(), -3600)
SELECT * FROM t BEFORE(TIMESTAMP => '2026-11-21 08:00:00');  -- t - 1 ms
SELECT * FROM t FOR TIMESTAMP AS OF '2026-11-21 08:00:00';
INSERT INTO t_restored SELECT * FROM t AT(OFFSET => -600);
```
Token baru `AT_TIME_TRAVEL` ('AT', non-reserved). Semuanya dipetakan ke `QueryPeriod(TIMESTAMP, expr)` yang sudah ada.

**Resolusi:** `TimeTravelResolver` (dipanggil `QueryAnalyzer` untuk tabel OLAP dengan klausa temporal) mengevaluasi titik waktu di FE, menolak titik di masa depan atau di luar retensi, lalu menghitung versi per partisi fisik dan menyimpannya di `ConnectContext` (`timeTravelScanVersions`). `OlapScanNode.chooseScanVersion` memakainya. State dibersihkan oleh `TimeTravelScope` di `StatementPlanner.plan` (sadar nesting). Partisi tanpa data pada titik itu membaca versi 1 (kosong).

**Pengaman hasil-salah yang ditemukan saat pengujian:**
- `count(*)` diubah optimizer menjadi MetaScan yang membaca metadata tablet *terkini*. `RewriteSimpleAggToMetaScanRule` kini dilewati bila ada time travel.
- MV rewrite (MV berisi data terkini), query cache, dan short-circuit read juga dilewati.
- Membaca tabel yang sama "sekarang" dan "di masa lalu" dalam satu statement, atau di dua titik waktu berbeda, ditolak, karena pin versi berbasis id partisi.
- View/MV dengan time travel ditolak (pengecekan yang sudah ada).

**Pengujian:** `TimeTravelTest` (7 test: titik waktu, BEFORE/OFFSET/AS OF, batas retensi, global var vs property, ALTER/SHOW CREATE, VersionHistory + Gson, target config BE) plus regresi sekitar 1100 test analyzer/planner/alter/iceberg/MV. E2E nyata di cluster: `e2e/time_travel_e2e.sh` (PK insert/update/delete, agregat masa lalu, compaction lalu tetap terbaca, restore via INSERT…SELECT).

**Batasan:**
- Shared-nothing saja.
- Schema saat ini yang dipakai.
- Partisi yang diganti (TRUNCATE, INSERT OVERWRITE, replace temp partition termasuk OPTIMIZE dan merge partition) serta schema change yang menulis ulang data tidak menyimpan histori sebelum penggantian. Sejak branch `feature/hardening`, membaca titik sebelum penggantian itu **ditolak dengan error yang jelas** (bukan lagi terbaca kosong). Waktu batasnya (`historyFloorTime` per partisi fisik) diambil dari data yang ikut di journal, jadi leader dan follower sama. Titik sesudahnya tetap bisa dibaca.
- `UNDROP TABLE/DATABASE/PARTITION` tersedia sebagai alias `RECOVER`; masa simpannya mengikuti `catalog_trash_expire_second`, bukan retensi.
- `FOR VERSION AS OF`, `AT(STATEMENT => …)` dan CLONE belum ada.
- Versi di bawah `minReadableVersion` replika (mis. setelah clone) gagal dengan error replika.

---

# 2. Task ala Snowflake

## 2.1 Perilaku Snowflake yang ditiru

```sql
CREATE [OR REPLACE] TASK [IF NOT EXISTS] db.nightly_agg
  WAREHOUSE = etl_wh
  SCHEDULE = 'USING CRON 0 2 * * * Asia/Jakarta'       -- atau '15 MINUTE'
  ALLOW_OVERLAPPING_EXECUTION = FALSE
  USER_TASK_TIMEOUT_MS = 3600000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  TASK_AUTO_RETRY_ATTEMPTS = 2
  query_timeout = 3600                                -- session parameter
  COMMENT = 'agregasi harian'
  [AFTER db.task_a, db.task_b]                        -- anak dalam DAG (tanpa SCHEDULE)
  [WHEN <ekspresi boolean>]
AS
  INSERT INTO agg SELECT … ;                           -- atau: CALL proc_etl();

ALTER TASK db.nightly_agg RESUME;      -- task dibuat dalam keadaan SUSPENDED
ALTER TASK db.nightly_agg SUSPEND;
EXECUTE TASK db.nightly_agg;            -- jalankan sekali sekarang
ALTER TASK db.nightly_agg SET SCHEDULE = '30 MINUTE';
ALTER TASK db.nightly_agg UNSET COMMENT;
ALTER TASK db.nightly_agg MODIFY AS INSERT INTO …;
ALTER TASK db.child ADD AFTER db.task_c;  /  REMOVE AFTER db.task_a;
SHOW TASKS [LIKE 'pat'] [IN DATABASE db];
DESCRIBE TASK db.nightly_agg;
DROP TASK [IF EXISTS] db.nightly_agg;
SELECT * FROM information_schema.task_history WHERE task_name = 'NIGHTLY_AGG';
```

## 2.2 Kondisi di StarRocks saat ini

**Grammar** (`G/StarRocks.g4:709–730`):
```
SUBMIT TASK name? (PROPERTIES(...) | SCHEDULE [START('ts')] EVERY(INTERVAL n unit))*
   AS (CTAS | INSERT | CACHE SELECT)
ALTER TASK [IF EXISTS] name (RESUME | SUSPEND | SET (props))
DROP TASK [IF EXISTS] name [FORCE]
```

**Yang tidak ada:**
- Statement `CREATE TASK`, `EXECUTE TASK`, `SHOW TASKS`, `DESC TASK`, `ALTER … MODIFY AS`.
- Body task selain CTAS, INSERT, atau `CACHE SELECT`.
- Task baru langsung ACTIVE. Task tanpa jadwal langsung jalan sekali dan kedaluwarsa dalam 1 hari.

**Engine** (`C/scheduler/`):
- **Model:** `Task` (persisten via Gson), `TaskSchedule = {startTime, period, timeUnit}`. Tidak ada cron dan tidak ada zona waktu.
- **Penjadwalan:** `TaskManager` memakai satu `ScheduledExecutorService` dengan `scheduleAtFixedRate` per task.
- **Antrean:** `TaskRunManager`/`TaskRunScheduler`.
  - Maksimal 1 run per task berjalan bersamaan.
  - Maksimal `task_runs_concurrency` (4) run total.
  - Run yang tertunda di-*merge*.
- **Eksekusi:** `TaskRun.buildTaskRunConnectContext`.
  - Berjalan sebagai creator, atau sebagai root, tergantung config `mv_use_creator_based_authorization`.
  - Properti diterapkan sebagai session variable. Properti `warehouse` memilih warehouse.
- **Gagal & timeout:**
  - Tidak ada retry.
  - Auto-suspend setelah N kegagalan **hanya untuk MV**, dan counter-nya tidak persisten.
  - Timeout global 4 jam, dihitung sejak run *dibuat*, bukan sejak mulai jalan.
- **Persistensi:** op `OP_CREATE_TASK 10071`, `OP_DROP_TASKS 10072`, `OP_ALTER_TASK 10085`, dan seterusnya. Image disimpan di blok `TASK_MGR`.
- **Histori:** di memori (7 hari) dan diarsipkan ke `_statistics_.task_run_history`. Terlihat lewat `information_schema.tasks` (tanpa kolom STATE) dan `task_runs`.
- **HA:** hanya leader yang menjalankan task. Saat failover, semua run PENDING/RUNNING ditandai FAILED.
- **Nama task global:** `nameToTaskMap` di-key oleh nama saja, bagian db diabaikan.

**Pengguna model Task yang harus tetap kompatibel:**
- Refresh MV (task `mv-<id>`, paling banyak dipakai): `TaskBuilder`, `LocalMetastore`, `AlterJobMgr`, `LoadJobMVListener`.
- Pipe: `C/load/pipe/Pipe.java`.
- Optimize / merge partition: `C/alter/OptimizeJobV2.java`, `MergePartitionJob.java`.
- DataCache warmup.

## 2.3 Desain yang diusulkan

### Model data: perluasan `Task`

Semua field baru bersifat opsional dan aman untuk Gson:

```
Task {
  … field lama …
  long   dbId;                       // namespace per database (baru)
  String comment;
  ScheduleSpec schedule;             // INTERVAL {n, unit} | CRON {expr, timeZone}
  boolean allowOverlapping = false;
  int    autoRetryAttempts = 0;
  int    suspendAfterNumFailures = 10;   // default Snowflake
  long   timeoutMs;                  // dihitung dari mulai eksekusi
  List<Long> predecessorIds;         // AFTER (DAG)
  Long   finalizerOfRootId;          // FINALIZE
  String whenCondition;              // WHEN
  int    consecutiveFailCount;       // kini persisten
  TaskSource source = USER_SQL;      // sumber baru, untuk membedakan dari MV/PIPE/…
}
```
- State: `PAUSE` dipetakan ke `SUSPENDED` dan `ACTIVE` ke `STARTED` (tampilan ala Snowflake).
- Task hasil `CREATE TASK` **dimulai SUSPENDED** dan tanpa `expireTime`.

### Komponen yang perlu dibuat

| # | Komponen | Detail | File utama | Kesulitan |
|---|---|---|---|---|
| K1 | Grammar | `createTaskStatement`, perluasan `alterTaskStatement` (SET/UNSET props, MODIFY AS, MODIFY WHEN, ADD/REMOVE AFTER), `executeTaskStatement` (tidak bentrok dengan `EXECUTE stmt USING` karena `TASK` keyword tersendiri), `showTasksStatement`, `describeTaskStatement`. Body: **satu statement apa pun** (DML/DDL/`CALL`) yang disimpan sebagai teks dan divalidasi parse saat create | `G/StarRocks.g4`, `G/StarRocksLex.g4` | Sedang |
| K2 | AST & analyzer | `CreateTaskStmt`, `ExecuteTaskStmt`, `ShowTasksStmt`, `DescTaskStmt` (immutable, sesuai aturan `fe/AGENTS.md`). Validasi session param terhadap `VariableMgr`, cron, dan siklus DAG | `P/`, `C/sql/analyzer/TaskAnalyzer.java` | Sedang |
| K3 | Namespace per database | Re-key `nameToTaskMap` menjadi `(dbId, name)`. Nama lama (global) tetap bisa ditemukan untuk kompatibilitas `SUBMIT TASK` dan `mv-<id>` | `C/scheduler/TaskManager.java` | Sedang |
| K4 | Cron + zona waktu | Ganti `scheduleAtFixedRate` dengan **hitung waktu fire berikutnya lalu `schedule()` ulang setelah tiap fire**. Parser cron: library berlisensi Apache 2.0 (mis. `cron-utils`) atau implementasi kecil 5-field. Perbaiki bug `nextScheduleTime` UTC di `registerScheduler` (~l.1079) | `C/scheduler/TaskManager.java`, `C/scheduler/persist/TaskSchedule.java` | Sedang |
| K5 | SUSPEND/RESUME/EXECUTE | SUSPEND ala Snowflake: run yang sedang jalan dibiarkan selesai (pakai `suspendTaskWithoutKilling`). `EXECUTE TASK` memanggil `TaskManager.executeTask(name, manual)` dan boleh untuk task yang SUSPENDED | `C/qe/DDLStmtExecutor.java`, `TaskManager` | Mudah |
| K6 | Overlap, retry, timeout, auto-suspend | `ALLOW_OVERLAPPING_EXECUTION=FALSE`: **skip** tick bila sedang jalan (Snowflake skip, tidak antre). `TRUE`: cabut invariant "1 run per task" (`runningTaskRunMap: Map<Long, TaskRun>` → multimap). Retry: loop di `TaskRunExecutor` + nomor attempt di `TaskRunStatus`. Timeout dihitung dari `processStartTime`. Auto-suspend digeneralisasi dari MV ke semua task (`TaskRun.executeTaskRun` ~l.524) | `C/scheduler/TaskRun*.java` | Sedang (overlap = Sedang–Sulit) |
| K7 | DAG (`AFTER`, `FINALIZE`) | Hanya root yang punya SCHEDULE. Setiap fire root membuat `graphRunId`. Anak di-dispatch saat **semua** pendahulunya SUCCESS. Kalau pendahulu gagal, anak tidak jalan. Finalizer selalu jalan di akhir. Mengubah anak hanya boleh saat root SUSPENDED (aturan Snowflake). Hook di titik selesainya run (`checkRunningTaskRun`). Saat failover, graph run ditandai FAILED (v1) | Kelas baru `C/scheduler/graph/TaskGraphManager.java` | **Sulit** |
| K8 | `WHEN` | Evaluasi `SELECT <expr>` lewat `SimpleExecutor` sebelum run. Kalau FALSE, run ditandai `SKIPPED` (state ini sudah ada) | `C/scheduler/TaskRun.java` | Sedang |
| K9 | Observabilitas | `SHOW TASKS` / `DESC TASK`. Tambah kolom STATE, OWNER, SCHEDULE, PREDECESSORS, CONDITION, COMMENT, LAST/NEXT_RUN ke `TasksSystemTable` dan `TTaskInfo` (thrift: field **optional**, ordinal baru). `information_schema.task_history` menampilkan scheduled time, attempt, dan graph run id | `C/catalog/system/information/TasksSystemTable.java`, `TaskRunsSystemTable.java`, `gensrc/thrift/FrontendService.thrift` | Rendah–Sedang |
| K10 | Privilege | `ObjectType.TASK` + `TaskPEntryObject` (meniru PIPE). Privilege: `CREATE TASK` (pada DB), `ALTER`, `DROP`, `OPERATE` (suspend/resume/execute), `MONITOR`. GRANT grammar + cek di `AuthorizerStmtVisitor` untuk CREATE/ALTER/DROP/EXECUTE/SHOW. **Menutup celah DROP/ALTER tanpa cek** | `C/authorization/ObjectType.java`, `PrivilegeType.java`, `C/sql/analyzer/AuthorizerStmtVisitor.java` | Sedang–Sulit |
| K11 | Kompatibilitas | `SUBMIT TASK` dan `SCHEDULE START … EVERY` tetap jalan, dipetakan ke model baru (dibuat ACTIVE seperti sekarang). Task MV/PIPE/Optimize/DataCache tetap memakai `TaskSource` dan processor masing-masing, beserta aturan khususnya (merge run, larangan drop, catch-up) | — | Sedang |

### Tahapan

1. **MVP (3–5 minggu):** K1–K5, K6 (tanpa overlap=TRUE), K9, K10, K11.
2. **Lanjutan (3–4 minggu):** K7 (DAG + FINALIZE), K8 (WHEN), overlap=TRUE.
3. Setelah Stored Procedure tersedia: `AS CALL proc()` memungkinkan task multi-statement.

### Risiko

- **Re-key nama per database** menyentuh banyak tempat yang mengandalkan nama global, termasuk `information_schema` dan task MV.
- **Overlap=TRUE** mematahkan invariant "1 run per task" yang dipakai di seluruh `TaskRunManager`.
- **DAG saat failover leader:** v1 cukup menggagalkan graph run. Melanjutkannya butuh state persisten tambahan.

## 2.4 Body task multi-statement (eksekusi multi-baris)

### Masalah saat ini

Body task sekarang hanya mendukung satu statement, dan jenisnya sempit:

| Batasan | Lokasi |
|---|---|
| Grammar hanya menerima `CTAS \| INSERT \| CACHE SELECT` sesudah `AS` | `G/StarRocks.g4:709–713` |
| Saat dijalankan, hanya **statement pertama** yang dieksekusi: `SqlParser.parse(definition).get(0)` | `C/scheduler/SqlTaskRunProcessor.java` (~l.49) |
| Hasil run ditentukan dari `QueryState` milik *satu* context setelah processor selesai | `C/scheduler/TaskRun.java` (~l.557) |
| Bila `Config.mv_use_creator_based_authorization = false`, task berjalan sebagai **root**. Selama body hanya INSERT/CTAS yang dicek saat submit, ini aman. Kalau body dibebaskan, ini menjadi **eskalasi privilege** | `TaskRun.switchUser` (~l.380) |
| Pengecekan privilege dilakukan sekali saat `SUBMIT`, dan hanya untuk statement tunggal itu | `AuthorizerStmtVisitor.visitSubmitTaskStatement` |

### Perilaku Snowflake

Di Snowflake, body task bisa berupa:
- satu statement SQL (DML/DDL apa pun),
- `CALL procedure(...)`,
- **blok Snowflake Scripting** (`BEGIN … END` / `EXECUTE IMMEDIATE $$ … $$`). Inilah cara Snowflake menjalankan banyak statement, lengkap dengan variabel, IF, LOOP, dan EXCEPTION.

Objek di dalam body **tidak divalidasi saat CREATE**, melainkan saat dijalankan. Body berjalan dengan privilege role pemilik task.

### Desain bertingkat untuk StarRocks

| Level | Bentuk body | Isi | Kapan |
|---|---|---|---|
| **L1** | Satu statement apa pun | DML (INSERT/UPDATE/DELETE/INSERT OVERWRITE), DDL (CREATE/ALTER/DROP/TRUNCATE), `ANALYZE`, `REFRESH MATERIALIZED VIEW`, `SUBMIT`/`EXECUTE TASK`, dan sebagainya | MVP |
| **L2** | **Skrip multi-statement** `AS $$ stmt1; stmt2; … $$` | Dijalankan berurutan dalam **satu sesi**, sehingga `SET`, `USE`, dan temporary table tetap berlaku untuk statement berikutnya. Berhenti di error pertama. Transaksi eksplisit `BEGIN; … COMMIT;` boleh dipakai di dalamnya | MVP |
| **L3** | Blok scripting `AS BEGIN DECLARE … IF … LOOP … EXCEPTION … END` | Memakai interpreter yang sama dengan Stored Procedure (bagian 3) | Setelah Stored Procedure |

Contoh L2:

```sql
-- mysql CLI: ganti delimiter, karena client memecah input di ';' (berlaku juga untuk $$)
DELIMITER //
CREATE TASK db.etl_harian
  SCHEDULE = 'USING CRON 0 2 * * * Asia/Jakarta'
  COMMENT = 'ETL harian orders'
AS $$
  SET query_timeout = 3600;
  TRUNCATE TABLE stg_orders;
  INSERT INTO stg_orders SELECT * FROM raw_orders WHERE dt = date_sub(current_date(), 1);
  DELETE FROM fact_orders WHERE dt = date_sub(current_date(), 1);
  INSERT INTO fact_orders SELECT dt, customer_id, sum(amount) FROM stg_orders GROUP BY 1, 2;
  ANALYZE TABLE fact_orders;
$$//
DELIMITER ;

ALTER TASK db.etl_harian RESUME;
EXECUTE TASK db.etl_harian;      -- jalankan sekarang untuk dicoba
```

### Rincian implementasi L1 + L2

1. **Grammar.** Body diubah menjadi `AS (ATTACHMENT | statement)`.
   - `ATTACHMENT` (`$$…$$`) sudah menjadi **satu token** di lexer (`G/StarRocksLex.g4:649`), jadi tidak perlu grammar baru untuk skrip L2.
   - Body disimpan sebagai **teks**.
   - `SUBMIT TASK` lama tetap memakai grammar lama.
2. **Validasi saat CREATE.**
   - Parse **semua** statement untuk memastikan syntax benar.
   - **Tidak** di-analyze. Sama seperti Snowflake, tabel yang dibuat oleh statement sebelumnya di skrip yang sama belum ada saat CREATE.
   - Statement yang tidak masuk akal di dalam task ditolak: `KILL`, `SHUTDOWN`/`ADMIN` tertentu, serta statement session-only seperti `EXECUTE AS` dan `USE CATALOG` lintas catalog bila tidak didukung.
   - Batas jumlah statement per skrip diatur lewat config baru, mis. `task_script_max_statements = 100`.
3. **Eksekusi.** `ScriptTaskRunProcessor` baru menangani `TaskSource.USER_SQL`. Untuk setiap statement:
   - parse, lalu `StmtExecutor.newInternalExecutor(ctx, stmt)` dan `execute()`;
   - **cek `ctx.getState()` setelah tiap statement**; kalau `ERR`, berhenti dan laporkan *"statement #k gagal: …"*;
   - reset state sebelum statement berikutnya.

   Hal lain yang perlu dijaga:
   - **Query ID:** setiap statement mendapat query id sendiri, sehingga audit log dan query detail per statement tetap rapi.
   - **Timeout:** total `USER_TASK_TIMEOUT_MS` dicek di antara statement. Kill task membatalkan statement yang sedang jalan.
   - **Transaksi:** kalau skrip gagal di tengah transaksi eksplisit, lakukan rollback otomatis.
4. **Identitas (keamanan).** Task `USER_SQL` **selalu** berjalan sebagai pemilik, dengan role pemilik, tanpa melihat `mv_use_creator_based_authorization`. Ini menutup jalur eskalasi ke root. Privilege dicek ulang per statement **saat dijalankan**, mengikuti alur `StmtExecutor` biasa.
5. **Observabilitas.**
   - `information_schema.task_runs` menampilkan progres (`PROGRESS = "3/6"`) dan error dengan nomor statement.
   - Kolom `QUERY_ID` diisi query id statement terakhir.
   - Audit log mencatat setiap statement dengan `QuerySource.TASK`.
6. **Kompatibilitas.** `SUBMIT TASK` dan task MV/PIPE/Optimize tetap memakai `SqlTaskRunProcessor` lama dan tidak berubah.

### Batasan dan catatan

- **Client SQL memecah input di `;`.** Ini juga terjadi di dalam `$$…$$`, persis seperti saat membuat stored procedure di MySQL. Solusinya:
  - mysql CLI: pakai `DELIMITER //`;
  - DBeaver/DataGrip: jalankan blok sebagai *satu statement* (blok teks lalu Ctrl+Enter), atau aktifkan opsi *dollar-quoted string*.
- **Belum ada variabel atau percabangan di L2.** Kebutuhan seperti "jalankan B hanya kalau A menghasilkan baris" baru bisa dipenuhi di L3 (scripting) atau lewat `WHEN` (K8).
- **Estimasi tambahan:** L1+L2 kira-kira **+1–2 minggu** di atas MVP Task (bagian 2.3).

## 2.5 Task graph: chaining antar task (AFTER, kondisi sukses/gagal, finalizer)

Sumber: dokumentasi Snowflake [Task graphs](https://docs.snowflake.com/en/user-guide/tasks-graphs), [CREATE TASK](https://docs.snowflake.com/en/sql-reference/sql/create-task), [ALTER TASK](https://docs.snowflake.com/en/sql-reference/sql/alter-task).

### Perilaku Snowflake

- **Child task** dibuat dengan `CREATE TASK b ... AFTER a [, a2 ...] AS ...`. Child task **tidak boleh punya SCHEDULE**. Hanya root task yang dijadwalkan atau dijalankan lewat `EXECUTE TASK`.
- **Default:** child hanya jalan kalau *semua* predecessor sukses. Kalau ada yang gagal, child tidak dijadwalkan dan turunannya juga tidak jalan.
- **Advanced branching:** `WHEN SYSTEM$PREDECESSORS_STATE('<state>')` mengatur kapan child jalan. State yang tersedia:
  - `ALL_SUCCEEDED` (default)
  - `AT_LEAST_ONE_FAILED`
  - `ALL_FAILED`
  - `ALL_DONE`
  - `NONE_FAILED_MIN_ONE_SUCCEEDED`

  Dengan ini kita bisa membuat cabang "on failure", misalnya task alert.
- **Finalizer:** `CREATE TASK f FINALIZE = root AS ...` jalan sekali setelah semua task lain dalam satu graph run selesai, baik sukses maupun gagal. Biasanya dipakai untuk cleanup atau notifikasi. Aturannya:
  - satu root hanya punya satu finalizer;
  - finalizer tidak boleh punya schedule maupun child.
- **Root harus di-SUSPEND dulu** sebelum task mana pun dalam graph diubah, ditambah, di-suspend, atau di-resume.
- **Task yang di-suspend tidak ikut** graph run dan tidak dihitung sebagai predecessor.
- **Batasan graph:**
  - semua task dalam graph berada di schema yang sama dan punya owner yang sama;
  - maksimal 100 predecessor dan 100 child per task, dan maksimal 1000 task per graph.
- **Kalau semua predecessor sebuah child dihapus,** child itu menjadi standalone/root task dalam keadaan suspended.
- **Overlap:** default `NO_OVERLAP`, yaitu root tidak jalan lagi selama graph run sebelumnya belum selesai.
- **Syntax ALTER:**
  - `ADD AFTER`, `REMOVE AFTER`
  - `MODIFY WHEN`, `REMOVE WHEN`
  - `SET FINALIZE`, `UNSET FINALIZE`

### Yang diimplementasikan di StarRocks (branch `feature/snowflake-task`)

```sql
CREATE TASK load_root SCHEDULE = 'USING CRON 0 2 * * * Asia/Jakarta' AS $$ ... $$;
CREATE TASK load_orders    AFTER load_root AS INSERT INTO ...;
CREATE TASK load_customers AFTER load_root AS INSERT INTO ...;
CREATE TASK publish AFTER load_orders, load_customers AS INSERT OVERWRITE ...;   -- ALL_SUCCEEDED
CREATE TASK alert   AFTER load_orders, load_customers
  WHEN SYSTEM$PREDECESSORS_STATE('AT_LEAST_ONE_FAILED') AS INSERT INTO load_errors ...;
CREATE TASK cleanup FINALIZE = load_root AS DROP TABLE IF EXISTS tmp_stage;

-- child di-resume dulu, root terakhir
ALTER TASK load_orders RESUME; ... ALTER TASK load_root RESUME;
ALTER TASK publish ADD AFTER other_task;  ALTER TASK alert MODIFY WHEN SYSTEM$PREDECESSORS_STATE('ALL_FAILED');
```

- **Penyimpanan:** `AFTER`, `WHEN`, dan `FINALIZE` disimpan di `taskOptions` milik `Task`, yang sudah persist lewat edit log dan image. Tidak ada format metadata baru.
- **Validasi** (`UserTaskGraph`, dijalankan di bawah task lock) menolak:
  - child yang punya SCHEDULE;
  - predecessor yang tidak ada, beda database, atau beda owner;
  - cycle;
  - dua root dalam satu graph;
  - child yang berada di bawah finalizer;
  - dua finalizer untuk root yang sama;
  - finalizer yang punya schedule;
  - `WHEN` tanpa `AFTER`.

  Root harus suspended untuk CREATE/ALTER/SUSPEND/RESUME task dalam graph-nya.
- **Eksekusi** (`UserTaskGraphRunner`, hanya di leader):
  - Saat run root selesai, sukses maupun gagal, graph run dimulai. Pemicunya hook di `TaskRunExecutor`.
  - Peserta graph run adalah child yang ACTIVE dan terjangkau dari root lewat task yang ACTIVE.
  - Setiap child diputuskan setelah semua predecessor pesertanya selesai: kondisinya dievaluasi, lalu child dijalankan atau di-*skip*. Child yang di-skip tetap terhitung "selesai" bagi turunannya, jadi turunan dengan `ALL_DONE` tetap jalan.
  - Finalizer jalan paling akhir.
  - Jadwal root di-skip selama graph run masih berjalan (`NO_OVERLAP`). `EXECUTE TASK root` ditolak selama graph run masih berjalan.
  - `EXECUTE TASK child` menjalankan child itu sendirian, tanpa memicu graph.
- **DROP TASK** pada predecessor melepasnya dari `AFTER` milik child-nya. Child yang tidak punya predecessor lagi menjadi root dalam keadaan suspended. Kalau yang di-drop adalah root, `FINALIZE` pada finalizer-nya dikosongkan.
- **Observability:**
  - log FE untuk start, submit, skip (beserta alasan kondisinya), dan finish setiap graph run;
  - SHOW TASKS dan DESC TASK punya kolom `PREDECESSORS`, `CONDITION`, dan `FINALIZE`;
  - run tiap task tetap tercatat di `information_schema.task_runs`.

### Batasan versi ini

- **State graph run hanya ada di memori leader.** Kalau leader berganti, graph run yang sedang berjalan berhenti. Run root berikutnya memulai graph run baru.
- **Sudah ditambahkan di branch `feature/hardening`:**
  - `TASK_AUTO_RETRY_ATTEMPTS` (0–30, diset di root): run root, child, atau finalizer yang gagal dijalankan ulang sampai n kali sebelum dihitung gagal.
  - `CALL SYSTEM$SET_RETURN_VALUE('...')` dan `SYSTEM$GET_PREDECESSOR_RETURN_VALUE(['task'])`. Nilainya tampil sebagai `EXTRA_MESSAGE` di `task_runs`.
  - Task yang di-skip dicatat sebagai run `SKIPPED` di `task_runs`, dengan alasannya di `ERROR_MESSAGE`.
- **Belum didukung:**
  - `OVERLAP_POLICY` selain `NO_OVERLAP`;
  - `SYSTEM$STREAM_HAS_DATA` (StarRocks belum punya stream);
  - `SYSTEM$TASK_DEPENDENTS_ENABLE`, sehingga child harus di-RESUME satu per satu.

---

# 3. Stored Procedure ala Snowflake

## 3.1 Perilaku Snowflake yang ditiru (target: LANGUAGE SQL / Snowflake Scripting)

```sql
CREATE OR REPLACE PROCEDURE db.load_daily(p_date DATE, p_limit INT DEFAULT 1000)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  n INT DEFAULT 0;
  c1 CURSOR FOR SELECT id FROM staging WHERE dt = :p_date;
  bad_data EXCEPTION (-20001, 'data kosong');
BEGIN
  INSERT INTO fact SELECT * FROM staging WHERE dt = :p_date LIMIT :p_limit;
  n := SQLROWCOUNT;
  IF (n = 0) THEN
    RAISE bad_data;
  END IF;
  FOR r IN c1 DO
    CALL db.audit(r.id);
  END FOR;
  RETURN 'loaded ' || n;
EXCEPTION
  WHEN bad_data THEN RETURN 'skip';
  WHEN OTHER THEN RAISE;
END;
$$;

CALL db.load_daily('2026-11-20');
SHOW PROCEDURES IN DATABASE db;
DESCRIBE PROCEDURE db.load_daily(DATE, INT);
DROP PROCEDURE db.load_daily(DATE, INT);

-- Blok anonim
EXECUTE IMMEDIATE $$ BEGIN … END; $$;
```

Fitur Snowflake Scripting yang perlu dicakup: `DECLARE`, `LET`, `:=`, binding `:var` di SQL, `IF/ELSEIF/ELSE`, `CASE`, `FOR i IN a TO b`, `FOR r IN cursor`, `WHILE`, `REPEAT … UNTIL`, `LOOP`, `BREAK/CONTINUE`, cursor (`OPEN/FETCH/CLOSE`), `RESULTSET := (query)`, `RETURN TABLE(rs)`, `EXECUTE IMMEDIATE`, `SQLROWCOUNT/SQLFOUND/SQLNOTFOUND`, `RAISE`, blok `EXCEPTION WHEN … THEN`, serta transaksi di dalam procedure.

## 3.2 Kondisi di StarRocks saat ini

**`CALL` sudah ada di grammar** (`G/StarRocks.g4:2495`): `CALL qualifiedName '(' argumentList? ')'`
- Mendukung argumen posisional maupun bernama (`name => expr`).
- `C/sql/analyzer/CallProcedureAnalyzer.java` sudah menangani resolusi nama, validasi argumen, konversi ke konstanta, dan cast tipe. Ini **bisa langsung dipakai ulang**.
- Eksekusinya lewat `MetadataMgr.callProcedure` → `ConnectorMetadata.getProcedure`. Saat ini **hanya Iceberg** yang mengimplementasikan.
- Base class `C/connector/Procedure.java`: `execute(...)` mengembalikan `void`, jadi belum ada hasil.
- `CALL` **tanpa cek privilege** (`AuthorizerStmtVisitor` tidak punya case-nya).

**Token `$$…$$` sudah ada:** `ATTACHMENT : '$$' .*? '$$'` (`G/StarRocksLex.g4:649`).
- Body procedure yang berisi banyak `;` sudah ter-lex sebagai **satu token**.
- Sudah dipakai untuk UDF Python/Java inline.

**Framework UDF**, precedent terdekat untuk objek bernama, bertipe, dan ber-overload:
- `C/catalog/Function.java`: Gson polymorphic, `argTypes`, `argNames`, `CompareMode` untuk overloading.
- Disimpan di `Database.name2Function` (`C/catalog/Database.java:101`) dan `GlobalFunctionMgr`.
- Edit log: `OP_ADD_FUNCTION_V2 13050`, `OP_DROP_FUNCTION_V2 13051`.
- Privilege: `ObjectType.FUNCTION`, plus GRANT `ON FUNCTION name(types)`.

**Mesin eksekusi SQL bersarang:** `C/qe/SimpleExecutor.java`
- Fungsi: `executeDML`, `executeDQL`, `executeDDL`.
- `createCallerQueryContext` (`:173–254`) membuat child `ConnectContext` yang mewarisi user, role, db, sql_mode, zona waktu, timeout, dan resource group, sekaligus terhubung ke parent untuk cancel dan timeout.
- Kalau parent di-kill, statement anak ikut di-cancel (`registerSubStmtExecutor`).
- Jumlah baris terdampak diambil dari `QueryState.getAffectedRows()`.

**Variabel:**
- `SET @v = expr` dievaluasi lewat constant folding (`ExprUtils.analyzeAndCastFold`).
- Kalau tidak bisa di-fold, dijalankan sebagai `SELECT expr`: `UserVariable.deriveUserVariableExpressionResult`. Ini **persis** mekanisme yang dibutuhkan `LET x := (SELECT …)`.

**Prepared statement:** `PREPARE` / `EXECUTE … USING` dengan parameter `?` bisa menjadi dasar `EXECUTE IMMEDIATE … USING`.

**Transaksi eksplisit:** `BEGIN/COMMIT/ROLLBACK` (`C/transaction/TransactionStmtExecutor.java`).
- Hanya mengizinkan DML, SELECT, SET, dan SHOW. **Tidak boleh DDL**.
- Tidak boleh membaca tabel yang sudah dimodifikasi di transaksi yang sama.

**Hasil ke klien:**
- `ShowResultSet` dari `DDLStmtExecutor` sudah dikirim otomatis, cocok untuk nilai skalar atau tabel kecil.
- Multi result set didukung (`SERVER_MORE_RESULTS_EXISTS`).

**Precedent "jalan sebagai pemilik":**
- `TaskRun.buildTaskRunConnectContext` dan `ExecuteAsExecutor`.
- View `SECURITY INVOKER/NONE` sebagai precedent definer vs invoker.

**Stub yang sudah ada tapi kosong:**
- `SHOW PROCEDURE STATUS`: `C/qe/ShowExecutor.java:434` mengembalikan hasil kosong.
- `information_schema.routines` (`RoutinesSystemTable`): tidak punya scanner di BE, jadi selalu kosong.

## 3.3 Desain yang diusulkan

### Arsitektur

```
CREATE PROCEDURE ── parse header (StarRocks.g4) ── parse body $$…$$ (StarRocksScript.g4) ── validasi
        │                                                                         │
        ▼                                                                         ▼
  StoredProcedure (catalog, Gson) ── edit log OP_CREATE_PROCEDURE ── image block   AST skrip (cache)

CALL p(args) ── CallProcedureAnalyzer (resolusi: procedure user → procedure konektor)
        │
        ▼
  ProcedureInterpreter (FE, AST-walking)
     ├─ Frame/Scope variabel (Type + ConstantOperator)
     ├─ Evaluasi ekspresi: constant-fold, atau "SELECT expr" via SimpleExecutor
     ├─ Statement SQL: rewrite :var → literal, lalu SimpleExecutor pada child ConnectContext
     │     (EXECUTE AS OWNER → identitas pemilik; CALLER → identitas pemanggil)
     ├─ Kontrol alur, cursor/RESULTSET, exception (ErrorCode/SQLSTATE → handler)
     └─ RETURN skalar → ShowResultSet | RETURN TABLE → stream query terakhir
```

### Komponen yang perlu dibuat

| # | Komponen | Detail | File utama | Kesulitan |
|---|---|---|---|---|
| S1 | Grammar DDL | `CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] name(arg type [DEFAULT expr], …) RETURNS type [NOT NULL] \| TABLE(col type, …) LANGUAGE SQL [EXECUTE AS OWNER\|CALLER] [COMMENT '…'] AS ATTACHMENT`. Juga `DROP PROCEDURE name(types)`, `ALTER PROCEDURE … RENAME/SET COMMENT/EXECUTE AS`, `SHOW PROCEDURES`, `DESC PROCEDURE`. Tiru pola `createFunctionStatement` (`G/StarRocks.g4:1663`) | `G/StarRocks.g4`, `G/StarRocksLex.g4` (tambah `LANGUAGE`, `OWNER`, `CALLER`) | Rendah |
| S2 | **Grammar skrip** | Grammar ANTLR baru `StarRocksScript.g4` yang **meng-import** lexer dan rule `statement`/`expression` dari grammar utama: `block : (DECLARE decl+)? BEGIN scriptStmt* (EXCEPTION handler+)? END ';'`, plus `IF/CASE/FOR/WHILE/REPEAT/LOOP/BREAK/CONTINUE/LET/:=/RETURN/RAISE/OPEN/FETCH/CLOSE/EXECUTE IMMEDIATE`. Token baru: `DECLARE`, `LOOP`, `WHILE`, `REPEAT`, `ELSEIF`, `RAISE`, `CURSOR`, `RESULTSET`, `:=`. Binding `:var` sebagai rule `primaryExpression` baru. Hati-hati: `:` sudah dipakai di map literal / array slice, dan `::` adalah cast | `G/StarRocksScript.g4` (baru), `C/sql/parser/ScriptParser.java` (baru) | **Sulit** |
| S3 | Objek katalog & persistensi | `StoredProcedure {id, dbId, name, args[{name,type,defaultExprSql}], returnType \| returnTable, language, securityMode, ownerUserIdentity, body, comment, createdTime}`. Namespace **terpisah** dari function. Disimpan di map baru di `Database`, op baru `OP_CREATE_PROCEDURE` / `OP_DROP_PROCEDURE` (ordinal baru), blok image baru, serta ikut backup/restore | `C/catalog/StoredProcedure.java` (baru), `C/catalog/Database.java`, `C/persist/OperationType.java` | Sedang |
| S4 | Resolusi & overloading | Pakai `Function.compare(CompareMode)` / pola `AnalyzerUtils.getUdfFunction`. Isi argumen DEFAULT. `CALL` mencari procedure user di db aktif dulu, baru procedure konektor (Iceberg `system.*`) | `C/sql/analyzer/CallProcedureAnalyzer.java`, `C/server/MetadataMgr.java:1150` | Rendah–Sedang |
| S5 | **Interpreter** | AST-walking dengan `Frame` bersarang. Variabel bertipe (`ConstantOperator.castTo`). Kontrol alur. `SQLROWCOUNT` dari `QueryState`. Exception: map `ErrorCode`/SQLSTATE → `STATEMENT_ERROR`/`EXPRESSION_ERROR`/`OTHER` + exception user. `RAISE` | `C/procedure/ProcedureInterpreter.java` (paket baru) | **Sulit** |
| S6 | SQL bersarang & binding | Rewrite AST: `:var` → literal bertipe sebelum analisis. Eksekusi via `SimpleExecutor` pada child context. `EXECUTE IMMEDIATE 'str' [USING …]` → `SqlParser.parse` + parameter seperti PREPARE | `C/qe/SimpleExecutor.java` | Sedang |
| S7 | Cursor & RESULTSET | v1: materialisasi hasil (batas baris/byte dikonfigurasi). Hasil besar memakai tabel sementara (`TemporaryTableMgr`) | — | Sedang–Sulit |
| S8 | Mengembalikan hasil | Skalar → `ShowResultSet` satu baris. `RETURN TABLE(rs)` → re-plan query terakhir dan stream lewat jalur query normal, supaya tidak dimaterialisasi | `C/qe/DDLStmtExecutor.java`, `C/qe/StmtExecutor.java` | Sedang |
| S9 | EXECUTE AS | OWNER (default Snowflake): child context memakai `ownerUserIdentity` + role-nya (meniru `TaskRun`). CALLER: context pemanggil. Session variable tidak bocor keluar dari procedure OWNER | `C/qe/ConnectContext.java` | Sedang |
| S10 | Transaksi | Child context berbagi `txnId` dengan pemanggil. Exception tak tertangani memicu rollback transaksi yang dibuka di dalam procedure. `BEGIN` di skrip berarti blok, sedangkan transaksi memakai `BEGIN TRANSACTION` (seperti Snowflake). Pembatasan DDL dalam transaksi tetap berlaku | `C/transaction/TransactionStmtExecutor.java` | Sedang–Sulit |
| S11 | Privilege | `ObjectType.PROCEDURE`, `CREATE PROCEDURE` pada DB, `USAGE` pada procedure, GRANT `ON PROCEDURE name(types)`. **Tambah cek di `CALL`** (sekarang tidak ada) | `C/authorization/*`, `AuthorizerStmtVisitor.java` | Sedang |
| S12 | Metadata | `SHOW PROCEDURES`, `DESC PROCEDURE`, isi `SHOW PROCEDURE STATUS` (stub) dan `information_schema.routines` (kosong) | `C/qe/ShowExecutor.java:434`, `RoutinesSystemTable.java` | Sedang |
| S13 | Batas & keamanan | Kedalaman rekursi, batas iterasi loop, batas jumlah statement, total timeout lewat `remainingQueryTimeoutS`. Kill query membatalkan seluruh procedure | — | Rendah–Sedang |
| S14 | HA / forwarding | v1: `CALL` diproses di FE leader (`RedirectStatus` → forward). Ini cara paling sederhana agar DDL di dalam procedure aman. Optimasi nanti: jalankan di FE lokal, dan teruskan hanya statement yang butuh leader | `C/qe/RedirectStatus.java` | Sedang |
| S15 | Blok anonim | `EXECUTE IMMEDIATE $$ … $$` memakai interpreter yang sama. `BEGIN … END` di level atas **tidak** didukung di v1, karena bentrok dengan `BEGIN` transaksi dan klien MySQL memecah input di `;` | — | Rendah |
| S16 | LANGUAGE JAVA/PYTHON | FE tidak punya sandbox runtime. UDF berjalan di BE. **Ditunda**; LANGUAGE SQL adalah target realistis pertama | — | Sulit |

### Tahapan

1. **MVP (5–8 minggu):**
   - S1, S3, S4, S11, S12, S13, S14.
   - Bagian dasar S2/S5/S6: `DECLARE`, `LET`/`:=`, `IF`, `CASE`, `FOR` range, `WHILE`, `LOOP`/`BREAK`, `RETURN` skalar, `:var`, DML/DDL/SELECT INTO, `SQLROWCOUNT`.
   - S9 (OWNER/CALLER), S15.
2. **Lanjutan (4–6 minggu):** cursor dan `FOR r IN cursor`, `RESULTSET` + `RETURN TABLE` (S7, S8), exception lengkap + `RAISE`, transaksi (S10), `EXECUTE IMMEDIATE … USING`.

### Risiko

- **Bentrok grammar.** Ada tabrakan `BEGIN` (blok vs transaksi), `EXECUTE` (prepared statement vs `EXECUTE IMMEDIATE`), dan `:`. Mengisolasi skrip di grammar terpisah yang hanya aktif di dalam `$$…$$` mengurangi risiko ini secara drastis.
- **Performa.** Setiap statement di dalam loop menjalani parse, analisis, dan plan penuh. Perlu cache AST per statement dan constant folding untuk ekspresi murni.
- **Keamanan.** Procedure OWNER adalah jalur eskalasi privilege. Pemilik harus dicatat dan divalidasi ulang saat CALL, misalnya saat pemilik dihapus atau role-nya dicabut.


## 3.4 Yang diimplementasikan (branch `feature/stored-procedure`)

Keputusan utama: **lexer + parser recursive-descent tulisan tangan untuk Snowflake Scripting** (paket `com.starrocks.procedure.script`), bukan grammar ANTLR kedua. Body procedure tetap string `$$…$$` di grammar utama, sehingga bentrok `BEGIN`/`EXECUTE`/`:` tidak terjadi. Statement SQL di dalam body diteruskan apa adanya ke parser StarRocks.

**DDL dan metadata:**
```sql
CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] db.name(arg type [DEFAULT expr], ...)
  RETURNS type [NOT NULL] LANGUAGE SQL [EXECUTE AS OWNER | CALLER] [COMMENT '...'] AS $$ ... $$;
DROP PROCEDURE [IF EXISTS] name(types);  SHOW PROCEDURES [IN db] [LIKE '...'];
DESC PROCEDURE name(types);  CALL name(args);  EXECUTE IMMEDIATE $$ ... $$;
```
Procedure disimpan di `Database` (field Gson `sp`) dengan edit log `OP_CREATE_PROCEDURE`=30001 dan `OP_DROP_PROCEDURE`=30002 (rentang tinggi, `@IgnorableOnReplayFailed`). Overloading berdasarkan tipe argumen.

**Bahasa yang didukung:** DECLARE (variabel, CURSOR, EXCEPTION), LET, `:=`, IF/ELSEIF, CASE (simple dan searched), FOR range (REVERSE) dan FOR cursor, WHILE, REPEAT, LOOP, BREAK/CONTINUE dengan label, OPEN/FETCH/CLOSE, `SELECT … INTO :a, :b`, RAISE / re-raise, handler `WHEN x / OTHER THEN` dengan SQLCODE/SQLERRM/SQLSTATE, SQLROWCOUNT/SQLFOUND/SQLNOTFOUND, CALL bersarang (`CALL p() INTO :v`, maks. kedalaman 16), `IDENTIFIER(:v)`, EXECUTE IMMEDIATE, transaksi eksplisit (di-rollback bila procedure gagal), dan RETURN dengan cast ke tipe RETURNS (+ NOT NULL).

**Eksekusi:** ekspresi konstan di-fold di FE. Selain itu dijalankan sebagai `SELECT expr` lewat planner, dengan hasil JSON. Setiap CALL mendapat child `ConnectContext` sendiri dengan sisa timeout, catalog, dan `PIPES_AS_CONCAT` (agar `||` seperti Snowflake). Batas 10 juta langkah mencegah loop tak berujung. Statement di dalam procedure tercatat di audit log.

**Hak akses:** OWNER (default, seperti Snowflake) menjalankan body dengan identitas dan role pemilik di database procedure. Hanya pemilik atau user dengan OPERATE yang boleh CALL. CALLER memakai hak pemanggil. CREATE butuh privilege apa pun pada database.

**Pengujian:** `ScriptParserTest` (5) dan `StoredProcedureTest` (13), plus regresi ParserTest, RedirectStatusTest, PrivilegeCheckerTest, dan UserTask*: total 461 test lulus. E2E nyata: `e2e/procedure_e2e.sh` (transfer dengan DML dan handler, cursor + IDENTIFIER, CALL bersarang, rollback transaksi, owner vs caller).

**Batasan:**
- Sejak branch `feature/hardening`, `RESULTSET`, `RETURNS TABLE (...)` / `RETURN TABLE(...)`, dan `EXECUTE IMMEDIATE … USING` sudah ada. Bahasa selain SQL belum ada.
- Sejak branch `feature/hardening`, CALL dan EXECUTE IMMEDIATE di-forward ke leader, jadi bisa dijalankan dari FE mana pun (sudah diuji di cluster 3 FE).
- Fungsi yang tidak bisa di-fold di FE butuh BE (satu `SELECT` per evaluasi), sehingga loop berat bisa lambat.

---

# 4. Lintas fitur

## 4.1 Ketergantungan

```
Stored Procedure ──(CALL di body task)──▶ Task (multi-statement)
Time Travel ── independen ──▶ (opsional) dipakai di procedure/task: INSERT … SELECT … AT(…)
Privilege TASK & PROCEDURE ── sama-sama meniru pola PIPE / FUNCTION
```

## 4.2 Strategi agar fork tetap mudah di-sync dengan upstream

Ketiga fitur menyentuh file "panas" upstream, yaitu `StarRocks.g4`, `AstBuilder.java`, `TaskManager.java`, dan `OperationType.java`. Hampir setiap commit upstream ikut mengubah file-file ini. Saran:
- Logika baru diletakkan di **paket baru**: `com.starrocks.procedure`, `com.starrocks.scheduler.graph`, dan `com.starrocks.catalog.history`. File upstream cukup diberi kait kecil.
- Grammar skrip procedure dipisah di file `.g4` sendiri.
- **Ordinal edit log** baru dipilih dari rentang tinggi yang tidak mungkin dipakai upstream (mis. 30000+). Lalu dicatat di satu tempat, karena kalau upstream kebetulan memakai angka yang sama, metadata bisa rusak.
- Semua field thrift/proto baru bersifat `optional` dengan ordinal baru (aturan repo di `AGENTS.md`).
- Sync dengan upstream sesering mungkin (mingguan).

## 4.3 Rencana pengujian per fitur

| Lapisan | Cara |
|---|---|
| Unit test FE | Grammar/AST, analyzer, `TaskManager` (cron, state, DAG), interpreter procedure (`fe/fe-core/src/test/.../scheduler`, paket baru `procedure`), plan test untuk `AT(...)` |
| SQL integration (`test/sql`) | Skenario end-to-end: task create → resume → execute → history; procedure dengan loop/exception/cursor; time travel setelah INSERT, UPDATE, DELETE, INSERT OVERWRITE, dan schema change |
| Pembanding | Container StarRocks 4.1.4 stok (`rosemary-oracle-*`) sebagai baseline perilaku lama |
| Ketahanan | Failover leader saat task/graph run berjalan, restart FE (replay edit log + image), upgrade dari metadata versi lama |
| Storage (time travel) | Ukur pertumbuhan storage dan dampak compaction dengan retensi 1/7 hari |

## 4.4 Langkah konkret berikutnya yang disarankan

1. **Perbaiki dua bug temuan:** cek privilege `DROP/ALTER TASK`, dan `BEFORE` pada tabel lake yang harus error. Keduanya kecil dan langsung meningkatkan keamanan serta kebenaran hasil.
2. **Prototipe Task MVP:** `CREATE TASK` (dibuat SUSPENDED), `RESUME/SUSPEND/EXECUTE`, `SHOW/DESC`, cron + zona waktu, dan privilege TASK.
3. Spike 1–2 hari untuk **grammar skrip** (`StarRocksScript.g4` meng-import grammar utama), guna memastikan pendekatan import ANTLR bisa jalan sebelum membangun interpreter.
4. Spike 1–2 hari untuk **time travel lake**: paksa `scanVersionOverride` ke versi lama pada tabel lake, lalu pastikan datanya benar sebelum membangun ledger histori.
