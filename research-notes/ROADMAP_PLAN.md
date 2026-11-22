# Rencana Implementasi ROADMAP_IDEA.md

> Tanggal: 2026-11-22 · Basis: fork `dwickyfp/starrocks`, `main` = `4495ecb2` (time travel, task graph, stored procedure, hardening sudah masuk).
> Dokumen ini adalah hasil bedah `ROADMAP_IDEA.md` terhadap kode saat ini: status yang dicek ulang, urutan kerja, desain per fitur, dan strategi test.

## 1. Hasil cek ulang status (kode `main`, bukan asumsi)

| Item | Klaim roadmap | Hasil cek | Dampak ke rencana |
|---|---|---|---|
| 3.2 `QUALIFY` | terbatas | Benar: `QUALIFY selectItem cmp INTEGER` (`StarRocks.g4:2673`) | Ubah ke `QUALIFY expression` |
| 3.3 `* REPLACE/RENAME/ILIKE` | belum | Benar: hanya `excludeClause` | Grammar + ekspansi star |
| 3.4 `ORDER BY ALL` | **sudah ada di upstream** (dicek di cluster: ASC/DESC/NULLS) | Salah | Tidak perlu kerja |
| 3.5 `UNPIVOT` | belum | Benar | Rewrite di AstBuilder/analyzer |
| 3.13 `IFF`, `NVL2`, `ZEROIFNULL`, `DIV0`, `EQUAL_NULL`, `TRY_CAST`, `TRY_TO_*`, `ILIKE ANY` | sebagian | Semua belum ada sebagai nama fungsi | Rewrite di FE ke fungsi yang sudah ada, tanpa BE |
| 2.2 `UNDROP` | alias RECOVER | **Sudah dikerjakan** (`aec0c68a`) | Tinggal `SHOW TABLES HISTORY` |
| 3.7 `IDENTIFIER()` / `EXECUTE IMMEDIATE` | sebagian | `EXECUTE IMMEDIATE` dan `IDENTIFIER(:v)` **sudah ada di scripting** | Tinggal `IDENTIFIER('lit')`/variabel sesi di SQL biasa |
| 3.8 anonymous block | sebagian | **Sudah** (`EXECUTE IMMEDIATE $$ ... $$`, `RESULTSET`, `RETURN TABLE`) | Selesai |
| 6.1 task graph | sebagian | **Sudah** (+ retry, return value, SKIPPED) | Tinggal `SYSTEM$STREAM_HAS_DATA` (butuh stream) |
| 3.1 `MERGE INTO` | Iceberg saja | Benar (`MergeIntoAnalyzer`) | Rewrite ke INSERT dengan kolom `__op` ke PK table |
| 5.1 masking & row access policy | belum, effort L | **Hook sudah ada**: `Authorizer.getColumnMaskingPolicy/getRowAccessPolicy` + `SecurityPolicyRewriteRule` (dipakai Ranger); `NativeAccessController` mengembalikan null | Effort turun ke M: cukup objek policy native + implementasi hook |

## 2. Prinsip

1. **FE dulu, BE terakhir.** Fitur yang bisa dibangun sebagai rewrite di FE (dialect, MERGE, REPLACE WHERE, INSERT ALL, history/restore, policy) dikerjakan dulu: risiko rendah, mudah di-test, tidak butuh build BE.
2. **Isolasi dari file inti upstream.** Kode baru di package/kelas baru; perubahan pada `StarRocks.g4`, `AstBuilder`, `SelectAnalyzer`, `StmtExecutor` dibuat kecil.
3. **Kompatibel Snowflake/Databricks.** Syntax dan semantik mengikuti dokumentasi mereka; perbedaan dicatat di docs.
4. **Test berlapis untuk setiap fitur:**
   - Unit test parser/analyzer (bentuk AST, error yang jelas).
   - Plan test (rewrite benar, pola lama tetap teroptimasi, misalnya QUALIFY top-N tetap memakai `PartitionTopN`).
   - E2E di cluster 3 FE nyata: hasil dibandingkan dengan query ekuivalen yang ditulis manual (oracle), termasuk NULL, tabel kosong, tipe campuran, dan dijalankan lewat follower.
   - Regresi test yang ada di area yang disentuh.

## 3. Urutan kerja

### Gelombang A — Dialect quick wins (FE saja) — ✅ SELESAI (commit `34e915e3`, di `main` fork)

Verifikasi: 13 plan test + 2 test SHOW TABLES HISTORY/query_tag (masing-masing beberapa skenario), regresi FE 6059 test (0 gagal), checkstyle bersih, e2e `tools/snowflake_compat_e2e/dialect_e2e.sh` 54 cek oracle di leader dan kedua follower. Dokumen: `docs/en/using_starrocks/Snowflake_dialect.md`.

1. `QUALIFY <expression>` penuh (boleh merujuk alias select, beberapa window function, AND/OR), pola top-N lama tetap dioptimasi.
2. `SELECT * [EXCLUDE] [REPLACE (...)] [RENAME (...)]`, `* ILIKE 'pat'` (urutan klausa seperti Snowflake: ILIKE/EXCLUDE, REPLACE, RENAME).
3. ~~`ORDER BY ALL`~~ — sudah ada di upstream, tidak dikerjakan.
4. `UNPIVOT [INCLUDE NULLS | EXCLUDE NULLS] (val FOR name IN (c1 [AS 'x'], ...))`, termasuk beberapa kolom nilai.
5. Fungsi kompatibilitas: `IFF`, `NVL2`, `ZEROIFNULL`, `NULLIFZERO`, `DIV0`, `DIV0NULL`, `EQUAL_NULL`, `TRY_CAST`, `TRY_TO_NUMBER/DECIMAL/DOUBLE`, `TRY_TO_DATE/TIMESTAMP`, `x [NOT] ILIKE pat`, `LIKE/ILIKE ANY|ALL (...)`.
6. `SHOW TABLES HISTORY` (isi recycle bin), session variable `query_tag` (masuk audit log dan `information_schema`).

### Gelombang B — DML data engineering (FE saja)

Status B1 MERGE: query tunggal source LEFT/FULL JOIN target → CASE per kolom + `__op` (0 upsert / 1 delete) ke OLAP sink, cek duplikat via `count(*) OVER (PARTITION BY key)` + `assert_true` (session var `error_on_nondeterministic_merge`). Verifikasi: 7 UT baru + 1 UT Iceberg, regresi 1795 test (0 gagal), e2e `merge_e2e.sh` 22 cek oracle (50k×20k baris) di leader dan kedua follower.

1. ✅ (commit `7457d22d`) `MERGE INTO` untuk Primary Key table: `WHEN MATCHED [AND] UPDATE/DELETE`, `WHEN NOT MATCHED [AND] INSERT`, `WHEN NOT MATCHED BY SOURCE` (Databricks), `UPDATE SET *` / `INSERT *`, error multi-match, di dalam transaksi.
2. ✅ (commit `95191dd7`) `INSERT INTO t REPLACE WHERE pred SELECT ...` (PK table: satu plan FULL JOIN target terfilter + `__op`, cek baris di luar predikat via `assert_true`). Regresi 1796 test: 0 gagal kecuali `testIceberg` yang hang di spawn proses macOS (lingkungan).
3. ✅ (commit `a15a6c81`) `INSERT ALL / INSERT FIRST` multi-tabel atomik: satu `INSERT ... SELECT ... UNION ALL` per tabel target (INTO ke tabel yang sama digabung, kolom yang tidak disebut diisi default), dalam satu transaksi (milik caller atau sendiri). UT 6 + 274 test terkait, e2e `insert_multi_e2e.sh` 26 cek oracle (30k baris + NULL) di leader dan kedua follower.
4. ✅ (commit `478a4fdf`) `DESCRIBE HISTORY t` (log commit per tabel dari edit log: waktu, operasi, baris, partisi, txn, label, RESTORABLE) dan `RESTORE TABLE t TO TIMESTAMP AS OF / AT / BEFORE` (staging table lalu satu transaksi: PK via REPLACE WHERE TRUE, bisa di-undo; non-PK via INSERT OVERWRITE). UT 5, regresi 1071 test, e2e `history_restore_e2e.sh` 25 cek di 3 FE + cek restart.

### Gelombang C — CDC
1. ✅ (commit `be16931b`) Stream S1: objek stream (`CREATE/DROP/SHOW/DESC STREAM`), offset per partisi, `APPEND_ONLY` untuk semua tipe tabel, standard stream untuk PK table (diff dua snapshot time travel, net change), konsumsi transaksional (offset maju hanya saat DML commit), stale. Terimplementasi: dua scan time travel yang di-pin per relasi (`__stream_cur`/`__stream_old`) hanya di partisi yang berubah; PK/UNIQUE/AGG diff per key, Duplicate Key diff multiset, APPEND_ONLY, SHOW_INITIAL_ROWS, AT/BEFORE saat create; offset dibawa commit attachment, dicek + di-reserve di bawah lock txn mgr (konsumen konkuren: tepat satu yang commit), diterapkan di leader dan replay. UT 9, regresi 375 test, e2e `stream_e2e.sh` 44 cek (oracle) di 3 FE + cek restart.
2. ✅ (commit `bb49b49f`) `SYSTEM$STREAM_HAS_DATA` + `WHEN` di root task + triggered task. Terimplementasi: fungsi konstan FE-only (cek versi partisi vs offset, tanpa scan; stale/missing = error); `WHEN` jadi ekspresi (AND/OR/NOT atas STREAM_HAS_DATA + satu PREDECESSORS_STATE top-level), disimpan di opsi WHEN yang kompatibel dengan nilai lama; run terjadwal/child yang kondisinya false dicatat SKIPPED + alasan; triggered task (tanpa SCHEDULE) dipoll leader tiap 1 detik dengan `USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS` (default 30), tidak overlap; EXECUTE TASK tanpa syarat. UT 5 baru, regresi 481 test, e2e `stream_task_e2e.sh` 33 cek di 3 FE + cek restart; task_graph & stream e2e tetap lulus.
3. ✅ (commit `e2a46f30`) `CHANGES` clause di atas mesin yang sama. Terimplementasi: `CHANGES(INFORMATION => DEFAULT | APPEND_ONLY)` + `AT/BEFORE(TIMESTAMP|OFFSET)` atau `AT(STREAM => 's')` + `END(...)` opsional (default now); dianalisis jadi query diff stream (`TableStreamMgr.changes`) dengan scan time travel yang di-pin, hanya partisi yang berubah; tidak memindah offset stream; validasi retensi/TRUNCATE/END < start/stream tabel lain; view & MV yang membaca stream atau CHANGES ditolak (sebelumnya view atas stream diam-diam kehilangan versi pin). UT `testChangesClause`, regresi 779 test, e2e `changes_e2e.sh` 34 cek (oracle copy time travel) di 3 FE; stream/time travel/stream task/dialect/merge/history e2e tetap lulus.

### Gelombang D — Tabel & governance
1. ✅ (commit `8c168cc5`) Masking policy & row access policy native. Terimplementasi: `CREATE [OR REPLACE] MASKING | ROW ACCESS POLICY ... AS (args) RETURNS t -> body`, ALTER (SET BODY/COMMENT, RENAME), DROP, SHOW/DESC (attached_to); attach via `ALTER TABLE ... MODIFY COLUMN c SET MASKING POLICY p [USING (...)] [FORCE]` / `UNSET`, `ADD | DROP ROW ACCESS POLICY`, `DROP ALL ROW ACCESS POLICIES`; semua butuh privilege SECURITY. Native policy digabung di lookup `Authorizer` (mask native menimpa Ranger, row access di-AND), berlaku di query/join/view/CTE/stream/CHANGES/task/procedure; UPDATE/DELETE(PK)/MERGE hanya mengubah baris yang lolos row access; fail-closed bila policy hilang; tolak drop/replace policy yang ter-attach, drop/rename kolom yang dibaca policy, MV atas tabel ber-policy, DELETE Duplicate Key ber-row-access. Edit log 30005-30007 (tidak ignorable). UT 6, regresi 834 test, e2e `policy_e2e.sh` 56 cek multi-user di 3 FE + verifikasi setelah restart.
2. `CREATE OR ALTER TABLE` (diff kolom → fast schema evolution).
3. Sequence object, `CHECK` constraint/expectations di jalur INSERT.
4. Dynamic table sebagai sugar di atas async MV dengan `TARGET_LAG`.

### Ditunda (alasan)
- Zero-copy clone shared-nothing: butuh RPC BE baru (hard link) dan aturan GC lintas tablet; dikerjakan setelah gelombang FE stabil.
- `MATCH_RECOGNIZE`, clustering Z-order: operator/compaction BE baru, effort XL.
- Shared-data (lake) untuk stream/time travel: cluster uji saat ini shared-nothing.
- Pipe syntax `|>`, `FLATTEN`/path `:`: nilai lebih rendah, bentrok syntax `:` dengan variabel.
