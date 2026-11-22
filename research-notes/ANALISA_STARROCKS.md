# Analisa Codebase StarRocks

- **Sumber:** https://github.com/starrocks/starrocks (shallow clone, branch `main`)
- **Commit:** `a9a9fd73` (21 Nov 2026): *[BugFix] Forward partition_order_hint to the lake scan node (#82959)*
- **Lokasi clone:** `./starrocks` (915 MB, 22.714 file)
- **Lisensi:** Apache 2.0, proyek Linux Foundation

---

## 1. Gambaran Umum

StarRocks adalah **mesin query OLAP MPP** untuk analitik sub-detik, baik di storage miliknya sendiri maupun langsung di data lakehouse (Hive, Iceberg, Hudi, Delta Lake, Paimon). Sistem ini kompatibel dengan protokol MySQL.

Arsitekturnya terdiri dari dua proses:

| Komponen | Bahasa | Peran |
|---|---|---|
| **FE (Frontend)** | Java 17 | Parsing SQL, optimizer (CBO), metadata, HA, koordinasi, manajemen load/transaksi |
| **BE (Backend) / CN (Compute Node)** | C++ | Eksekusi vectorized (pipeline), storage engine, pembaca format file, cache |

Ada dua mode deploy:
- **Shared-nothing:** BE menyimpan data dengan replika lokal.
- **Shared-data (sejak 3.0):** data disimpan di object storage (S3/HDFS/Azure), dan CN stateless dengan data cache lokal. Shard dikelola oleh StarOS, dengan konsep *warehouse* untuk isolasi compute.

### Ukuran kode (termasuk test)

| Bahasa | Baris |
|---|---|
| Java | ~1,85 juta |
| C++ (.cpp/.h/.cc) | ~1,48 juta |
| Python (test & tooling) | ~60 ribu |
| Thrift / Protobuf | ~12 ribu / ~5,5 ribu |
| Grammar ANTLR | ~4 ribu |

### Peta direktori utama

| Direktori | Isi |
|---|---|
| `fe/` | Frontend Java (Maven utama, Gradle sebagai mirror) |
| `be/` | Backend C++ (~832 ribu baris di `be/src`) |
| `gensrc/` | Definisi Thrift/Protobuf (kontrak RPC FE↔BE) dan generator fungsi |
| `java-extensions/` | Reader JNI: Hudi, Paimon, Iceberg metadata, Kudu, ODPS, Fluss, JDBC bridge, UDF |
| `test/` | Framework SQL integration test (Python) dengan ~1.234 kasus |
| `docs/` | Dokumentasi en/zh/ja (Docusaurus) |
| `handbook/` | Dokumentasi arsitektur internal untuk kontributor/agent |
| `thirdparty/` | Skrip build dependensi (`vars.sh`) |

---

## 2. Siklus Hidup Query (end-to-end)

```
Klien MySQL
  │  mysql/MysqlServer (NIO)
  ▼
qe/ConnectProcessor → qe/StmtExecutor
  │
  ├─ Parse     : SqlParser + StarRocks.g4 (ANTLR 4.9.3) → AstBuilder → AST (fe-parser)
  ├─ Analyze   : sql/analyzer/* (visitor, ~124 kelas)
  ├─ Transform : RelationTransformer → OptExpression (logical plan)
  ├─ Optimize  : QueryOptimizer
  │     1. rewrite heuristik (rule/tree, 64 rule)
  │     2. rewrite Materialized View transparan
  │     3. Cascades/Memo search (Memo, Group, GroupExpression, TaskScheduler)
  │        · 197 rule transformasi (TF_*) + 45 rule implementasi (IMP_*)
  │        · statistik (histogram, NDV) + CostModel
  │     4. rewrite fisik → best plan
  ├─ Fragment  : PlanFragmentBuilder → PlanFragment / *Node
  └─ Dispatch  : DefaultCoordinator → qe/scheduler/Deployer
                 → Thrift TExecBatchPlanFragmentsParams via bRPC
  ▼
BE: orchestration/fragment_mgr → pipeline engine
  │  scan (morsel) → operator → exchange (bRPC transmit_chunk antar-BE)
  ▼
Hasil dikembalikan ke FE, lalu ke klien
```

---

## 3. Frontend (FE)

### Modul

| Modul | Fungsi | LOC main (perkiraan) |
|---|---|---|
| `fe-core` | Hampir semua logika | ~901 ribu (3.770 file) |
| `fe-parser` | Kelas AST | ~27,6 ribu |
| `fe-grammar` | `StarRocks.g4` (3.449 baris) + Lexer | grammar saja |
| `fe-type` | Sistem tipe | ~3,8 ribu |
| `fe-spi` | SPI tipis (auth, ErrorCode) | ~1,7 ribu |
| `fe-server` | Entry point `StarRocksFE.java` | 187 |
| `plugin/spark-dpp`, `hive-udf` | Spark Load ETL dan jembatan Hive UDF | kecil |

### Subsistem penting

- **Metadata & HA**
  - `GlobalStateMgr` (singleton) adalah state root.
  - Perubahan dicatat sebagai journal **BDB-JE**: `EditLog` dengan ~200 tipe entri log.
  - Image/checkpoint berformat `SRMetaBlock`, diserialisasi dengan Gson.
  - Leader election memakai replikasi BDB-JE, dengan role LEADER / FOLLOWER / OBSERVER.
- **Catalog eksternal**
  - Enum `ConnectorType` berisi: Hive, Iceberg, Hudi, DeltaLake, Paimon, JDBC, ES, ODPS, Kudu, Fluss, Lance, Unified, Benchmark.
  - Antarmukanya adalah `Connector` / `ConnectorMetadata`.
  - Setiap connector punya rule implementasi scan dan scan node sendiri.
- **Materialized View**
  - Rewrite otomatis ada di `rule/transformation/materialization/` (63 file), termasuk aggregate rollup, union compensation, dan time-series.
  - Refresh dijalankan lewat `TaskManager`, dengan partition-change tracking (`mv/pct/`).
  - Ada IVM (incremental view maintenance) di `rule/ivm/` dan `MVEpoch`.
- **Load**
  - Stream Load (HTTP), Broker/Spark Load (`loadv2`), Routine Load (Kafka, Pulsar), INSERT, Pipe, dan batch-write/merge commit.
- **Transaksi**
  - `GlobalTransactionMgr` → `DatabaseTransactionMgr`, dengan alur 2PC: commit → publish (`PublishVersionDaemon`) → visible.
  - Mendukung transaksi eksplisit BEGIN/COMMIT.
- **Shared-data**
  - `lake/`: LakeTable, StarOSAgent, compaction, vacuum.
  - `staros/`: StarManager embedded.
  - `warehouse/`: CN group.

---

## 4. Backend (BE)

### Layering yang ditegakkan secara mekanis

- `be/module_boundary_manifest.json` mendefinisikan **52 modul** beserta dependensi yang diizinkan.
- Aturan ini dicek oleh `build-support/check_be_module_boundaries.py`.
- Baseline utang hanya berisi **1 pelanggaran** (shrink-only).

Urutan layer dari bawah ke atas:

```
gutil/base → common → io → fs → types → platform → column → runtime → exprs
 → exec_primitive → storage_primitive → cache → compute_env
 → storage / formats / connector / data_sink → exec → data_workflows
 → orchestration → agent / http → service
```

### Mesin eksekusi

- **Model vectorized**
  - `Column` (Fixed, Binary, Nullable, Const, Array, Map, Struct, JSON, Decimal), digabung dalam `Chunk`.
  - Ada juga varian `german_string` dan `adaptive_nullable`.
- **Pipeline engine** (seluruh eksekusi berbasis pipeline)
  - Interface Operator: `need_input` / `push_chunk` / `pull_chunk` / `has_output`.
  - `PipelineDriver` adalah state machine (READY, INPUT_EMPTY, OUTPUT_FULL, ...). Driver di-yield setelah 100 ms, atau 5 ms bila di-preempt.
  - Antrean multi-level per query dan antrean berbasis *resource group* (`WorkGroupDriverQueue`).
  - Scan berbasis **morsel**, dijalankan di scan executor terpisah.
- **Operator kunci**
  - Hash join dengan beberapa varian hash map (linear-chained, bucket-chained, direct-mapping, ASOF).
  - Aggregation (streaming dan blocking).
  - Runtime filter (bloom/IN), dikoordinasikan antar-node.
  - **Spill** ke disk untuk join, agg, dan sort.
- **SIMD:** `base/simd/` ditambah xsimd, simdjson, dan simdutf.
- **JIT ekspresi:** LLVM 18 ORCv2 (`exprs/jit/`), lengkap dengan object cache.
- **Registry fungsi:** dibangkitkan dari `gensrc/script/functions.py`.

### Storage engine

- **Format segmen:** encoding bitshuffle, dict, RLE, FOR, plain. Indeks zone-map, bloom, bitmap, ordinal, ditambah indeks inverted (CLucene) dan vektor.
- **Primary Key model**
  - Persistent index (berbasis SSTable) dan delete vector.
  - Partial update mode kolom (`delta_column_group`).
  - Compaction khusus PK.
- **Shared-data (`storage/lake/`, ~190 file):** metadata ada di object storage, plus vacuum, tablet split/reshard, dan lokasi via StarOS.
- **Data cache:** disk (StarCache), memori (LRU/page cache), dan peer cache.

### Format dan konektor

- Parquet (lazy materialization), ORC (Apache ORC yang di-vendor dan dimodifikasi), CSV, JSON (simdjson), Avro, Arrow.
- Delete file untuk Iceberg, Delta, dan Paimon.
- Format berbasis Java (Hudi, Paimon, Kudu, ...) dibaca lewat **JNI scanner** ke `java-extensions/`.

### Layanan dan observability

- **Entry:** `be/src/service/starrocks_main.cpp`. BE dan CN memakai binary yang sama.
- **Port yang dibuka:** Thrift BackendService, bRPC (`PInternalService`, `LakeService`), Arrow Flight SQL, HTTP, dan Heartbeat.
- **Memori:** hierarki `MemTracker` (process → query → resource group), dengan hook jemalloc 5.3.0.
- **Observability:** `RuntimeProfile` (query/load profile) dan registry metrik per modul.

### Dependensi pihak ketiga utama

| Kategori | Library |
|---|---|
| RPC & serialisasi | brpc 1.18, Thrift 0.24, Protobuf 3.16, gRPC 1.43 |
| Format & storage | Arrow 24, RocksDB 6.22, CRoaring 4.2, CLucene |
| Core | LLVM 18.1.8, Boost 1.80, abseil, jemalloc 5.3 |
| Cloud & messaging | aws-sdk-cpp, Azure, Hadoop 3.4.3, librdkafka 2.11, Pulsar |
| Kompresi | zstd, lz4, snappy, zlib-ng, libdeflate |

---

## 5. Pengujian

| Lapisan | Jumlah |
|---|---|
| FE unit test | ~2.250 file, **~24,7 ribu `@Test`**, ~867 ribu LOC test (JMockit dominan, plus Mockito) |
| BE unit test | ~1.030 file `*_test.cpp` (GoogleTest), per-modul binary |
| SQL integration | ~1.234 kasus di `test/sql` (`python3 run.py`) |

Rasio LOC test FE terhadap kode produksi hampir 1:1, yang menandakan budaya testing yang kuat.

---

## 6. Kualitas Kode dan Utang Teknis

### Kekuatan
- Arsitektur BE berlapis yang **ditegakkan otomatis** dan hampir bebas pelanggaran.
- Optimizer Cascades yang matang (240+ rule), MV rewrite yang canggih, dan JIT LLVM.
- Cakupan test sangat luas, ditambah handbook internal yang terstruktur (bahkan ramah agent AI: `AGENTS.md`/`CLAUDE.md`).
- Kontrak RPC Thrift/Protobuf dijaga ketat (field wajib optional, ordinal tidak boleh dipakai ulang).

### Kelemahan / sinyal utang
1. **`fe-core` monolitik (~900 ribu LOC).** Pemisahan connector ke modul SPI baru berupa README (`fe/connector/`), sehingga connector masih bergantung pada internal fe-core.
2. **Singleton `GlobalStateMgr` dipanggil ~3.700 kali.** Akibatnya coupling tinggi dan test sulit diisolasi.
3. **File raksasa**:

   | File | Baris |
   |---|---|
   | `fe/.../sql/parser/AstBuilder.java` | 10.346 |
   | `fe/.../qe/SessionVariable.java` | 7.464 |
   | `fe/.../server/LocalMetastore.java` | 6.421 |
   | `be/src/storage/tablet_updates.cpp` | 6.165 |
   | `fe/.../alter/SchemaChangeHandler.java` | 6.037 |
   | `fe/.../common/Config.java` | 5.908 |
   | `be/src/storage/rowset/segment_iterator.cpp` | 5.565 |
   | `be/src/exprs/string_functions.cpp` | 5.523 |
   | `be/src/storage/persistent_index.cpp` | 5.500 |

4. **Dua build system FE** (Maven dan Gradle) harus dipelihara paralel.
5. ~480 penanda TODO/FIXME di FE. Di BE, layering FE belum punya padanan pengecekan otomatis.
6. Beberapa target BE besar (Exec, Formats, Connector) belum punya entri manifest sendiri, sehingga tidak dicek boundary-nya.

---

## 7. Titik Masuk yang Disarankan untuk Mempelajari Kode

| Tujuan | Mulai dari |
|---|---|
| Alur SQL | `fe/fe-core/.../qe/StmtExecutor.java` |
| Optimizer | `fe/fe-core/.../sql/optimizer/QueryOptimizer.java`, `rule/RuleType.java` |
| Penjadwalan fragment | `fe/fe-core/.../qe/scheduler/Deployer.java` |
| Eksekusi BE | `be/src/exec/runtime/pipeline_driver.h`, `be/src/exec_primitive/pipeline/operator.h` |
| Storage | `be/src/storage/tablet.h`, `be/src/storage/rowset/segment.h`, `be/src/storage/lake/` |
| Kontrak FE↔BE | `gensrc/thrift/InternalService.thrift`, `PlanNodes.thrift` |
| Panduan internal | `handbook/index.md`, `AGENTS.md` |

Build: `./build.sh --fe` / `./build.sh --be`. Build BE butuh Linux + toolchain thirdparty; di macOS lebih praktis memakai `build-in-docker.sh`.
