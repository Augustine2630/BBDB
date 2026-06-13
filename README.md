# BBDB — BigBrotherDB

Write-heavy append-only хранилище телеком событий (звонки, SMS, метаданные).
Оптимизировано под закон Яровой: максимальная скорость записи, компактное хранение, медленное но возможное чтение.

---

## Ключевые особенности

- **Append-only** — данные никогда не изменяются, только добавляются и удаляются целыми блоками по TTL
- **Шардирование** — `shard_id = event_type << 8 | xxHash64(partition_key) % 256`, события с одним `partition_key + event_type` всегда попадают в один шард
- **Колоночное хранение** — блок на диске разделён на колонки `key_hash[]`, `timestamp[]`, `payload[]`, каждая сжата zstd (SpeedBestCompression)
- **Bloom-фильтры** — каждый блок сопровождается `.bloom` файлом, отсекает ~99% лишних обращений к диску при чтении
- **Тиры хранения** — hot / warm / cold, данные мигрируют по мере старения
- **TTL** — удаление целыми блоками (`unlink`), без compaction

---

## Путь записи

```
gRPC Write (bidirectional stream)
  └─> RingBuf (16384 slots, backpressure)
        └─> ShardWriter (горутина на шард, flush каждые 2ms)
              └─> WAL (pebble, Sync commit, group commit ≤2ms)
              └─> Memtable (in-memory, sorted by key_hash+timestamp)
                    └─> Seal → .block + .bloom файл на диске (hot tier)
```

**Триггеры seal:**
- Граница часа (новый UTC-час)
- Размер memtable ≥ 256 MiB
- Остановка сервера (forceSeal, блокирующий — данные гарантированно сбрасываются на диск)

**Формат блока на диске:**
```
HEADER  (64 bytes)  — magic, version, shard_id, event_type, opened_at, sealed_at, row_count
COLUMNS             — key_hash[] | timestamp[] | payload[]  (каждая колонка: zstd SpeedBestCompression)
FOOTER  (48 bytes)  — offsets, sizes, checksum, footer_magic
```

Delta encoding применяется к `timestamp[]` перед сжатием — улучшает ratio для монотонно растущих временных меток.

---

## Путь чтения

```
gRPC Query (server-side stream)
  └─> xxHash64(partition_key) → keyHash
  └─> Sparse index scan (pebble, prefix idx:{event_type}:{keyHash}:)
        └─> Bloom filter check (память → файл)
              └─> Параллельное чтение блоков (до 8 воркеров)
                    └─> footer → header → key_hash[] → timestamp[] → payload[] (только при совпадении)
                          └─> K-way heap merge (блоки pre-sorted по key_hash+timestamp)
```

Чтение намеренно медленное: нет кэша декомпрессированных данных, нет индексов по payload. Bloom-фильтр и sparse index минимизируют количество блоков для открытия, но при большом диапазоне дат каждый блок читается с диска.

---

## Конфигурация

```yaml
# configs/bbdb.example.yaml
grpc:
  listen_addr: ":7070"

ingestion:
  batch_interval: 2ms
  max_block_bytes: 268435456  # 256 MiB

ttl:
  retention_period: 43800h    # 5 лет
  reaper_interval: 10m

log:
  level: info       # debug | info | warn | error
  format: text      # text | json | json-ecs
```

---

## Запуск

```bash
# Сборка
make build/bbdb                        # bin/bbdb (версия dev)
VERSION=1.0.0 make build/bbdb          # bin/bbdb (версия 1.0.0)

# Запуск
./bin/bbdb start --config configs/bbdb.example.yaml

# Версия
./bin/bbdb version
```

---

## gRPC API

**Write** — bidirectional streaming (`EventIngestion.Write`):
```
WriteRequest  { events: [Event], batch_id }
WriteResponse { accepted, partition_keys, batch_id, error? }
```

**Query** — server-side streaming (`EventQuery.Query`):
```
QueryRequest  { partition_key, event_type?, from_ns, to_ns }
QueryResponse { events: [Event], is_last, total, error? }
```

Go-драйвер: [github.com/Augustine2630/bbdb-driver-go](https://github.com/Augustine2630/bbdb-driver-go)
