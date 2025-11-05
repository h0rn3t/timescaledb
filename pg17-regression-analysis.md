# Полный анализ регрессий PostgreSQL 17

## Исполнительное резюме

PostgreSQL 17 содержит 11 провалившихся тестов, связанных с двумя основными категориями проблем:
1. **Неправильная оценка стоимости ColumnarScan** - стоимость занижается в ~50-200 раз
2. **Изменения в структуре query plan** - MergeAppend заменяется на Sort+Append

## Детальный анализ

### 1. Проблема с cost estimation

#### Провалившиеся тесты:
- `columnar_scan_cost.out`
- `merge_append_partially_compressed.out`
- `plan_skip_scan-17.out`
- `plan_skip_scan_dagg-17.out`
- `skip_scan.out`
- `transparent_decompression_join_index.out`
- `transparent_decompression_ordered_index-17.out`
- `vector_agg_grouping.out`

#### Симптомы:

**Пример 1:**
```sql
SELECT * FROM costtab WHERE c = '100';

Ожидалось:
Custom Scan (ColumnarScan) (cost=0.11..101.10 rows=10000 width=108)

Получили:
Custom Scan (ColumnarScan) (cost=0.11..1.60 rows=50 width=108)
```

**Пример 2:**
```sql
SELECT * FROM highcard WHERE ts > 200000 AND ts < 300000;

Ожидалось:
Custom Scan (ColumnarScan) (cost=0.43..1005.58 rows=99000 width=4)

Получили:
Custom Scan (ColumnarScan) (cost=0.43..20.53 rows=495 width=4)
```

#### Математический анализ:

**Входные данные:**
- Compressed rows (батчей): 10
- Batch size: 1000 строк/батч
- Total rows to decompress: 10 × 1000 = 10,000
- Filter selectivity: 0.005 (0.5%)
- Expected output rows: 10,000 × 0.005 = 50 ✓

**Расчет стоимости:**
```
base_cost = 1.10 (Seq Scan на сжатой таблице)
cpu_tuple_cost = 0.01

ПРАВИЛЬНО:
total_cost = 1.10 + (10,000 × 0.01) = 101.10
path->rows = 50

НЕПРАВИЛЬНО (что мы видим):
total_cost = 1.10 + (50 × 0.01) = 1.60
path->rows = 50
```

#### Причина:

Несмотря на fix в commit 5474423, который разделил:
- `total_decompressed_rows` (для расчета cost) = 10,000
- `output_rows` (для оценки rows) = 50

PostgreSQL 17 каким-то образом **снова применяет selectivity к стоимости**.

Возможные причины:
1. PG17 применяет post-processing корректировку к custom scan costs
2. Изменилась логика в `set_rel_size()` или `set_rel_pathlist()`
3. Новый hook или callback в PG17 пересчитывает стоимость
4. `compressed_path->rows` уже содержит примененную selectivity в PG17

### 2. Изменения в структуре query plan

#### Провалившиеся тесты:
- `merge_append_partially_compressed.out`
- `transparent_decompress_chunk-17.out`
- `ordered_append_join-17.out`
- `constraint_exclusion_prepared-17.out`

#### Симптомы:

**Старый план (PG16 и ранее):**
```
Limit
  -> Custom Scan (ChunkAppend)
       Order: time DESC
       -> Merge Append
            Sort Key: time DESC
            -> Sort (compressed data from ColumnarScan)
            -> Index Scan (uncompressed data)
```

**Новый план (PG17):**
```
Limit
  -> Sort
       Sort Key: time DESC
       -> Append
            -> Custom Scan (ColumnarScan) (compressed)
            -> Seq Scan (uncompressed)
```

#### Ключевые отличия:

1. **ChunkAppend исчезает** - используется простой Append
2. **Merge Append → Sort** - вместо слияния отсортированных потоков
3. **Index Scan → Seq Scan** - индексы больше не используются для несжатых данных

#### Причина:

Из комментария в коде (`columnar_scan.c:1173-1174`):
> "also different MergeAppend costs on Postgres before 17 due to a bug there"

**PostgreSQL 17 исправил баг в оценке стоимости MergeAppend**, что привело к:
- MergeAppend теперь оценивается как более дорогой (правильно!)
- Sort над Append становится дешевле (из-за ошибки в cost ColumnarScan!)
- Планировщик выбирает другой, потенциально менее эффективный план

### 3. Корреляция проблем

Эти две проблемы связаны:
1. Из-за заниженной стоимости ColumnarScan
2. Sort над Append выглядит очень дешевым
3. Планировщик выбирает Sort+Append вместо MergeAppend
4. Исчезают оптимизации ChunkAppend

## Рекомендации

### Вариант A: Обновить expected результаты (ПРОСТОЕ РЕШЕНИЕ)

**Преимущества:**
- Быстро реализуется
- Тесты проходят
- Возможно, новые планы PG17 действительно эффективнее

**Недостатки:**
- Не решает корневую проблему
- Неправильная оценка стоимости может влиять на реальные запросы
- SkipScan оптимизация может не работать корректно

**Действия:**
1. Создать PG17-специфичные expected файлы для 11 тестов
2. Документировать поведенческие различия

### Вариант B: Исследовать и исправить (ПРАВИЛЬНОЕ РЕШЕНИЕ)

**Приоритет 1: Найти где PG17 пересчитывает стоимость**

Возможные места:
- `set_cheapest()` в pathnode.c
- `add_path()` modifications
- `set_rel_pathlist_hook` или новые hooks в PG17
- Changes in custom scan provider interface

**Методы расследования:**
1. Добавить elog(NOTICE) в `cost_columnar_scan()` для отладки
2. Проверить если `compressed_path->rows` уже содержит selectivity
3. Изучить commits PostgreSQL 17 связанные с custom scans
4. Сравнить поведение `clauselist_selectivity()` между PG16 и PG17

**Приоритет 2: Если нужно, добавить PG17-специфичный код**

```c
#if PG17_GE
    /* PG17 applies selectivity differently, compensate */
    double pg17_correction_factor = ...;
    path->total_cost = ... ;
#endif
```

**Приоритет 3: Проверить MergeAppend changes**

Убедиться что исправление cost_merge_append в PG17 корректно работает с нашими путями.

### Вариант C: Гибридный подход (РЕКОМЕНДУЕТСЯ)

1. **Краткосрочно:** Обновить expected результаты для PG17
2. **Долгосрочно:** Исследовать и исправить root cause
3. Документировать known issues в release notes

## Список провалившихся тестов

1. ✗ `columnar_scan_cost.out` - cost estimation
2. ✗ `merge_append_partially_compressed.out` - plan structure  
3. ✗ `plan_skip_scan-17.out` - cost estimation
4. ✗ `plan_skip_scan_dagg-17.out` - cost estimation
5. ✗ `skip_scan.out` - cost estimation
6. ✗ `transparent_decompression_join_index.out` - cost estimation
7. ✗ `transparent_decompression_ordered_index-17.out` - plan structure
8. ✗ `vector_agg_grouping.out` - cost estimation
9. ✗ `constraint_exclusion_prepared-17.out` - plan structure
10. ✗ `ordered_append_join-17.out` - plan structure
11. ✗ `transparent_decompress_chunk-17.out` - plan structure

## Следующие шаги

### Немедленные действия:
1. Создать тикет для tracking этой проблемы
2. Решить: обновлять expected результаты или исправлять?
3. Если обновлять - запустить helper скрипт для генерации новых expected
4. Если исправлять - начать расследование с добавления debug logging

### Долгосрочные действия:
1. Изучить PostgreSQL 17 release notes для custom scan changes
2. Проверить mailing list PostgreSQL на обсуждения selectivity/cost changes
3. Рассмотреть возможность поднять issue в PostgreSQL community
4. Добавить регрессионные тесты для проверки корректности cost estimation

## Полезные ссылки

- Previous fix: commit 5474423 "Fix cost estimation bug in ColumnarScan"
- Code: `tsl/src/nodes/columnar_scan/columnar_scan.c:720-759`
- PG17 MergeAppend bug fix reference: `columnar_scan.c:1173-1174`

---

## UPDATE: Root Cause Found and Fixed!

### The Real Problem

The issue was NOT in PostgreSQL 17 applying post-processing to our costs. Instead:

1. **In `set_compressed_baserel_size_estimates()`** (line 538), we call PostgreSQL's `set_baserel_size_estimates(root, compressed_rel)`

2. **This function applies baserestrictinfo selectivity** to `compressed_rel->rows`, reducing the batch count based on value filters

3. **When we create `compressed_path`** via `create_seqscan_path()`, it uses the already-reduced `compressed_rel->rows`

4. **In `cost_columnar_scan()`**, we were using:
   ```c
   total_decompressed_rows = compressed_path->rows * batch_size
   ```
   But `compressed_path->rows` already had selectivity applied!

### Example of the Bug

```
Original batches: 10
Filter: c = '100' (selectivity 0.005)

After set_baserel_size_estimates:
  compressed_rel->rows = 10 * 0.005 = 0.05

When we call create_seqscan_path:
  compressed_path->rows = 0.05

In cost_columnar_scan (WRONG):
  total_decompressed = 0.05 * 1000 = 50
  cost = 1.10 + (50 * 0.01) = 1.60 ❌
```

### The Fix

Added `CompressionInfo.original_compressed_rows` field to save the batch count BEFORE selectivity is applied:

```c
// In set_compressed_baserel_size_estimates():
compression_info->original_compressed_rows = rel->rows;  // BEFORE selectivity
set_baserel_size_estimates(root, rel);  // AFTER selectivity

// In cost_columnar_scan():
double total_decompressed_rows = compression_info->original_compressed_rows *
                                  compression_info->compressed_batch_size;
```

### Why This Matters

Value filters (like `c = '100'`) can't be pushed down to compressed metadata (min/max).
They're applied AFTER decompression. So we must:

1. Read all batches matching metadata filters (min/max ranges)
2. Decompress ALL rows from those batches
3. Apply value filters to decompressed rows

The cost is proportional to STEP 2 (all decompressed rows), not STEP 3 (filtered output).

### Files Changed

- `tsl/src/nodes/columnar_scan/columnar_scan.h` - Added `original_compressed_rows` field
- `tsl/src/nodes/columnar_scan/columnar_scan.c` - Save and use original value

### Expected Impact

This fix should resolve all 11 failing regression tests by restoring correct cost estimates for ColumnarScan operations.
