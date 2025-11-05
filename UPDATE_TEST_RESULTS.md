# Updating Test Expected Results After cost_columnar_scan Changes

The changes to `cost_columnar_scan()` improved query planner cost estimates by considering filter selectivity. This causes some tests to fail because the expected query plans have changed.

## Failed Tests

The following tests need their expected results updated:

**TSL tests:**
- `columnar_scan_cost` ⭐ (main test for this change)
- `merge_append_partially_compressed`
- `plan_skip_scan-18`
- `plan_skip_scan_dagg-18`
- `skip_scan`
- `transparent_decompression-18`
- `transparent_decompression_join_index`
- `transparent_decompression_ordered_index-18`

**Shared tests:**
- `constraint_exclusion_prepared-18`
- `ordered_append_join-18`
- `transparent_decompress_chunk-18`

## How to Update

### Option 1: Download from CI

1. Go to the failed GitHub Actions run
2. Download the "regression.diffs" artifact
3. Review the changes to understand what improved
4. Apply the changes to expected files

### Option 2: Run Tests Locally

```bash
# Build TimescaleDB
./bootstrap
cd build
make -j4
make install

# Run just the failed tests
make regresscheck-t TESTS="columnar_scan_cost"

# If the results look correct, copy them to expected:
cp tsl/test/results/columnar_scan_cost.out ../tsl/test/expected/columnar_scan_cost.out

# Repeat for other tests
for test in merge_append_partially_compressed plan_skip_scan-18 plan_skip_scan_dagg-18 skip_scan; do
    make regresscheck-t TESTS="$test"
    cp "tsl/test/results/${test}.out" "../tsl/test/expected/${test}.out"
done

# For shared tests
for test in constraint_exclusion_prepared-18 ordered_append_join-18 transparent_decompress_chunk-18; do
    make regresscheck-shared TESTS="$test"
    cp "tsl/test/shared/results/${test}.out" "../tsl/test/shared/expected/${test}.out"
done
```

### Option 3: Use the Script

```bash
chmod +x update_test_results.sh
./update_test_results.sh
```

## What Changed

The improvements cause:
- **Lower row estimates** for queries with selective WHERE clauses on compressed columns
- **Better plan choices** when filters eliminate most rows
- **More accurate costs** for decompression operations

These are **expected improvements** and the new plans should be faster.

## Commit Message

When you update the expected results, use:

```
Update test expected results after cost_columnar_scan improvements

The improved cost estimation in cost_columnar_scan() now considers
filter selectivity during decompression, leading to more accurate
row estimates and better query plans.

Updated expected results for tests that verify query plans with:
- Compressed chunks with selective filters
- Skip scans on compressed data
- Transparent decompression queries

All changes reflect improved planner behavior.
```
