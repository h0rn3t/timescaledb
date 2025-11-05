#!/bin/bash
# Script to update test expected files after cost_columnar_scan changes

set -e

FAILED_TESTS=(
    "columnar_scan_cost"
    "merge_append_partially_compressed"
    "plan_skip_scan-18"
    "plan_skip_scan_dagg-18"
    "skip_scan"
    "transparent_decompression-18"
    "transparent_decompression_join_index"
    "transparent_decompression_ordered_index-18"
    "constraint_exclusion_prepared-18"
    "ordered_append_join-18"
    "transparent_decompress_chunk-18"
)

echo "Building TimescaleDB..."
cd build
make -j4

echo "Running failed tests and updating expected results..."

for test in "${FAILED_TESTS[@]}"; do
    echo "Processing test: $test"

    # Run the test
    if make regresscheck-t TESTS="$test" 2>&1 | tee /tmp/test_output.log; then
        echo "  ✓ Test $test now passes"
    else
        # If test fails, copy actual results to expected
        if [ -f "tsl/test/results/${test}.out" ]; then
            echo "  → Updating expected results for $test"
            cp "tsl/test/results/${test}.out" "../tsl/test/expected/${test}.out"
        fi
    fi
done

echo ""
echo "Done! Please review the changes and commit:"
echo "  git add tsl/test/expected/"
echo "  git commit -m 'Update test expected results for cost_columnar_scan changes'"
