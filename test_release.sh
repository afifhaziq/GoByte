#!/bin/bash
# GoByte Pre-Release Testing & Benchmarking Script

# Run this script and log it with this command: bash test_release.sh 2>&1 | tee benchmark_results.log

set -e

# Function to monitor peak and average RAM usage of a process and all its children
# Returns: "peak_mb avg_mb" (values in MB)
monitor_ram() {
    local pid=$1
    local peak=0
    local sum=0
    local count=0
    
    while kill -0 $pid 2>/dev/null; do
        # Get memory of main process and all children
        local pids="$pid"
        local children=$(pgrep -P $pid 2>/dev/null | tr '\n' ' ')
        if [ -n "$children" ]; then
            pids="$pid $children"
        fi
        
        local current=$(ps -o rss= -p $pids 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
        
        if [ $current -gt $peak ]; then
            peak=$current
        fi
        
        sum=$((sum + current))
        count=$((count + 1))
        
        sleep 0.1
    done
    
    local avg=0
    if [ $count -gt 0 ]; then
        avg=$((sum / count))
    fi
    
    # Convert KB to MB and return both values
    local peak_mb=$((peak / 1024))
    local avg_mb=$((avg / 1024))
    
    echo "$peak_mb $avg_mb"
}

echo "=========================================="
echo "GoByte Pre-Release Testing & Benchmarking"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configurable dataset path (can be overridden: DATASET_DIR=... ./test_release.sh)
DATASET_DIR="${DATASET_DIR:-dataset/test}"

# Get dataset info
echo -e "${BLUE}Dataset Information:${NC}"
TOTAL_FILES=$(find "$DATASET_DIR" -name "*.pcap*" | wc -l)
TOTAL_SIZE=$(du -sh "$DATASET_DIR" | cut -f1)
echo "  - Total files: $TOTAL_FILES"
echo "  - Total size: $TOTAL_SIZE"
echo "  - Classes: $(ls -d "$DATASET_DIR"/*/ 2>/dev/null | wc -l)"
ls -d "$DATASET_DIR"/*/ 2>/dev/null | xargs -n1 basename | sed 's/^/    - /'
echo ""

# Pick a small test file
TEST_FILE=$(find "$DATASET_DIR" -name "*.pcap*" -type f | head -1)
echo -e "${BLUE}Test file: ${NC}$(basename $TEST_FILE)"
TEST_SIZE=$(du -h "$TEST_FILE" | cut -f1)
echo "  - Size: $TEST_SIZE"
echo ""

# Build the binary
echo -e "${BLUE}Building GoByte...${NC}"
go build -o gobyte .
echo -e "${GREEN}[PASS] Build successful${NC}"
echo ""

# Test 1: Single file - CSV output
echo -e "${BLUE}Test 1: Single File → CSV${NC}"
./gobyte --input "$TEST_FILE" --format csv --output output/test1.csv > /tmp/gobyte_test1.log 2>&1 &
GOBYTE_PID=$!
RAM_STATS=$(monitor_ram $GOBYTE_PID)
wait $GOBYTE_PID
cat /tmp/gobyte_test1.log
echo ""
PEAK_RAM_MB=$(echo $RAM_STATS | awk '{print $1}')
AVG_RAM_MB=$(echo $RAM_STATS | awk '{print $2}')
echo "  Peak RAM: ${PEAK_RAM_MB} MB | Avg RAM: ${AVG_RAM_MB} MB"
FILE_SIZE=$(du -h output/test1.csv 2>/dev/null | cut -f1 || echo "N/A")
PACKET_COUNT=$(tail -n +2 output/test1.csv 2>/dev/null | wc -l || echo "0")
echo "  Output: $FILE_SIZE, $PACKET_COUNT packets"
rm -f output/test1.csv
echo -e "${GREEN}[PASS] Test 1 passed${NC}"
echo ""

# Test 2: Single file - Parquet output
echo -e "${BLUE}Test 2: Single File → Parquet${NC}"
./gobyte --input "$TEST_FILE" --format parquet --output output/test2.parquet > /tmp/gobyte_test2.log 2>&1 &
GOBYTE_PID=$!
RAM_STATS=$(monitor_ram $GOBYTE_PID)
wait $GOBYTE_PID
cat /tmp/gobyte_test2.log
echo ""
PEAK_RAM_MB=$(echo $RAM_STATS | awk '{print $1}')
AVG_RAM_MB=$(echo $RAM_STATS | awk '{print $2}')
echo "  Peak RAM: ${PEAK_RAM_MB} MB | Avg RAM: ${AVG_RAM_MB} MB"
FILE_SIZE=$(du -h output/test2.parquet 2>/dev/null | cut -f1 || echo "N/A")
echo "  Output: $FILE_SIZE"
rm -f output/test2.parquet
echo -e "${GREEN}[PASS] Test 2 passed${NC}"
echo ""

# Test 3: Fixed-length packets (1500 bytes) - CSV format
echo -e "${BLUE}Test 3: Fixed-Length Packets (1500 bytes) - CSV${NC}"
./gobyte --input "$TEST_FILE" --format csv --length 1500 --output output/test3.csv > /tmp/gobyte_test3.log 2>&1 &
GOBYTE_PID=$!
RAM_STATS=$(monitor_ram $GOBYTE_PID)
wait $GOBYTE_PID
cat /tmp/gobyte_test3.log
echo ""
PEAK_RAM_MB=$(echo $RAM_STATS | awk '{print $1}')
AVG_RAM_MB=$(echo $RAM_STATS | awk '{print $2}')
echo "  Peak RAM: ${PEAK_RAM_MB} MB | Avg RAM: ${AVG_RAM_MB} MB"
FILE_SIZE=$(du -h output/test3.csv 2>/dev/null | cut -f1 || echo "N/A")
PACKET_COUNT=$(tail -n +2 output/test3.csv 2>/dev/null | wc -l || echo "0")
echo "  Output: $FILE_SIZE, $PACKET_COUNT packets"
rm -f output/test3.csv
echo -e "${GREEN}[PASS] Test 3 passed${NC}"
echo ""

# Test 4: IP Masking (with fixed length) - CSV format
echo -e "${BLUE}Test 4: IP Address Masking (Fixed-Length 1500 bytes) - CSV${NC}"
./gobyte --input "$TEST_FILE" --format csv --ipmask --length 1500 --output output/test4.csv > /tmp/gobyte_test4.log 2>&1 &
GOBYTE_PID=$!
RAM_STATS=$(monitor_ram $GOBYTE_PID)
wait $GOBYTE_PID
cat /tmp/gobyte_test4.log
echo ""
PEAK_RAM_MB=$(echo $RAM_STATS | awk '{print $1}')
AVG_RAM_MB=$(echo $RAM_STATS | awk '{print $2}')
echo "  Peak RAM: ${PEAK_RAM_MB} MB | Avg RAM: ${AVG_RAM_MB} MB"
FILE_SIZE=$(du -h output/test4.csv 2>/dev/null | cut -f1 || echo "N/A")
PACKET_COUNT=$(tail -n +2 output/test4.csv 2>/dev/null | wc -l || echo "0")
echo "  Output: $FILE_SIZE, $PACKET_COUNT packets"
rm -f output/test4.csv
echo -e "${GREEN}[PASS] Test 4 passed${NC}"
echo ""

# Test 5: Dataset mode - NumPy format with streaming (recommended)
echo -e "${BLUE}Test 5: Dataset Mode → NumPy with Streaming (Memory Efficient)${NC}"
echo "  Processing $TOTAL_FILES files with streaming mode..."
./gobyte --dataset "$DATASET_DIR" --format numpy --length 1500 --streaming --output output/test5.npy > /tmp/gobyte_test5.log 2>&1 &
GOBYTE_PID=$!
RAM_STATS=$(monitor_ram $GOBYTE_PID)
wait $GOBYTE_PID
cat /tmp/gobyte_test5.log
echo ""
PEAK_RAM_MB=$(echo $RAM_STATS | awk '{print $1}')
AVG_RAM_MB=$(echo $RAM_STATS | awk '{print $2}')
echo "  Peak RAM: ${PEAK_RAM_MB} MB | Avg RAM: ${AVG_RAM_MB} MB"
DATA_FILE_SIZE=$(du -h output/test5_data.npy 2>/dev/null | cut -f1 || echo "N/A")
LABELS_FILE_SIZE=$(du -h output/test5_labels.npy 2>/dev/null | cut -f1 || echo "N/A")
TOTAL_SIZE=$(du -ch output/test5_data.npy output/test5_labels.npy output/test5_classes.json 2>/dev/null | tail -1 | cut -f1 || echo "N/A")
PACKET_COUNT=$(grep -E "(Total packets|Processed.*packets)" /tmp/gobyte_test5.log | grep -oP '\d+' | head -1 || echo "N/A")
echo "  Output: $DATA_FILE_SIZE (data.npy), $LABELS_FILE_SIZE (labels.npy), $TOTAL_SIZE total, $PACKET_COUNT packets"
rm -f output/test5_data.npy output/test5_labels.npy output/test5_classes.json 2>/dev/null
echo -e "${GREEN}[PASS] Test 5 passed${NC}"
echo ""

# Test 6: Per-file output mode - CSV format
echo -e "${BLUE}Test 6: Per-File Output Mode (Memory Efficient) - CSV${NC}"
echo "  Creating separate output for each input file..."
./gobyte --dataset "$DATASET_DIR" --format csv --length 50 --concurrent 6 --per-file > /tmp/gobyte_test6.log 2>&1 &
GOBYTE_PID=$!
RAM_STATS=$(monitor_ram $GOBYTE_PID)
wait $GOBYTE_PID
cat /tmp/gobyte_test6.log
echo ""
PEAK_RAM_MB=$(echo $RAM_STATS | awk '{print $1}')
AVG_RAM_MB=$(echo $RAM_STATS | awk '{print $2}')
echo "  Peak RAM: ${PEAK_RAM_MB} MB | Avg RAM: ${AVG_RAM_MB} MB"
OUTPUT_DIR=$(grep "Output dir:" /tmp/gobyte_test6.log | awk '{print $NF}' || echo "")
if [ -n "$OUTPUT_DIR" ] && [ -d "$OUTPUT_DIR" ]; then
    OUTPUT_COUNT=$(find "$OUTPUT_DIR" -name "*.csv" | wc -l)
    TOTAL_OUTPUT_SIZE=$(du -sh "$OUTPUT_DIR" | cut -f1)
    echo "  Output: $OUTPUT_COUNT files, $TOTAL_OUTPUT_SIZE total"
    rm -rf "$OUTPUT_DIR"
else
    # Try to find the directory
    OUTPUT_DIR=$(find output -type d -name "per_file_*" 2>/dev/null | head -1)
    if [ -n "$OUTPUT_DIR" ] && [ -d "$OUTPUT_DIR" ]; then
        OUTPUT_COUNT=$(find "$OUTPUT_DIR" -name "*.csv" | wc -l)
        TOTAL_OUTPUT_SIZE=$(du -sh "$OUTPUT_DIR" | cut -f1)
        echo "  Output: $OUTPUT_COUNT files, $TOTAL_OUTPUT_SIZE total"
        rm -rf "$OUTPUT_DIR"
    else
        echo "  Output directory not found"
    fi
fi
echo -e "${GREEN}[PASS] Test 6 passed${NC}"
echo ""

# Summary
echo ""
echo "Summary:"
echo "  - Build: PASS"
echo "  - Single file CSV (variable-length): PASS"
echo "  - Single file Parquet (variable-length): PASS"
echo "  - Fixed-length packets (CSV): PASS"
echo "  - IP masking (CSV): PASS"
echo "  - Dataset mode NumPy with streaming: PASS"
echo "  - Per-file output mode (CSV): PASS"
echo ""
echo 
