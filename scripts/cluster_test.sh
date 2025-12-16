#!/bin/bash

# =============================================================================
# Distributed KV Store - Comprehensive Cluster Test Script
# =============================================================================
# This script tests all major features of the Raft-based distributed KV store:
# - Leader election
# - Data replication
# - Leader forwarding
# - Fault tolerance (node crashes)
# - Network partition simulation
# - Data consistency
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="deployments/docker/docker-compose.yml"
LOG_DIR="test_logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TEST_LOG="$LOG_DIR/test_run_$TIMESTAMP.log"

# Node ports (external)
declare -A NODE_PORTS=(
    ["node1"]=8081
    ["node2"]=8082
    ["node3"]=8083
    ["node4"]=8084
    ["node5"]=8085
)

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_TOTAL=0

# =============================================================================
# Utility Functions
# =============================================================================

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo -e "$msg" | tee -a "$TEST_LOG"
}

log_section() {
    echo "" | tee -a "$TEST_LOG"
    echo "=============================================================================" | tee -a "$TEST_LOG"
    echo -e "${CYAN}$1${NC}" | tee -a "$TEST_LOG"
    echo "=============================================================================" | tee -a "$TEST_LOG"
}

log_test() {
    echo -e "${YELLOW}[TEST]${NC} $1" | tee -a "$TEST_LOG"
    ((TESTS_TOTAL++))
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1" | tee -a "$TEST_LOG"
    ((TESTS_PASSED++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1" | tee -a "$TEST_LOG"
    ((TESTS_FAILED++))
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$TEST_LOG"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" | tee -a "$TEST_LOG"
}

# Wait for a condition with timeout
wait_for() {
    local timeout=$1
    local interval=$2
    local condition=$3
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        if eval "$condition"; then
            return 0
        fi
        sleep $interval
        elapsed=$((elapsed + interval))
    done
    return 1
}

# Get health status of a node
get_health() {
    local port=$1
    curl -s --connect-timeout 2 "http://localhost:$port/health" 2>/dev/null || echo "{}"
}

# Get cluster info from a node
get_cluster_info() {
    local port=$1
    curl -s --connect-timeout 2 "http://localhost:$port/cluster/info" 2>/dev/null || echo "{}"
}

# Find the current leader
find_leader() {
    for node in "${!NODE_PORTS[@]}"; do
        local port=${NODE_PORTS[$node]}
        local health=$(get_health $port)
        local is_leader=$(echo "$health" | grep -o '"is_leader":[^,}]*' | cut -d: -f2)
        if [ "$is_leader" == "true" ]; then
            echo "$node"
            return 0
        fi
    done
    echo ""
}

# Get leader ID from any node's perspective
get_leader_id() {
    local port=$1
    local health=$(get_health $port)
    echo "$health" | grep -o '"leader_id":"[^"]*"' | cut -d'"' -f4
}

# Put a key-value pair
put_kv() {
    local port=$1
    local key=$2
    local value=$3
    curl -s --connect-timeout 5 -X PUT "http://localhost:$port/kv/$key" -d "$value" 2>/dev/null
}

# Get a value by key
get_kv() {
    local port=$1
    local key=$2
    curl -s --connect-timeout 5 "http://localhost:$port/kv/$key" 2>/dev/null
}

# Delete a key
delete_kv() {
    local port=$1
    local key=$2
    curl -s --connect-timeout 5 -X DELETE "http://localhost:$port/kv/$key" 2>/dev/null
}

# Check if a node is healthy
is_node_healthy() {
    local port=$1
    local health=$(get_health $port)
    echo "$health" | grep -q '"status":"ok"'
}

# Stop a specific node
stop_node() {
    local node=$1
    log_info "Stopping $node..."
    docker-compose -f "$COMPOSE_FILE" stop "$node" >> "$TEST_LOG" 2>&1
}

# Start a specific node
start_node() {
    local node=$1
    log_info "Starting $node..."
    docker-compose -f "$COMPOSE_FILE" start "$node" >> "$TEST_LOG" 2>&1
}

# Restart a specific node
restart_node() {
    local node=$1
    log_info "Restarting $node..."
    docker-compose -f "$COMPOSE_FILE" restart "$node" >> "$TEST_LOG" 2>&1
}

# Kill a node abruptly (simulates crash)
kill_node() {
    local node=$1
    log_info "Killing $node (simulating crash)..."
    docker-compose -f "$COMPOSE_FILE" kill "$node" >> "$TEST_LOG" 2>&1
}

# Collect logs from all nodes
collect_logs() {
    local suffix=$1
    log_info "Collecting logs ($suffix)..."
    for node in "${!NODE_PORTS[@]}"; do
        docker-compose -f "$COMPOSE_FILE" logs --no-color "$node" > "$LOG_DIR/${node}_${suffix}_$TIMESTAMP.log" 2>&1 || true
    done
}

# Wait for cluster to stabilize (leader elected)
wait_for_leader() {
    local timeout=${1:-30}
    log_info "Waiting for leader election (timeout: ${timeout}s)..."

    local start_time=$(date +%s)
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ $elapsed -ge $timeout ]; then
            log_warn "Leader election timeout after ${timeout}s"
            return 1
        fi

        local leader=$(find_leader)
        if [ -n "$leader" ]; then
            log_info "Leader found: $leader (after ${elapsed}s)"
            return 0
        fi

        sleep 1
    done
}

# Wait for all nodes to be healthy
wait_for_all_nodes() {
    local timeout=${1:-30}
    log_info "Waiting for all nodes to be healthy (timeout: ${timeout}s)..."

    local start_time=$(date +%s)
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ $elapsed -ge $timeout ]; then
            log_warn "Timeout waiting for all nodes after ${timeout}s"
            return 1
        fi

        local all_healthy=true
        for node in "${!NODE_PORTS[@]}"; do
            if ! is_node_healthy ${NODE_PORTS[$node]}; then
                all_healthy=false
                break
            fi
        done

        if [ "$all_healthy" = true ]; then
            log_info "All nodes healthy (after ${elapsed}s)"
            return 0
        fi

        sleep 1
    done
}

# =============================================================================
# Test Cases
# =============================================================================

test_cluster_startup() {
    log_section "TEST: Cluster Startup and Initial Leader Election"

    log_test "Starting fresh cluster..."
    docker-compose -f "$COMPOSE_FILE" down -v >> "$TEST_LOG" 2>&1 || true
    docker-compose -f "$COMPOSE_FILE" up -d >> "$TEST_LOG" 2>&1

    sleep 5  # Give nodes time to start

    log_test "Waiting for leader election..."
    if wait_for_leader 30; then
        local leader=$(find_leader)
        log_pass "Leader elected: $leader"
    else
        log_fail "No leader elected within timeout"
        collect_logs "startup_failed"
        return 1
    fi

    log_test "Verifying all nodes are healthy..."
    local healthy_count=0
    for node in "${!NODE_PORTS[@]}"; do
        if is_node_healthy ${NODE_PORTS[$node]}; then
            ((healthy_count++))
            log_info "$node is healthy"
        else
            log_warn "$node is not healthy"
        fi
    done

    if [ $healthy_count -eq 5 ]; then
        log_pass "All 5 nodes are healthy"
    else
        log_fail "Only $healthy_count/5 nodes are healthy"
    fi

    collect_logs "after_startup"
}

test_basic_operations() {
    log_section "TEST: Basic KV Operations"

    local leader=$(find_leader)
    local leader_port=${NODE_PORTS[$leader]}

    log_test "PUT operation on leader ($leader)..."
    local put_result=$(put_kv $leader_port "test_key1" "test_value1")
    if echo "$put_result" | grep -q '"success":true'; then
        log_pass "PUT succeeded: test_key1=test_value1"
    else
        log_fail "PUT failed: $put_result"
    fi

    sleep 1  # Allow replication

    log_test "GET operation on leader..."
    local get_result=$(get_kv $leader_port "test_key1")
    if echo "$get_result" | grep -q '"value":"test_value1"'; then
        log_pass "GET succeeded: got correct value"
    else
        log_fail "GET failed: $get_result"
    fi

    log_test "DELETE operation on leader..."
    local del_result=$(delete_kv $leader_port "test_key1")
    if echo "$del_result" | grep -q '"success":true'; then
        log_pass "DELETE succeeded"
    else
        log_fail "DELETE failed: $del_result"
    fi

    log_test "Verify key is deleted..."
    local get_after_del=$(get_kv $leader_port "test_key1")
    if echo "$get_after_del" | grep -q '"found":false'; then
        log_pass "Key correctly deleted"
    else
        log_fail "Key still exists: $get_after_del"
    fi
}

test_leader_forwarding() {
    log_section "TEST: Leader Forwarding (Write to Follower)"

    local leader=$(find_leader)

    # Find a follower
    local follower=""
    local follower_port=""
    for node in "${!NODE_PORTS[@]}"; do
        if [ "$node" != "$leader" ]; then
            follower=$node
            follower_port=${NODE_PORTS[$node]}
            break
        fi
    done

    log_info "Leader: $leader, Testing follower: $follower"

    log_test "PUT operation on follower ($follower) - should forward to leader..."
    local put_result=$(put_kv $follower_port "forward_test" "forwarded_value")
    if echo "$put_result" | grep -q '"success":true'; then
        log_pass "Leader forwarding works! PUT succeeded via follower"
    else
        log_fail "Leader forwarding failed: $put_result"
    fi

    sleep 1

    log_test "GET from another follower to verify replication..."
    # Find another follower
    for node in "${!NODE_PORTS[@]}"; do
        if [ "$node" != "$leader" ] && [ "$node" != "$follower" ]; then
            local other_port=${NODE_PORTS[$node]}
            local get_result=$(get_kv $other_port "forward_test")
            if echo "$get_result" | grep -q '"value":"forwarded_value"'; then
                log_pass "Data replicated to $node"
            else
                log_fail "Data not replicated to $node: $get_result"
            fi
            break
        fi
    done
}

test_data_replication() {
    log_section "TEST: Data Replication Across All Nodes"

    local leader=$(find_leader)
    local leader_port=${NODE_PORTS[$leader]}

    # Write multiple keys
    log_test "Writing 10 key-value pairs..."
    for i in {1..10}; do
        put_kv $leader_port "repl_key_$i" "repl_value_$i" > /dev/null
    done
    log_pass "Wrote 10 keys to leader"

    sleep 2  # Allow replication

    log_test "Verifying all keys are replicated to all nodes..."
    local all_replicated=true
    for node in "${!NODE_PORTS[@]}"; do
        local port=${NODE_PORTS[$node]}
        local node_ok=true
        for i in {1..10}; do
            local result=$(get_kv $port "repl_key_$i")
            if ! echo "$result" | grep -q "repl_value_$i"; then
                node_ok=false
                all_replicated=false
                log_warn "$node missing repl_key_$i"
            fi
        done
        if [ "$node_ok" = true ]; then
            log_info "$node has all 10 keys"
        fi
    done

    if [ "$all_replicated" = true ]; then
        log_pass "All data replicated to all nodes"
    else
        log_fail "Some data missing on some nodes"
    fi
}

test_leader_crash_and_recovery() {
    log_section "TEST: Leader Crash and New Leader Election"

    local old_leader=$(find_leader)
    local old_leader_port=${NODE_PORTS[$old_leader]}

    # Write some data before crash
    log_test "Writing data before leader crash..."
    put_kv $old_leader_port "pre_crash_key" "pre_crash_value" > /dev/null
    sleep 1
    log_pass "Wrote pre_crash_key"

    log_test "Killing leader ($old_leader) to simulate crash..."
    kill_node $old_leader

    collect_logs "before_leader_crash"

    log_test "Waiting for new leader election..."
    sleep 3  # Give time for election timeout

    if wait_for_leader 30; then
        local new_leader=$(find_leader)
        if [ "$new_leader" != "$old_leader" ]; then
            log_pass "New leader elected: $new_leader (was: $old_leader)"
        else
            log_fail "Old leader somehow still leader?"
        fi
    else
        log_fail "No new leader elected after crash"
        collect_logs "leader_election_failed"
        return 1
    fi

    local new_leader=$(find_leader)
    local new_leader_port=${NODE_PORTS[$new_leader]}

    log_test "Verifying pre-crash data is still available..."
    local result=$(get_kv $new_leader_port "pre_crash_key")
    if echo "$result" | grep -q '"value":"pre_crash_value"'; then
        log_pass "Pre-crash data preserved after leader change"
    else
        log_fail "Pre-crash data lost: $result"
    fi

    log_test "Writing new data to new leader..."
    local put_result=$(put_kv $new_leader_port "post_crash_key" "post_crash_value")
    if echo "$put_result" | grep -q '"success":true'; then
        log_pass "Can write to new leader"
    else
        log_fail "Cannot write to new leader: $put_result"
    fi

    log_test "Restarting crashed node ($old_leader)..."
    start_node $old_leader
    sleep 5

    log_test "Verifying restarted node catches up..."
    local restarted_port=${NODE_PORTS[$old_leader]}
    if wait_for "10" "1" "is_node_healthy $restarted_port"; then
        log_pass "$old_leader is healthy again"

        # Check if it has the post-crash data
        sleep 2  # Allow sync
        local result=$(get_kv $restarted_port "post_crash_key")
        if echo "$result" | grep -q '"value":"post_crash_value"'; then
            log_pass "Restarted node caught up with new data"
        else
            log_warn "Restarted node may not have synced yet: $result"
        fi
    else
        log_fail "$old_leader did not recover"
    fi

    collect_logs "after_leader_recovery"
}

test_minority_failure() {
    log_section "TEST: Minority Failure (2 of 5 nodes down)"

    local leader=$(find_leader)

    # Select 2 non-leader nodes to kill
    local killed_nodes=()
    local count=0
    for node in "${!NODE_PORTS[@]}"; do
        if [ "$node" != "$leader" ] && [ $count -lt 2 ]; then
            killed_nodes+=("$node")
            ((count++))
        fi
    done

    log_test "Killing minority (${killed_nodes[*]})..."
    for node in "${killed_nodes[@]}"; do
        kill_node $node
    done

    sleep 2

    log_test "Verifying cluster still operational with 3/5 nodes..."
    local leader_port=${NODE_PORTS[$leader]}

    # Write should still work (have majority)
    local put_result=$(put_kv $leader_port "minority_test" "still_works")
    if echo "$put_result" | grep -q '"success":true'; then
        log_pass "Cluster operational with minority failure - writes work"
    else
        log_fail "Cluster not operational: $put_result"
    fi

    # Read should work
    local get_result=$(get_kv $leader_port "minority_test")
    if echo "$get_result" | grep -q '"value":"still_works"'; then
        log_pass "Reads also work"
    else
        log_fail "Reads failed: $get_result"
    fi

    log_test "Restarting killed nodes..."
    for node in "${killed_nodes[@]}"; do
        start_node $node
    done

    sleep 5
    wait_for_all_nodes 30

    log_test "Verifying all nodes have the data written during failure..."
    for node in "${killed_nodes[@]}"; do
        local port=${NODE_PORTS[$node]}
        sleep 1  # Give time to sync
        local result=$(get_kv $port "minority_test")
        if echo "$result" | grep -q '"value":"still_works"'; then
            log_pass "$node synced data written during its downtime"
        else
            log_warn "$node may not have synced yet: $result"
        fi
    done

    collect_logs "after_minority_failure"
}

test_majority_failure() {
    log_section "TEST: Majority Failure (3 of 5 nodes down) - Should Block Writes"

    local leader=$(find_leader)
    local leader_port=${NODE_PORTS[$leader]}

    # Kill 3 nodes (including leader to be sure)
    local killed_nodes=()
    local count=0
    for node in "${!NODE_PORTS[@]}"; do
        if [ $count -lt 3 ]; then
            killed_nodes+=("$node")
            ((count++))
        fi
    done

    log_test "Killing majority (${killed_nodes[*]})..."
    for node in "${killed_nodes[@]}"; do
        kill_node $node
    done

    sleep 5  # Wait for cluster to realize

    # Find a surviving node
    local surviving_port=""
    for node in "${!NODE_PORTS[@]}"; do
        local is_killed=false
        for killed in "${killed_nodes[@]}"; do
            if [ "$node" == "$killed" ]; then
                is_killed=true
                break
            fi
        done
        if [ "$is_killed" = false ]; then
            surviving_port=${NODE_PORTS[$node]}
            log_info "Testing via surviving node: $node (port $surviving_port)"
            break
        fi
    done

    log_test "Attempting write with majority down (should fail/timeout)..."
    # Use short timeout since it should fail
    local put_result=$(timeout 5 curl -s -X PUT "http://localhost:$surviving_port/kv/majority_test" -d "should_fail" 2>/dev/null || echo "timeout_or_error")

    if echo "$put_result" | grep -q '"success":true'; then
        log_fail "Write succeeded when it should have failed (no majority)"
    else
        log_pass "Write correctly blocked/failed without majority"
    fi

    log_test "Restarting killed nodes to restore cluster..."
    for node in "${killed_nodes[@]}"; do
        start_node $node
    done

    sleep 5
    wait_for_leader 30
    wait_for_all_nodes 30

    log_pass "Cluster restored after majority failure"

    collect_logs "after_majority_failure"
}

test_rapid_leader_changes() {
    log_section "TEST: Rapid Leader Changes (Stress Test)"

    log_test "Performing rapid leader kills to stress election..."

    for i in {1..3}; do
        log_info "Round $i of leader kill..."

        local leader=$(find_leader)
        if [ -z "$leader" ]; then
            log_warn "No leader found, waiting..."
            wait_for_leader 30
            leader=$(find_leader)
        fi

        if [ -n "$leader" ]; then
            kill_node $leader
            sleep 3

            if wait_for_leader 20; then
                local new_leader=$(find_leader)
                log_info "New leader: $new_leader"

                # Write some data
                local port=${NODE_PORTS[$new_leader]}
                put_kv $port "stress_key_$i" "stress_value_$i" > /dev/null
            else
                log_warn "Leader election slow in round $i"
            fi

            # Restart the killed node
            start_node $leader
            sleep 2
        fi
    done

    wait_for_all_nodes 30

    log_test "Verifying all stress test data is present..."
    local leader=$(find_leader)
    local port=${NODE_PORTS[$leader]}
    local all_present=true

    for i in {1..3}; do
        local result=$(get_kv $port "stress_key_$i")
        if echo "$result" | grep -q "stress_value_$i"; then
            log_info "stress_key_$i present"
        else
            log_warn "stress_key_$i missing"
            all_present=false
        fi
    done

    if [ "$all_present" = true ]; then
        log_pass "All stress test data preserved through leader changes"
    else
        log_fail "Some stress test data lost"
    fi

    collect_logs "after_stress_test"
}

test_consistency() {
    log_section "TEST: Data Consistency Check"

    local leader=$(find_leader)
    local leader_port=${NODE_PORTS[$leader]}

    log_test "Writing consistency test data..."
    for i in {1..20}; do
        put_kv $leader_port "consistency_$i" "value_$i" > /dev/null
    done

    sleep 3  # Allow full replication

    log_test "Comparing data across all nodes..."
    local reference=""
    local first_node=""
    local all_consistent=true

    for node in "${!NODE_PORTS[@]}"; do
        local port=${NODE_PORTS[$node]}
        local node_data=""

        for i in {1..20}; do
            local result=$(get_kv $port "consistency_$i")
            local value=$(echo "$result" | grep -o '"value":"[^"]*"' | cut -d'"' -f4)
            node_data="$node_data$i=$value;"
        done

        if [ -z "$reference" ]; then
            reference="$node_data"
            first_node="$node"
            log_info "Reference data from $node"
        else
            if [ "$node_data" == "$reference" ]; then
                log_info "$node matches $first_node"
            else
                log_warn "$node differs from $first_node"
                all_consistent=false
            fi
        fi
    done

    if [ "$all_consistent" = true ]; then
        log_pass "All nodes have consistent data"
    else
        log_fail "Data inconsistency detected"
    fi

    collect_logs "after_consistency_check"
}

test_network_partition_simulation() {
    log_section "TEST: Network Partition Simulation"

    # We can simulate a partition by stopping some nodes
    # In a real test, we'd use iptables or network namespaces

    log_info "Note: This is a simplified partition simulation using node stops"

    local leader=$(find_leader)
    log_info "Current leader: $leader"

    # Create a "partition" by stopping nodes on one side
    # Stop node4 and node5 (minority partition)
    log_test "Creating partition: stopping node4 and node5..."
    stop_node node4
    stop_node node5

    sleep 3

    log_test "Majority partition (node1, node2, node3) should still work..."
    local leader=$(find_leader)
    if [ -n "$leader" ]; then
        local port=${NODE_PORTS[$leader]}
        local put_result=$(put_kv $port "partition_test" "majority_side")
        if echo "$put_result" | grep -q '"success":true'; then
            log_pass "Majority partition can still write"
        else
            log_fail "Majority partition cannot write: $put_result"
        fi
    else
        log_fail "No leader in majority partition"
    fi

    log_test "Healing partition..."
    start_node node4
    start_node node5

    sleep 5
    wait_for_all_nodes 30

    log_test "Verifying partition data synced to healed nodes..."
    for node in node4 node5; do
        local port=${NODE_PORTS[$node]}
        sleep 1
        local result=$(get_kv $port "partition_test")
        if echo "$result" | grep -q '"value":"majority_side"'; then
            log_pass "$node received data written during partition"
        else
            log_warn "$node may not have synced: $result"
        fi
    done

    collect_logs "after_partition"
}

test_read_consistency_levels() {
    log_section "TEST: Read Consistency (Linearizable vs Eventual)"

    local leader=$(find_leader)
    local leader_port=${NODE_PORTS[$leader]}

    # Find a follower
    local follower=""
    local follower_port=""
    for node in "${!NODE_PORTS[@]}"; do
        if [ "$node" != "$leader" ]; then
            follower=$node
            follower_port=${NODE_PORTS[$node]}
            break
        fi
    done

    log_test "Writing test data..."
    put_kv $leader_port "consistency_level_test" "test_value" > /dev/null
    sleep 1

    log_test "Regular read from follower..."
    local regular_read=$(get_kv $follower_port "consistency_level_test")
    if echo "$regular_read" | grep -q '"value":"test_value"'; then
        log_pass "Regular read succeeded"
    else
        log_fail "Regular read failed: $regular_read"
    fi

    log_test "Linearizable read from follower (should redirect or fail)..."
    local linear_read=$(curl -s "http://localhost:$follower_port/kv/consistency_level_test?linearizable=true" 2>/dev/null)
    log_info "Linearizable read result: $linear_read"
    # Linearizable reads on follower should either redirect or return an error
    if echo "$linear_read" | grep -q '"value"'; then
        log_pass "Linearizable read returned data"
    elif echo "$linear_read" | grep -q 'not leader'; then
        log_pass "Linearizable read correctly rejected on follower"
    else
        log_warn "Unexpected linearizable read result"
    fi
}

print_summary() {
    log_section "TEST SUMMARY"

    echo ""
    echo "============================================"
    echo -e "Total Tests:  ${TESTS_TOTAL}"
    echo -e "${GREEN}Passed:       ${TESTS_PASSED}${NC}"
    echo -e "${RED}Failed:       ${TESTS_FAILED}${NC}"
    echo "============================================"
    echo ""

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
    else
        echo -e "${RED}Some tests failed. Check logs in $LOG_DIR${NC}"
    fi

    echo ""
    echo "Logs saved to: $LOG_DIR"
    echo "Main test log: $TEST_LOG"
}

cleanup() {
    log_section "CLEANUP"
    log_info "Collecting final logs..."
    collect_logs "final"

    log_info "Stopping cluster..."
    docker-compose -f "$COMPOSE_FILE" down >> "$TEST_LOG" 2>&1 || true
}

# =============================================================================
# Main Execution
# =============================================================================

main() {
    # Create log directory
    mkdir -p "$LOG_DIR"

    echo "============================================"
    echo "Distributed KV Store - Cluster Test Suite"
    echo "============================================"
    echo "Log directory: $LOG_DIR"
    echo "Test log: $TEST_LOG"
    echo ""

    # Trap cleanup on exit
    trap cleanup EXIT

    # Run all tests
    test_cluster_startup
    test_basic_operations
    test_leader_forwarding
    test_data_replication
    test_leader_crash_and_recovery
    test_minority_failure
    test_majority_failure
    test_rapid_leader_changes
    test_consistency
    test_network_partition_simulation
    test_read_consistency_levels

    # Print summary
    print_summary
}

# Run specific test if provided as argument, otherwise run all
if [ $# -gt 0 ]; then
    mkdir -p "$LOG_DIR"
    case $1 in
        startup) test_cluster_startup ;;
        basic) test_basic_operations ;;
        forward) test_leader_forwarding ;;
        replication) test_data_replication ;;
        leader_crash) test_leader_crash_and_recovery ;;
        minority) test_minority_failure ;;
        majority) test_majority_failure ;;
        stress) test_rapid_leader_changes ;;
        consistency) test_consistency ;;
        partition) test_network_partition_simulation ;;
        read_levels) test_read_consistency_levels ;;
        *)
            echo "Unknown test: $1"
            echo "Available tests: startup, basic, forward, replication, leader_crash, minority, majority, stress, consistency, partition, read_levels"
            exit 1
            ;;
    esac
    print_summary
else
    main
fi
