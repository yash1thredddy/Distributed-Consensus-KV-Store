#!/usr/bin/env python3
"""
Comprehensive Cluster Testing Suite
====================================
Tests for load, consistency, chaos, and monkey testing of the Raft-based KV store.

Usage:
    python test_cluster.py [test_name] [options]

Tests:
    load        - Load test with 100k+ entries
    consistency - Consistency verification
    chaos       - Chaos testing with random node failures
    monkey      - Monkey testing with random operations
    all         - Run all tests

Options:
    --nodes N       Number of entries for load test (default: 100000)
    --duration N    Duration in seconds for chaos/monkey tests (default: 60)
    --base-port N   Base HTTP port (default: 8081)
"""

import argparse
import json
import random
import requests
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class ClusterConfig:
    nodes: List[str]
    base_http_port: int = 8081

    def get_node_url(self, node_idx: int) -> str:
        return f"http://localhost:{self.base_http_port + node_idx}"

    def get_all_urls(self) -> List[str]:
        return [self.get_node_url(i) for i in range(len(self.nodes))]


DEFAULT_CONFIG = ClusterConfig(
    nodes=["node1", "node2", "node3", "node4", "node5"],
    base_http_port=8081
)


# =============================================================================
# Helper Functions
# =============================================================================

def get_cluster_info(base_url: str) -> Optional[dict]:
    """Get cluster info from a node."""
    try:
        resp = requests.get(f"{base_url}/cluster/info", timeout=2)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def find_leader(config: ClusterConfig) -> Tuple[Optional[str], Optional[int]]:
    """Find the current leader in the cluster."""
    for idx, url in enumerate(config.get_all_urls()):
        info = get_cluster_info(url)
        if info and info.get("state") == "Leader":
            return url, idx
    return None, None


def put_value(base_url: str, key: str, value: str, timeout: float = 5.0) -> Tuple[bool, Optional[str]]:
    """Put a value in the KV store."""
    try:
        resp = requests.put(f"{base_url}/kv/{key}", data=value, timeout=timeout)
        if resp.status_code == 200:
            return True, None
        return False, f"HTTP {resp.status_code}: {resp.text}"
    except requests.exceptions.Timeout:
        return False, "timeout"
    except Exception as e:
        return False, str(e)


def get_value(base_url: str, key: str, timeout: float = 5.0) -> Tuple[bool, Optional[str], Optional[str]]:
    """Get a value from the KV store. Returns (success, value, error)."""
    try:
        resp = requests.get(f"{base_url}/kv/{key}", timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("found"):
                return True, data.get("value"), None
            return True, None, None
        return False, None, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, None, str(e)


def delete_value(base_url: str, key: str, timeout: float = 5.0) -> Tuple[bool, Optional[str]]:
    """Delete a value from the KV store."""
    try:
        resp = requests.delete(f"{base_url}/kv/{key}", timeout=timeout)
        if resp.status_code == 200:
            return True, None
        return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)


def get_metrics(base_url: str) -> Optional[dict]:
    """Get Prometheus metrics as a dict."""
    try:
        resp = requests.get(f"{base_url}/metrics", timeout=2)
        if resp.status_code == 200:
            metrics = {}
            for line in resp.text.split("\n"):
                if line and not line.startswith("#"):
                    parts = line.split(" ")
                    if len(parts) >= 2:
                        metrics[parts[0]] = float(parts[1])
            return metrics
    except Exception:
        pass
    return None


def docker_stop_node(node_name: str) -> bool:
    """Stop a Docker container."""
    try:
        subprocess.run(
            ["docker", "stop", f"raft-{node_name}"],
            capture_output=True,
            timeout=10
        )
        return True
    except Exception:
        return False


def docker_start_node(node_name: str) -> bool:
    """Start a Docker container."""
    try:
        subprocess.run(
            ["docker", "start", f"raft-{node_name}"],
            capture_output=True,
            timeout=10
        )
        return True
    except Exception:
        return False


def print_result(test_name: str, passed: bool, message: str = ""):
    """Print test result."""
    status = "PASS" if passed else "FAIL"
    color = "\033[92m" if passed else "\033[91m"
    reset = "\033[0m"
    print(f"[{color}{status}{reset}] {test_name}: {message}")


# =============================================================================
# Load Testing
# =============================================================================

def test_load(config: ClusterConfig, num_entries: int = 100000) -> bool:
    """
    Load test: Insert 100k+ entries and verify consistency.
    """
    print(f"\n{'='*60}")
    print(f"LOAD TEST: {num_entries:,} entries")
    print(f"{'='*60}")

    leader_url, _ = find_leader(config)
    if not leader_url:
        print_result("Find Leader", False, "No leader found")
        return False
    print_result("Find Leader", True, leader_url)

    # Phase 1: Insert entries
    print(f"\nPhase 1: Inserting {num_entries:,} entries...")
    success_count = 0
    error_count = 0
    start_time = time.time()

    batch_size = 1000
    for batch in range(0, num_entries, batch_size):
        # Check for leader changes
        current_leader, _ = find_leader(config)
        if current_leader:
            leader_url = current_leader

        for i in range(batch, min(batch + batch_size, num_entries)):
            key = f"load-test-key-{i}"
            value = f"load-test-value-{i}-{'x' * 100}"  # ~120 bytes per entry

            ok, err = put_value(leader_url, key, value)
            if ok:
                success_count += 1
            else:
                error_count += 1
                # Try to find new leader on error
                if "503" in str(err) or "not leader" in str(err).lower():
                    new_leader, _ = find_leader(config)
                    if new_leader:
                        leader_url = new_leader

        # Progress report
        if (batch + batch_size) % 10000 == 0 or batch + batch_size >= num_entries:
            elapsed = time.time() - start_time
            rate = success_count / elapsed if elapsed > 0 else 0
            print(f"  Progress: {batch + batch_size:,}/{num_entries:,} "
                  f"({success_count:,} success, {error_count:,} errors, {rate:.0f}/sec)")

    duration = time.time() - start_time
    throughput = success_count / duration if duration > 0 else 0

    print(f"\nPhase 1 Complete:")
    print(f"  Duration: {duration:.2f}s")
    print(f"  Success: {success_count:,} ({100*success_count/num_entries:.1f}%)")
    print(f"  Errors: {error_count:,}")
    print(f"  Throughput: {throughput:.0f} entries/sec")

    # Phase 2: Verify consistency
    print(f"\nPhase 2: Verifying consistency across nodes...")
    time.sleep(2)  # Allow replication to settle

    sample_size = min(1000, num_entries)
    sample_keys = random.sample(range(num_entries), sample_size)

    consistency_errors = 0
    for node_idx, url in enumerate(config.get_all_urls()):
        node_errors = 0
        for key_idx in sample_keys[:100]:  # Check 100 samples per node
            key = f"load-test-key-{key_idx}"
            expected_value = f"load-test-value-{key_idx}-{'x' * 100}"

            ok, value, err = get_value(url, key)
            if not ok or value != expected_value:
                node_errors += 1

        if node_errors > 0:
            consistency_errors += node_errors
            print(f"  {config.nodes[node_idx]}: {node_errors} inconsistencies")
        else:
            print(f"  {config.nodes[node_idx]}: OK")

    # Results
    passed = success_count >= num_entries * 0.95 and consistency_errors == 0
    print_result("Load Test", passed,
                 f"{success_count:,} entries, {throughput:.0f}/sec, {consistency_errors} inconsistencies")

    return passed


def test_load_concurrent(config: ClusterConfig, num_entries: int = 100000, num_clients: int = 10) -> bool:
    """
    Concurrent load test: Multiple clients inserting simultaneously.
    """
    print(f"\n{'='*60}")
    print(f"CONCURRENT LOAD TEST: {num_entries:,} entries, {num_clients} clients")
    print(f"{'='*60}")

    entries_per_client = num_entries // num_clients
    results = {"success": 0, "error": 0}
    lock = threading.Lock()

    def client_worker(client_id: int):
        local_success = 0
        local_error = 0

        for i in range(entries_per_client):
            leader_url, _ = find_leader(config)
            if not leader_url:
                local_error += 1
                continue

            key = f"concurrent-{client_id}-{i}"
            value = f"value-{client_id}-{i}"

            ok, _ = put_value(leader_url, key, value, timeout=10)
            if ok:
                local_success += 1
            else:
                local_error += 1

        with lock:
            results["success"] += local_success
            results["error"] += local_error

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [executor.submit(client_worker, i) for i in range(num_clients)]
        for future in as_completed(futures):
            future.result()

    duration = time.time() - start_time
    throughput = results["success"] / duration if duration > 0 else 0

    print(f"\nResults:")
    print(f"  Duration: {duration:.2f}s")
    print(f"  Success: {results['success']:,}")
    print(f"  Errors: {results['error']:,}")
    print(f"  Throughput: {throughput:.0f} entries/sec")

    passed = results["success"] >= num_entries * 0.8
    print_result("Concurrent Load Test", passed, f"{throughput:.0f} entries/sec")

    return passed


# =============================================================================
# Consistency Testing
# =============================================================================

def test_consistency(config: ClusterConfig) -> bool:
    """
    Test data consistency across all nodes.
    """
    print(f"\n{'='*60}")
    print("CONSISTENCY TEST")
    print(f"{'='*60}")

    leader_url, _ = find_leader(config)
    if not leader_url:
        print_result("Find Leader", False, "No leader found")
        return False

    # Insert test data
    test_data = {}
    num_entries = 100

    print(f"\nInserting {num_entries} test entries...")
    for i in range(num_entries):
        key = f"consistency-key-{i}"
        value = f"consistency-value-{i}-{random.randint(1000, 9999)}"
        test_data[key] = value

        ok, err = put_value(leader_url, key, value)
        if not ok:
            print(f"  Failed to insert {key}: {err}")

    # Wait for replication
    print("Waiting for replication...")
    time.sleep(3)

    # Verify all nodes have same data
    print("\nVerifying consistency across nodes...")
    all_consistent = True

    for node_idx, url in enumerate(config.get_all_urls()):
        node_name = config.nodes[node_idx]
        errors = []

        for key, expected_value in test_data.items():
            ok, value, err = get_value(url, key)
            if not ok:
                errors.append(f"{key}: read error - {err}")
            elif value != expected_value:
                errors.append(f"{key}: expected '{expected_value}', got '{value}'")

        if errors:
            all_consistent = False
            print(f"  {node_name}: {len(errors)} errors")
            for err in errors[:5]:  # Show first 5 errors
                print(f"    - {err}")
        else:
            print(f"  {node_name}: OK ({num_entries} entries verified)")

    # Check commit indices
    print("\nChecking commit indices...")
    commit_indices = []
    for node_idx, url in enumerate(config.get_all_urls()):
        metrics = get_metrics(url)
        if metrics and "raft_commit_index" in metrics:
            idx = int(metrics["raft_commit_index"])
            commit_indices.append((config.nodes[node_idx], idx))

    if commit_indices:
        max_idx = max(idx for _, idx in commit_indices)
        for node, idx in commit_indices:
            lag = max_idx - idx
            status = "OK" if lag == 0 else f"LAG: {lag}"
            print(f"  {node}: commit_index={idx} ({status})")

    print_result("Consistency Test", all_consistent,
                 f"{num_entries} entries verified across {len(config.nodes)} nodes")

    return all_consistent


# =============================================================================
# Chaos Testing
# =============================================================================

def test_chaos(config: ClusterConfig, duration: int = 60) -> bool:
    """
    Chaos test: Random node failures while maintaining operations.
    """
    print(f"\n{'='*60}")
    print(f"CHAOS TEST: {duration}s duration")
    print(f"{'='*60}")

    results = {
        "put_success": 0,
        "put_error": 0,
        "node_stops": 0,
        "node_starts": 0,
        "leader_changes": 0,
    }
    stopped_nodes = set()
    lock = threading.Lock()
    running = True
    last_leader = None

    def operation_worker():
        nonlocal last_leader
        entry_num = 0

        while running:
            leader_url, leader_idx = find_leader(config)

            if leader_url:
                if last_leader and leader_url != last_leader:
                    with lock:
                        results["leader_changes"] += 1
                last_leader = leader_url

                key = f"chaos-key-{entry_num}"
                value = f"chaos-value-{entry_num}"

                ok, _ = put_value(leader_url, key, value, timeout=3)
                with lock:
                    if ok:
                        results["put_success"] += 1
                    else:
                        results["put_error"] += 1

                entry_num += 1
            else:
                with lock:
                    results["put_error"] += 1

            time.sleep(0.05)  # 20 ops/sec target

    def chaos_worker():
        while running:
            time.sleep(random.uniform(2, 5))

            if not running:
                break

            running_count = len(config.nodes) - len(stopped_nodes)

            # Ensure majority (3 of 5)
            if running_count > 3 and random.random() < 0.4:
                # Stop a random running node
                available = [n for n in config.nodes if n not in stopped_nodes]
                if available:
                    node = random.choice(available)
                    print(f"  [CHAOS] Stopping {node}")
                    if docker_stop_node(node):
                        stopped_nodes.add(node)
                        with lock:
                            results["node_stops"] += 1

            elif stopped_nodes and random.random() < 0.5:
                # Restart a random stopped node
                node = random.choice(list(stopped_nodes))
                print(f"  [CHAOS] Starting {node}")
                if docker_start_node(node):
                    stopped_nodes.discard(node)
                    with lock:
                        results["node_starts"] += 1

    # Start workers
    print("\nStarting chaos test...")
    op_thread = threading.Thread(target=operation_worker)
    chaos_thread = threading.Thread(target=chaos_worker)

    op_thread.start()
    chaos_thread.start()

    # Run for specified duration
    time.sleep(duration)
    running = False

    op_thread.join()
    chaos_thread.join()

    # Restart any stopped nodes
    print("\nRestarting stopped nodes...")
    for node in list(stopped_nodes):
        docker_start_node(node)
        print(f"  Restarted {node}")

    # Wait for cluster to stabilize
    time.sleep(5)

    # Results
    total_ops = results["put_success"] + results["put_error"]
    success_rate = 100 * results["put_success"] / total_ops if total_ops > 0 else 0

    print(f"\nResults:")
    print(f"  Operations: {total_ops:,} ({results['put_success']:,} success, {results['put_error']:,} error)")
    print(f"  Success Rate: {success_rate:.1f}%")
    print(f"  Node Stops: {results['node_stops']}")
    print(f"  Node Starts: {results['node_starts']}")
    print(f"  Leader Changes: {results['leader_changes']}")

    passed = success_rate >= 50
    print_result("Chaos Test", passed, f"{success_rate:.1f}% success rate")

    return passed


# =============================================================================
# Monkey Testing
# =============================================================================

def test_monkey(config: ClusterConfig, duration: int = 60) -> bool:
    """
    Monkey test: Completely random operations including reads, writes, deletes.
    """
    print(f"\n{'='*60}")
    print(f"MONKEY TEST: {duration}s duration")
    print(f"{'='*60}")

    results = {
        "put": 0,
        "get": 0,
        "delete": 0,
        "put_error": 0,
        "get_error": 0,
        "delete_error": 0,
        "node_ops": 0,
    }
    keys_written = set()
    stopped_nodes = set()
    lock = threading.Lock()
    running = True

    def monkey_worker():
        while running:
            action = random.random()

            leader_url, _ = find_leader(config)
            if not leader_url:
                time.sleep(0.1)
                continue

            if action < 0.5:  # 50% PUT
                key = f"monkey-key-{random.randint(1, 10000)}"
                value = f"monkey-value-{random.randint(1, 999999)}"
                ok, _ = put_value(leader_url, key, value, timeout=2)
                with lock:
                    if ok:
                        results["put"] += 1
                        keys_written.add(key)
                    else:
                        results["put_error"] += 1

            elif action < 0.8:  # 30% GET
                if keys_written:
                    key = random.choice(list(keys_written))
                else:
                    key = f"monkey-key-{random.randint(1, 100)}"

                ok, _, _ = get_value(leader_url, key, timeout=2)
                with lock:
                    if ok:
                        results["get"] += 1
                    else:
                        results["get_error"] += 1

            else:  # 20% DELETE
                if keys_written:
                    key = random.choice(list(keys_written))
                    ok, _ = delete_value(leader_url, key, timeout=2)
                    with lock:
                        if ok:
                            results["delete"] += 1
                            keys_written.discard(key)
                        else:
                            results["delete_error"] += 1

            time.sleep(random.uniform(0.01, 0.05))

    def node_chaos_worker():
        while running:
            time.sleep(random.uniform(3, 8))

            if not running:
                break

            running_count = len(config.nodes) - len(stopped_nodes)

            if running_count > 3 and random.random() < 0.3:
                available = [n for n in config.nodes if n not in stopped_nodes]
                if available:
                    node = random.choice(available)
                    print(f"  [MONKEY] Stopping {node}")
                    if docker_stop_node(node):
                        stopped_nodes.add(node)
                        with lock:
                            results["node_ops"] += 1

            elif stopped_nodes and random.random() < 0.4:
                node = random.choice(list(stopped_nodes))
                print(f"  [MONKEY] Starting {node}")
                if docker_start_node(node):
                    stopped_nodes.discard(node)
                    with lock:
                        results["node_ops"] += 1

    # Start workers
    print("\nStarting monkey test...")
    workers = [threading.Thread(target=monkey_worker) for _ in range(5)]
    chaos_thread = threading.Thread(target=node_chaos_worker)

    for w in workers:
        w.start()
    chaos_thread.start()

    time.sleep(duration)
    running = False

    for w in workers:
        w.join()
    chaos_thread.join()

    # Restart stopped nodes
    for node in list(stopped_nodes):
        docker_start_node(node)

    time.sleep(3)

    # Results
    total_success = results["put"] + results["get"] + results["delete"]
    total_error = results["put_error"] + results["get_error"] + results["delete_error"]
    total = total_success + total_error
    success_rate = 100 * total_success / total if total > 0 else 0

    print(f"\nResults:")
    print(f"  PUT: {results['put']:,} success, {results['put_error']:,} error")
    print(f"  GET: {results['get']:,} success, {results['get_error']:,} error")
    print(f"  DELETE: {results['delete']:,} success, {results['delete_error']:,} error")
    print(f"  Node Operations: {results['node_ops']}")
    print(f"  Total: {total:,} operations, {success_rate:.1f}% success")

    passed = success_rate >= 40
    print_result("Monkey Test", passed, f"{total:,} operations, {success_rate:.1f}% success")

    return passed


# =============================================================================
# Leader Resilience Test
# =============================================================================

def test_leader_resilience(config: ClusterConfig) -> bool:
    """
    Test cluster resilience by repeatedly killing leaders.
    """
    print(f"\n{'='*60}")
    print("LEADER RESILIENCE TEST")
    print(f"{'='*60}")

    num_kills = 5
    killed_leaders = []
    successful_writes = 0

    for i in range(num_kills):
        print(f"\nIteration {i+1}/{num_kills}")

        # Find current leader
        leader_url, leader_idx = find_leader(config)
        if not leader_url:
            print("  No leader found, waiting...")
            time.sleep(3)
            continue

        leader_name = config.nodes[leader_idx]
        print(f"  Current leader: {leader_name}")

        # Write some data
        key = f"leader-test-{i}"
        value = f"value-{i}"
        ok, _ = put_value(leader_url, key, value)
        if ok:
            successful_writes += 1
            print(f"  Write succeeded: {key}")

        # Kill the leader
        print(f"  Killing leader: {leader_name}")
        docker_stop_node(leader_name)
        killed_leaders.append(leader_name)

        # Wait for new leader
        time.sleep(3)

        new_leader_url, new_leader_idx = find_leader(config)
        if new_leader_url:
            print(f"  New leader elected: {config.nodes[new_leader_idx]}")
        else:
            print("  WARNING: No new leader elected")

        # Check if we lost majority
        running = len(config.nodes) - len(killed_leaders)
        if running < 3:
            print("  Stopping test: lost majority")
            break

    # Restart killed leaders
    print("\nRestarting killed leaders...")
    for node in killed_leaders:
        docker_start_node(node)
        print(f"  Restarted {node}")

    time.sleep(5)

    # Verify final state
    leader_url, _ = find_leader(config)

    passed = leader_url is not None and successful_writes >= num_kills - 1
    print_result("Leader Resilience Test", passed,
                 f"{successful_writes}/{num_kills} writes successful")

    return passed


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Cluster Testing Suite")
    parser.add_argument("test", nargs="?", default="all",
                       choices=["load", "load_concurrent", "consistency",
                               "chaos", "monkey", "leader", "all"],
                       help="Test to run")
    parser.add_argument("--entries", type=int, default=100000,
                       help="Number of entries for load test")
    parser.add_argument("--duration", type=int, default=60,
                       help="Duration in seconds for chaos/monkey tests")
    parser.add_argument("--base-port", type=int, default=8081,
                       help="Base HTTP port")

    args = parser.parse_args()

    config = ClusterConfig(
        nodes=["node1", "node2", "node3", "node4", "node5"],
        base_http_port=args.base_port
    )

    print("="*60)
    print("RAFT CLUSTER TESTING SUITE")
    print("="*60)

    # Check cluster is running
    leader_url, _ = find_leader(config)
    if not leader_url:
        print("\nERROR: No leader found. Is the cluster running?")
        print("Start the cluster with: docker-compose -f deployments/docker/docker-compose.yml up -d")
        sys.exit(1)

    print(f"\nCluster is running. Leader: {leader_url}")

    results = {}

    if args.test in ["load", "all"]:
        results["load"] = test_load(config, args.entries)

    if args.test in ["load_concurrent", "all"]:
        results["load_concurrent"] = test_load_concurrent(config, args.entries // 10, 10)

    if args.test in ["consistency", "all"]:
        results["consistency"] = test_consistency(config)

    if args.test in ["chaos", "all"]:
        results["chaos"] = test_chaos(config, args.duration)

    if args.test in ["monkey", "all"]:
        results["monkey"] = test_monkey(config, args.duration)

    if args.test in ["leader", "all"]:
        results["leader"] = test_leader_resilience(config)

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    all_passed = True
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        color = "\033[92m" if passed else "\033[91m"
        reset = "\033[0m"
        print(f"  {test_name}: [{color}{status}{reset}]")
        if not passed:
            all_passed = False

    print("\n" + "="*60)
    if all_passed:
        print("\033[92mALL TESTS PASSED\033[0m")
    else:
        print("\033[91mSOME TESTS FAILED\033[0m")
    print("="*60)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
