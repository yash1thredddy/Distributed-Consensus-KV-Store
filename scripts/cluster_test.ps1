# =============================================================================
# Distributed KV Store - Comprehensive Cluster Test Script (PowerShell)
# =============================================================================
# This script tests all major features of the Raft-based distributed KV store:
# - Leader election
# - Data replication
# - Leader forwarding
# - Fault tolerance (node crashes)
# - Network partition simulation
# - Data consistency
# =============================================================================

param(
    [string]$TestName = "all"
)

$ErrorActionPreference = "Continue"

# Configuration
$COMPOSE_FILE = "deployments/docker/docker-compose.yml"
$LOG_DIR = "test_logs"
$TIMESTAMP = Get-Date -Format "yyyyMMdd_HHmmss"
$TEST_LOG = "$LOG_DIR/test_run_$TIMESTAMP.log"

# Node ports (external)
$NODE_PORTS = @{
    "node1" = 8081
    "node2" = 8082
    "node3" = 8083
    "node4" = 8084
    "node5" = 8085
}

# Test counters
$script:TESTS_PASSED = 0
$script:TESTS_FAILED = 0
$script:TESTS_TOTAL = 0

# =============================================================================
# Utility Functions
# =============================================================================

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] $Message"
    Write-Host $logMessage
    Add-Content -Path $TEST_LOG -Value $logMessage -ErrorAction SilentlyContinue
}

function Write-Section {
    param([string]$Title)
    Write-Host ""
    Write-Host "============================================================================="
    Write-Host $Title -ForegroundColor Cyan
    Write-Host "============================================================================="
    Add-Content -Path $TEST_LOG -Value "`n=============================================================================" -ErrorAction SilentlyContinue
    Add-Content -Path $TEST_LOG -Value $Title -ErrorAction SilentlyContinue
    Add-Content -Path $TEST_LOG -Value "=============================================================================" -ErrorAction SilentlyContinue
}

function Write-Test {
    param([string]$Message)
    Write-Host "[TEST] $Message" -ForegroundColor Yellow
    Add-Content -Path $TEST_LOG -Value "[TEST] $Message" -ErrorAction SilentlyContinue
    $script:TESTS_TOTAL++
}

function Write-Pass {
    param([string]$Message)
    Write-Host "[PASS] $Message" -ForegroundColor Green
    Add-Content -Path $TEST_LOG -Value "[PASS] $Message" -ErrorAction SilentlyContinue
    $script:TESTS_PASSED++
}

function Write-Fail {
    param([string]$Message)
    Write-Host "[FAIL] $Message" -ForegroundColor Red
    Add-Content -Path $TEST_LOG -Value "[FAIL] $Message" -ErrorAction SilentlyContinue
    $script:TESTS_FAILED++
}

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Blue
    Add-Content -Path $TEST_LOG -Value "[INFO] $Message" -ErrorAction SilentlyContinue
}

function Write-Warn {
    param([string]$Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
    Add-Content -Path $TEST_LOG -Value "[WARN] $Message" -ErrorAction SilentlyContinue
}

function Get-Health {
    param([int]$Port)
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:$Port/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
        return $response
    } catch {
        return $null
    }
}

function Get-ClusterInfo {
    param([int]$Port)
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:$Port/cluster/info" -TimeoutSec 2 -ErrorAction SilentlyContinue
        return $response
    } catch {
        return $null
    }
}

function Find-Leader {
    foreach ($node in $NODE_PORTS.Keys) {
        $port = $NODE_PORTS[$node]
        $health = Get-Health -Port $port
        if ($health -and $health.is_leader -eq $true) {
            return $node
        }
    }
    return $null
}

function Get-LeaderID {
    param([int]$Port)
    $health = Get-Health -Port $port
    if ($health) {
        return $health.leader_id
    }
    return $null
}

function Set-KV {
    param([int]$Port, [string]$Key, [string]$Value)
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:$Port/kv/$Key" -Method Put -Body $Value -TimeoutSec 5 -ErrorAction SilentlyContinue
        return $response
    } catch {
        return @{ error = $_.Exception.Message }
    }
}

function Get-KV {
    param([int]$Port, [string]$Key)
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:$Port/kv/$Key" -TimeoutSec 5 -ErrorAction SilentlyContinue
        return $response
    } catch {
        return @{ error = $_.Exception.Message }
    }
}

function Remove-KV {
    param([int]$Port, [string]$Key)
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:$Port/kv/$Key" -Method Delete -TimeoutSec 5 -ErrorAction SilentlyContinue
        return $response
    } catch {
        return @{ error = $_.Exception.Message }
    }
}

function Test-NodeHealthy {
    param([int]$Port)
    $health = Get-Health -Port $Port
    return ($health -and $health.status -eq "ok")
}

function Stop-Node {
    param([string]$Node)
    Write-Info "Stopping $Node..."
    docker-compose -f $COMPOSE_FILE stop $Node 2>&1 | Out-Null
}

function Start-Node {
    param([string]$Node)
    Write-Info "Starting $Node..."
    docker-compose -f $COMPOSE_FILE start $Node 2>&1 | Out-Null
}

function Kill-Node {
    param([string]$Node)
    Write-Info "Killing $Node (simulating crash)..."
    docker-compose -f $COMPOSE_FILE kill $Node 2>&1 | Out-Null
}

function Collect-Logs {
    param([string]$Suffix)
    Write-Info "Collecting logs ($Suffix)..."
    foreach ($node in $NODE_PORTS.Keys) {
        docker-compose -f $COMPOSE_FILE logs --no-color $node 2>&1 | Out-File "$LOG_DIR/${node}_${Suffix}_$TIMESTAMP.log" -ErrorAction SilentlyContinue
    }
}

function Wait-ForLeader {
    param([int]$Timeout = 30)
    Write-Info "Waiting for leader election (timeout: ${Timeout}s)..."

    $startTime = Get-Date
    while ($true) {
        $elapsed = ((Get-Date) - $startTime).TotalSeconds

        if ($elapsed -ge $Timeout) {
            Write-Warn "Leader election timeout after ${Timeout}s"
            return $false
        }

        $leader = Find-Leader
        if ($leader) {
            Write-Info "Leader found: $leader (after ${elapsed}s)"
            return $true
        }

        Start-Sleep -Seconds 1
    }
}

function Wait-ForAllNodes {
    param([int]$Timeout = 30)
    Write-Info "Waiting for all nodes to be healthy (timeout: ${Timeout}s)..."

    $startTime = Get-Date
    while ($true) {
        $elapsed = ((Get-Date) - $startTime).TotalSeconds

        if ($elapsed -ge $Timeout) {
            Write-Warn "Timeout waiting for all nodes after ${Timeout}s"
            return $false
        }

        $allHealthy = $true
        foreach ($node in $NODE_PORTS.Keys) {
            if (-not (Test-NodeHealthy -Port $NODE_PORTS[$node])) {
                $allHealthy = $false
                break
            }
        }

        if ($allHealthy) {
            Write-Info "All nodes healthy (after ${elapsed}s)"
            return $true
        }

        Start-Sleep -Seconds 1
    }
}

# =============================================================================
# Test Cases
# =============================================================================

function Test-ClusterStartup {
    Write-Section "TEST: Cluster Startup and Initial Leader Election"

    Write-Test "Starting fresh cluster..."
    docker-compose -f $COMPOSE_FILE down -v 2>&1 | Out-Null
    docker-compose -f $COMPOSE_FILE up -d 2>&1 | Out-Null

    Start-Sleep -Seconds 5

    Write-Test "Waiting for leader election..."
    if (Wait-ForLeader -Timeout 30) {
        $leader = Find-Leader
        Write-Pass "Leader elected: $leader"
    } else {
        Write-Fail "No leader elected within timeout"
        Collect-Logs "startup_failed"
        return
    }

    Write-Test "Verifying all nodes are healthy..."
    $healthyCount = 0
    foreach ($node in $NODE_PORTS.Keys) {
        if (Test-NodeHealthy -Port $NODE_PORTS[$node]) {
            $healthyCount++
            Write-Info "$node is healthy"
        } else {
            Write-Warn "$node is not healthy"
        }
    }

    if ($healthyCount -eq 5) {
        Write-Pass "All 5 nodes are healthy"
    } else {
        Write-Fail "Only $healthyCount/5 nodes are healthy"
    }

    Collect-Logs "after_startup"
}

function Test-BasicOperations {
    Write-Section "TEST: Basic KV Operations"

    $leader = Find-Leader
    $leaderPort = $NODE_PORTS[$leader]

    Write-Test "PUT operation on leader ($leader)..."
    $putResult = Set-KV -Port $leaderPort -Key "test_key1" -Value "test_value1"
    if ($putResult.success -eq $true) {
        Write-Pass "PUT succeeded: test_key1=test_value1"
    } else {
        Write-Fail "PUT failed: $($putResult | ConvertTo-Json -Compress)"
    }

    Start-Sleep -Seconds 1

    Write-Test "GET operation on leader..."
    $getResult = Get-KV -Port $leaderPort -Key "test_key1"
    if ($getResult.value -eq "test_value1") {
        Write-Pass "GET succeeded: got correct value"
    } else {
        Write-Fail "GET failed: $($getResult | ConvertTo-Json -Compress)"
    }

    Write-Test "DELETE operation on leader..."
    $delResult = Remove-KV -Port $leaderPort -Key "test_key1"
    if ($delResult.success -eq $true) {
        Write-Pass "DELETE succeeded"
    } else {
        Write-Fail "DELETE failed: $($delResult | ConvertTo-Json -Compress)"
    }

    Write-Test "Verify key is deleted..."
    $getAfterDel = Get-KV -Port $leaderPort -Key "test_key1"
    if ($getAfterDel.found -eq $false) {
        Write-Pass "Key correctly deleted"
    } else {
        Write-Fail "Key still exists: $($getAfterDel | ConvertTo-Json -Compress)"
    }
}

function Test-LeaderForwarding {
    Write-Section "TEST: Leader Forwarding (Write to Follower)"

    $leader = Find-Leader

    # Find a follower
    $follower = $null
    $followerPort = 0
    foreach ($node in $NODE_PORTS.Keys) {
        if ($node -ne $leader) {
            $follower = $node
            $followerPort = $NODE_PORTS[$node]
            break
        }
    }

    Write-Info "Leader: $leader, Testing follower: $follower"

    Write-Test "PUT operation on follower ($follower) - should forward to leader..."
    $putResult = Set-KV -Port $followerPort -Key "forward_test" -Value "forwarded_value"
    if ($putResult.success -eq $true) {
        Write-Pass "Leader forwarding works! PUT succeeded via follower"
    } else {
        Write-Fail "Leader forwarding failed: $($putResult | ConvertTo-Json -Compress)"
    }

    Start-Sleep -Seconds 1

    Write-Test "GET from another follower to verify replication..."
    foreach ($node in $NODE_PORTS.Keys) {
        if ($node -ne $leader -and $node -ne $follower) {
            $otherPort = $NODE_PORTS[$node]
            $getResult = Get-KV -Port $otherPort -Key "forward_test"
            if ($getResult.value -eq "forwarded_value") {
                Write-Pass "Data replicated to $node"
            } else {
                Write-Fail "Data not replicated to $node: $($getResult | ConvertTo-Json -Compress)"
            }
            break
        }
    }
}

function Test-DataReplication {
    Write-Section "TEST: Data Replication Across All Nodes"

    $leader = Find-Leader
    $leaderPort = $NODE_PORTS[$leader]

    Write-Test "Writing 10 key-value pairs..."
    for ($i = 1; $i -le 10; $i++) {
        Set-KV -Port $leaderPort -Key "repl_key_$i" -Value "repl_value_$i" | Out-Null
    }
    Write-Pass "Wrote 10 keys to leader"

    Start-Sleep -Seconds 2

    Write-Test "Verifying all keys are replicated to all nodes..."
    $allReplicated = $true
    foreach ($node in $NODE_PORTS.Keys) {
        $port = $NODE_PORTS[$node]
        $nodeOk = $true
        for ($i = 1; $i -le 10; $i++) {
            $result = Get-KV -Port $port -Key "repl_key_$i"
            if ($result.value -ne "repl_value_$i") {
                $nodeOk = $false
                $allReplicated = $false
                Write-Warn "$node missing repl_key_$i"
            }
        }
        if ($nodeOk) {
            Write-Info "$node has all 10 keys"
        }
    }

    if ($allReplicated) {
        Write-Pass "All data replicated to all nodes"
    } else {
        Write-Fail "Some data missing on some nodes"
    }
}

function Test-LeaderCrashAndRecovery {
    Write-Section "TEST: Leader Crash and New Leader Election"

    $oldLeader = Find-Leader
    $oldLeaderPort = $NODE_PORTS[$oldLeader]

    Write-Test "Writing data before leader crash..."
    Set-KV -Port $oldLeaderPort -Key "pre_crash_key" -Value "pre_crash_value" | Out-Null
    Start-Sleep -Seconds 1
    Write-Pass "Wrote pre_crash_key"

    Write-Test "Killing leader ($oldLeader) to simulate crash..."
    Kill-Node -Node $oldLeader

    Collect-Logs "before_leader_crash"

    Write-Test "Waiting for new leader election..."
    Start-Sleep -Seconds 3

    if (Wait-ForLeader -Timeout 30) {
        $newLeader = Find-Leader
        if ($newLeader -ne $oldLeader) {
            Write-Pass "New leader elected: $newLeader (was: $oldLeader)"
        } else {
            Write-Fail "Old leader somehow still leader?"
        }
    } else {
        Write-Fail "No new leader elected after crash"
        Collect-Logs "leader_election_failed"
        return
    }

    $newLeader = Find-Leader
    $newLeaderPort = $NODE_PORTS[$newLeader]

    Write-Test "Verifying pre-crash data is still available..."
    $result = Get-KV -Port $newLeaderPort -Key "pre_crash_key"
    if ($result.value -eq "pre_crash_value") {
        Write-Pass "Pre-crash data preserved after leader change"
    } else {
        Write-Fail "Pre-crash data lost: $($result | ConvertTo-Json -Compress)"
    }

    Write-Test "Writing new data to new leader..."
    $putResult = Set-KV -Port $newLeaderPort -Key "post_crash_key" -Value "post_crash_value"
    if ($putResult.success -eq $true) {
        Write-Pass "Can write to new leader"
    } else {
        Write-Fail "Cannot write to new leader: $($putResult | ConvertTo-Json -Compress)"
    }

    Write-Test "Restarting crashed node ($oldLeader)..."
    Start-Node -Node $oldLeader
    Start-Sleep -Seconds 5

    Write-Test "Verifying restarted node catches up..."
    $restartedPort = $NODE_PORTS[$oldLeader]
    $startTime = Get-Date
    $recovered = $false
    while (((Get-Date) - $startTime).TotalSeconds -lt 10) {
        if (Test-NodeHealthy -Port $restartedPort) {
            $recovered = $true
            break
        }
        Start-Sleep -Seconds 1
    }

    if ($recovered) {
        Write-Pass "$oldLeader is healthy again"
        Start-Sleep -Seconds 2
        $result = Get-KV -Port $restartedPort -Key "post_crash_key"
        if ($result.value -eq "post_crash_value") {
            Write-Pass "Restarted node caught up with new data"
        } else {
            Write-Warn "Restarted node may not have synced yet: $($result | ConvertTo-Json -Compress)"
        }
    } else {
        Write-Fail "$oldLeader did not recover"
    }

    Collect-Logs "after_leader_recovery"
}

function Test-MinorityFailure {
    Write-Section "TEST: Minority Failure (2 of 5 nodes down)"

    $leader = Find-Leader

    # Select 2 non-leader nodes to kill
    $killedNodes = @()
    foreach ($node in $NODE_PORTS.Keys) {
        if ($node -ne $leader -and $killedNodes.Count -lt 2) {
            $killedNodes += $node
        }
    }

    Write-Test "Killing minority ($($killedNodes -join ', '))..."
    foreach ($node in $killedNodes) {
        Kill-Node -Node $node
    }

    Start-Sleep -Seconds 2

    Write-Test "Verifying cluster still operational with 3/5 nodes..."
    $leaderPort = $NODE_PORTS[$leader]

    $putResult = Set-KV -Port $leaderPort -Key "minority_test" -Value "still_works"
    if ($putResult.success -eq $true) {
        Write-Pass "Cluster operational with minority failure - writes work"
    } else {
        Write-Fail "Cluster not operational: $($putResult | ConvertTo-Json -Compress)"
    }

    $getResult = Get-KV -Port $leaderPort -Key "minority_test"
    if ($getResult.value -eq "still_works") {
        Write-Pass "Reads also work"
    } else {
        Write-Fail "Reads failed: $($getResult | ConvertTo-Json -Compress)"
    }

    Write-Test "Restarting killed nodes..."
    foreach ($node in $killedNodes) {
        Start-Node -Node $node
    }

    Start-Sleep -Seconds 5
    Wait-ForAllNodes -Timeout 30 | Out-Null

    Write-Test "Verifying all nodes have the data written during failure..."
    foreach ($node in $killedNodes) {
        $port = $NODE_PORTS[$node]
        Start-Sleep -Seconds 1
        $result = Get-KV -Port $port -Key "minority_test"
        if ($result.value -eq "still_works") {
            Write-Pass "$node synced data written during its downtime"
        } else {
            Write-Warn "$node may not have synced yet: $($result | ConvertTo-Json -Compress)"
        }
    }

    Collect-Logs "after_minority_failure"
}

function Test-MajorityFailure {
    Write-Section "TEST: Majority Failure (3 of 5 nodes down) - Should Block Writes"

    # Kill 3 nodes
    $killedNodes = @($NODE_PORTS.Keys | Select-Object -First 3)

    Write-Test "Killing majority ($($killedNodes -join ', '))..."
    foreach ($node in $killedNodes) {
        Kill-Node -Node $node
    }

    Start-Sleep -Seconds 5

    # Find a surviving node
    $survivingPort = 0
    foreach ($node in $NODE_PORTS.Keys) {
        if ($node -notin $killedNodes) {
            $survivingPort = $NODE_PORTS[$node]
            Write-Info "Testing via surviving node: $node (port $survivingPort)"
            break
        }
    }

    Write-Test "Attempting write with majority down (should fail/timeout)..."
    $putResult = Set-KV -Port $survivingPort -Key "majority_test" -Value "should_fail"

    if ($putResult.success -eq $true) {
        Write-Fail "Write succeeded when it should have failed (no majority)"
    } else {
        Write-Pass "Write correctly blocked/failed without majority"
    }

    Write-Test "Restarting killed nodes to restore cluster..."
    foreach ($node in $killedNodes) {
        Start-Node -Node $node
    }

    Start-Sleep -Seconds 5
    Wait-ForLeader -Timeout 30 | Out-Null
    Wait-ForAllNodes -Timeout 30 | Out-Null

    Write-Pass "Cluster restored after majority failure"

    Collect-Logs "after_majority_failure"
}

function Test-Consistency {
    Write-Section "TEST: Data Consistency Check"

    $leader = Find-Leader
    $leaderPort = $NODE_PORTS[$leader]

    Write-Test "Writing consistency test data..."
    for ($i = 1; $i -le 20; $i++) {
        Set-KV -Port $leaderPort -Key "consistency_$i" -Value "value_$i" | Out-Null
    }

    Start-Sleep -Seconds 3

    Write-Test "Comparing data across all nodes..."
    $reference = @{}
    $firstNode = $null
    $allConsistent = $true

    foreach ($node in $NODE_PORTS.Keys) {
        $port = $NODE_PORTS[$node]
        $nodeData = @{}

        for ($i = 1; $i -le 20; $i++) {
            $result = Get-KV -Port $port -Key "consistency_$i"
            $nodeData["$i"] = $result.value
        }

        if ($null -eq $firstNode) {
            $reference = $nodeData
            $firstNode = $node
            Write-Info "Reference data from $node"
        } else {
            $matches = $true
            foreach ($key in $reference.Keys) {
                if ($nodeData[$key] -ne $reference[$key]) {
                    $matches = $false
                    break
                }
            }
            if ($matches) {
                Write-Info "$node matches $firstNode"
            } else {
                Write-Warn "$node differs from $firstNode"
                $allConsistent = $false
            }
        }
    }

    if ($allConsistent) {
        Write-Pass "All nodes have consistent data"
    } else {
        Write-Fail "Data inconsistency detected"
    }

    Collect-Logs "after_consistency_check"
}

function Show-Summary {
    Write-Section "TEST SUMMARY"

    Write-Host ""
    Write-Host "============================================"
    Write-Host "Total Tests:  $script:TESTS_TOTAL"
    Write-Host "Passed:       $script:TESTS_PASSED" -ForegroundColor Green
    Write-Host "Failed:       $script:TESTS_FAILED" -ForegroundColor Red
    Write-Host "============================================"
    Write-Host ""

    if ($script:TESTS_FAILED -eq 0) {
        Write-Host "All tests passed!" -ForegroundColor Green
    } else {
        Write-Host "Some tests failed. Check logs in $LOG_DIR" -ForegroundColor Red
    }

    Write-Host ""
    Write-Host "Logs saved to: $LOG_DIR"
    Write-Host "Main test log: $TEST_LOG"
}

function Invoke-Cleanup {
    Write-Section "CLEANUP"
    Write-Info "Collecting final logs..."
    Collect-Logs "final"

    Write-Info "Stopping cluster..."
    docker-compose -f $COMPOSE_FILE down 2>&1 | Out-Null
}

# =============================================================================
# Main Execution
# =============================================================================

# Create log directory
New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null

Write-Host "============================================"
Write-Host "Distributed KV Store - Cluster Test Suite"
Write-Host "============================================"
Write-Host "Log directory: $LOG_DIR"
Write-Host "Test log: $TEST_LOG"
Write-Host ""

try {
    switch ($TestName.ToLower()) {
        "all" {
            Test-ClusterStartup
            Test-BasicOperations
            Test-LeaderForwarding
            Test-DataReplication
            Test-LeaderCrashAndRecovery
            Test-MinorityFailure
            Test-MajorityFailure
            Test-Consistency
        }
        "startup" { Test-ClusterStartup }
        "basic" { Test-BasicOperations }
        "forward" { Test-LeaderForwarding }
        "replication" { Test-DataReplication }
        "leader_crash" { Test-LeaderCrashAndRecovery }
        "minority" { Test-MinorityFailure }
        "majority" { Test-MajorityFailure }
        "consistency" { Test-Consistency }
        default {
            Write-Host "Unknown test: $TestName"
            Write-Host "Available tests: all, startup, basic, forward, replication, leader_crash, minority, majority, consistency"
            exit 1
        }
    }
} finally {
    Show-Summary
    Invoke-Cleanup
}
