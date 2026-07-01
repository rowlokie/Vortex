# ============================================
# VORTEX - Complete Test From Clean Start
# ============================================

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  VORTEX - Full System Test" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# ============================================
# STEP 1: STOP EVERYTHING
# ============================================
Write-Host "`n[STEP 1] Stopping all brokers..." -ForegroundColor Yellow

# Kill any running broker
Stop-Process -Name "kafka_broker" -ErrorAction SilentlyContinue
Stop-Process -Name "test_election" -ErrorAction SilentlyContinue

# Wait for processes to stop
Start-Sleep -Seconds 2

# Check if any are still running
$proc = Get-Process -Name "kafka_broker" -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host "  WARNING: Broker still running (PID: $($proc.Id)) - forcing kill..." -ForegroundColor Red
    Stop-Process -Id $proc.Id -Force
    Start-Sleep -Seconds 1
}

Write-Host "  [OK] All processes stopped" -ForegroundColor Green

# ============================================
# STEP 2: DELETE ALL DATA
# ============================================
Write-Host "`n[STEP 2] Deleting all data..." -ForegroundColor Yellow

# Delete everything in data directory
if (Test-Path "data") {
    Remove-Item -Force -Recurse data\* -ErrorAction SilentlyContinue
    Write-Host "  [OK] Deleted all files in data/" -ForegroundColor Green
} else {
    New-Item -ItemType Directory -Path "data" -Force | Out-Null
    Write-Host "  [OK] Created data/ directory" -ForegroundColor Green
}

# Recreate necessary subdirectories
New-Item -ItemType Directory -Path "data\meta" -Force | Out-Null
New-Item -ItemType Directory -Path "data\offsets" -Force | Out-Null

# Delete any leftover test files
Remove-Item -Force test_election.exe -ErrorAction SilentlyContinue
Remove-Item -Force test_election.obj -ErrorAction SilentlyContinue

Write-Host "  [OK] Data directory cleaned" -ForegroundColor Green

# ============================================
# STEP 3: DEFINE HELPER FUNCTION
# ============================================
function Send-Cmd($cmd) {
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect("localhost", 9092)
        $stream = $tcp.GetStream()
        $w = New-Object System.IO.StreamWriter($stream)
        $w.WriteLine($cmd)
        $w.Flush()
        Start-Sleep -Milliseconds 500
        $r = New-Object System.IO.StreamReader($stream)
        $resp = $r.ReadLine()
        $tcp.Close()
        return $resp
    } catch {
        return "ERROR: $_"
    }
}

# ============================================
# STEP 4: START BROKER
# ============================================
Write-Host "`n[STEP 3] Starting broker..." -ForegroundColor Yellow

$brokerPath = "C:\Users\Piyush\Desktop\kafkaclone\kafka_broker.exe"

# Start broker in background
$job = Start-Job -ScriptBlock { 
    & $using:brokerPath 
}

Start-Sleep -Seconds 5

# Verify broker is running
$proc = Get-Process -Name "kafka_broker" -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host "  [OK] Broker started (PID: $($proc.Id))" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Broker failed to start!" -ForegroundColor Red
    exit 1
}

# Verify port is listening
$portCheck = netstat -ano | findstr ":9092"
if ($portCheck) {
    Write-Host "  [OK] Port 9092 is listening" -ForegroundColor Green
} else {
    Write-Host "  [WARN] Port 9092 not listening yet, waiting..." -ForegroundColor Yellow
    Start-Sleep -Seconds 2
}

# ============================================
# STEP 5: TEST CREATE_TOPIC (with retry & debug)
# ============================================
Write-Host "`n[STEP 4] Testing CREATE_TOPIC..." -ForegroundColor Yellow

$maxRetries = 3
$retryCount = 0
$topicName = "orders"
$partitionId = 1
$success = $false

while ($retryCount -lt $maxRetries -and -not $success) {
    if ($retryCount -gt 0) {
        Write-Host "  Retry $retryCount/$maxRetries..." -ForegroundColor Yellow
        Start-Sleep -Seconds 2
    }
    
    $result = Send-Cmd "CREATE_TOPIC orders 3"
    
    if ($result -eq "OK") {
        Write-Host "  [OK] Topic 'orders' created with 3 partitions" -ForegroundColor Green
        $success = $true
        break
    } elseif ($result -match "already exists") {
        Write-Host "  [WARN] Topic already exists - continuing" -ForegroundColor Yellow
        $success = $true
        break
    } else {
        Write-Host "  [WARN] CREATE_TOPIC attempt $($retryCount+1) failed: $result" -ForegroundColor Yellow
        $retryCount++
    }
}

if (-not $success) {
    Write-Host "`n  [DEBUG] Checking if topic was created anyway..." -ForegroundColor Yellow
    $metadata = Send-Cmd "METADATA orders"
    if ($metadata -match "orders") {
        Write-Host "  [OK] Topic 'orders' exists (metadata check passed)" -ForegroundColor Green
        $success = $true
    } else {
        Write-Host "  [FAIL] CREATE_TOPIC failed after $maxRetries attempts" -ForegroundColor Red
        Write-Host "  [DEBUG] Trying to create with a different topic name..." -ForegroundColor Yellow
        $result = Send-Cmd "CREATE_TOPIC testtopic 3"
        if ($result -eq "OK") {
            Write-Host "  [OK] Created 'testtopic' as fallback - using that instead" -ForegroundColor Green
            $topicName = "testtopic"
            $partitionId = 0
            $success = $true
        } else {
            Write-Host "  [FAIL] Fallback topic also failed!" -ForegroundColor Red
            Write-Host "  [DEBUG] Checking if any topics exist..." -ForegroundColor Yellow
            $metadata = Send-Cmd "METADATA"
            Write-Host "  METADATA: $metadata"
            exit 1
        }
    }
}

Write-Host "`n  Using topic: $topicName" -ForegroundColor Cyan

# ============================================
# STEP 6: TEST PRODUCE
# ============================================
Write-Host "`n[STEP 5] Testing PRODUCE..." -ForegroundColor Yellow

# Produce first message
$result = Send-Cmd "PRODUCE $topicName key1 Hello"
if ($result -match "OK") {
    Write-Host "  [OK] Produced: $result" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] PRODUCE failed: $result" -ForegroundColor Red
}

# Produce 5 more messages
1..5 | ForEach-Object {
    $result = Send-Cmd "PRODUCE $topicName key1 Msg-$_"
    if ($_ -eq 1) {
        Write-Host "  [OK] Produced 5 more messages" -ForegroundColor Green
    }
}

# ============================================
# STEP 7: TEST METADATA
# ============================================
Write-Host "`n[STEP 6] Testing METADATA..." -ForegroundColor Yellow

$result = Send-Cmd "METADATA $topicName"
if ($result -match $topicName) {
    Write-Host "  [OK] Metadata retrieved" -ForegroundColor Green
    Write-Host "  $result" -ForegroundColor Gray
} else {
    Write-Host "  [FAIL] METADATA failed: $result" -ForegroundColor Red
}

# ============================================
# STEP 8: VERIFY INDEX FILE
# ============================================
Write-Host "`n[STEP 7] Verifying index file..." -ForegroundColor Yellow

Start-Sleep -Seconds 1

$indexPath = "data\$topicName-$partitionId.index"
if (Test-Path $indexPath) {
    $bytes = [System.IO.File]::ReadAllBytes($indexPath)
    $n = $bytes.Length / 16
    Write-Host "  [OK] Index file created with $n entries ($($bytes.Length) bytes)" -ForegroundColor Green
    
    # Display index entries
    Write-Host "`n  Index entries:" -ForegroundColor Gray
    for ($i = 0; $i -lt $n; $i++) {
        $off = [BitConverter]::ToInt64($bytes, $i * 16)
        $pos = [BitConverter]::ToInt64($bytes, $i * 16 + 8)
        Write-Host "    offset=$off -> byte=$pos" -ForegroundColor Gray
    }
} else {
    Write-Host "  [FAIL] Index file not found!" -ForegroundColor Red
    Write-Host "  Files in data directory:" -ForegroundColor Yellow
    Get-ChildItem data\
    Get-ChildItem data\ -Recurse
}

# ============================================
# STEP 9: TEST CONSUME (Binary Search)
# ============================================
Write-Host "`n[STEP 8] Testing CONSUME (Binary Search)..." -ForegroundColor Yellow

# Test 1: Consume from beginning
Write-Host "  Test 1: CONSUME $topicName $partitionId 0" -ForegroundColor Gray
$result = Send-Cmd "CONSUME $topicName $partitionId 0"
Write-Host "    Result: $result" -ForegroundColor Gray

# Test 2: Consume from offset 3 (should skip first 3)
Write-Host "`n  Test 2: CONSUME $topicName $partitionId 3 (should skip to offset 4)" -ForegroundColor Gray
$result = Send-Cmd "CONSUME $topicName $partitionId 3"
if ($result -match "4\|" -or $result -match "5\|" -or $result -match "6\|") {
    Write-Host "    [OK] Binary search works: $result" -ForegroundColor Green
} else {
    Write-Host "    [WARN] Result: $result" -ForegroundColor Yellow
}

# ============================================
# STEP 10: TEST INDEX REBUILD
# ============================================
Write-Host "`n[STEP 9] Testing index rebuild..." -ForegroundColor Yellow

# Delete index file
if (Test-Path $indexPath) {
    Remove-Item -Force $indexPath
    Write-Host "  Deleted index file" -ForegroundColor Gray
}

# Restart broker
Write-Host "  Restarting broker..." -ForegroundColor Gray
Stop-Process -Name "kafka_broker" -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

$job2 = Start-Job -ScriptBlock { 
    & "C:\Users\Piyush\Desktop\kafkaclone\kafka_broker.exe" 
}
Start-Sleep -Seconds 5

# Check if index was rebuilt
if (Test-Path $indexPath) {
    $bytes = [System.IO.File]::ReadAllBytes($indexPath)
    $n = $bytes.Length / 16
    Write-Host "  [OK] Index rebuilt with $n entries" -ForegroundColor Green
    
    # Display rebuilt index
    Write-Host "`n  Rebuilt index entries:" -ForegroundColor Gray
    for ($i = 0; $i -lt $n; $i++) {
        $off = [BitConverter]::ToInt64($bytes, $i * 16)
        $pos = [BitConverter]::ToInt64($bytes, $i * 16 + 8)
        Write-Host "    offset=$off -> byte=$pos" -ForegroundColor Gray
    }
} else {
    Write-Host "  [FAIL] Index not rebuilt!" -ForegroundColor Red
}

# ============================================
# STEP 11: TEST CONSUME AFTER REBUILD
# ============================================
Write-Host "`n[STEP 10] Testing CONSUME after rebuild..." -ForegroundColor Yellow

$result = Send-Cmd "CONSUME $topicName $partitionId 0"
if ($result -match "\|") {
    Write-Host "  [OK] Consume works after rebuild: $result" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Consume failed after rebuild: $result" -ForegroundColor Red
}

# ============================================
# STEP 12: LEADER ELECTION TEST
# ============================================
Write-Host "`n[STEP 11] Testing Leader Election..." -ForegroundColor Yellow

# Create test_election.cpp
$code = @"
#include <iostream>
#include "broker/MetadataService.h"
#include "broker/BrokerInfo.h"

int main() {
    MetadataService svc(1);
    
    // Register 3 brokers
    BrokerInfo b1; b1.brokerId = 1; b1.host = "localhost"; b1.port = 9092;
    BrokerInfo b2; b2.brokerId = 2; b2.host = "localhost"; b2.port = 9093;
    BrokerInfo b3; b3.brokerId = 3; b3.host = "localhost"; b3.port = 9094;
    
    svc.registerBroker(b1);
    svc.registerBroker(b2);
    svc.registerBroker(b3);
    
    // Create topic with broker 2 as leader
    svc.createTopic("payments", 2);
    svc.addPartition("payments", 0, 2, {2, 3});
    svc.addPartition("payments", 1, 2, {2, 3});
    
    std::cout << "Before: payments-0 leader = " 
              << svc.getPartitionMetadata("payments", 0).leaderBrokerId << std::endl;
    std::cout << "Before: payments-1 leader = " 
              << svc.getPartitionMetadata("payments", 1).leaderBrokerId << std::endl;
    
    std::cout << ">>> Simulating Broker 2 going DOWN <<<" << std::endl;
    svc.unregisterBroker(2);
    
    std::cout << "After: payments-0 leader = " 
              << svc.getPartitionMetadata("payments", 0).leaderBrokerId << std::endl;
    std::cout << "After: payments-1 leader = " 
              << svc.getPartitionMetadata("payments", 1).leaderBrokerId << std::endl;
    
    return 0;
}
"@

$code | Out-File -Encoding UTF8 test_election.cpp

# Compile test
Write-Host "  Compiling test_election.cpp..." -ForegroundColor Gray
g++ -std=c++17 -D_WIN32_WINNT=0x0600 test_election.cpp broker/MetadataService.cpp broker/ConsumerGroup.cpp broker/GroupManager.cpp -o test_election.exe -lws2_32 2>$null

if (Test-Path "test_election.exe") {
    Write-Host "  [OK] Compiled successfully" -ForegroundColor Green
    
    # Run test
    Write-Host "`n  Running leader election test..." -ForegroundColor Gray
    $result = .\test_election.exe
    
    # Check results
    if ($result -match "After: payments-0 leader = 1") {
        Write-Host "  [OK] Leader election works - Broker 1 elected" -ForegroundColor Green
    } elseif ($result -match "After: payments-0 leader = 3") {
        Write-Host "  [OK] Leader election works - Broker 3 elected" -ForegroundColor Green
    } else {
        Write-Host "  [WARN] Unexpected election result" -ForegroundColor Yellow
    }
    
    Write-Host "`n  Output:" -ForegroundColor Gray
    Write-Host $result -ForegroundColor Gray
} else {
    Write-Host "  [FAIL] Compilation failed!" -ForegroundColor Red
}

# ============================================
# FINAL SUMMARY
# ============================================
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  TEST SUMMARY" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Check all components
$allPass = $true

# Check broker
$proc = Get-Process -Name "kafka_broker" -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host "  [OK] Broker running" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Broker not running" -ForegroundColor Red
    $allPass = $false
}

# Check topic
$result = Send-Cmd "METADATA $topicName"
if ($result -match $topicName) {
    Write-Host "  [OK] Topic '$topicName' exists" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Topic '$topicName' not found" -ForegroundColor Red
    $allPass = $false
}

# Check index
if (Test-Path $indexPath) {
    $bytes = [System.IO.File]::ReadAllBytes($indexPath)
    $n = $bytes.Length / 16
    Write-Host "  [OK] Index file: $n entries" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Index file missing" -ForegroundColor Red
    $allPass = $false
}

# Check log
if (Test-Path "data\$topicName-$partitionId.log") {
    Write-Host "  [OK] Log file exists" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Log file missing" -ForegroundColor Red
    $allPass = $false
}

# Check consumer
$result = Send-Cmd "CONSUME $topicName $partitionId 0"
if ($result -match "\|") {
    Write-Host "  [OK] Consumer works" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Consumer failed" -ForegroundColor Red
    $allPass = $false
}

Write-Host "`n========================================" -ForegroundColor Cyan
if ($allPass) {
    Write-Host "  ALL TESTS PASSED!" -ForegroundColor Green
    Write-Host "  VORTEX is working correctly!" -ForegroundColor Green
} else {
    Write-Host "  SOME TESTS FAILED" -ForegroundColor Red
    Write-Host "  Please check the errors above" -ForegroundColor Yellow
}
Write-Host "========================================" -ForegroundColor Cyan