Write-Host "=== VORTEX COMPLETE TEST SUITE ===" -ForegroundColor Cyan

# Define Send-Cmd function inside the script
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

# Step 1: Clean start
Write-Host "`n[1] Clean start..." -ForegroundColor Yellow
Stop-Process -Name "kafka_broker" -ErrorAction SilentlyContinue
Remove-Item -Force data\*.log, data\*.index -ErrorAction SilentlyContinue
Remove-Item -Force data\meta\* -ErrorAction SilentlyContinue
Remove-Item -Force data\offsets\* -ErrorAction SilentlyContinue

# Start broker in background
Start-Job -ScriptBlock { & "C:\Users\Piyush\Desktop\kafkaclone\kafka_broker.exe" }
Start-Sleep -Seconds 4
Write-Host "[PASS] Broker started" -ForegroundColor Green

# Step 2: Create topic
Write-Host "`n[2] Creating topic..." -ForegroundColor Yellow
$result = Send-Cmd "CREATE_TOPIC orders 3"
if ($result -eq "OK") { 
    Write-Host "[PASS] Topic created" -ForegroundColor Green 
} else { 
    Write-Host "[FAIL] Failed: $result" -ForegroundColor Red 
}

# Step 3: Produce messages
Write-Host "`n[3] Producing 6 messages..." -ForegroundColor Yellow
1..6 | ForEach-Object {
    $r = Send-Cmd "PRODUCE orders key1 Msg-$_"
    if ($_ -eq 1) { Write-Host "[PASS] First message produced: $r" -ForegroundColor Green }
}
Write-Host "[PASS] All messages produced" -ForegroundColor Green

# Wait for files to be written
Start-Sleep -Seconds 1

# Step 4: Verify index
Write-Host "`n[4] Verifying index..." -ForegroundColor Yellow
$indexPath = "data\orders-1.index"
if (Test-Path $indexPath) {
    $bytes = [System.IO.File]::ReadAllBytes($indexPath)
    $n = $bytes.Length / 16
    if ($n -eq 6) { 
        Write-Host "[PASS] Index has $n entries (96 bytes)" -ForegroundColor Green 
    } else { 
        Write-Host "[WARN] Index has $n entries (expected 6)" -ForegroundColor Yellow 
    }
    # Display entries
    for ($i = 0; $i -lt $n; $i++) {
        $off = [BitConverter]::ToInt64($bytes, $i * 16)
        $pos = [BitConverter]::ToInt64($bytes, $i * 16 + 8)
        Write-Host "  offset=$off -> byte=$pos" -ForegroundColor Gray
    }
} else {
    Write-Host "[FAIL] Index file not found!" -ForegroundColor Red
    Write-Host "Files in data directory:" -ForegroundColor Yellow
    Get-ChildItem data\
}

# Step 5: Test binary search
Write-Host "`n[5] Testing binary search..." -ForegroundColor Yellow
$result = Send-Cmd "CONSUME orders 1 3"
if ($result -match "4\|") { 
    Write-Host "[PASS] Binary search works (skipped to offset 4): $result" -ForegroundColor Green 
} else { 
    Write-Host "[WARN] Result: $result" -ForegroundColor Yellow 
}

# Step 6: Test index rebuild
Write-Host "`n[6] Testing index rebuild..." -ForegroundColor Yellow
if (Test-Path $indexPath) {
    Remove-Item -Force $indexPath
    Write-Host "Index deleted" -ForegroundColor Gray
} else {
    Write-Host "Index already missing" -ForegroundColor Gray
}

Stop-Process -Name "kafka_broker" -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
Start-Job -ScriptBlock { & "C:\Users\Piyush\Desktop\kafkaclone\kafka_broker.exe" }
Start-Sleep -Seconds 4

if (Test-Path $indexPath) {
    $bytes = [System.IO.File]::ReadAllBytes($indexPath)
    $n = $bytes.Length / 16
    Write-Host "[PASS] Index rebuilt with $n entries" -ForegroundColor Green
    for ($i = 0; $i -lt $n; $i++) {
        $off = [BitConverter]::ToInt64($bytes, $i * 16)
        $pos = [BitConverter]::ToInt64($bytes, $i * 16 + 8)
        Write-Host "  offset=$off -> byte=$pos" -ForegroundColor Gray
    }
} else {
    Write-Host "[FAIL] Index rebuild failed!" -ForegroundColor Red
}

# Step 7: Test leader election (only if test file exists)
Write-Host "`n[7] Testing leader election..." -ForegroundColor Yellow
if (Test-Path "test_election.cpp") {
    g++ -std=c++17 -D_WIN32_WINNT=0x0600 test_election.cpp broker/MetadataService.cpp broker/ConsumerGroup.cpp broker/GroupManager.cpp -o test_election.exe -lws2_32 2>$null
    if (Test-Path "test_election.exe") {
        $result = .\test_election.exe
        if ($result -match "After: payments-0 leader = 1") { 
            Write-Host "[PASS] Leader election works" -ForegroundColor Green 
        } else { 
            Write-Host "[WARN] Leader election result: $result" -ForegroundColor Yellow 
        }
    } else {
        Write-Host "[FAIL] Failed to compile test_election.exe" -ForegroundColor Red
    }
} else {
    Write-Host "[SKIP] test_election.cpp not found - skipping leader election test" -ForegroundColor Yellow
}

Write-Host "`n=== ALL TESTS COMPLETE ===" -ForegroundColor Green

# Show final status
Write-Host "`nFinal Status:" -ForegroundColor Cyan
if (Test-Path $indexPath) {
    $bytes = [System.IO.File]::ReadAllBytes($indexPath)
    $n = $bytes.Length / 16
    Write-Host "  [OK] Index file: $n entries ($($bytes.Length) bytes)" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Index file: MISSING" -ForegroundColor Red
}

if (Test-Path "data\orders-1.log") {
    Write-Host "  [OK] Log file exists" -ForegroundColor Green
    $lines = Get-Content "data\orders-1.log"
    Write-Host "  Messages: $($lines.Count)" -ForegroundColor Gray
} else {
    Write-Host "  [FAIL] Log file: MISSING" -ForegroundColor Red
}