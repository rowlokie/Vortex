# 🚀 VORTEX - Distributed Messaging System

A production-grade distributed messaging system built from scratch in **C++17**, featuring log-structured storage, binary indexing, consumer groups, and ISR-based leader election.

> *"Where data flows"*

---

## 📖 Overview

**VORTEX** is a **Kafka-like distributed messaging broker** built from the ground up in C++. It implements:

- **Publish-Subscribe** with topic-based routing
- **Log-structured storage** for high-throughput writes
- **Binary indexing** for O(log N) message lookups
- **Consumer groups** with automatic rebalancing
- **ISR-based leader election** for fault tolerance

---

## ✨ Features

### Core Broker Engine
- ✅ Custom TCP protocol over port 9092
- ✅ Multi-threaded client handling
- ✅ Commands: `CREATE_TOPIC`, `PRODUCE`, `CONSUME`, `METADATA`
- ✅ Graceful error handling

### Storage Engine
- ✅ Append-only log files (`<topic>-<partition>.log`)
- ✅ Binary index files (`<topic>-<partition>.index`)
- ✅ **O(log N)** message fetch via binary search
- ✅ Auto-index rebuild on corruption
- ✅ Crash recovery from disk

### Consumer Groups
- ✅ `JOIN_GROUP`, `LEAVE_GROUP`, `HEARTBEAT` commands
- ✅ Round-robin partition assignment
- ✅ Dead consumer detection (heartbeat timeout)

### Fault Tolerance
- ✅ ISR (In-Sync Replicas) tracking
- ✅ 3-tier leader election
- ✅ Metadata persistence to disk

---

## 🏗️ Architecture

```
┌─────────────┐     TCP (9092)     ┌─────────────────┐
│  Producer   │ ─────────────────> │                 │
│     SDK     │                    │     Broker      │
└─────────────┘                    │                 │
                                   │ • TopicManager  │
┌─────────────┐                    │ • GroupManager  │
│  Consumer   │ <───────────────── │ • MetadataSvc   │
│     SDK     │     TCP (9092)     └────────┬────────┘
└─────────────┘                             │
                                            ▼
                                   ┌───────────────┐
                                   │  Disk Store   │
                                   │  .log .index  │
                                   │ .meta .offset │
                                   └───────────────┘
```

**Data Flow:**
1. **Producer** sends message → Broker via TCP.
2. **Broker** routes to partition: `hash(key) % partitionCount`.
3. **Message** appended to `.log` file.
4. **Index** updated with offset → byte position.
5. **Consumer** requests messages by offset.
6. **Binary search** finds position → `seekg()` → reads message.

---

## 🚀 Quick Start

### Prerequisites
- C++17 compiler (g++ or MSVC)
- Make (optional)
- Windows (Winsock2) or Linux (POSIX)

### Build
To compile the broker:
```bash
g++ -std=c++17 -D_WIN32_WINNT=0x0600 broker/*.cpp -o kafka_broker.exe -lws2_32
```

### Run Broker
```bash
# Windows
.\kafka_broker.exe

# Linux
./kafka_broker
```

### Test with Sample Commands
```bash
# Create topic with 3 partitions
> CREATE_TOPIC orders 3
OK

# Produce message with key
> PRODUCE orders key1 "Hello World"
{"status":"OK","partition":1,"offset":1}

# Get metadata
> METADATA orders
{"partitions":[{"id":0,...},{"id":1,...},{"id":2,...}]}

# Consume from offset 0
> CONSUME orders 1 0
1|1734567890|Hello World
```

---

## 🧪 Running Tests

### Quick Test
Run the automated test script in PowerShell:
```powershell
.\test_now.ps1
```

**Expected Output:**
```text
=== VORTEX - Full System Test ===

[STEP 1] Stopping all brokers...
  [OK] All processes stopped

[STEP 2] Deleting all data...
  [OK] Data directory cleaned

[STEP 3] Starting broker...
  [OK] Broker started (PID: 12345)
  [OK] Port 9092 is listening

[STEP 4] Testing CREATE_TOPIC...
  [OK] Topic 'orders' created

[STEP 5] Testing PRODUCE...
  [OK] Produced: {"status":"OK","partition":1,"offset":1}

[STEP 6] Testing METADATA...
  [OK] Metadata retrieved

[STEP 7] Verifying index file...
  [OK] Index file created

[STEP 8] Testing CONSUME (Binary Search)...
  [OK] Binary search works

[STEP 9] Testing index rebuild...
  [OK] Index rebuilt

[STEP 10] Testing CONSUME after rebuild...
  [OK] Consume works

[STEP 11] Testing Leader Election...
  [OK] Leader election works

========================================
  ALL TESTS PASSED!
  VORTEX is working correctly!
========================================
```

---

## 🔬 Technical Deep Dive

### 1. Binary Index (O(log N) Lookup)
**Index Format (16 bytes per entry):**
```text
| int64 offset (8 bytes) | int64 bytePosition (8 bytes) |
```

**Why fixed-width?**
- O(1) access to any entry by index
- Binary search without parsing
- Simple to rebuild from log

**Lookup Process:**
- Binary search index for target offset
- Read byte position at that entry
- `seekg()` directly to byte position
- Read the message line

**Performance:**
- **Before:** O(N) scanning
- **After:** O(log N) binary search + O(1) seek

---

### 2. Leader Election Protocol
**3-Tier Election Strategy:**
| Priority | Strategy | Data Loss Risk |
|---|---|---|
| 1 | ISR Priority | ✅ No data loss |
| 2 | Replica Fallback | ⚠️ Possible lag |
| 3 | Unclean Election | ❌ Data loss possible |

**ISR (In-Sync Replicas):**
- Followers fully caught up with leader
- Tracked per partition
- Used for safe leader election

---

### 3. Consumer Group Protocol
**Rebalancing Process:**
- Consumer joins via `JOIN_GROUP`
- `GroupManager` triggers rebalance
- Partitions assigned via round-robin
- Heartbeats maintain membership
- Failure triggers rebalance

---

## 📚 Learnings

Building this project taught me:
- **Distributed Systems:** CAP theorem, consistency vs availability
- **Storage Engines:** Append-only logs, indexing, binary formats
- **Network Programming:** TCP sockets, threading, protocol design
- **C++ Systems Programming:** RAII, smart pointers, concurrency
- **Database Internals:** O(log N) indexing, file I/O optimization

---

## 📬 Contact

- **Name:** Piyush Batavale
- **Email:** piyushbatavale02@gmail.com
- **GitHub:** [github.com/rowlokie/vortex](https://github.com/rowlokie/vortex)
- **LinkedIn:** [linkedin.com/in/piyushbatavale](https://linkedin.com/in/piyushbatavale)
