# Learning etcd: A Deep Dive for Distributed Systems Practitioners

> **Goal**: Understand etcd's internals to build expertise in distributed systems.
> **Approach**: Map DDIA concepts → etcd implementation → trace operations through layers.

---

## Table of Contents

1. [Mental Model](#1-mental-model)
2. [DDIA Concepts in etcd](#2-ddia-concepts-in-etcd)
3. [Architecture Overview](#3-architecture-overview)
4. [Layer-by-Layer Deep Dive](#4-layer-by-layer-deep-dive)
5. [Tracing a PUT Operation](#5-tracing-a-put-operation)
6. [Tracing a GET Operation](#6-tracing-a-get-operation)
7. [Key Components Reference](#7-key-components-reference)
8. [Hands-On Exercises](#8-hands-on-exercises)
9. [Learning Roadmap](#9-learning-roadmap)

---

## 1. Mental Model

Think of etcd as a **distributed, strongly consistent, ordered key-value store** with these layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                             │
│  (gRPC client, load balancing, retries, connection management)   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          API LAYER                               │
│        (gRPC services: KV, Watch, Lease, Auth, Cluster)          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       SERVER LAYER                               │
│  (EtcdServer: request handling, linearizable reads, raft calls)  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CONSENSUS LAYER                             │
│          (Raft: leader election, log replication, safety)        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       APPLY LAYER                                │
│     (State machine: applies committed entries to storage)        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                               │
│                    ┌──────────────────┐                          │
│                    │  MVCC (index)    │  ← In-memory B-tree      │
│                    └────────┬─────────┘                          │
│                             │                                    │
│                    ┌────────▼─────────┐                          │
│                    │  Backend/BoltDB  │  ← Persistent storage    │
│                    └──────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

### Key Insight: The "Replicated State Machine" Pattern

etcd implements the **replicated state machine** architecture:

1. **Clients** send requests to the **leader**
2. **Leader** appends the command to its **log** and replicates to **followers**
3. Once a **majority** acknowledges, the entry is **committed**
4. **All nodes** apply committed entries to their **state machine** (storage)
5. **Leader** responds to the client

This guarantees that all nodes see the same sequence of operations.

---

## 2. DDIA Concepts in etcd

| DDIA Concept | etcd Implementation | Key Files |
|--------------|---------------------|-----------|
| **Replication** (Ch 5) | Raft consensus - single leader, log replication | `server/etcdserver/raft.go` |
| **Partitions** (Ch 6) | N/A - etcd doesn't partition data | - |
| **Transactions** (Ch 7) | Mini-transactions with Compare-and-Swap | `api/etcdserverpb/rpc.proto` (Txn) |
| **Consistency** (Ch 9) | Linearizability via Raft + ReadIndex | `server/etcdserver/v3_server.go` |
| **Consensus** (Ch 9) | Raft implementation | `raft/` module (external) |
| **MVCC** (Ch 7) | Multi-version storage with revisions | `server/storage/mvcc/` |
| **B-Trees** (Ch 3) | BoltDB uses B+ trees, in-memory B-tree for index | `server/storage/mvcc/index.go` |
| **WAL** (Ch 3) | Write-ahead log for Raft entries | `server/storage/wal/` |
| **Leases** (Ch 8) | Time-bounded key associations | `server/lease/` |

### Deep Dive: Linearizability in etcd

etcd provides **linearizable reads** by default:

```go
// server/etcdserver/v3_server.go
func (s *EtcdServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
    if !r.Serializable {
        // Wait for leader confirmation that we're up-to-date
        err = s.linearizableReadNotify(ctx)
    }
    // Now safe to read from local state
}
```

**How it works**:
1. Client sends read request
2. Server calls `ReadIndex()` - asks Raft "what's the current commit index?"
3. Leader confirms it's still leader (via heartbeat round)
4. Server waits until local apply index >= commit index
5. Now the read reflects all committed writes

This is the **ReadIndex** optimization from the Raft paper!

---

## 3. Architecture Overview

### 3.1 Directory Structure

```
etcd/
├── api/                    # Protocol buffer definitions
│   └── etcdserverpb/       # gRPC service definitions (rpc.proto)
├── client/v3/              # Go client library
├── server/
│   ├── etcdmain/           # Entry point and startup
│   ├── etcdserver/         # Core server logic
│   │   ├── api/
│   │   │   ├── v3rpc/      # gRPC handlers
│   │   │   ├── rafthttp/   # Peer-to-peer communication
│   │   │   └── membership/ # Cluster membership
│   │   ├── apply/          # State machine application
│   │   └── txn/            # Transaction execution
│   ├── storage/
│   │   ├── mvcc/           # Multi-version concurrency control
│   │   ├── backend/        # BoltDB wrapper
│   │   └── wal/            # Write-ahead log
│   └── lease/              # Lease management
├── etcdctl/                # CLI tool
└── pkg/                    # Shared utilities
```

### 3.2 Key Structs

```go
// The main server - server/etcdserver/server.go
type EtcdServer struct {
    r         raftNode              // Raft consensus
    kv        mvcc.WatchableKV      // Key-value storage
    lessor    lease.Lessor          // Lease management
    authStore auth.AuthStore        // Auth/authz
    be        backend.Backend       // BoltDB backend
    cluster   *membership.RaftCluster
}

// MVCC storage - server/storage/mvcc/kv.go
type store struct {
    b          backend.Backend      // Persistent storage
    kvindex    index                // In-memory B-tree
    currentRev int64                // Global revision counter
}

// Key index - tracks all revisions of a key
type keyIndex struct {
    key         []byte
    modified    Revision            // Last modification
    generations []generation        // History (tombstones create new generations)
}
```

---

## 4. Layer-by-Layer Deep Dive

### Layer 1: Client (`client/v3/`)

The client provides a high-level API:

```go
// Creating a client
cli, _ := clientv3.New(clientv3.Config{
    Endpoints:   []string{"localhost:2379"},
    DialTimeout: 5 * time.Second,
})

// Operations
cli.Put(ctx, "key", "value")
cli.Get(ctx, "key")
cli.Watch(ctx, "prefix", clientv3.WithPrefix())
```

**Key files**:
- `client.go` - Connection management
- `kv.go` - KV operations
- `watch.go` - Watch streaming
- `op.go` - Operation construction with options

### Layer 2: API (`server/etcdserver/api/v3rpc/`)

gRPC service handlers that bridge client requests to the server:

```go
// server/etcdserver/api/v3rpc/key.go
func (s *kvServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
    resp, err := s.kv.Put(ctx, r)  // Calls EtcdServer.Put
    s.hdr.fill(resp.Header)
    return resp, nil
}
```

**Key files**:
- `grpc.go` - Server setup and service registration
- `key.go` - KV operations (Put, Range, DeleteRange, Txn)
- `watch.go` - Watch streaming
- `interceptor.go` - Request logging, metrics

### Layer 3: Server (`server/etcdserver/`)

The core logic that coordinates Raft and storage:

```go
// server/etcdserver/v3_server.go
func (s *EtcdServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
    // Wrap in InternalRaftRequest and propose to Raft
    resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Put: r})
    return resp.(*pb.PutResponse), nil
}

func (s *EtcdServer) raftRequest(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
    return s.processInternalRaftRequestOnce(ctx, r)
}

func (s *EtcdServer) processInternalRaftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
    // 1. Create unique ID and register wait channel
    id := r.Header.ID
    ch := s.w.Register(id)
    
    // 2. Propose to Raft
    s.r.Propose(ctx, data)
    
    // 3. Wait for apply
    select {
    case x := <-ch:
        return x.resp, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

**Key files**:
- `server.go` - Main EtcdServer struct and lifecycle
- `v3_server.go` - v3 API implementation
- `raft.go` - Raft integration

### Layer 4: Consensus (Raft)

etcd uses the `raft` library (originally in-tree, now at `go.etcd.io/raft/v3`):

```go
// server/etcdserver/raft.go
type raftNode struct {
    raft.Node                       // The Raft state machine
    storage *raft.MemoryStorage     // In-memory Raft log
    // ...
}

func (r *raftNode) start(rh *raftReadyHandler) {
    go func() {
        for {
            select {
            case rd := <-r.Ready():  // Get state changes from Raft
                // 1. Save to WAL
                // 2. Save snapshot if needed
                // 3. Send messages to peers
                // 4. Push committed entries to apply channel
                r.applyc <- toApply{entries: rd.CommittedEntries}
            }
        }
    }()
}
```

**Raft guarantees**:
- **Leader election**: Exactly one leader at a time
- **Log replication**: All nodes see same log entries
- **Safety**: Committed entries are durable and agreed upon

### Layer 5: Apply (`server/etcdserver/apply/`)

Applies committed Raft entries to the state machine:

```go
// server/etcdserver/server.go
func (s *EtcdServer) run() {
    for {
        select {
        case ap := <-s.r.apply():
            s.applyAll(&ep, &ap)  // Apply entries and snapshots
        }
    }
}

func (s *EtcdServer) applyEntryNormal(e *raftpb.Entry) {
    var raftReq pb.InternalRaftRequest
    raftReq.Unmarshal(e.Data)
    
    // Apply to state machine
    ar := s.uberApply.Apply(&raftReq, shouldApplyV3)
    
    // Notify waiting request
    s.w.Trigger(id, ar.resp)
}
```

### Layer 6: Storage (`server/storage/`)

#### MVCC Store (`server/storage/mvcc/`)

Multi-version storage with revision tracking:

```go
// server/storage/mvcc/kvstore_txn.go
func (tw *storeTxnWrite) put(key, value []byte, leaseID lease.LeaseID) {
    // 1. Calculate new revision
    rev := tw.beginRev + 1
    idxRev := Revision{Main: rev, Sub: int64(len(tw.changes))}
    
    // 2. Look up or create key index
    _, created, ver, err := tw.s.kvindex.Get(key, rev)
    
    // 3. Create KeyValue with metadata
    kv := mvccpb.KeyValue{
        Key:            key,
        Value:          value,
        CreateRevision: created.Main,
        ModRevision:    rev,
        Version:        ver + 1,
        Lease:          int64(leaseID),
    }
    
    // 4. Write to backend (BoltDB)
    tw.tx.UnsafeSeqPut(schema.Key, revisionBytes, kvBytes)
    
    // 5. Update in-memory index
    tw.s.kvindex.Put(key, idxRev)
}
```

#### Backend (`server/storage/backend/`)

BoltDB wrapper with batching:

```go
// server/storage/backend/backend.go
type backend struct {
    db            *bolt.DB
    batchTx       *batchTxBuffered  // Batched writes
    batchInterval time.Duration     // Auto-commit interval (100ms)
    batchLimit    int               // Auto-commit threshold (10k ops)
}
```

**Key insight**: Writes are batched for performance but immediately visible through an in-memory buffer.

---

## 5. Tracing a PUT Operation

Let's trace `etcdctl put foo bar` through the entire system:

### Step 1: Client
```
client/v3/kv.go:Put()
  → OpPut("foo", "bar")
  → kv.Do(ctx, op)
  → kv.remote.Put(ctx, &pb.PutRequest{Key: "foo", Value: "bar"})
  → [gRPC over network]
```

### Step 2: API Layer
```
server/etcdserver/api/v3rpc/key.go:Put()
  → s.kv.Put(ctx, r)  // Calls EtcdServer
```

### Step 3: Server Layer
```
server/etcdserver/v3_server.go:Put()
  → s.raftRequest(ctx, InternalRaftRequest{Put: r})
  → s.processInternalRaftRequestOnce(ctx, r)
      → ch := s.w.Register(id)     // Register wait channel
      → s.r.Propose(ctx, data)     // Propose to Raft
      → <-ch                        // Wait for apply
```

### Step 4: Raft Consensus
```
raft library:
  → Leader appends to log
  → Leader replicates to followers
  → Majority acknowledges
  → Entry marked committed
  
server/etcdserver/raft.go:
  → rd := <-r.Ready()
  → Save to WAL
  → r.applyc <- toApply{entries: rd.CommittedEntries}
```

### Step 5: Apply Layer
```
server/etcdserver/server.go:run()
  → ap := <-s.r.apply()
  → s.applyAll(&ep, &ap)
  → s.applyEntryNormal(entry)
      → s.uberApply.Apply(&raftReq)
      → s.w.Trigger(id, response)  // Wake up waiting request
```

### Step 6: Storage Layer
```
server/etcdserver/apply/apply.go:
  → Put(txn, p.Key, p.Value, leaseID)

server/storage/mvcc/kvstore_txn.go:put()
  → Calculate revision (e.g., {Main: 5, Sub: 0})
  → Create KeyValue protobuf
  → tw.tx.UnsafeSeqPut(schema.Key, revBytes, kvBytes)  // BoltDB
  → tw.s.kvindex.Put(key, revision)                    // In-memory index
```

### Step 7: Response
```
← Response flows back through all layers
← Client receives PutResponse
```

---

## 6. Tracing a GET Operation

Let's trace `etcdctl get foo`:

### Linearizable Read (Default)

```
1. Client: Get("foo")
   ↓
2. API: kvServer.Range()
   ↓
3. Server: EtcdServer.Range()
   → if !r.Serializable {
       s.linearizableReadNotify(ctx)  // Ensure up-to-date
     }
   ↓
4. LinearizableReadNotify:
   → s.readwaitc <- struct{}{}        // Signal read request
   → s.r.ReadIndex(ctx)               // Ask Raft for current index
   → Wait for s.readNotifier          // Wait until applied
   ↓
5. Once applied index >= commit index:
   → txn := s.kv.Read(...)            // Create read transaction
   → txn.Range(key, end, ro)          // Query MVCC store
   ↓
6. MVCC Read:
   → s.kvindex.Get(key, rev)          // In-memory B-tree lookup
   → revpairs := s.kvindex.Revisions(key, end, atRev)
   → tx.UnsafeRange(schema.Key, revBytes...)  // BoltDB read
   → Deserialize KeyValue protobufs
   ↓
7. Return response to client
```

### Serializable Read (Fast, Eventually Consistent)

```go
// With WithSerializable() option
cli.Get(ctx, "foo", clientv3.WithSerializable())
```

This skips the `linearizableReadNotify()` step - reads local state immediately.
Faster but might return stale data if the node is behind.

---

## 7. Key Components Reference

### Revisions

```go
type Revision struct {
    Main int64  // Transaction counter (global, monotonic)
    Sub  int64  // Key counter within transaction
}

// Example: Transaction at rev 5 modifying 3 keys:
// Key 1: Revision{Main: 5, Sub: 0}
// Key 2: Revision{Main: 5, Sub: 1}
// Key 3: Revision{Main: 5, Sub: 2}
```

### Key Index (in-memory B-tree)

```go
type keyIndex struct {
    key         []byte
    modified    Revision
    generations []generation  // Tombstones create new generations
}

// Example timeline for key "foo":
// put("foo", "v1") at rev 2  → gen[0].revs = [{2,0}]
// put("foo", "v2") at rev 5  → gen[0].revs = [{2,0}, {5,0}]
// delete("foo") at rev 7     → gen[0].revs = [{2,0}, {5,0}, {7,0}(tomb)]
// put("foo", "v3") at rev 9  → gen[1].revs = [{9,0}]  (new generation!)
```

### Backend Storage Format

```
BoltDB Buckets:
├── "key" bucket
│   └── Key: revision bytes (17 bytes: [8-byte main][_][8-byte sub])
│   └── Value: serialized mvccpb.KeyValue protobuf
│
└── "meta" bucket
    └── Metadata (compaction state, etc.)
```

---

## 8. Hands-On Exercises

### Exercise 1: Run a Local Cluster

```bash
# Build etcd
make build

# Start a 3-node cluster
goreman start

# In another terminal, interact with it
./bin/etcdctl put foo bar
./bin/etcdctl get foo
./bin/etcdctl watch foo &
./bin/etcdctl put foo baz  # Watch should trigger
```

### Exercise 2: Add Debug Logging

Add a log statement to trace a PUT:

```go
// server/etcdserver/v3_server.go:Put()
func (s *EtcdServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
    s.lg.Info("PUT request received",
        zap.ByteString("key", r.Key),
        zap.ByteString("value", r.Value))
    // ... rest of function
}
```

Rebuild and test:
```bash
make build
./bin/etcd
./bin/etcdctl put mykey myvalue
# Check logs for your message
```

### Exercise 3: Explore MVCC Revisions

```bash
# Put multiple versions
./bin/etcdctl put counter 1
./bin/etcdctl put counter 2
./bin/etcdctl put counter 3

# Get current value
./bin/etcdctl get counter

# Get with revision history (shows create/mod revisions)
./bin/etcdctl get counter -w json | jq

# Watch from a past revision
./bin/etcdctl watch counter --rev=1
```

### Exercise 4: Trace Raft

Enable verbose Raft logging:
```bash
ETCD_DEBUG=true ./bin/etcd
```

Watch for messages like:
- `MsgApp` - Log append (leader → follower)
- `MsgAppResp` - Append response
- `MsgHeartbeat` - Leader heartbeat
- `MsgVote` - Leader election

### Exercise 5: Read the Tests

Tests are excellent documentation:
```bash
# MVCC tests
go test -v ./server/storage/mvcc/... -run TestPut

# Server tests
go test -v ./server/etcdserver/... -run TestV3

# Integration tests
go test -v ./tests/integration/... -run TestV3Put
```

---

## 9. Learning Roadmap

### Phase 1: Foundation (Week 1-2)
- [ ] Read DDIA Chapters 5 (Replication) and 9 (Consistency)
- [ ] Run etcd locally and use `etcdctl`
- [ ] Read `api/etcdserverpb/rpc.proto` - understand the API
- [ ] Trace a PUT through the code with debugger/logs

### Phase 2: Raft Deep Dive (Week 3-4)
- [ ] Read the [Raft paper](https://raft.github.io/raft.pdf)
- [ ] Watch [etcd deep dive video](https://www.youtube.com/watch?v=D2pm6ufIt98)
- [ ] Study `server/etcdserver/raft.go`
- [ ] Understand leader election and log replication

### Phase 3: Storage Internals (Week 5-6)
- [ ] Read DDIA Chapter 3 (Storage)
- [ ] Study `server/storage/mvcc/` - especially `key_index.go` and `store.go`
- [ ] Understand revision system and generations
- [ ] Learn about BoltDB (B+ trees, ACID transactions)

### Phase 4: Contributing (Ongoing)
- [ ] Find a ["good first issue"](https://github.com/etcd-io/etcd/labels/good%20first%20issue)
- [ ] Set up dev environment with tests passing
- [ ] Fix a flaky test or documentation issue
- [ ] Work up to ["help wanted"](https://github.com/etcd-io/etcd/labels/help%20wanted) issues

### Recommended Resources

1. **Papers**
   - [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)
   - [In Search of an Understandable Consensus Algorithm](https://raft.github.io/)

2. **Videos**
   - [etcd Deep Dive](https://www.youtube.com/watch?v=D2pm6ufIt98)
   - [etcd Code Walkthrough](https://www.youtube.com/watch?v=H3XaSF6wF7w)

3. **Documentation**
   - [etcd Learning Resources](https://etcd.io/docs/latest/learning/)
   - [etcd API Reference](https://etcd.io/docs/latest/learning/api/)

---

## Quick Reference: Key Files by Topic

| Topic | Files |
|-------|-------|
| Entry point | `server/etcdmain/etcd.go`, `server/main.go` |
| Core server | `server/etcdserver/server.go`, `v3_server.go` |
| Raft integration | `server/etcdserver/raft.go` |
| gRPC handlers | `server/etcdserver/api/v3rpc/*.go` |
| Apply loop | `server/etcdserver/apply/*.go` |
| MVCC store | `server/storage/mvcc/store.go`, `kvstore_txn.go` |
| Key indexing | `server/storage/mvcc/index.go`, `key_index.go` |
| Backend | `server/storage/backend/backend.go` |
| Client | `client/v3/client.go`, `kv.go`, `watch.go` |
| Proto definitions | `api/etcdserverpb/rpc.proto` |

---

*Happy learning! Remember: the best way to learn a codebase is to trace real operations through it.*
