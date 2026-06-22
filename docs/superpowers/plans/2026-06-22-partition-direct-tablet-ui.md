# partition_direct Tablet UI (read-only) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only HTTP monitoring page to the `partition_direct` tablet showing local-DB contents, barriers, dirty-map / DDisk / PBuffer state, host stats, and DBG topology.

**Architecture:** The tablet actor (`TPartitionActor`) handles `NMon::TEvRemoteHttpInfo`. It reads persisted rows via a read-only flat-tablet transaction (`TTxMonitoring`) and gathers runtime state from the coroutine-executor side (`TFastPathService` → each `TDirectBlockGroup` → `TVChunk`/`TOracle`/`TBlocksDirtyMap`) as plain snapshot structs via a future, mirroring the existing `Dump()` aggregation. A pure function `RenderMonPage(TMonPageData)` turns the combined data into HTML using the `library/cpp/monlib/service/pages/templates.h` macros.

**Tech Stack:** C++20, YDB actor system, flat-tablet (`NIceDb`) transactions, `NThreading::TFuture`, `library/cpp/monlib` HTML macros, NBS `ya make` build (on cloud).

**Build/test environment (per project rules):** edits on Mac in `/Users/barkovbg/ydb_partition_ui` (auto-synced by Mutagen session `ydb-partition-ui`); build & test on cloud in `~/ydb_partition_ui`. Folder shorthand below:
`PD = ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct`.

Build/test command template (run on cloud):
```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo -tA $PD -F '*<filter>*' 2>&1 | tail -40"
```
Before trusting a green build, flush sync and confirm the edit landed on cloud:
```bash
mutagen sync flush ydb-partition-ui
ssh cloud "cd ~/ydb_partition_ui && grep -rl '<unique-symbol-from-change>' $PD | head"
```

---

## File Structure

**New files (all under `$PD`):**
- `monitoring/mon_snapshot.h` — plain snapshot structs (`TMonSnapshot`, `TDbgSnapshot`, `THostSnapshot`, `TVChunkSnapshot`, view structs) + render-input aggregate `TMonPageData`. Depends only on light model headers.
- `monitoring/mon_render.h` / `monitoring/mon_render.cpp` — `RenderMonPage(const TMonPageData&) -> TString` (pure; no actor).
- `monitoring/mon_render_ut.cpp` — unit tests for the renderer.
- `part_monitoring.cpp` — `TPartitionActor::HandleHttpInfo`, `TTxMonitoring` Prepare/Execute/Complete, `HandleMonSnapshotReady`, timeout handling, page assembly + reply.

**Modified files:**
- `model/host_stat.h` — none (already exposes `InflightCount`, `GetErrorsInfo`, `TErrorsInfo`).
- `model/oracle.h` / `oracle.cpp` — add `TVector<THostSnapshot> BuildHostSnapshots(TInstant now) const`.
- `dirty_map/dirty_map.h` / `dirty_map.cpp` — add `TDirtyMapSnapshot BuildSnapshot() const`.
- `vchunk.h` / `vchunk.cpp` — add `TVChunkSnapshot BuildSnapshot() const`.
- `direct_block_group.h` — add `virtual NThreading::TFuture<TDbgSnapshot> GatherMonSnapshot() = 0;` to `IDirectBlockGroup`.
- `direct_block_group_impl.h` / `direct_block_group_impl.cpp` — implement `GatherMonSnapshot()` (executor-marshaled, mirrors existing `Dump()`).
- `direct_block_group_mock.h` / `direct_block_group_mock.cpp` — add `GatherMonSnapshot()` stub (interface grew).
- `fast_path_service.h` / `fast_path_service.cpp` — add `NThreading::TFuture<TMonSnapshot> GatherMonSnapshot()` + gather bookkeeping; expose `ui64 GetLsnCounter() const` and `std::optional<ui64> GetLastSafeBarrier() const`.
- `partition_direct_actor.h` — declare `HandleHttpInfo`, `TTxMonitoring` (via the transactions macro), `HandleMonSnapshotReady`, the per-request map, and a monotonic cookie counter.
- `partition_direct_events_private.h` — add `TEvMonSnapshotReady` (+ a `TEvMonRenderTimeout` wakeup).
- `part_tx.h` — add `Monitoring` to `BLOCKSTORE_PARTITION_TRANSACTIONS` and a `TMonitoring` args struct.
- `partition_direct_actor.cpp` — register `HFunc(NMon::TEvRemoteHttpInfo, HandleHttpInfo)` and the two private events in `StateWork`.
- `ya.make` — add `monitoring/mon_render.cpp`, `monitoring/mon_snapshot.cpp` (if any .cpp), `part_monitoring.cpp`.
- `ut/ya.make` (the unit-test target that builds `partition_direct_ut.cpp`) — add `monitoring/mon_render_ut.cpp`; add the mon assertions to `partition_direct_ut.cpp`.

**Snapshot ownership / threading:** `TVChunk::BuildSnapshot`, `TBlocksDirtyMap::BuildSnapshot`, `TOracle::BuildHostSnapshots`, and `TDirectBlockGroup::GatherMonSnapshot`'s body all run on the DBG **executor thread** (same constraint as `Dump()` / `GetSafeBarrierForErase`). `RenderMonPage` and `TTxMonitoring` run in the **actor**.

---

## Task 1: Snapshot data model (`mon_snapshot.h`)

**Files:**
- Create: `$PD/monitoring/mon_snapshot.h`

- [ ] **Step 1: Create the header with all snapshot structs**

```cpp
#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>   // EOperation, OperationCount, THostStat::TErrorsInfo
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>  // EHostState

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>
#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// Mirror of EHostHealth (model/oracle.h) kept here to avoid pulling the Oracle
// header into the snapshot model; the builder maps one to the other.
enum class EHostHealthView
{
    Online,
    Sufferer,
    TemporaryOffline,
    Offline,
};

struct TConnSnapshot
{
    THostIndex HostIndex = InvalidHostIndex;
    TString DDiskId;          // formatted on the executor side
    TString PBufferId;        // formatted on the executor side
    TString DDiskSession;     // "NotLocked" / "Locked" / "Broken"
    bool PBufferConnected = false;
};

struct THostSnapshot
{
    THostIndex Index = InvalidHostIndex;
    EHostState State = EHostState::Online;
    EHostHealthView Health = EHostHealthView::Online;
    std::array<size_t, OperationCount> InflightByOp{};
    THostStat::TErrorsInfo Errors;
    ui64 PBufferUsedSize = 0;
};

struct TDDiskStateView
{
    THostIndex HostIndex = InvalidHostIndex;
    TString State;                       // "Disabled" / "Operational" / "Fresh"
    ui64 OperationalBlockCount = 0;
    std::optional<ui64> FreshWatermark;  // bytes offset; nullopt == full
};

struct TPBufferCountersView
{
    THostIndex HostIndex = InvalidHostIndex;
    ui64 CurrentRecords = 0;
    ui64 CurrentBytes = 0;
    ui64 CurrentLockedRecords = 0;
    ui64 CurrentLockedBytes = 0;
    ui64 TotalRecords = 0;
    ui64 TotalBytes = 0;
};

struct THostRoleView
{
    THostIndex HostIndex = InvalidHostIndex;
    TString PBufferRole;   // "Primary" / "HandOff" / "None"
    TString DDiskRole;     // "Primary" / "None"
    bool Enabled = false;
    std::optional<ui64> Watermark;
};

struct TDirtyMapCountsView
{
    size_t Inflight = 0;
    size_t FlushPending = 0;
    size_t ErasePending = 0;
    size_t EraseBelated = 0;
    ui64 MinFlushPendingLsn = 0;
    ui64 MinErasePendingLsn = 0;
};

struct TVChunkSnapshot
{
    ui32 Index = 0;
    size_t HostCount = 0;
    TVector<THostRoleView> Roles;
    TDirtyMapCountsView Counts;
    std::optional<ui64> SafeBarrier;
    TVector<TDDiskStateView> DDiskStates;
    TVector<TPBufferCountersView> PBuffers;
};

struct TDbgSnapshot
{
    size_t Index = 0;
    TVector<TConnSnapshot> Connections;
    TVector<THostSnapshot> Hosts;
    TVector<TVChunkSnapshot> VChunks;
};

struct TMonSnapshot
{
    ui64 LsnCounter = 0;
    std::optional<ui64> GlobalSafeBarrier;
    TVector<TDbgSnapshot> Dbgs;
};

// ---- render input ----------------------------------------------------------

struct THeaderInfo
{
    ui64 TabletId = 0;
    ui32 Generation = 0;
    TString DiskId;
    TString State;        // "BOOT" / "INIT" / "WORK" / "ZOMBIE"
    TDuration Uptime;
};

struct TVChunkConfigRow   // one persisted VChunkConfigs row
{
    ui32 VChunkIndex = 0;
    TString Summary;      // TVChunkConfig::DebugPrint()
};

struct TDbContents
{
    bool HasVolumeConfig = false;
    TString VolumeConfigText;            // VolumeConfig.DebugString()
    bool HasConnections = false;
    TString ConnectionsText;             // DirectBlockGroupsConnections.DebugString()
    TString StorageConfigText;           // short summary
    TVector<TVChunkConfigRow> VChunkConfigs;
};

struct TCgiFilters
{
    std::optional<size_t> Dbg;
    std::optional<ui32> VChunk;
};

struct TMonPageData
{
    THeaderInfo Header;
    TDbContents Db;
    std::optional<TMonSnapshot> Runtime;   // absent in BOOT/INIT or on gather error
    std::optional<TString> RuntimeError;   // banner text when Runtime is absent
    TCgiFilters Filters;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
```

- [ ] **Step 2: Verify it compiles standalone**

Add `monitoring/mon_snapshot.h` is header-only; verified indirectly by Task 2's build. No separate step.

- [ ] **Step 3: Commit**

```bash
cd /Users/barkovbg/ydb_partition_ui
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h
git commit -m "partition_direct mon: snapshot data model"
```

---

## Task 2: `TBlocksDirtyMap::BuildSnapshot`

**Files:**
- Modify: `$PD/dirty_map/dirty_map.h` (add struct + method decl)
- Modify: `$PD/dirty_map/dirty_map.cpp` (impl)
- Test: covered by `vchunk` snapshot test (Task 4) + the integration test (Task 9); a direct micro-test is added in `dirty_map_ut.cpp` if that target exists.

- [ ] **Step 1: Add the snapshot struct + method declaration in `dirty_map.h`**

Add near the top (after includes), and a public method on `TBlocksDirtyMap`:

```cpp
// add to includes:
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>
```

```cpp
// inside class TBlocksDirtyMap, public section, next to the Get* getters:
    [[nodiscard]] TDirtyMapCountsView GetCountsSnapshot() const;
    [[nodiscard]] TVector<TDDiskStateView> GetDDiskStatesSnapshot() const;
    [[nodiscard]] TVector<TPBufferCountersView> GetPBufferCountersSnapshot() const;
    [[nodiscard]] size_t GetHostCount() const;
```

- [ ] **Step 2: Implement in `dirty_map.cpp`**

```cpp
TDirtyMapCountsView TBlocksDirtyMap::GetCountsSnapshot() const
{
    TDirtyMapCountsView v;
    v.Inflight = GetInflightCount();
    v.FlushPending = GetFlushPendingCount();
    v.ErasePending = GetErasePendingCount();
    v.EraseBelated = GetEraseBelatedCount();
    v.MinFlushPendingLsn = GetMinFlushPendingLsn();
    v.MinErasePendingLsn = GetMinErasePendingLsn();
    return v;
}

size_t TBlocksDirtyMap::GetHostCount() const
{
    return DDiskStates.size();
}

TVector<TDDiskStateView> TBlocksDirtyMap::GetDDiskStatesSnapshot() const
{
    TVector<TDDiskStateView> out;
    out.reserve(DDiskStates.size());
    for (size_t host = 0; host < DDiskStates.size(); ++host) {
        const auto& s = DDiskStates[host];
        TDDiskStateView v;
        v.HostIndex = static_cast<THostIndex>(host);
        switch (s.GetState()) {
            case TDDiskState::EState::Disabled: v.State = "Disabled"; break;
            case TDDiskState::EState::Operational: v.State = "Operational"; break;
            case TDDiskState::EState::Fresh: v.State = "Fresh"; break;
        }
        v.OperationalBlockCount = s.GetOperationalBlockCount();
        v.FreshWatermark = GetFreshWatermark(static_cast<THostIndex>(host));
        out.push_back(std::move(v));
    }
    return out;
}

TVector<TPBufferCountersView> TBlocksDirtyMap::GetPBufferCountersSnapshot() const
{
    TVector<TPBufferCountersView> out;
    out.reserve(PBufferCounters.size());
    for (size_t host = 0; host < PBufferCounters.size(); ++host) {
        const auto& c = PBufferCounters[host];
        TPBufferCountersView v;
        v.HostIndex = static_cast<THostIndex>(host);
        v.CurrentRecords = c.CurrentRecordsCount;
        v.CurrentBytes = c.CurrentBytesCount;
        v.CurrentLockedRecords = c.CurrentLockedRecordsCount;
        v.CurrentLockedBytes = c.CurrentLockedBytesCount;
        v.TotalRecords = c.TotalRecordsCount;
        v.TotalBytes = c.TotalBytesCount;
        out.push_back(std::move(v));
    }
    return out;
}
```

> Note: `GetMinFlushPendingLsn`/`GetMinErasePendingLsn` may assert/UB when the queue is empty — guard in the renderer by checking the corresponding count before showing the LSN. Confirm their empty-queue behavior in `dirty_map.cpp` while implementing; if they read `*begin()` unconditionally, set `MinFlushPendingLsn`/`MinErasePendingLsn` to 0 here when `FlushPending`/`ErasePending` is 0.

- [ ] **Step 3: Build**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo $PD/dirty_map 2>&1 | tail -20"
```
Expected: builds clean.

- [ ] **Step 4: Commit**

```bash
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.cpp
git commit -m "partition_direct mon: dirty map snapshot accessors"
```

---

## Task 3: `TOracle::BuildHostSnapshots`

**Files:**
- Modify: `$PD/model/oracle.h` (method decl)
- Modify: `$PD/model/oracle.cpp` (impl)

- [ ] **Step 1: Declare in `oracle.h`**

```cpp
// add include at top:
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>
```

```cpp
// in class TOracle, public section:
    [[nodiscard]] TVector<THostSnapshot> BuildHostSnapshots(TInstant now) const;
```

- [ ] **Step 2: Implement in `oracle.cpp`**

```cpp
TVector<THostSnapshot> TOracle::BuildHostSnapshots(TInstant now) const
{
    TVector<THostSnapshot> out;
    const size_t hostCount = HostStatistics.size();
    out.reserve(hostCount);
    for (size_t host = 0; host < hostCount; ++host) {
        THostSnapshot s;
        s.Index = static_cast<THostIndex>(host);
        s.State = HostStates[host].State;
        s.PBufferUsedSize = HostStates[host].PBufferUsedSize;
        switch (HostsHealths[host]) {
            case EHostHealth::Online: s.Health = EHostHealthView::Online; break;
            case EHostHealth::Sufferer: s.Health = EHostHealthView::Sufferer; break;
            case EHostHealth::TemporaryOffline:
                s.Health = EHostHealthView::TemporaryOffline; break;
            case EHostHealth::Offline: s.Health = EHostHealthView::Offline; break;
        }
        for (size_t op = 0; op < OperationCount; ++op) {
            s.InflightByOp[op] =
                HostStatistics[host].InflightCount(static_cast<EOperation>(op));
        }
        s.Errors = HostStatistics[host].GetErrorsInfo(now);
        out.push_back(std::move(s));
    }
    return out;
}
```

- [ ] **Step 3: Build**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo $PD/model 2>&1 | tail -20"
```
Expected: builds clean.

- [ ] **Step 4: Commit**

```bash
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.cpp
git commit -m "partition_direct mon: oracle host snapshots"
```

---

## Task 4: `TVChunk::BuildSnapshot`

**Files:**
- Modify: `$PD/vchunk.h` (method decl)
- Modify: `$PD/vchunk.cpp` (impl)

- [ ] **Step 1: Declare in `vchunk.h`**

```cpp
// add include at top:
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>
```

```cpp
// in class TVChunk, public section, next to GetSafeBarrierForErase():
    // Build a read-only snapshot of this vchunk's config + dirty-map state.
    // Must run on the executor thread.
    [[nodiscard]] TVChunkSnapshot BuildSnapshot() const;
```

- [ ] **Step 2: Implement in `vchunk.cpp`**

```cpp
TVChunkSnapshot TVChunk::BuildSnapshot() const
{
    ExecutorThreadChecker.CheckThread();

    TVChunkSnapshot s;
    s.Index = VChunkConfig.GetVChunkIndex();
    s.HostCount = VChunkConfig.GetHostCount();
    s.SafeBarrier = GetSafeBarrierForErase();
    s.Counts = BlocksDirtyMap.GetCountsSnapshot();
    s.DDiskStates = BlocksDirtyMap.GetDDiskStatesSnapshot();
    s.PBuffers = BlocksDirtyMap.GetPBufferCountersSnapshot();

    auto roleName = [](EHostRole r) -> TString {
        switch (r) {
            case EHostRole::Primary: return "Primary";
            case EHostRole::HandOff: return "HandOff";
            case EHostRole::None: return "None";
        }
        return "?";
    };

    const auto disabled = VChunkConfig.GetDisabledHosts();
    s.Roles.reserve(s.HostCount);
    for (size_t host = 0; host < s.HostCount; ++host) {
        const auto hi = static_cast<THostIndex>(host);
        THostRoleView r;
        r.HostIndex = hi;
        r.PBufferRole = roleName(VChunkConfig.GetPBufferRole(hi));
        r.DDiskRole = roleName(VChunkConfig.GetDDiskRole(hi));
        r.Enabled = !disabled.Test(hi);
        r.Watermark = VChunkConfig.GetWatermark(hi);
        s.Roles.push_back(std::move(r));
    }
    return s;
}
```

> Note: confirm `THostMask::Test(THostIndex)` is the membership predicate while implementing (see `model/host_mask.h`); if the method is named differently (e.g. `Has`/`Contains`), use that. `GetDisabledHosts()` returns enabled-complement, hence `Enabled = !disabled.Test(hi)`.

- [ ] **Step 3: Build**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo $PD 2>&1 | tail -20"
```

- [ ] **Step 4: Commit**

```bash
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk.cpp
git commit -m "partition_direct mon: vchunk snapshot"
```

---

## Task 5: `IDirectBlockGroup::GatherMonSnapshot` (+ impl + mock)

**Files:**
- Modify: `$PD/direct_block_group.h` (interface method)
- Modify: `$PD/direct_block_group_impl.h` (override decl + private helper)
- Modify: `$PD/direct_block_group_impl.cpp` (impl)
- Modify: `$PD/direct_block_group_mock.h` / `direct_block_group_mock.cpp` (stub)

- [ ] **Step 1: Add to the interface in `direct_block_group.h`**

```cpp
// in class IDirectBlockGroup, near Dump():
    // Read-only structured snapshot of this DBG (connections, oracle host
    // state, per-vchunk dirty-map state). Resolves on the executor thread.
    virtual NThreading::TFuture<TDbgSnapshot> GatherMonSnapshot() = 0;
```
Add include at top of `direct_block_group.h`:
```cpp
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>
```

- [ ] **Step 2: Declare the override in `direct_block_group_impl.h`**

```cpp
// public, near Dump():
    NThreading::TFuture<TDbgSnapshot> GatherMonSnapshot() override;
private:
    TDbgSnapshot DoBuildMonSnapshot();   // runs on executor thread
```

- [ ] **Step 3: Implement in `direct_block_group_impl.cpp`**

Mirror the existing `TDirectBlockGroup::Dump()` executor-marshaling (find `Dump()` in this file — it posts a callback onto `Executor` and fulfills a promise). Replace its body's dump construction with:

```cpp
NThreading::TFuture<TDbgSnapshot> TDirectBlockGroup::GatherMonSnapshot()
{
    auto promise = NThreading::NewPromise<TDbgSnapshot>();
    auto future = promise.GetFuture();
    Executor->Execute([this, promise]() mutable {
        promise.SetValue(DoBuildMonSnapshot());
    });
    return future;
}

TDbgSnapshot TDirectBlockGroup::DoBuildMonSnapshot()
{
    ExecutorThreadChecker.CheckThread();

    TDbgSnapshot s;
    s.Index = DirectBlockGroupIndex;

    // Connections: DDisk session states + PBuffer connectivity per host.
    const size_t hostCount = DDiskConnections.size();
    s.Connections.reserve(hostCount);
    for (size_t host = 0; host < hostCount; ++host) {
        TConnSnapshot c;
        c.HostIndex = static_cast<THostIndex>(host);
        switch (DDiskConnections[host].SessionState) {
            case EDDiskSessionState::NotLocked: c.DDiskSession = "NotLocked"; break;
            case EDDiskSessionState::Locked: c.DDiskSession = "Locked"; break;
            case EDDiskSessionState::Broken: c.DDiskSession = "Broken"; break;
        }
        c.PBufferConnected =
            host < PBufferConnections.size() &&
            PBufferConnections[host].GetFuture().HasValue();
        // Ids: format from the host connection descriptor. Use the connection's
        // ddisk/pbuffer id accessor (see THostConnection in storage_transport.h);
        // fall back to empty string if not exposed.
        c.DDiskId = DDiskConnections[host].HostConnection.DebugId();
        c.PBufferId = (host < PBufferConnections.size())
            ? PBufferConnections[host].HostConnection.DebugId()
            : TString();
        s.Connections.push_back(std::move(c));
    }

    // Oracle host stats.
    s.Hosts = Oracle.BuildHostSnapshots(TInstant::Now());

    // Per-vchunk snapshots (skip expired weak ptrs).
    s.VChunks.reserve(VChunks.size());
    for (const auto& weak: VChunks) {
        if (auto vchunk = weak.lock()) {
            s.VChunks.push_back(vchunk->BuildSnapshot());
        }
    }
    return s;
}
```

> Notes while implementing:
> - Confirm the exact executor-post API by copying it from the existing `Dump()` in the same file (it may be `Executor->Execute(...)`, `Schedule(...)`, or a coroutine spawn). Use the identical mechanism so threading is correct.
> - `THostConnection::DebugId()` is a guess for an id-printer. Open `storage_transport/storage_transport.h`; use whatever public accessor prints the ddisk/pbuffer id, or build the string from the proto id the connection was constructed with. If none exists, store the index-derived label `Sprintf("dbg=%zu host=%zu", DirectBlockGroupIndex, host)` so the column is still meaningful. Do NOT add a new public method to `THostConnection` for this — keep the change local.
> - `EDDiskSessionState` and `DDiskConnections`/`PBufferConnections` are private members of `TDirectBlockGroup` (see `direct_block_group_impl.h`) — accessible here.

- [ ] **Step 4: Add the mock stub in `direct_block_group_mock.h/.cpp`**

```cpp
// direct_block_group_mock.h, in the mock class:
    NThreading::TFuture<TDbgSnapshot> GatherMonSnapshot() override;
```
```cpp
// direct_block_group_mock.cpp:
NThreading::TFuture<TDbgSnapshot> TDirectBlockGroupMock::GatherMonSnapshot()
{
    return NThreading::MakeFuture(TDbgSnapshot{});
}
```
> Match the mock's actual class name and existing return-style (some mocks use `MakeFuture`, others a stored promise). Check how the mock implements `Dump()` and follow it.

- [ ] **Step 5: Build**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo $PD 2>&1 | tail -30"
```
Expected: builds clean (interface addition forces mock + impl to compile).

- [ ] **Step 6: Commit**

```bash
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group_impl.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group_impl.cpp \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group_mock.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group_mock.cpp
git commit -m "partition_direct mon: per-DBG snapshot gather"
```

---

## Task 6: `TFastPathService::GatherMonSnapshot` (aggregation)

**Files:**
- Modify: `$PD/fast_path_service.h`
- Modify: `$PD/fast_path_service.cpp`

- [ ] **Step 1: Declare in `fast_path_service.h`**

```cpp
// add include at top:
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>
```
```cpp
// public:
    NThreading::TFuture<TMonSnapshot> GatherMonSnapshot();
    [[nodiscard]] ui64 GetLsnCounter() const;
    [[nodiscard]] std::optional<ui64> GetLastSafeBarrier() const;
```
```cpp
// private members, near CleanupGather:
    struct TMonGather
    {
        NThreading::TPromise<TMonSnapshot> Promise;
        TMonSnapshot Snapshot;
        std::atomic<size_t> Pending{0};
    };
    std::shared_ptr<TMonGather> ActiveMonGather;  // guarded by DumpLock
    // last barrier passed to MaybeTriggerPBufferCleanup / computed in
    // FinishPBufferCleanup; set there, read by GetLastSafeBarrier().
    std::atomic<ui64> LastSafeBarrier{0};
    std::atomic<bool> HasLastSafeBarrier{false};
```

- [ ] **Step 2: Implement in `fast_path_service.cpp`**

```cpp
ui64 TFastPathService::GetLsnCounter() const
{
    return SequenceGenerator.load();
}

std::optional<ui64> TFastPathService::GetLastSafeBarrier() const
{
    if (!HasLastSafeBarrier.load()) {
        return std::nullopt;
    }
    return LastSafeBarrier.load();
}

NThreading::TFuture<TMonSnapshot> TFastPathService::GatherMonSnapshot()
{
    auto gather = std::make_shared<TMonGather>();
    gather->Promise = NThreading::NewPromise<TMonSnapshot>();
    gather->Snapshot.LsnCounter = GetLsnCounter();
    gather->Snapshot.GlobalSafeBarrier = GetLastSafeBarrier();
    gather->Snapshot.Dbgs.resize(DirectBlockGroups.size());
    gather->Pending.store(DirectBlockGroups.size());
    auto future = gather->Promise.GetFuture();

    if (DirectBlockGroups.empty()) {
        gather->Promise.SetValue(std::move(gather->Snapshot));
        return future;
    }

    for (size_t i = 0; i < DirectBlockGroups.size(); ++i) {
        DirectBlockGroups[i]->GatherMonSnapshot().Subscribe(
            [gather, i](NThreading::TFuture<TDbgSnapshot> f)
            {
                gather->Snapshot.Dbgs[i] = f.GetValue();
                if (gather->Pending.fetch_sub(1) == 1) {
                    gather->Promise.SetValue(std::move(gather->Snapshot));
                }
            });
    }
    return future;
}
```
> This mirrors the `GatherSafeBarrierForErase`/`OnGatherSafeBarrierForErase` countdown but keeps per-call state in a heap `TMonGather` (so concurrent mon requests don't clobber each other), rather than the single `CleanupGather` member. `SequenceGenerator` is the existing `std::atomic<ui64>`.

- [ ] **Step 3: Populate `LastSafeBarrier` in `FinishPBufferCleanup`**

In `TFastPathService::FinishPBufferCleanup()` (existing), after the barrier `globalMin` is computed (the value broadcast to `BarrierEraseFromPBuffer`), add:
```cpp
    LastSafeBarrier.store(barrier);     // 'barrier' = the computed global minimum
    HasLastSafeBarrier.store(true);
```
> Use the actual local variable name holding the computed barrier in that function. If cleanup computes "nothing to erase", leave `HasLastSafeBarrier` as-is.

- [ ] **Step 4: Build**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo $PD 2>&1 | tail -30"
```

- [ ] **Step 5: Commit**

```bash
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.cpp
git commit -m "partition_direct mon: aggregate snapshot across DBGs"
```

---

## Task 7: `RenderMonPage` (pure renderer) + unit tests

**Files:**
- Create: `$PD/monitoring/mon_render.h`
- Create: `$PD/monitoring/mon_render.cpp`
- Test: `$PD/monitoring/mon_render_ut.cpp`

- [ ] **Step 1: Write the failing test (`mon_render_ut.cpp`)**

```cpp
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_render.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(TMonRenderTest)
{
    TMonPageData MakeData()
    {
        TMonPageData d;
        d.Header.TabletId = 42;
        d.Header.Generation = 7;
        d.Header.DiskId = "vol-1";
        d.Header.State = "WORK";
        d.Header.Uptime = TDuration::Seconds(125);

        d.Db.HasVolumeConfig = true;
        d.Db.VolumeConfigText = "BlockSize: 4096";
        d.Db.VChunkConfigs.push_back({0, "vchunk#0 default"});

        TMonSnapshot snap;
        snap.LsnCounter = 100;
        snap.GlobalSafeBarrier = 90;
        TDbgSnapshot dbg;
        dbg.Index = 0;
        THostSnapshot h;
        h.Index = 0;
        h.State = EHostState::Online;
        h.Health = EHostHealthView::Online;
        h.InflightByOp[static_cast<size_t>(EOperation::WriteToPBuffer)] = 3;
        dbg.Hosts.push_back(h);
        TVChunkSnapshot vc;
        vc.Index = 0;
        vc.HostCount = 1;
        vc.SafeBarrier = 88;
        vc.Counts.Inflight = 2;
        dbg.VChunks.push_back(vc);
        snap.Dbgs.push_back(dbg);
        d.Runtime = snap;
        return d;
    }

    Y_UNIT_TEST(RendersAllSections)
    {
        const TString html = RenderMonPage(MakeData());
        UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "Local DB");
        UNIT_ASSERT_STRING_CONTAINS(html, "Direct Block Groups");
        UNIT_ASSERT_STRING_CONTAINS(html, "Hosts");
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunks");
        UNIT_ASSERT_STRING_CONTAINS(html, "Barriers");
        UNIT_ASSERT_STRING_CONTAINS(html, "vol-1");
        UNIT_ASSERT_STRING_CONTAINS(html, "WriteToPBuffer");
    }

    Y_UNIT_TEST(EscapesHtml)
    {
        TMonPageData d = MakeData();
        d.Db.VolumeConfigText = "<script>alert(1)</script>";
        const TString html = RenderMonPage(d);
        UNIT_ASSERT(!html.Contains("<script>alert(1)</script>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "&lt;script&gt;");
    }

    Y_UNIT_TEST(ShowsRuntimeErrorBanner)
    {
        TMonPageData d = MakeData();
        d.Runtime.reset();
        d.RuntimeError = "tablet is initializing";
        const TString html = RenderMonPage(d);
        UNIT_ASSERT_STRING_CONTAINS(html, "tablet is initializing");
    }
}

}   // namespace ...
```

- [ ] **Step 2: Run the test, expect FAIL (no `mon_render.h`)**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo -tA $PD/monitoring 2>&1 | tail -20"
```
Expected: compile failure (header missing) — confirms the test target picks up the file once created.

- [ ] **Step 3: Write `mon_render.h`**

```cpp
#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>

#include <util/generic/string.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// Pure function: render the full read-only monitoring page as HTML.
[[nodiscard]] TString RenderMonPage(const TMonPageData& data);

}   // namespace ...
```

- [ ] **Step 4: Write `mon_render.cpp`**

```cpp
#include "mon_render.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/string/cast.h>
#include <util/string/subst.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

TString HtmlEscape(TStringBuf in)
{
    TString s(in);
    SubstGlobal(s, "&", "&amp;");
    SubstGlobal(s, "<", "&lt;");
    SubstGlobal(s, ">", "&gt;");
    SubstGlobal(s, "\"", "&quot;");
    return s;
}

const char* OperationName(EOperation op)
{
    switch (op) {
        case EOperation::ReadFromPBuffer: return "ReadFromPBuffer";
        case EOperation::ReadFromDDisk: return "ReadFromDDisk";
        case EOperation::WriteToPBuffer: return "WriteToPBuffer";
        case EOperation::WriteToManyPBuffers: return "WriteToManyPBuffers";
        case EOperation::WriteToDDisk: return "WriteToDDisk";
        case EOperation::Flush: return "Flush";
        case EOperation::FlushCrossNode: return "FlushCrossNode";
        case EOperation::Erase: return "Erase";
        case EOperation::BarrierErase: return "BarrierErase";
        case EOperation::Count_: return "?";
    }
    return "?";
}

const char* HostStateName(EHostState s)
{
    switch (s) {
        case EHostState::Online: return "Online";
        case EHostState::TemporaryOffline: return "TemporaryOffline";
        case EHostState::Offline: return "Offline";
    }
    return "?";
}

const char* HealthName(EHostHealthView h)
{
    switch (h) {
        case EHostHealthView::Online: return "Online";
        case EHostHealthView::Sufferer: return "Sufferer";
        case EHostHealthView::TemporaryOffline: return "TemporaryOffline";
        case EHostHealthView::Offline: return "Offline";
    }
    return "?";
}

TString OptLsn(const std::optional<ui64>& v)
{
    return v ? ToString(*v) : TString("-");
}

void RenderHeader(IOutputStream& str, const THeaderInfo& h)
{
    HTML(str) {
        TAG(TH3) { str << "partition_direct tablet " << h.TabletId; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "TabletId"; } TABLED() { str << h.TabletId; } }
                TABLER() { TABLED() { str << "Generation"; } TABLED() { str << h.Generation; } }
                TABLER() { TABLED() { str << "DiskId"; } TABLED() { str << HtmlEscape(h.DiskId); } }
                TABLER() { TABLED() { str << "State"; } TABLED() { str << HtmlEscape(h.State); } }
                TABLER() { TABLED() { str << "Uptime"; } TABLED() { str << h.Uptime.ToString(); } }
            }
        }
    }
}

void RenderOverview(IOutputStream& str, const TMonPageData& d)
{
    HTML(str) {
        TAG(TH3) { str << "Overview"; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                if (d.Runtime) {
                    const auto& r = *d.Runtime;
                    TABLER() { TABLED() { str << "DirectBlockGroups"; } TABLED() { str << r.Dbgs.size(); } }
                    size_t vchunks = 0;
                    for (const auto& dbg : r.Dbgs) { vchunks += dbg.VChunks.size(); }
                    TABLER() { TABLED() { str << "VChunks"; } TABLED() { str << vchunks; } }
                    TABLER() { TABLED() { str << "LSN counter"; } TABLED() { str << r.LsnCounter; } }
                    TABLER() { TABLED() { str << "Global safe barrier"; } TABLED() { str << OptLsn(r.GlobalSafeBarrier); } }
                } else {
                    TABLER() { TABLED() { str << "Runtime"; } TABLED() { str << "unavailable"; } }
                }
            }
        }
    }
}

void RenderRuntimeBanner(IOutputStream& str, const TMonPageData& d)
{
    if (d.Runtime || !d.RuntimeError) {
        return;
    }
    HTML(str) {
        DIV_CLASS("alert alert-warning") {
            str << "Runtime state unavailable: " << HtmlEscape(*d.RuntimeError);
        }
    }
}

void RenderLocalDb(IOutputStream& str, const TDbContents& db)
{
    HTML(str) {
        TAG(TH3) { str << "Local DB"; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "StorageConfig"; } TABLED() { str << HtmlEscape(db.StorageConfigText); } }
                TABLER() { TABLED() { str << "VolumeConfig"; }
                    TABLED() { str << (db.HasVolumeConfig ? "" : "(none)"); str << "<pre>" << HtmlEscape(db.VolumeConfigText) << "</pre>"; } }
                TABLER() { TABLED() { str << "DirectBlockGroupsConnections"; }
                    TABLED() { str << (db.HasConnections ? "" : "(none)"); str << "<pre>" << HtmlEscape(db.ConnectionsText) << "</pre>"; } }
            }
        }
        TAG(TH4) { str << "VChunkConfigs (persisted overrides)"; }
        TABLE_CLASS("table table-condensed") {
            TABLEHEAD() { TABLER() { TABLEH() { str << "VChunkIndex"; } TABLEH() { str << "Config"; } } }
            TABLEBODY() {
                for (const auto& row : db.VChunkConfigs) {
                    TABLER() {
                        TABLED() { str << row.VChunkIndex; }
                        TABLED() { str << "<pre>" << HtmlEscape(row.Summary) << "</pre>"; }
                    }
                }
            }
        }
    }
}

void RenderConnections(IOutputStream& str, const TMonSnapshot& r, const TCgiFilters& f)
{
    HTML(str) {
        TAG(TH3) { str << "Direct Block Groups"; }
        for (const auto& dbg : r.Dbgs) {
            if (f.Dbg && *f.Dbg != dbg.Index) { continue; }
            TAG(TH4) { str << "DBG #" << dbg.Index; }
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() { TABLER() {
                    TABLEH() { str << "Host"; }
                    TABLEH() { str << "DDisk id"; }
                    TABLEH() { str << "PBuffer id"; }
                    TABLEH() { str << "DDisk session"; }
                    TABLEH() { str << "PBuffer connected"; }
                } }
                TABLEBODY() {
                    for (const auto& c : dbg.Connections) {
                        TABLER() {
                            TABLED() { str << (int)c.HostIndex; }
                            TABLED() { str << HtmlEscape(c.DDiskId); }
                            TABLED() { str << HtmlEscape(c.PBufferId); }
                            TABLED() { str << HtmlEscape(c.DDiskSession); }
                            TABLED() { str << (c.PBufferConnected ? "yes" : "no"); }
                        }
                    }
                }
            }
        }
    }
}

void RenderHosts(IOutputStream& str, const TMonSnapshot& r, const TCgiFilters& f)
{
    HTML(str) {
        TAG(TH3) { str << "Hosts"; }
        for (const auto& dbg : r.Dbgs) {
            if (f.Dbg && *f.Dbg != dbg.Index) { continue; }
            TAG(TH4) { str << "DBG #" << dbg.Index << " hosts"; }
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() { TABLER() {
                    TABLEH() { str << "Host"; }
                    TABLEH() { str << "State"; }
                    TABLEH() { str << "Health"; }
                    TABLEH() { str << "PBuffer used"; }
                    TABLEH() { str << "Errors"; }
                    for (size_t op = 0; op < OperationCount; ++op) {
                        TABLEH() { str << OperationName(static_cast<EOperation>(op)); }
                    }
                } }
                TABLEBODY() {
                    for (const auto& h : dbg.Hosts) {
                        TABLER() {
                            TABLED() { str << (int)h.Index; }
                            TABLED() { str << HostStateName(h.State); }
                            TABLED() { str << HealthName(h.Health); }
                            TABLED() { str << h.PBufferUsedSize; }
                            TABLED() { str << h.Errors.ErrorCount; }
                            for (size_t op = 0; op < OperationCount; ++op) {
                                TABLED() { str << h.InflightByOp[op]; }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderVChunks(IOutputStream& str, const TMonSnapshot& r, const TCgiFilters& f)
{
    HTML(str) {
        TAG(TH3) { str << "VChunks"; }
        for (const auto& dbg : r.Dbgs) {
            if (f.Dbg && *f.Dbg != dbg.Index) { continue; }
            for (const auto& vc : dbg.VChunks) {
                if (f.VChunk && *f.VChunk != vc.Index) { continue; }
                TAG(TH4) { str << "DBG #" << dbg.Index << " vchunk #" << vc.Index; }
                TABLE_CLASS("table table-condensed") {
                    TABLEBODY() {
                        TABLER() { TABLED() { str << "Safe barrier"; } TABLED() { str << OptLsn(vc.SafeBarrier); } }
                        TABLER() { TABLED() { str << "Inflight"; } TABLED() { str << vc.Counts.Inflight; } }
                        TABLER() { TABLED() { str << "Flush pending"; } TABLED() { str << vc.Counts.FlushPending; } }
                        TABLER() { TABLED() { str << "Erase pending"; } TABLED() { str << vc.Counts.ErasePending; } }
                        TABLER() { TABLED() { str << "Erase belated"; } TABLED() { str << vc.Counts.EraseBelated; } }
                    }
                }
                TAG(TH5) { str << "Roles / DDisk / PBuffer per host"; }
                TABLE_CLASS("table table-condensed") {
                    TABLEHEAD() { TABLER() {
                        TABLEH() { str << "Host"; }
                        TABLEH() { str << "PBuffer role"; }
                        TABLEH() { str << "DDisk role"; }
                        TABLEH() { str << "Enabled"; }
                        TABLEH() { str << "Watermark"; }
                        TABLEH() { str << "DDisk state"; }
                        TABLEH() { str << "OperBlocks"; }
                        TABLEH() { str << "PB cur recs"; }
                        TABLEH() { str << "PB cur bytes"; }
                        TABLEH() { str << "PB locked recs"; }
                    } }
                    TABLEBODY() {
                        for (size_t host = 0; host < vc.HostCount; ++host) {
                            TABLER() {
                                TABLED() { str << host; }
                                TABLED() { str << (host < vc.Roles.size() ? vc.Roles[host].PBufferRole : TString("-")); }
                                TABLED() { str << (host < vc.Roles.size() ? vc.Roles[host].DDiskRole : TString("-")); }
                                TABLED() { str << (host < vc.Roles.size() && vc.Roles[host].Enabled ? "yes" : "no"); }
                                TABLED() { str << (host < vc.Roles.size() ? OptLsn(vc.Roles[host].Watermark) : TString("-")); }
                                TABLED() { str << (host < vc.DDiskStates.size() ? vc.DDiskStates[host].State : TString("-")); }
                                TABLED() { str << (host < vc.DDiskStates.size() ? ToString(vc.DDiskStates[host].OperationalBlockCount) : TString("-")); }
                                TABLED() { str << (host < vc.PBuffers.size() ? ToString(vc.PBuffers[host].CurrentRecords) : TString("-")); }
                                TABLED() { str << (host < vc.PBuffers.size() ? ToString(vc.PBuffers[host].CurrentBytes) : TString("-")); }
                                TABLED() { str << (host < vc.PBuffers.size() ? ToString(vc.PBuffers[host].CurrentLockedRecords) : TString("-")); }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderBarriers(IOutputStream& str, const TMonSnapshot& r)
{
    HTML(str) {
        TAG(TH3) { str << "Barriers"; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "LSN generator"; } TABLED() { str << r.LsnCounter; } }
                TABLER() { TABLED() { str << "Global safe barrier"; } TABLED() { str << OptLsn(r.GlobalSafeBarrier); } }
            }
        }
        TABLE_CLASS("table table-condensed") {
            TABLEHEAD() { TABLER() {
                TABLEH() { str << "DBG"; } TABLEH() { str << "VChunk"; } TABLEH() { str << "Safe barrier"; }
            } }
            TABLEBODY() {
                for (const auto& dbg : r.Dbgs) {
                    for (const auto& vc : dbg.VChunks) {
                        TABLER() {
                            TABLED() { str << dbg.Index; }
                            TABLED() { str << vc.Index; }
                            TABLED() { str << OptLsn(vc.SafeBarrier); }
                        }
                    }
                }
            }
        }
    }
}

}   // namespace

TString RenderMonPage(const TMonPageData& data)
{
    TStringStream str;
    HTML(str) {
        // Simple auto-refresh (full reload). Phase 2 replaces with AJAX.
        str << "<script>setTimeout(function(){location.reload();}, 5000);</script>";
    }
    RenderHeader(str, data.Header);
    RenderRuntimeBanner(str, data);
    RenderOverview(str, data);
    RenderLocalDb(str, data.Db);
    if (data.Runtime) {
        RenderConnections(str, *data.Runtime, data.Filters);
        RenderHosts(str, *data.Runtime, data.Filters);
        RenderVChunks(str, *data.Runtime, data.Filters);
        RenderBarriers(str, *data.Runtime);
    }
    return str.Str();
}

}   // namespace ...
```

- [ ] **Step 5: Create `monitoring/ya.make` (or extend an existing test target)**

If `$PD/monitoring/` needs its own library + test, create `$PD/monitoring/ya.make`:
```
LIBRARY()
SRCS(
    mon_render.cpp
)
PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
    library/cpp/monlib/service/pages
)
END()

RECURSE_FOR_TESTS(
    ut
)
```
and `$PD/monitoring/ut/ya.make`:
```
UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring)
SRCS(
    mon_render_ut.cpp
)
END()
```
> Match the exact `ya.make` idioms used elsewhere in `$PD` (open `$PD/ya.make` and `$PD/ut/ya.make` and copy their macro style, OWNER/SUBSCRIBER lines, etc.). Simpler alternative: skip a sub-library, add `monitoring/mon_render.cpp` to `$PD/ya.make`'s `SRCS` and `monitoring/mon_render_ut.cpp` to the existing `$PD/ut/ya.make` (or wherever `partition_direct_ut.cpp` lives). Prefer the simpler alternative unless the directory already follows the sub-library pattern.

- [ ] **Step 6: Run the renderer tests, expect PASS**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo -tA $PD/monitoring -F '*TMonRenderTest*' 2>&1 | tail -30"
```
Expected: 3 tests PASS. (If using the simpler alternative, filter against the `$PD/ut` target.)

- [ ] **Step 7: Commit**

```bash
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/
git commit -m "partition_direct mon: pure HTML renderer + tests"
```

---

## Task 8: Actor wiring — `NMon::TEvRemoteHttpInfo`, `TTxMonitoring`, snapshot join

**Files:**
- Modify: `$PD/part_tx.h`
- Modify: `$PD/partition_direct_events_private.h`
- Modify: `$PD/partition_direct_actor.h`
- Create: `$PD/part_monitoring.cpp`
- Modify: `$PD/partition_direct_actor.cpp` (StateWork routing)
- Modify: `$PD/ya.make`

- [ ] **Step 1: Add the `Monitoring` transaction to `part_tx.h`**

In `BLOCKSTORE_PARTITION_TRANSACTIONS`:
```cpp
#define BLOCKSTORE_PARTITION_TRANSACTIONS(xxx, ...) \
    xxx(InitSchema, __VA_ARGS__)                    \
    xxx(LoadState, __VA_ARGS__)                     \
    xxx(StoreVolumeConfig, __VA_ARGS__)             \
    xxx(StorePartitionIds, __VA_ARGS__)             \
    xxx(UpdateVChunkConfig, __VA_ARGS__)            \
    xxx(AddHostToDBG, __VA_ARGS__)                  \
    xxx(Monitoring, __VA_ARGS__)
```
Add the args struct inside `struct TTxPartition`:
```cpp
    //
    // Monitoring (read-only): collects persisted rows for the mon page.
    //
    struct TMonitoring
    {
        const ui64 Cookie;   // correlates with the TMonRequest map in the actor

        TMaybe<NKikimrBlockStore::TVolumeConfig> VolumeConfig;
        TMaybe<TDirectBlockGroupsConnections> DirectBlockGroupsConnections;
        TVector<TVChunkConfig> VChunkConfigs;

        explicit TMonitoring(ui64 cookie)
            : Cookie(cookie)
        {}

        void Clear()
        {
            VolumeConfig.Clear();
            DirectBlockGroupsConnections.Clear();
            VChunkConfigs.clear();
        }
    };
```

- [ ] **Step 2: Add private events in `partition_direct_events_private.h`**

Follow the existing event-declaration idiom in that file (open it; events likely declared via an `EvXxx` enum + `struct TEvXxx`). Add:
```cpp
    // mon snapshot gathered off the executor; carries the structured runtime
    // state back to the actor for rendering.
    struct TEvMonSnapshotReady
        : public NActors::TEventLocal<TEvMonSnapshotReady, EvMonSnapshotReady>
    {
        ui64 Cookie;
        TMonSnapshot Snapshot;

        TEvMonSnapshotReady(ui64 cookie, TMonSnapshot snapshot)
            : Cookie(cookie)
            , Snapshot(std::move(snapshot))
        {}
    };

    struct TEvMonRenderTimeout
        : public NActors::TEventLocal<TEvMonRenderTimeout, EvMonRenderTimeout>
    {
        ui64 Cookie;
        explicit TEvMonRenderTimeout(ui64 cookie) : Cookie(cookie) {}
    };
```
Add `EvMonSnapshotReady` and `EvMonRenderTimeout` to that file's private event enum, and `#include` `monitoring/mon_snapshot.h`.

- [ ] **Step 3: Extend `partition_direct_actor.h`**

Add includes (`<ydb/library/actors/core/mon.h>` for `NMon`, `monitoring/mon_snapshot.h`). Inside `TPartitionActor`, private section:
```cpp
    struct TMonRequest
    {
        NActors::TActorId Requester;
        ui64 SubRequestId = 0;
        TCgiFilters Filters;
        std::optional<TMonSnapshot> Runtime;
        std::optional<TString> RuntimeError;
        // DB contents, filled by TTxMonitoring::Complete.
        TDbContents Db;
        bool DbReady = false;
        bool Replied = false;
    };

    ui64 MonCookieCounter = 0;
    THashMap<ui64, TMonRequest> MonRequests;

    void HandleHttpInfo(
        NMon::TEvRemoteHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleMonSnapshotReady(
        const TEvPartitionDirectPrivate::TEvMonSnapshotReady::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleMonRenderTimeout(
        const TEvPartitionDirectPrivate::TEvMonRenderTimeout::TPtr& ev,
        const NActors::TActorContext& ctx);
    void MaybeReplyMon(const NActors::TActorContext& ctx, ui64 cookie);
```
The `TMonitoring` transaction's `Prepare/Execute/Complete` are declared by the `BLOCKSTORE_PARTITION_TRANSACTIONS` macro expansion already present at the bottom of the class — no extra declaration needed.

- [ ] **Step 4: Create `part_monitoring.cpp`**

```cpp
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_render.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <ydb/library/actors/core/mon.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

TCgiFilters ParseFilters(const TCgiParameters& params)
{
    TCgiFilters f;
    size_t dbg = 0;
    if (params.Has("dbg") && TryFromString(params.Get("dbg"), dbg)) {
        f.Dbg = dbg;
    }
    ui32 vchunk = 0;
    if (params.Has("vchunk") && TryFromString(params.Get("vchunk"), vchunk)) {
        f.VChunk = vchunk;
    }
    return f;
}

TString StateName(/* pass the actor's EState */ int state);   // mapped below

}   // namespace

void TPartitionActor::HandleHttpInfo(
    NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ++MonCookieCounter;

    TMonRequest req;
    req.Requester = ev->Sender;
    req.SubRequestId = ev->Get()->GetSubRequestId();   // confirm accessor name
    req.Filters = ParseFilters(ev->Get()->Cgi());
    MonRequests[cookie] = std::move(req);

    // 1) read persisted rows.
    ExecuteTx(ctx, CreateTx<TMonitoring>(cookie));

    // 2) timeout guard.
    ctx.Schedule(
        TDuration::Seconds(5),
        new TEvPartitionDirectPrivate::TEvMonRenderTimeout(cookie));
}

bool TPartitionActor::PrepareMonitoring(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMonitoring& args)
{
    Y_UNUSED(ctx);
    TPartitionDatabase db(tx.DB);
    std::initializer_list<bool> results = {
        db.ReadVolumeConfig(args.VolumeConfig),
        db.ReadDirectBlockGroupsConnections(args.DirectBlockGroupsConnections),
        db.ReadAllVChunkConfigs(args.VChunkConfigs),
    };
    return std::accumulate(
        results.begin(), results.end(), true, std::logical_and<>());
}

void TPartitionActor::ExecuteMonitoring(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMonitoring& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteMonitoring(
    const TActorContext& ctx,
    TTxPartition::TMonitoring& args)
{
    auto it = MonRequests.find(args.Cookie);
    if (it == MonRequests.end()) {
        return;   // timed out already
    }
    auto& req = it->second;

    if (args.VolumeConfig.Defined()) {
        req.Db.HasVolumeConfig = true;
        req.Db.VolumeConfigText = args.VolumeConfig->DebugString();
    }
    if (args.DirectBlockGroupsConnections.Defined()) {
        req.Db.HasConnections = true;
        req.Db.ConnectionsText = args.DirectBlockGroupsConnections->DebugString();
    }
    if (StorageConfig) {
        req.Db.StorageConfigText = "loaded";   // keep short; see note
    }
    for (const auto& cfg : args.VChunkConfigs) {
        req.Db.VChunkConfigs.push_back({cfg.GetVChunkIndex(), cfg.DebugPrint()});
    }
    req.DbReady = true;

    // Now gather runtime state (if the service is up).
    if (FastPathService) {
        const ui64 cookie = args.Cookie;
        FastPathService->GatherMonSnapshot().Subscribe(
            [actorId = ctx.SelfID, cookie](
                NThreading::TFuture<TMonSnapshot> f) mutable
            {
                TActivationContext::Send(
                    new IEventHandle(
                        actorId,
                        actorId,
                        new TEvPartitionDirectPrivate::TEvMonSnapshotReady(
                            cookie, f.ExtractValue())));
            });
    } else {
        req.RuntimeError = "tablet is still initializing (no FastPathService)";
        MaybeReplyMon(ctx, args.Cookie);
    }
}

void TPartitionActor::HandleMonSnapshotReady(
    const TEvPartitionDirectPrivate::TEvMonSnapshotReady::TPtr& ev,
    const TActorContext& ctx)
{
    auto it = MonRequests.find(ev->Get()->Cookie);
    if (it == MonRequests.end()) {
        return;
    }
    it->second.Runtime = std::move(ev->Get()->Snapshot);
    MaybeReplyMon(ctx, ev->Get()->Cookie);
}

void TPartitionActor::HandleMonRenderTimeout(
    const TEvPartitionDirectPrivate::TEvMonRenderTimeout::TPtr& ev,
    const TActorContext& ctx)
{
    auto it = MonRequests.find(ev->Get()->Cookie);
    if (it == MonRequests.end() || it->second.Replied) {
        return;
    }
    if (!it->second.Runtime) {
        it->second.RuntimeError = "runtime snapshot timed out";
    }
    // Force a reply with whatever we have (DB may or may not be ready).
    it->second.DbReady = true;
    MaybeReplyMon(ctx, ev->Get()->Cookie);
}

void TPartitionActor::MaybeReplyMon(const TActorContext& ctx, ui64 cookie)
{
    auto it = MonRequests.find(cookie);
    if (it == MonRequests.end()) {
        return;
    }
    auto& req = it->second;
    if (req.Replied) {
        return;
    }
    // Reply when DB is ready AND (runtime present OR runtime error set).
    const bool runtimeResolved = req.Runtime.has_value() || req.RuntimeError.has_value();
    if (!req.DbReady || !runtimeResolved) {
        return;
    }

    TMonPageData data;
    data.Header.TabletId = TabletID();
    data.Header.Generation = Executor()->Generation();
    data.Header.DiskId = VolumeConfig.GetDiskId();
    data.Header.State = "WORK";   // map from the actor's EState; see note
    data.Header.Uptime = TDuration::Zero();   // optional; see note
    data.Db = std::move(req.Db);
    data.Runtime = std::move(req.Runtime);
    data.RuntimeError = std::move(req.RuntimeError);
    data.Filters = req.Filters;

    const TString html = RenderMonPage(data);
    ctx.Send(
        req.Requester,
        new NMon::TEvRemoteHttpInfoRes(html));
    req.Replied = true;
    MonRequests.erase(it);
}

}   // namespace ...
```

> Notes while implementing (verify against the actual APIs, do NOT invent):
> - `ev->Get()->Cgi()` / `GetSubRequestId()` / the reply ctor `NMon::TEvRemoteHttpInfoRes(html)`: confirm exact names against `ydb/library/actors/core/mon.h`. The datashard handler in `ydb/core/tx/datashard/datashard__monitoring.cpp` is a working reference for the same event — copy its accessor + reply usage.
> - `StorageConfig`/`VolumeConfig`/`FastPathService` are existing `TPartitionActor` members (see `partition_direct_actor.h`). `FastPathService` is a `std::shared_ptr` — truthiness check is valid.
> - State string: map the actor's `EState` (STATE_BOOT/INIT/WORK/ZOMBIE). The actor sets state via `Become`; if there is no stored enum, hardcode "WORK" here and refine later — keep it a single literal, not a TODO.
> - `Executor()->Generation()` / `TabletID()` come from `TTabletExecutedFlat`/`TTabletBase`. Confirm `Generation()` accessor name; if absent, drop the Generation row rather than inventing.
> - Posting the future result back to the actor: use the codebase's standard "future → self event" bridge. The shown `TActivationContext::Send(new IEventHandle(...))` is the generic form; if the file already has a helper for this, use it.

- [ ] **Step 5: Register events in `StateWork` (`partition_direct_actor.cpp`)**

Add inside the `switch` in `STFUNC(TPartitionActor::StateWork)` (before `default:`):
```cpp
        HFunc(NMon::TEvRemoteHttpInfo, HandleHttpInfo);
        HFunc(
            TEvPartitionDirectPrivate::TEvMonSnapshotReady,
            HandleMonSnapshotReady);
        HFunc(
            TEvPartitionDirectPrivate::TEvMonRenderTimeout,
            HandleMonRenderTimeout);
```
Add `#include <ydb/library/actors/core/mon.h>` to `partition_direct_actor.cpp` if not already pulled in via the header.

- [ ] **Step 6: Add `part_monitoring.cpp` to `$PD/ya.make`**

Add `part_monitoring.cpp` (and `monitoring/mon_render.cpp`, `monitoring/mon_snapshot.cpp` if a .cpp was created) to the `SRCS(...)` list of `$PD/ya.make`. Add `library/cpp/monlib/service/pages` to `PEERDIR` if not present.

- [ ] **Step 7: Build the whole partition_direct target**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo $PD 2>&1 | tail -40"
```
Expected: builds clean.

- [ ] **Step 8: Commit**

```bash
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_events_private.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_monitoring.cpp \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.cpp \
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/ya.make
git commit -m "partition_direct mon: actor wiring for read-only mon page"
```

---

## Task 9: Integration test — mon request against the tablet

**Files:**
- Modify: `$PD/partition_direct_ut.cpp` (add a test) — or the test fixture file `base_test_fixture.*` if a tablet harness lives there.

- [ ] **Step 1: Write the test**

Use the existing partition_direct test fixture (see `partition_direct_ut.cpp` / `base_test_fixture.h` for how a tablet is booted and pipe/requests are sent). Add:
```cpp
Y_UNIT_TEST_F(MonPageRendersInWorkState, TBaseFixture)
{
    // Boot the tablet to WORK state using the fixture's existing helper.
    // ... (mirror an existing test's setup that reaches WORK) ...

    auto sender = Runtime.AllocateEdgeActor();
    Runtime.SendToPipe(
        TabletId,
        sender,
        new NMon::TEvRemoteHttpInfo("/app?TabletID=" + ToString(TabletId)));

    auto response = Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(sender);
    UNIT_ASSERT(response);
    const TString& html = response->Get()->Html;   // confirm field accessor
    UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
    UNIT_ASSERT_STRING_CONTAINS(html, "Direct Block Groups");
    UNIT_ASSERT_STRING_CONTAINS(html, "Barriers");
}
```
> Confirm against the fixture: tablet-id member name, how to send a tablet-targeted event (`SendToPipe` vs `Send`), and the `TEvRemoteHttpInfoRes` payload accessor (`Html` vs `Answer`). Mirror an existing test in `partition_direct_ut.cpp` that already sends an event to the tablet and grabs a response.

- [ ] **Step 2: Run, expect PASS**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo -tA $PD -F '*MonPageRendersInWorkState*' 2>&1 | tail -40"
```
Expected: PASS. If it reports `0 tests` / SKIPPED, the build dir is stale — flush sync and re-check (see header).

- [ ] **Step 3: Commit**

```bash
git add ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_ut.cpp
git commit -m "partition_direct mon: integration test for mon page"
```

---

## Task 10: Full build + test sweep

- [ ] **Step 1: Build + run the whole partition_direct test suite**

```bash
ssh cloud "cd ~/ydb_partition_ui && ./ya make --build relwithdebinfo -tA $PD 2>&1 | tail -60"
```
Expected: all tests pass (no regressions from the interface change to `IDirectBlockGroup`).

- [ ] **Step 2: Manual sanity (optional)**

If a local cluster is available, open the tablet's monitoring page in the YDB viewer and confirm sections render. Otherwise rely on the integration test.

- [ ] **Step 3: Final commit / summary**

```bash
git log --oneline -12
```
Leave the branch ready; do not push or open a PR unless the user asks.

---

## Self-Review notes (addressed)

- **Spec coverage:** Local DB (Task 8 `TTxMonitoring` + Task 7 Local DB section); barriers (Task 4 per-vchunk + Task 6 global + Barriers render section); stats (Task 3 oracle host stats + Hosts render); dirty-map/DDisk/PBuffer (Tasks 2/4/7 VChunks section); DBG topology/connections (Task 5 + Connections section); read-only (no mutating handlers added); error handling (runtime banner + timeout in Task 8); tests (Tasks 7 + 9).
- **Type consistency:** `RenderMonPage(const TMonPageData&)`, `BuildSnapshot()`/`GetCountsSnapshot()`/`GetDDiskStatesSnapshot()`/`GetPBufferCountersSnapshot()`/`BuildHostSnapshots(TInstant)`/`GatherMonSnapshot()` names are used identically across tasks. View struct field names match between `mon_snapshot.h` (Task 1), the builders (Tasks 2-6), and the renderer (Task 7).
- **Known confirm-at-implementation points** (flagged inline, not placeholders): exact executor-post API in `Dump()`, `THostMask` membership method name, `THostConnection` id printer, `NMon` accessor/reply names, fixture send/grab API, `ya.make` macro style. Each has a concrete fallback so no step is left open.
- **Phase 2 (out of scope, not in any task):** action buttons, AJAX, filter checkboxes, SVG, latency percentiles, `TVolumeCounters` enrichment.
