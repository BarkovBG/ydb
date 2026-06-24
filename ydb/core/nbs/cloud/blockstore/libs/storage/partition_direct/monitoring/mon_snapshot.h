#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>   // EOperation, OperationCount, THostStat::TErrorsInfo
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>   // EHostState

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>
#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

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
    TString DDiskId;        // formatted on the executor side
    TString PBufferId;      // formatted on the executor side
    TString DDiskSession;   // "NotLocked" / "Locked" / "Broken"
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
    TString State;   // "Disabled" / "Operational" / "Fresh"
    ui64 OperationalBlockCount = 0;
    std::optional<ui64> FreshWatermark;   // bytes offset; nullopt == full
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

////////////////////////////////////////////////////////////////////////////////
// Render input

struct THeaderInfo
{
    ui64 TabletId = 0;
    ui32 Generation = 0;
    TString DiskId;
    TString State;   // "BOOT" / "INIT" / "WORK" / "ZOMBIE"
    TDuration Uptime;
};

struct TVChunkConfigRow   // one persisted VChunkConfigs row
{
    ui32 VChunkIndex = 0;
    TString Summary;   // TVChunkConfig::DebugPrint()
};

struct TDbContents
{
    bool HasVolumeConfig = false;
    TString VolumeConfigText;   // VolumeConfig.DebugString()
    bool HasConnections = false;
    TString ConnectionsText;     // DirectBlockGroupsConnections.DebugString()
    TString StorageConfigText;   // short summary
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
    std::optional<TMonSnapshot>
        Runtime;   // absent in BOOT/INIT or on gather error
    std::optional<TString> RuntimeError;   // banner text when Runtime is absent
    TCgiFilters Filters;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
