#pragma once

#include "direct_block_group.h"
#include "part_counters.h"
#include "partition_direct_events_private.h"

#include <ydb/core/nbs/cloud/blockstore/config/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_pool.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/mon.h>

#include <util/generic/hash.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionActor
    : public NActors::TActor<TPartitionActor>
    , public TTabletBase<TPartitionActor>
{
    using TDirectBlockGroupsConnections =
        ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;

    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_ZOMBIE,
        STATE_MAX,
    };

private:
    TLogTitle LogTitle;
    TStorageConfigPtr StorageConfig;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;
    NActors::TActorId BSControllerPipeClient;

    NActors::TActorId LoadActorAdapter;
    bool DdiskBlockGroupAllocated = false;
    std::shared_ptr<TFastPathService> FastPathService;

    // Cached copy of the latest persisted Direct Block Groups Connections.
    // Mirrors what is written to TStorePartitionIds / TAddHostToDBG; used as
    // the starting point for an incremental AddHost request.
    TDirectBlockGroupsConnections DirectBlockGroupsConnections;

    // In-flight AddHost operations, keyed by DirectBlockGroupId. Several
    // add-hosts (on different Direct Block Groups) can run concurrently; a
    // second add-host for a DBG already present here is rejected.
    struct TAddHostInFlight
    {
        NActors::TActorId Requester;
        THostIndex NewHostIndex = InvalidHostIndex;
        ui32 ExpectedCurrentHostCount = 0;
        // Per-request BSC pipe client (kept separate from the shared
        // BSControllerPipeClient used by the initial allocation).
        NActors::TActorId BSPipeClient;
    };

    THashMap<size_t, TAddHostInFlight> AddHostsInFlight;

    // One in-flight monitoring (read-only) request: joins the local-DB read
    // (TTxMonitoring) with the async runtime snapshot, then renders + replies.
    struct TMonRequest
    {
        NActors::TActorId Requester;
        TCgiFilters Filters;
        TDbContents Db;
        std::optional<TMonSnapshot> Runtime;
        std::optional<TString> RuntimeError;
        bool DbReady = false;
        bool Replied = false;
    };

    ui64 MonCookieCounter = 0;
    THashMap<ui64, TMonRequest> MonRequests;

public:
    TPartitionActor(
        const NActors::TActorId& tablet,
        NKikimr::TTabletStorageInfo* info);

    ~TPartitionActor() override;
    void PassAway() override;

    static constexpr ui32 LogComponent = NKikimrServices::NBS_PARTITION;
    using TCounters = TPartitionCounters;

private:
    void StateInit(TAutoPtr<NActors::IEventHandle>& ev);
    STFUNC(StateWork);

    void OnDetach(const NActors::TActorContext& ctx) override;
    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDestroyed(
        const NKikimr::TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReportTabletState(const NActors::TActorContext& ctx);

    void CreateBSControllerPipeClient(const NActors::TActorContext& ctx);

    void AllocateDDiskBlockGroup(const NActors::TActorContext& ctx);

    void HandleControllerAllocateDDiskBlockGroupResult(
        const NKikimr::TEvBlobStorage::
            TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetLoadActorAdapterActorId(
        const NYdb::NBS::NBlockStore::TEvService::
            TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfig(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVChunkConfig(
        const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFastPathServiceReady(
        const TEvPartitionDirectPrivate::TEvFastPathServiceReady::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddHostToDBG(
        const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Post-persist step: forwards the added host to the DBG and replies to
    // the requester. Called directly from CompleteAddHostToDBG once the new
    // connection is durable.
    void OnAddHostPersisted(
        const NActors::TActorContext& ctx,
        size_t dbgId,
        THostIndex newHostIndex,
        NKikimrBlobStorage::NDDisk::TDDiskId newDDiskId,
        NKikimrBlobStorage::NDDisk::TDDiskId newPBufferId,
        const NActors::TActorId& requester);

    void ReplyAddHostError(
        const NActors::TActorContext& ctx,
        const NActors::TActorId& requester,
        size_t dbgId,
        ui32 errorCode,
        TString message);

    // Read-only tablet monitoring page (see part_monitoring.cpp).
    void HandleHttpInfo(
        NActors::NMon::TEvRemoteHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleMonSnapshotReady(
        const TEvPartitionDirectPrivate::TEvMonSnapshotReady::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleMonRenderTimeout(
        const TEvPartitionDirectPrivate::TEvMonRenderTimeout::TPtr& ev,
        const NActors::TActorContext& ctx);
    // Renders and replies once both the DB read and the runtime snapshot (or
    // its error/timeout) are available.
    void MaybeReplyMon(const NActors::TActorContext& ctx, ui64 cookie);

    void Start(
        const NActors::TActorContext& ctx,
        TDirectBlockGroupsConnections directBlockGroupsConnections,
        TVector<TVChunkConfig> vChunkConfigs);

    TVector<IDirectBlockGroupPtr> CreateDirectBlockGroups(
        TDirectBlockGroupsConnections directBlockGroupsConnections);

    BLOCKSTORE_PARTITION_TRANSACTIONS(
        BLOCKSTORE_IMPLEMENT_TRANSACTION,
        TTxPartition)
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
