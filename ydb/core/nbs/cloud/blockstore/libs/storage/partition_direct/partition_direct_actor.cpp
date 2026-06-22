#include "partition_direct_actor.h"

#include "direct_block_group_impl.h"
#include "fast_path_service.h"
#include "load_actor_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <util/system/fs.h>

#include <unistd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NActors;

TPartitionActor::TPartitionActor(
    const TActorId& tablet,
    NKikimr::TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletBase<TPartitionActor>(
          tablet,
          NKikimr::TTabletStorageInfoPtr(info),
          nullptr)
    , LogTitle{GetCycleCount(), TLogTitle::TPartitionDirect{.TabletId = TabletID()}}
    , StorageConfig(GetNbsService()->StorageConfig)
{
    LOG_INFO(
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s TPartitionActor: initialization started",
        LogTitle.GetWithTime().c_str());
}

TPartitionActor::~TPartitionActor() = default;

void TPartitionActor::PassAway()
{
    LOG_INFO(
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "TPartitionActor: before detach");
}

void TPartitionActor::OnDetach(const TActorContext& ctx)
{
    Die(ctx);
}

void TPartitionActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

void TPartitionActor::OnActivateExecutor(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Started NBS partition: actor id %s",
        LogTitle.GetWithTime().c_str(),
        SelfId().ToString().data());

    if (!Executor()->GetStats().IsFollower()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Executing InitSchema transaction",
            LogTitle.GetWithTime().c_str());
        ExecuteTx(ctx, CreateTx<TInitSchema>());
    }

    // allow pipes to connect
    SignalTabletActive(ctx);
}

void TPartitionActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
}

void TPartitionActor::ReportTabletState(const TActorContext& ctx)
{
    auto service =
        NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<
        NNodeWhiteboard::TEvWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
        TabletID(),
        STATE_WORK);

    NYdb::NBS::Send(ctx, service, std::move(request));
}

void TPartitionActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Pipe client %s server %s connected to volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TPartitionActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Pipe client %s server %s disconnected from volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TPartitionActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Pipe client %s server %s got destroyed for volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::StateInit(TAutoPtr<NActors::IEventHandle>& ev)
{
    StateInitImpl(ev, SelfId());
}

TVector<IDirectBlockGroupPtr> TPartitionActor::CreateDirectBlockGroups(
    TDirectBlockGroupsConnections directBlockGroupsConnections)
{
    const auto nbsService = GetNbsService();
    TVector<IDirectBlockGroupPtr> directBlockGroups;
    auto executors =
        nbsService->ExecutorPool.GetExecutors(DirectBlockGroupsCount);

    for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
        const auto& conn =
            directBlockGroupsConnections.GetDirectBlockGroupConnections(i);
        TVector<NBsController::TDDiskId> ddiskIds;
        for (const auto& connection: conn.GetConnections()) {
            ddiskIds.push_back(
                NBsController::TDDiskId(connection.GetDDiskId()));
        }
        TVector<NBsController::TDDiskId> persistentBufferDDiskIds;
        for (const auto& connection: conn.GetConnections()) {
            persistentBufferDDiskIds.push_back(NBsController::TDDiskId(
                connection.GetPersistentBufferDDiskId()));
        }

        auto directBlockGroup = std::make_shared<TDirectBlockGroup>(
            TActivationContext::ActorSystem(),
            nbsService->StorageConfig,
            executors[i],
            VolumeConfig.GetDiskId(),
            TabletID(),
            Executor()->Generation(),   // generation
            i,                          // direct block group index
            std::move(ddiskIds),
            std::move(persistentBufferDDiskIds),
            std::make_unique<NTransport::TICStorageTransport>(
                TActivationContext::ActorSystem()));

        directBlockGroups.emplace_back(std::move(directBlockGroup));
    }

    return directBlockGroups;
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::CreateBSControllerPipeClient(
    const NActors::TActorContext& ctx)
{
    BSControllerPipeClient = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
}

void TPartitionActor::AllocateDDiskBlockGroup(const NActors::TActorContext& ctx)
{
    CreateBSControllerPipeClient(ctx);

    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig->GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig->GetPersistentBufferDDiskPoolName());

    // TODO: fill with tablet id
    request->Record.SetTabletId(TabletID());

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    const ui64 regionsCount =
        AlignUp(blockCount * VolumeConfig.GetBlockSize(), RegionSize) /
        RegionSize;

    for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
        auto* query = request->Record.AddQueries();
        query->SetDirectBlockGroupId(i);
        query->SetTargetNumVChunks(regionsCount);
    }

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TPartitionActor::Start(
    const NActors::TActorContext& ctx,
    TDirectBlockGroupsConnections directBlockGroupsConnections,
    TVector<TVChunkConfig> vChunkConfigs)
{
    LogTitle.SetDiskId(VolumeConfig.GetDiskId());
    LogTitle.SetGeneration(Executor()->Generation());

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Starting",
        LogTitle.GetWithTime().c_str());

    auto nbsService = GetNbsService();
    Y_ABORT_UNLESS(nbsService);
    Y_ABORT_UNLESS(nbsService->Scheduler);
    Y_ABORT_UNLESS(nbsService->Timer);

    TVChunkConfigByIndex vChunkConfigsByIndex;
    vChunkConfigsByIndex.reserve(vChunkConfigs.size());
    for (const auto& cfg: vChunkConfigs) {
        vChunkConfigsByIndex[cfg.GetVChunkIndex()] = cfg;
    }

    // Stash the persisted connections so AddHost can read the current host
    // count and build an incremental update from them.
    DirectBlockGroupsConnections = directBlockGroupsConnections;

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    FastPathService = std::make_shared<TFastPathService>(
        TActivationContext::ActorSystem(),
        SelfId(),
        TabletID(),
        VolumeConfig.GetDiskId(),
        blockCount,
        VolumeConfig.GetBlockSize(),
        CreateDirectBlockGroups(std::move(directBlockGroupsConnections)),
        std::move(vChunkConfigsByIndex),
        StorageConfig,
        nbsService->Scheduler,
        nbsService->Timer,
        AppData()->Counters);

    // Synchronous start mode - requests pass as the initial quorum of Locked
    // DDisk sessions across all DBGs is achieved.
    // TODO: make optional via StorageConfig after implementation of async mode.
    FastPathService->Run().Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = SelfId()]   //
        (const NThreading::TFuture<void>&) mutable
        {
            // This callback runs OUTSIDE the actor thread - on the DBG's
            // executor-thread
            auto event = std::make_unique<
                TEvPartitionDirectPrivate::TEvFastPathServiceReady>();
            actorSystem->Send(selfId, event.release());
        });
}

void TPartitionActor::HandleFastPathServiceReady(
    const TEvPartitionDirectPrivate::TEvFastPathServiceReady::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s All DBGs reached initial locked quorum, opening endpoint",
        LogTitle.GetWithTime().c_str());

    LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, FastPathService);

    {
        auto service = GetNbsService();

        const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
        TString socketPath = "/tmp/" + VolumeConfig.GetDiskId() + ".sock";
        NVhost::TStorageOptions options{
            .DiskId = VolumeConfig.GetDiskId(),
            .ClientId = "client-1",
            .BlockSize = VolumeConfig.GetBlockSize(),
            .StripeSize = StorageConfig->GetStripeSize(),
            .BlocksCount = blockCount,
            .VChunkSize = StorageConfig->GetVChunkSize(),
            .VhostQueuesCount = StorageConfig->GetVhostQueuesCount()};
        service->VhostServer->StartEndpoint(
            std::move(socketPath),
            FastPathService,
            FastPathService,
            options);
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Started NBS LoadActorAdapter: %s",
        LogTitle.GetWithTime().c_str(),
        LoadActorAdapter.ToString().c_str());
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s HandleControllerAllocateDDiskBlockGroupResult record is: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.DebugString().data());

    // The initial allocation response arrives while DdiskBlockGroupAllocated
    // is still false. Every later allocate response is an add-host: the flag
    // is set true after the initial allocate and restored from the DB on
    // restart.
    if (DdiskBlockGroupAllocated) {
        // Add-host response. BSController echoes our request cookie, which we
        // set to the DirectBlockGroupId.
        const size_t dbgId = ev->Cookie;
        auto it = AddHostsInFlight.find(dbgId);
        if (it == AddHostsInFlight.end()) {
            LOG_WARN(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s AddHost response for unknown dbgId=%lu (stale)",
                LogTitle.GetWithTime().c_str(),
                dbgId);
            return;
        }

        const auto requester = it->second.Requester;
        const auto expectedCurrent = it->second.ExpectedCurrentHostCount;
        const auto newHostIndex = it->second.NewHostIndex;
        NTabletPipe::CloseClient(ctx, it->second.BSPipeClient);

        if (msg->Record.GetStatus() != NKikimrProto::EReplyStatus::OK) {
            LOG_ERROR(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s AddHost: BSController returned error: %d, reason: %s",
                LogTitle.GetWithTime().c_str(),
                msg->Record.GetStatus(),
                msg->Record.GetErrorReason().data());
            ReplyAddHostError(
                ctx,
                requester,
                dbgId,
                E_REJECTED,
                TStringBuilder()
                    << "BSController error: " << msg->Record.GetErrorReason());
            AddHostsInFlight.erase(it);
            return;
        }

        // The add-host request is sent via DirectBlockGroupOperations, so
        // BSController returns the group descriptor in DirectBlockGroups (the
        // legacy Responses/Nodes shape is only filled for the initial Queries
        // path).
        const int groupCount =
            static_cast<int>(msg->Record.DirectBlockGroupsSize());
        int groupIdx = -1;
        for (int i = 0; i < groupCount; ++i) {
            if (msg->Record.GetDirectBlockGroups(i).GetDirectBlockGroupId() ==
                dbgId)
            {
                groupIdx = i;
                break;
            }
        }
        if (groupIdx < 0) {
            ReplyAddHostError(
                ctx,
                requester,
                dbgId,
                E_FAIL,
                "BSController response is missing the DirectBlockGroup");
            AddHostsInFlight.erase(it);
            return;
        }

        const auto& group = msg->Record.GetDirectBlockGroups(groupIdx);
        if (group.GetError()) {
            ReplyAddHostError(
                ctx,
                requester,
                dbgId,
                E_FAIL,
                "BSController reported an error for this DirectBlockGroup");
            AddHostsInFlight.erase(it);
            return;
        }
        if (static_cast<ui32>(group.DDiskIdSize()) != expectedCurrent + 1 ||
            static_cast<ui32>(group.PersistentBufferDDiskIdSize()) !=
                expectedCurrent + 1)
        {
            ReplyAddHostError(
                ctx,
                requester,
                dbgId,
                E_FAIL,
                TStringBuilder()
                    << "BSController returned " << group.DDiskIdSize()
                    << " ddisks / " << group.PersistentBufferDDiskIdSize()
                    << " pbuffers, expected " << expectedCurrent + 1);
            AddHostsInFlight.erase(it);
            return;
        }

        // Append-only invariant: TDefineDirectBlockGroup never removes hosts,
        // so the one new disk sits at index expectedCurrent with no holes; the
        // size check above guarantees the slot exists. This must be revisited
        // if host removal is ever added.
        auto newDDiskId = group.GetDDiskId(expectedCurrent);
        auto newPBufferId = group.GetPersistentBufferDDiskId(expectedCurrent);

        // Build the updated connections from the current cache and adopt it
        // in memory right away (before the tx commits). This keeps concurrent
        // add-hosts on *other* DBGs from snapshotting a stale cache and
        // clobbering this DBG's new host when their own tx persists the
        // whole connections blob.
        TDirectBlockGroupsConnections updated = DirectBlockGroupsConnections;
        auto* dbgConn = updated.MutableDirectBlockGroupConnections(dbgId);
        auto* connection = dbgConn->AddConnections();
        connection->MutableDDiskId()->CopyFrom(newDDiskId);
        connection->MutablePersistentBufferDDiskId()->CopyFrom(newPBufferId);
        DirectBlockGroupsConnections = updated;

        // Keep the in-flight entry until the tx commits
        // (CompleteAddHostToDBG erases it), so a concurrent add-host on the
        // same DBG stays rejected.
        ExecuteTx(
            ctx,
            CreateTx<TAddHostToDBG>(
                std::move(updated),
                dbgId,
                newHostIndex,
                std::move(newDDiskId),
                std::move(newPBufferId),
                requester));
        return;
    }

    // Initial allocation response.
    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::OK) {
        Y_ABORT_UNLESS(
            msg->Record.GetResponses().size() == DirectBlockGroupsCount);

        TDirectBlockGroupsConnections ids;
        for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
            auto* directBlockGroupConnections =
                ids.AddDirectBlockGroupConnections();
            const auto& response = msg->Record.GetResponses()[i];
            for (const auto& node: response.GetNodes()) {
                auto* connection =
                    directBlockGroupConnections->AddConnections();
                connection->MutableDDiskId()->CopyFrom(node.GetDDiskId());
                connection->MutablePersistentBufferDDiskId()->CopyFrom(
                    node.GetPersistentBufferDDiskId());
            }
        }

        DdiskBlockGroupAllocated = true;
        ExecuteTx(ctx, CreateTx<TStorePartitionIds>(std::move(ids)));
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s HandleControllerAllocateDDiskBlockGroupResult finished with "
            "error: %d, reason: %s",
            LogTitle.GetWithTime().c_str(),
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }

    NTabletPipe::CloseClient(ctx, BSControllerPipeClient);
}

void TPartitionActor::ReplyAddHostError(
    const NActors::TActorContext& ctx,
    const NActors::TActorId& requester,
    size_t dbgId,
    ui32 errorCode,
    TString message)
{
    if (!requester) {
        return;
    }
    auto response =
        std::make_unique<TEvPartitionDirectPrivate::TEvAddHostToDBGResponse>(
            MakeError(errorCode, std::move(message)),
            dbgId,
            InvalidHostIndex);
    ctx.Send(requester, response.release());
}

void TPartitionActor::HandleAddHostToDBG(
    const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto requester = ev->Sender;
    const auto dbgId = msg->DirectBlockGroupId;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle AddHostToDBG dbgId=%lu expectedCurrentHostCount=%u",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        msg->ExpectedCurrentHostCount);

    if (!DdiskBlockGroupAllocated) {
        ReplyAddHostError(
            ctx,
            requester,
            dbgId,
            E_REJECTED,
            "DDiskBlockGroup is not allocated yet");
        return;
    }

    if (AddHostsInFlight.contains(dbgId)) {
        ReplyAddHostError(
            ctx,
            requester,
            dbgId,
            E_REJECTED,
            "AddHost is already in flight for this DBG");
        return;
    }

    if (dbgId >=
        static_cast<size_t>(
            DirectBlockGroupsConnections.DirectBlockGroupConnectionsSize()))
    {
        ReplyAddHostError(
            ctx,
            requester,
            dbgId,
            E_ARGUMENT,
            TStringBuilder() << "DirectBlockGroupId out of range: " << dbgId);
        return;
    }

    const auto& dbgConn =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);
    const auto currentSize = static_cast<ui32>(dbgConn.GetConnections().size());

    if (currentSize != msg->ExpectedCurrentHostCount) {
        ReplyAddHostError(
            ctx,
            requester,
            dbgId,
            E_ABORTED,
            TStringBuilder() << "Host count mismatch (idempotency check): "
                             << "current=" << currentSize
                             << ", expected=" << msg->ExpectedCurrentHostCount);
        return;
    }

    if (currentSize >= MaxHostCount) {
        ReplyAddHostError(
            ctx,
            requester,
            dbgId,
            E_PRECONDITION_FAILED,
            TStringBuilder() << "MaxHostCount=" << MaxHostCount << " reached");
        return;
    }
    if (currentSize == 0) {
        ReplyAddHostError(
            ctx,
            requester,
            dbgId,
            E_FAIL,
            "AddHost on an empty DBG is not supported");
        return;
    }

    // Reuse a per-DBG VChunk count: the first VChunk's chunk claim equals the
    // existing NumChunksPerDDisk for that group. We do not have direct access
    // to it here, so use the same target we used at allocation time (regions
    // count from VolumeConfig).
    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    const ui64 regionsCount =
        AlignUp(blockCount * VolumeConfig.GetBlockSize(), RegionSize) /
        RegionSize;

    // Create a dedicated BSController pipe for this request so that several
    // concurrent add-hosts (on different DBGs) do not clobber each other.
    const auto pipe = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));

    AddHostsInFlight[dbgId] = TAddHostInFlight{
        .Requester = requester,
        .NewHostIndex = static_cast<THostIndex>(currentSize),
        .ExpectedCurrentHostCount = currentSize,
        .BSPipeClient = pipe,
    };

    // The request cookie is set to dbgId so the echoed response correlates
    // back to this request (BSController preserves the request cookie).
    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig->GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig->GetPersistentBufferDDiskPoolName());
    request->Record.SetTabletId(TabletID());

    auto* op = request->Record.AddDirectBlockGroupOperations();
    op->SetDirectBlockGroupId(dbgId);
    auto* define = op->MutableDefineDirectBlockGroup();
    define->SetNumDDisks(currentSize + 1);
    define->SetNumChunksPerDDisk(regionsCount);
    define->SetNumPersistentBuffers(currentSize + 1);

    NTabletPipe::SendData(ctx, pipe, request.release(), /* cookie = */ dbgId);
}

void TPartitionActor::HandleGetLoadActorAdapterActorId(
    const TEvService::TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvService::TEvGetLoadActorAdapterActorIdResponse>();
    response->Record.SetActorId(LoadActorAdapter.ToString());
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVolumeConfig request. Version: %d",
        LogTitle.GetWithTime().c_str(),
        msg->Record.GetVolumeConfig().GetVersion());

    if (DdiskBlockGroupAllocated) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Already has ddisk connections",
            LogTitle.GetWithTime().c_str());

        auto response = std::make_unique<
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
        response->Record.SetStatus(NKikimrBlockStore::ERROR);
        ctx.Send(ev->Sender, response.release());
        return;
    }

    const auto& volumeConfig = msg->Record.GetVolumeConfig();
    Y_ABORT_UNLESS(volumeConfig.PartitionsSize() == 1);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVolumeConfig request VolumeConfig: %s",
        LogTitle.GetWithTime().c_str(),
        volumeConfig.DebugString().c_str());

    ExecuteTx(ctx, CreateTx<TStoreVolumeConfig>(volumeConfig));

    // Send response back to volume
    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(msg->Record.GetTxId());
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(NKikimrBlockStore::OK);

    LOG_INFO(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s Sending UpdateVolumeConfig response OK",
        LogTitle.GetWithTime().c_str());

    ctx.Send(ev->Sender, response.release());
}

void TPartitionActor::HandleUpdateVChunkConfig(
    const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& cfg = ev->Get()->VChunkConfig;

    LOG_DEBUG_S(
        ctx,
        NKikimrServices::NBS_PARTITION,
        LogTitle.GetWithTime().c_str()
            << " Handle UpdateVChunkConfig, vChunkIndex: "
            << cfg.GetVChunkIndex());

    ExecuteTx(ctx, CreateTx<TUpdateVChunkConfig>(std::move(cfg)));
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s Processing event: %s from sender: %lu",
        LogTitle.GetWithTime().c_str(),
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult,
            HandleControllerAllocateDDiskBlockGroupResult);
        HFunc(
            TEvService::TEvGetLoadActorAdapterActorIdRequest,
            HandleGetLoadActorAdapterActorId);
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfig);
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateVChunkConfig,
            HandleUpdateVChunkConfig);
        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceReady,
            HandleFastPathServiceReady);
        HFunc(TEvPartitionDirectPrivate::TEvAddHostToDBG, HandleAddHostToDBG);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG_S(
                    TActivationContext::AsActorContext(),
                    NKikimrServices::NBS_PARTITION,
                    "Unhandled event type: " << ev->GetTypeRewrite()
                                             << " event: " << ev->ToString());
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
