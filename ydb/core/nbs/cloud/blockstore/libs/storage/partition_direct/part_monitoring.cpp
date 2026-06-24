#include "fast_path_service.h"
#include "partition_direct_actor.h"
#include "partition_direct_events_private.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_render.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <ydb/library/actors/core/mon.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/string/cast.h>

#include <functional>
#include <numeric>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo(
    NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ++MonCookieCounter;

    TMonRequest req;
    req.Requester = ev->Sender;
    req.Filters = ParseFilters(ev->Get()->Cgi());
    MonRequests[cookie] = std::move(req);

    // 1) read persisted rows.
    ExecuteTx(ctx, CreateTx<TMonitoring>(cookie));

    // 2) timeout guard, so a stuck executor cannot hang the request forever.
    ctx.Schedule(
        TDuration::Seconds(5),
        new TEvPartitionDirectPrivate::TEvMonRenderTimeout(cookie));
}

////////////////////////////////////////////////////////////////////////////////

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
        results.begin(),
        results.end(),
        true,
        std::logical_and<>());
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
        return;   // already timed out and replied
    }
    auto& req = it->second;

    if (args.VolumeConfig.Defined()) {
        req.Db.HasVolumeConfig = true;
        req.Db.VolumeConfigText = args.VolumeConfig->DebugString();
    }
    if (args.DirectBlockGroupsConnections.Defined()) {
        req.Db.HasConnections = true;
        req.Db.ConnectionsText =
            args.DirectBlockGroupsConnections->DebugString();
    }
    req.Db.StorageConfigText = StorageConfig ? "(loaded)" : "(none)";
    for (const auto& cfg: args.VChunkConfigs) {
        req.Db.VChunkConfigs.push_back(
            {cfg.GetVChunkIndex(), cfg.DebugPrint()});
    }
    req.DbReady = true;

    if (FastPathService) {
        const ui64 cookie = args.Cookie;
        FastPathService->GatherMonSnapshot().Subscribe(
            [actorSystem = TActivationContext::ActorSystem(),
             selfId = SelfId(),
             cookie](const NThreading::TFuture<TMonSnapshot>& f) mutable
            {
                // Runs OUTSIDE the actor thread (on a DBG executor thread).
                auto event = std::make_unique<
                    TEvPartitionDirectPrivate::TEvMonSnapshotReady>(
                    cookie,
                    f.GetValue());
                actorSystem->Send(selfId, event.release());
            });
    } else {
        req.RuntimeError =
            "tablet is still initializing (no FastPathService)";
        MaybeReplyMon(ctx, args.Cookie);
    }
}

////////////////////////////////////////////////////////////////////////////////

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
    // Reply with whatever is available (the DB read may still be pending; if
    // so, mark it ready so the page renders the local-DB section as empty).
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

    const bool runtimeResolved =
        req.Runtime.has_value() || req.RuntimeError.has_value();
    if (!req.DbReady || !runtimeResolved) {
        return;
    }

    TMonPageData data;
    data.Header.TabletId = TabletID();
    data.Header.Generation = Executor()->Generation();
    data.Header.DiskId = VolumeConfig.GetDiskId();
    data.Header.State = FastPathService ? "WORK" : "INIT";
    data.Header.Uptime = TDuration::Zero();
    data.Db = std::move(req.Db);
    data.Runtime = std::move(req.Runtime);
    data.RuntimeError = std::move(req.RuntimeError);
    data.Filters = req.Filters;

    const TString html = RenderMonPage(data);
    ctx.Send(req.Requester, new NMon::TEvRemoteHttpInfoRes(html));

    MonRequests.erase(it);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
