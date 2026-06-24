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
    TCgiFilters filters;
    ui32 vchunk = 0;
    if (params.Has("vchunk") && TryFromString(params.Get("vchunk"), vchunk)) {
        filters.VChunk = vchunk;
    }
    return filters;
}

EMonPage ParsePage(const TCgiParameters& params)
{
    const TString pageParam = params.Get("page");
    if (pageParam == "hosts") {
        return EMonPage::Hosts;
    }
    if (pageParam == "configs") {
        return EMonPage::Configs;
    }
    if (pageParam == "connections") {
        return EMonPage::Connections;
    }
    if (pageParam == "vchunk") {
        return EMonPage::VChunk;
    }
    return EMonPage::Overview;
}

// Only the Configs page needs the persisted local-DB rows; everything else is
// served from the runtime snapshot.
bool PageNeedsDb(EMonPage page)
{
    return page == EMonPage::Configs;
}

bool PageNeedsRuntime(EMonPage page)
{
    return page != EMonPage::Configs;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo(
    NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ++MonCookieCounter;

    const auto& cgi = ev->Get()->Cgi();
    TMonRequest req;
    req.Requester = ev->Sender;
    req.Page = ParsePage(cgi);
    req.Filters = ParseFilters(cgi);
    if (!PageNeedsDb(req.Page)) {
        req.DbReady = true;
    }
    MonRequests[cookie] = std::move(req);
    const EMonPage page = MonRequests[cookie].Page;

    // Local-DB rows (Configs page only).
    if (PageNeedsDb(page)) {
        ExecuteTx(ctx, CreateTx<TMonitoring>(cookie));
    }

    // Runtime snapshot (everything except Configs). Tablet-wide values are read
    // synchronously; per-DBG detail (Hosts / Connections / a single VChunk) is
    // gathered by kicking the owning DBG(s), each of which replies with a
    // TEvMonDbgSnapshotReady that HandleMonDbgSnapshotReady fans in by cookie.
    if (PageNeedsRuntime(page)) {
        if (FastPathService) {
            auto& monReq = MonRequests[cookie];
            TMonSnapshot snapshot;
            snapshot.LsnCounter = FastPathService->GetLsnCounter();
            snapshot.GlobalSafeBarrier = FastPathService->GetLastSafeBarrier();
            snapshot.TotalVChunks = FastPathService->GetTotalVChunkCount();
            snapshot.DbgCount = FastPathService->GetDirectBlockGroupCount();

            // Overview and the empty VChunk form need only the values above.
            const bool needsDbgs =
                page != EMonPage::Overview &&
                !(page == EMonPage::VChunk && !monReq.Filters.VChunk);
            if (needsDbgs) {
                snapshot.Dbgs.resize(snapshot.DbgCount);
                std::optional<ui32> vchunkIdx;
                if (page == EMonPage::VChunk) {
                    vchunkIdx = monReq.Filters.VChunk;
                }
                monReq.PendingDbgs = FastPathService->RequestMonSnapshot(
                    SelfId(),
                    cookie,
                    vchunkIdx);
            }
            monReq.Runtime = std::move(snapshot);
        } else {
            MonRequests[cookie].RuntimeError =
                "tablet is still initializing (no FastPathService)";
        }
    }

    // Reply now if nothing async is pending (e.g. Overview before BOOT, or a
    // page that needed neither DB nor runtime).
    MaybeReplyMon(ctx, cookie);

    // Timeout guard, so a stuck executor cannot hang the request forever.
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
    req.Db.StorageConfigText = StorageConfig ? "(loaded)" : "(none)";
    for (const auto& cfg: args.VChunkConfigs) {
        req.Db.VChunkConfigs.push_back(
            {cfg.GetVChunkIndex(), cfg.DebugPrint()});
    }
    req.DbReady = true;

    MaybeReplyMon(ctx, args.Cookie);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleMonDbgSnapshotReady(
    const TEvPartitionDirectPrivate::TEvMonDbgSnapshotReady::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto it = MonRequests.find(msg->Cookie);
    if (it == MonRequests.end()) {
        return;   // already replied or timed out
    }
    auto& req = it->second;
    if (req.Runtime && msg->DbgIndex < req.Runtime->Dbgs.size()) {
        req.Runtime->Dbgs[msg->DbgIndex] = std::move(msg->Snapshot);
    }
    if (req.PendingDbgs > 0) {
        --req.PendingDbgs;
    }
    MaybeReplyMon(ctx, msg->Cookie);
}

void TPartitionActor::HandleMonRenderTimeout(
    const TEvPartitionDirectPrivate::TEvMonRenderTimeout::TPtr& ev,
    const TActorContext& ctx)
{
    auto it = MonRequests.find(ev->Get()->Cookie);
    if (it == MonRequests.end() || it->second.Replied) {
        return;
    }
    auto& req = it->second;
    if (PageNeedsRuntime(req.Page) && req.PendingDbgs > 0) {
        // Some DBG(s) never answered; drop the partial snapshot and show the
        // banner instead.
        req.Runtime.reset();
        req.PendingDbgs = 0;
        req.RuntimeError = "runtime snapshot timed out";
    }
    // Reply with whatever is available (the DB read may still be pending; if
    // so, mark it ready so the page renders the local-DB section as empty).
    req.DbReady = true;
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
        !PageNeedsRuntime(req.Page) || req.RuntimeError.has_value() ||
        (req.Runtime.has_value() && req.PendingDbgs == 0);
    if (!req.DbReady || !runtimeResolved) {
        return;
    }

    TMonPageData data;
    data.Page = req.Page;
    data.Header.TabletId = TabletID();
    data.Header.Generation = Executor()->Generation();
    data.Header.DiskId = VolumeConfig.GetDiskId();
    data.Header.State = FastPathService ? "WORK" : "INIT";
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
