#include "fast_path_service.h"
#include "partition_direct_actor.h"
#include "partition_direct_events_private.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareAddHostToDBG(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddHostToDBG& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteAddHostToDBG(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddHostToDBG& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    db.StoreDirectBlockGroupsConnections(args.DirectBlockGroupsConnections);
}

void TPartitionActor::CompleteAddHostToDBG(
    const TActorContext& ctx,
    TTxPartition::TAddHostToDBG& args)
{
    // NB: the in-memory DirectBlockGroupsConnections cache was already
    // advanced in HandleControllerAllocateDDiskBlockGroupResult (before this
    // tx was queued). Do NOT reassign it here from args -- args is an older
    // snapshot and would drop hosts added concurrently to other DBGs.

    // The new connection is now durable. Hand off the rest of the work
    // (adding the host to the DBG, notifying vchunks, replying to the
    // requester).
    OnAddHostPersisted(
        ctx,
        args.DirectBlockGroupId,
        args.NewHostIndex,
        args.NewDDiskId,
        args.NewPBufferId,
        args.Requester);
}

void TPartitionActor::OnAddHostPersisted(
    const TActorContext& ctx,
    size_t dbgId,
    THostIndex newHostIndex,
    NKikimrBlobStorage::NDDisk::TDDiskId newDDiskId,
    NKikimrBlobStorage::NDDisk::TDDiskId newPBufferId,
    const TActorId& requester)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s AddHost persisted dbgId=%lu newHostIndex=%u",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        static_cast<ui32>(newHostIndex));

    if (!FastPathService) {
        AddHostsInFlight.erase(dbgId);
        ReplyAddHostError(
            ctx,
            requester,
            dbgId,
            E_REJECTED,
            "FastPathService is not running yet");
        return;
    }

    const auto& directBlockGroups = FastPathService->GetDirectBlockGroups();
    if (dbgId >= directBlockGroups.size()) {
        AddHostsInFlight.erase(dbgId);
        ReplyAddHostError(
            ctx,
            requester,
            dbgId,
            E_FAIL,
            "Direct Block Group disappeared");
        return;
    }

    // DBG state mutations run on the DBG's own Executor thread, so schedule
    // the append there. AddHost cannot fail (the new host is added as a
    // disabled spare and unused until explicitly enabled), and connections
    // are warmed up in the background, so we reply OK right away.
    auto dbgPtr = directBlockGroups[dbgId];
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple(
        [dbgPtr,
         newHostIndex,
         newDDiskId = std::move(newDDiskId),
         newPBufferId = std::move(newPBufferId)]() mutable
        {
            dbgPtr->AddHost(
                newHostIndex,
                std::move(newDDiskId),
                std::move(newPBufferId));
        });

    AddHostsInFlight.erase(dbgId);

    if (requester) {
        auto response = std::make_unique<
            TEvPartitionDirectPrivate::TEvAddHostToDBGResponse>(
            NProto::TError{},
            dbgId,
            newHostIndex);
        ctx.Send(requester, response.release());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
