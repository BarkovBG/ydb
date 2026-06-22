#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <util/generic/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadState& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    std::initializer_list<bool> results = {
        db.ReadVolumeConfig(args.VolumeConfig),
        db.ReadDirectBlockGroupsConnections(args.DirectBlockGroupsConnections),
        db.ReadAllVChunkConfigs(args.VChunkConfigs),
    };

    bool ready = std::accumulate(
        results.begin(),
        results.end(),
        true,
        std::logical_and<>());

    return ready;
}

void TPartitionActor::ExecuteLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxPartition::TLoadState& args)
{
    if (args.VolumeConfig.Defined()) {
        VolumeConfig = *args.VolumeConfig;

        if (args.DirectBlockGroupsConnections.Defined()) {
            // Connections restored from the DB mean initialization already
            // happened (possibly in a previous tablet generation). Mark it so
            // that subsequent allocate responses are treated as add-hosts and
            // a re-sent UpdateVolumeConfig does not re-allocate.
            DdiskBlockGroupAllocated = true;
            Start(
                ctx,
                std::move(*args.DirectBlockGroupsConnections),
                std::move(args.VChunkConfigs));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
