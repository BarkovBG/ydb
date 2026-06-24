#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TFastPathService;

////////////////////////////////////////////////////////////////////////////////

// Offset for the partition_direct actor's local-only events within
// ES_NBS_V2_SERVICE, kept clear of the public TEvService event IDs.
constexpr ui32 LocalEventsOffset = 1000;

// Local-only events for the partition_direct actor.
struct TEvPartitionDirectPrivate
{
    enum EEvents
    {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_NBS_V2_SERVICE) +
                  LocalEventsOffset,

        EvUpdateVChunkConfig,
        EvDBGsInitiallyReady,
        EvMonSnapshotReady,
        EvMonRenderTimeout,

        EvEnd,
    };

    struct TEvUpdateVChunkConfig
        : public NActors::
              TEventLocal<TEvUpdateVChunkConfig, EvUpdateVChunkConfig>
    {
        TVChunkConfig VChunkConfig;

        explicit TEvUpdateVChunkConfig(TVChunkConfig cfg)
            : VChunkConfig(std::move(cfg))
        {}
    };

    // Signals that FastPathServiceReady (and its DBGs) are ready.
    struct TEvFastPathServiceReady
        : public NActors::
              TEventLocal<TEvFastPathServiceReady, EvDBGsInitiallyReady>
    {
    };

    // Carries the structured runtime snapshot (gathered off the executor) back
    // to the actor for rendering the monitoring page.
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

    // Fires if the runtime snapshot gather does not complete in time; lets the
    // mon request reply with whatever data is available.
    struct TEvMonRenderTimeout
        : public NActors::TEventLocal<TEvMonRenderTimeout, EvMonRenderTimeout>
    {
        ui64 Cookie;

        explicit TEvMonRenderTimeout(ui64 cookie)
            : Cookie(cookie)
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
