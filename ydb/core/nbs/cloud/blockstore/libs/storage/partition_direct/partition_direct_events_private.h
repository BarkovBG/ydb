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
        EvFastPathServiceReady,

        EvFastPathServiceShutdown,
        EvFastPathServiceStopped,
        EvMonDbgSnapshotReady,
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
              TEventLocal<TEvFastPathServiceReady, EvFastPathServiceReady>
    {
    };

    // Triggers the shutdown of the fast path service
    struct TEvFastPathServiceShutdown
        : public NActors::
              TEventLocal<TEvFastPathServiceShutdown, EvFastPathServiceShutdown>
    {
    };

    // Signals that FastPathService stopped.
    struct TEvFastPathServiceStopped
        : public NActors::
              TEventLocal<TEvFastPathServiceStopped, EvFastPathServiceStopped>
    {
    };

    // Carries one DBG's runtime snapshot (built on its executor thread) back to
    // the actor, which fans them in by cookie and renders the monitoring page.
    struct TEvMonDbgSnapshotReady
        : public NActors::
              TEventLocal<TEvMonDbgSnapshotReady, EvMonDbgSnapshotReady>
    {
        ui64 Cookie;
        size_t DbgIndex;
        TDbgSnapshot Snapshot;

        TEvMonDbgSnapshotReady(
            ui64 cookie,
            size_t dbgIndex,
            TDbgSnapshot snapshot)
            : Cookie(cookie)
            , DbgIndex(dbgIndex)
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
