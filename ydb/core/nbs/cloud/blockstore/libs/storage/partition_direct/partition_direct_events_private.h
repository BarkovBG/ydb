#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

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
        EvAddHostToDBG,
        EvAddHostToDBGResponse,

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

    // Sent by the Oracle (or any internal admin path) to ask the partition
    // to extend a specific Direct Block Group by one host. The partition
    // contacts BSController, persists the new host list and notifies all
    // registered VChunks through TDirectBlockGroup::AddHost.
    struct TEvAddHostToDBG
        : public NActors::TEventLocal<TEvAddHostToDBG, EvAddHostToDBG>
    {
        size_t DirectBlockGroupId;
        // For idempotency / retry safety: the requester's view of the current
        // host count; the partition rejects the request if its own view does
        // not match.
        ui32 ExpectedCurrentHostCount;

        TEvAddHostToDBG(size_t dbgId, ui32 expectedCurrentHostCount)
            : DirectBlockGroupId(dbgId)
            , ExpectedCurrentHostCount(expectedCurrentHostCount)
        {}
    };

    struct TEvAddHostToDBGResponse
        : public NActors::
              TEventLocal<TEvAddHostToDBGResponse, EvAddHostToDBGResponse>
    {
        NProto::TError Error;
        size_t DirectBlockGroupId;
        // Only meaningful when !HasError(Error).
        THostIndex NewHostIndex = InvalidHostIndex;

        TEvAddHostToDBGResponse(
            NProto::TError error,
            size_t dbgId,
            THostIndex newHostIndex)
            : Error(std::move(error))
            , DirectBlockGroupId(dbgId)
            , NewHostIndex(newHostIndex)
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
