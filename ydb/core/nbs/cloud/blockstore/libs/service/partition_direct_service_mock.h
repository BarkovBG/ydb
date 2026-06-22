#pragma once

#include "partition_direct_service.h"

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDirectServiceMock: public IPartitionDirectService
{
    explicit TPartitionDirectServiceMock(bool dropScheduledCallbacks = false)
        : DropScheduledCallbacks(dropScheduledCallbacks)
    {}

    TVolumeConfigPtr VolumeConfig;
    bool DropScheduledCallbacks = false;

    [[nodiscard]] TVolumeConfigPtr GetVolumeConfig() const override
    {
        return VolumeConfig;
    }

    NWilson::TSpan CreteRootSpan(TStringBuf name) override
    {
        Y_UNUSED(name);
        return {};
    }

    void ScheduleAfterDelay(
        TExecutorPtr executor,
        TDuration delay,
        TCallback callback) override
    {
        Y_UNUSED(delay);
        if (DropScheduledCallbacks) {
            return;
        }
        executor->ExecuteSimple(std::move(callback));
    }

    void UpdateVChunkConfig(
        const NStorage::NPartitionDirect::TVChunkConfig& cfg) override
    {
        Y_UNUSED(cfg);
    }

    struct TAddHostRequest
    {
        size_t DirectBlockGroupId = 0;
        ui32 ExpectedCurrentHostCount = 0;
    };

    TVector<TAddHostRequest> AddHostRequests;

    void RequestAddHost(
        size_t directBlockGroupId,
        ui32 expectedCurrentHostCount) override
    {
        AddHostRequests.push_back(
            {directBlockGroupId, expectedCurrentHostCount});
    }

    ui64 LsnGenerator = 0;

    ui64 GenerateLsn() override
    {
        return ++LsnGenerator;
    }
};

using TPartitionDirectServiceMockPtr =
    std::shared_ptr<TPartitionDirectServiceMock>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
