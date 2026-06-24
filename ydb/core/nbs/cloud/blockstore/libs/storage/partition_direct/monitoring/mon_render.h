#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_snapshot.h>

#include <util/generic/string.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Pure function: render the full read-only monitoring page as HTML.
[[nodiscard]] TString RenderMonPage(const TMonPageData& data);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
