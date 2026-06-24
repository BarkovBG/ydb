#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_render.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMonRenderTest)
{
    TMonPageData MakeData()
    {
        TMonPageData d;
        d.Header.TabletId = 42;
        d.Header.Generation = 7;
        d.Header.DiskId = "vol-1";
        d.Header.State = "WORK";
        d.Header.Uptime = TDuration::Seconds(125);

        d.Db.HasVolumeConfig = true;
        d.Db.VolumeConfigText = "BlockSize: 4096";
        d.Db.VChunkConfigs.push_back({0, "vchunk#0 default"});

        TMonSnapshot snap;
        snap.LsnCounter = 100;
        snap.GlobalSafeBarrier = 90;

        TDbgSnapshot dbg;
        dbg.Index = 0;

        THostSnapshot h;
        h.Index = 0;
        h.State = EHostState::Online;
        h.Health = EHostHealthView::Online;
        h.InflightByOp[static_cast<size_t>(EOperation::WriteToPBuffer)] = 3;
        dbg.Hosts.push_back(h);

        TVChunkSnapshot vc;
        vc.Index = 0;
        vc.HostCount = 1;
        vc.SafeBarrier = 88;
        vc.Counts.Inflight = 2;
        dbg.VChunks.push_back(vc);

        snap.Dbgs.push_back(dbg);
        d.Runtime = snap;
        return d;
    }

    Y_UNIT_TEST(RendersAllSections)
    {
        const TString html = RenderMonPage(MakeData());
        UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "Local DB");
        UNIT_ASSERT_STRING_CONTAINS(html, "Direct Block Groups");
        UNIT_ASSERT_STRING_CONTAINS(html, "Hosts");
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunks");
        UNIT_ASSERT_STRING_CONTAINS(html, "Barriers");
        UNIT_ASSERT_STRING_CONTAINS(html, "vol-1");
        UNIT_ASSERT_STRING_CONTAINS(html, "WriteToPBuffer");
    }

    Y_UNIT_TEST(EscapesHtml)
    {
        TMonPageData d = MakeData();
        d.Db.VolumeConfigText = "<script>alert(1)</script>";
        const TString html = RenderMonPage(d);
        UNIT_ASSERT(!html.Contains("<script>alert(1)</script>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "&lt;script&gt;");
    }

    Y_UNIT_TEST(ShowsRuntimeErrorBanner)
    {
        TMonPageData d = MakeData();
        d.Runtime.reset();
        d.RuntimeError = "tablet is initializing";
        const TString html = RenderMonPage(d);
        UNIT_ASSERT_STRING_CONTAINS(html, "tablet is initializing");
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
