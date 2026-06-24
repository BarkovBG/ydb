#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/monitoring/mon_render.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMonRenderTest)
{
    TMonSnapshot MakeSnapshot()
    {
        TMonSnapshot snap;
        snap.LsnCounter = 100;
        snap.GlobalSafeBarrier = 90;
        snap.TotalVChunks = 7;
        snap.DbgCount = 1;

        TDbgSnapshot dbg;
        dbg.Index = 0;

        THostSnapshot host;
        host.Index = 0;
        host.State = EHostState::Online;
        host.Health = EHostHealthView::Online;
        host.InflightByOp[static_cast<size_t>(EOperation::WriteToPBuffer)] = 3;
        dbg.Hosts.push_back(host);

        TConnSnapshot conn;
        conn.HostIndex = 0;
        conn.DDiskId = "dd-1";
        conn.DDiskSession = "Locked";
        dbg.Connections.push_back(conn);

        snap.Dbgs.push_back(dbg);
        return snap;
    }

    TMonPageData MakeData(EMonPage page)
    {
        TMonPageData data;
        data.Page = page;
        data.Header.TabletId = 42;
        data.Header.Generation = 7;
        data.Header.DiskId = "vol-1";
        data.Header.State = "WORK";
        data.Runtime = MakeSnapshot();
        return data;
    }

    Y_UNIT_TEST(OverviewHasMenuAndSummary)
    {
        const TString html = RenderMonPage(MakeData(EMonPage::Overview));
        // Menu links to the other pages.
        UNIT_ASSERT_STRING_CONTAINS(html, "page=hosts");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunk");
        UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "Global safe barrier");
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunks (total)");
        UNIT_ASSERT_STRING_CONTAINS(html, "vol-1");
    }

    Y_UNIT_TEST(HostsPageShowsOpColumns)
    {
        const TString html = RenderMonPage(MakeData(EMonPage::Hosts));
        UNIT_ASSERT_STRING_CONTAINS(html, "Hosts");
        UNIT_ASSERT_STRING_CONTAINS(html, "WriteToPBuffer");
    }

    Y_UNIT_TEST(ConfigsPageEscapesHtml)
    {
        TMonPageData data = MakeData(EMonPage::Configs);
        data.Runtime.reset();   // Configs renders from the DB, not the runtime.
        data.Db.HasVolumeConfig = true;
        data.Db.VolumeConfigText = "<script>alert(1)</script>";
        data.Db.VChunkConfigs.push_back({0, "vc#0 default"});

        const TString html = RenderMonPage(data);
        UNIT_ASSERT(!html.Contains("<script>alert(1)</script>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "&lt;script&gt;");
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunkConfigs");
    }

    Y_UNIT_TEST(VChunkPageByIndex)
    {
        TMonPageData data = MakeData(EMonPage::VChunk);
        data.Filters.VChunk = 3;
        TVChunkSnapshot vchunk;
        vchunk.Index = 3;
        vchunk.HostCount = 1;
        vchunk.SafeBarrier = 88;
        data.Runtime->Dbgs[0].VChunks.push_back(vchunk);

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "vchunk #3");
        UNIT_ASSERT_STRING_CONTAINS(html, "Safe barrier");
        UNIT_ASSERT_STRING_CONTAINS(html, "total 7");
    }

    Y_UNIT_TEST(RuntimeErrorBanner)
    {
        TMonPageData data = MakeData(EMonPage::Overview);
        data.Runtime.reset();
        data.RuntimeError = "tablet is initializing";
        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "tablet is initializing");
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
