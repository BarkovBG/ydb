#include "mon_render.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString HtmlEscape(TStringBuf in)
{
    TString escaped(in);
    SubstGlobal(escaped, "&", "&amp;");
    SubstGlobal(escaped, "<", "&lt;");
    SubstGlobal(escaped, ">", "&gt;");
    SubstGlobal(escaped, "\"", "&quot;");
    return escaped;
}

const char* OperationName(EOperation op)
{
    switch (op) {
        case EOperation::ReadFromPBuffer:
            return "ReadFromPBuffer";
        case EOperation::ReadFromDDisk:
            return "ReadFromDDisk";
        case EOperation::WriteToPBuffer:
            return "WriteToPBuffer";
        case EOperation::WriteToManyPBuffers:
            return "WriteToManyPBuffers";
        case EOperation::WriteToDDisk:
            return "WriteToDDisk";
        case EOperation::Flush:
            return "Flush";
        case EOperation::FlushCrossNode:
            return "FlushCrossNode";
        case EOperation::Erase:
            return "Erase";
        case EOperation::BarrierErase:
            return "BarrierErase";
        case EOperation::Count_:
            return "?";
    }
    return "?";
}

const char* HostStateName(EHostState state)
{
    switch (state) {
        case EHostState::Online:
            return "Online";
        case EHostState::TemporaryOffline:
            return "TemporaryOffline";
        case EHostState::Offline:
            return "Offline";
    }
    return "?";
}

const char* HealthName(EHostHealthView health)
{
    switch (health) {
        case EHostHealthView::Online:
            return "Online";
        case EHostHealthView::Sufferer:
            return "Sufferer";
        case EHostHealthView::TemporaryOffline:
            return "TemporaryOffline";
        case EHostHealthView::Offline:
            return "Offline";
    }
    return "?";
}

TString OptLsn(const std::optional<ui64>& value)
{
    return value ? ToString(*value) : TString("-");
}

const char* PageParam(EMonPage page)
{
    switch (page) {
        case EMonPage::Overview:
            return "overview";
        case EMonPage::Hosts:
            return "hosts";
        case EMonPage::Configs:
            return "configs";
        case EMonPage::Connections:
            return "connections";
        case EMonPage::VChunk:
            return "vchunk";
    }
    return "overview";
}

const char* PageTitle(EMonPage page)
{
    switch (page) {
        case EMonPage::Overview:
            return "Overview";
        case EMonPage::Hosts:
            return "Hosts";
        case EMonPage::Configs:
            return "Local DB";
        case EMonPage::Connections:
            return "Connections";
        case EMonPage::VChunk:
            return "VChunk";
    }
    return "";
}

////////////////////////////////////////////////////////////////////////////////

void RenderHeader(IOutputStream& str, const THeaderInfo& header)
{
    HTML (str) {
        TAG (TH3) {
            str << "partition_direct tablet " << header.TabletId;
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "TabletId";
                    }
                    TABLED () {
                        str << header.TabletId;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Generation";
                    }
                    TABLED () {
                        str << header.Generation;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "DiskId";
                    }
                    TABLED () {
                        str << HtmlEscape(header.DiskId);
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "State";
                    }
                    TABLED () {
                        str << HtmlEscape(header.State);
                    }
                }
            }
        }
    }
}

void RenderMenu(IOutputStream& str, const THeaderInfo& header, EMonPage current)
{
    static const EMonPage pages[] = {
        EMonPage::Overview,
        EMonPage::Hosts,
        EMonPage::Configs,
        EMonPage::Connections,
        EMonPage::VChunk,
    };
    str << "<div style='margin:0.5em 0 1em;'>";
    for (EMonPage page: pages) {
        const char* btnClass =
            (page == current) ? "btn btn-primary" : "btn btn-default";
        str << "<a class='" << btnClass
            << "' style='margin-right:0.4em;'"
               " href='?TabletID="
            << header.TabletId << "&page=" << PageParam(page) << "'>"
            << PageTitle(page) << "</a>";
    }
    str << "</div>";
}

void RenderRuntimeBanner(IOutputStream& str, const TMonPageData& data)
{
    if (data.Runtime || !data.RuntimeError) {
        return;
    }
    HTML (str) {
        DIV_CLASS ("alert alert-warning") {
            str << "Runtime state unavailable: "
                << HtmlEscape(*data.RuntimeError);
        }
    }
}

void RenderOverview(IOutputStream& str, const TMonSnapshot& snapshot)
{
    HTML (str) {
        TAG (TH3) {
            str << "Overview";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "DirectBlockGroups";
                    }
                    TABLED () {
                        str << snapshot.DbgCount;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "VChunks (total)";
                    }
                    TABLED () {
                        str << snapshot.TotalVChunks;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "LSN counter";
                    }
                    TABLED () {
                        str << snapshot.LsnCounter;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Global safe barrier";
                    }
                    TABLED () {
                        str << OptLsn(snapshot.GlobalSafeBarrier);
                    }
                }
            }
        }
    }
}

void RenderConfigs(IOutputStream& str, const TDbContents& db)
{
    HTML (str) {
        TAG (TH3) {
            str << "Local DB";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "StorageConfig";
                    }
                    TABLED () {
                        str << HtmlEscape(db.StorageConfigText);
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "VolumeConfig";
                    }
                    TABLED () {
                        str << (db.HasVolumeConfig ? "" : "(none)");
                        str << "<pre>" << HtmlEscape(db.VolumeConfigText)
                            << "</pre>";
                    }
                }
            }
        }
        TAG (TH4) {
            str << "VChunkConfigs (persisted overrides)";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "VChunkIndex";
                    }
                    TABLEH () {
                        str << "Config";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& row: db.VChunkConfigs) {
                    TABLER () {
                        TABLED () {
                            str << row.VChunkIndex;
                        }
                        TABLED () {
                            str << "<pre>" << HtmlEscape(row.DebugText)
                                << "</pre>";
                        }
                    }
                }
            }
        }
    }
}

void RenderConnections(IOutputStream& str, const TMonSnapshot& snapshot)
{
    HTML (str) {
        TAG (TH3) {
            str << "Connections";
        }
        for (const auto& dbg: snapshot.Dbgs) {
            TAG (TH4) {
                str << "DBG #" << dbg.Index;
            }
            TABLE_CLASS ("table table-condensed") {
                TABLEHEAD () {
                    TABLER () {
                        TABLEH () {
                            str << "Host";
                        }
                        TABLEH () {
                            str << "DDisk id";
                        }
                        TABLEH () {
                            str << "PBuffer id";
                        }
                        TABLEH () {
                            str << "DDisk session";
                        }
                        TABLEH () {
                            str << "PBuffer connected";
                        }
                    }
                }
                TABLEBODY () {
                    for (const auto& conn: dbg.Connections) {
                        TABLER () {
                            TABLED () {
                                str << (int)conn.HostIndex;
                            }
                            TABLED () {
                                str << HtmlEscape(conn.DDiskId);
                            }
                            TABLED () {
                                str << HtmlEscape(conn.PBufferId);
                            }
                            TABLED () {
                                str << HtmlEscape(conn.DDiskSession);
                            }
                            TABLED () {
                                str << (conn.PBufferConnected ? "yes" : "no");
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderHosts(IOutputStream& str, const TMonSnapshot& snapshot)
{
    HTML (str) {
        TAG (TH3) {
            str << "Hosts";
        }
        for (const auto& dbg: snapshot.Dbgs) {
            TAG (TH4) {
                str << "DBG #" << dbg.Index << " hosts";
            }
            TABLE_CLASS ("table table-condensed") {
                TABLEHEAD () {
                    TABLER () {
                        TABLEH () {
                            str << "Host";
                        }
                        TABLEH () {
                            str << "State";
                        }
                        TABLEH () {
                            str << "Health";
                        }
                        TABLEH () {
                            str << "PBuffer used";
                        }
                        TABLEH () {
                            str << "Errors";
                        }
                        for (size_t op = 0; op < OperationCount; ++op) {
                            TABLEH () {
                                str << OperationName(
                                    static_cast<EOperation>(op));
                            }
                        }
                    }
                }
                TABLEBODY () {
                    for (const auto& host: dbg.Hosts) {
                        TABLER () {
                            TABLED () {
                                str << (int)host.Index;
                            }
                            TABLED () {
                                str << HostStateName(host.State);
                            }
                            TABLED () {
                                str << HealthName(host.Health);
                            }
                            TABLED () {
                                str << host.PBufferUsedSize;
                            }
                            TABLED () {
                                str << host.Errors.ErrorCount;
                            }
                            for (size_t op = 0; op < OperationCount; ++op) {
                                TABLED () {
                                    str << host.InflightByOp[op];
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderOneVChunk(
    IOutputStream& str,
    size_t dbgIndex,
    const TVChunkSnapshot& vchunk)
{
    HTML (str) {
        TAG (TH4) {
            str << "DBG #" << dbgIndex << " vchunk #" << vchunk.Index;
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "Safe barrier";
                    }
                    TABLED () {
                        str << OptLsn(vchunk.SafeBarrier);
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Inflight";
                    }
                    TABLED () {
                        str << vchunk.Counts.Inflight;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Flush pending";
                    }
                    TABLED () {
                        str << vchunk.Counts.FlushPending;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Erase pending";
                    }
                    TABLED () {
                        str << vchunk.Counts.ErasePending;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Erase belated";
                    }
                    TABLED () {
                        str << vchunk.Counts.EraseBelated;
                    }
                }
            }
        }
        TAG (TH5) {
            str << "Roles / DDisk / PBuffer per host";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Host";
                    }
                    TABLEH () {
                        str << "PBuffer role";
                    }
                    TABLEH () {
                        str << "DDisk role";
                    }
                    TABLEH () {
                        str << "Enabled";
                    }
                    TABLEH () {
                        str << "Watermark";
                    }
                    TABLEH () {
                        str << "DDisk state";
                    }
                    TABLEH () {
                        str << "OperBlocks";
                    }
                    TABLEH () {
                        str << "PB cur recs";
                    }
                    TABLEH () {
                        str << "PB cur bytes";
                    }
                    TABLEH () {
                        str << "PB locked recs";
                    }
                }
            }
            TABLEBODY () {
                for (size_t host = 0; host < vchunk.HostCount; ++host) {
                    TABLER () {
                        TABLED () {
                            str << host;
                        }
                        TABLED () {
                            str
                                << (host < vchunk.Roles.size()
                                        ? vchunk.Roles[host].PBufferRole
                                        : TString("-"));
                        }
                        TABLED () {
                            str
                                << (host < vchunk.Roles.size()
                                        ? vchunk.Roles[host].DDiskRole
                                        : TString("-"));
                        }
                        TABLED () {
                            str
                                << (host < vchunk.Roles.size() &&
                                            vchunk.Roles[host].Enabled
                                        ? "yes"
                                        : "no");
                        }
                        TABLED () {
                            str
                                << (host < vchunk.Roles.size()
                                        ? OptLsn(vchunk.Roles[host].Watermark)
                                        : TString("-"));
                        }
                        TABLED () {
                            str
                                << (host < vchunk.DDiskStates.size()
                                        ? vchunk.DDiskStates[host].State
                                        : TString("-"));
                        }
                        TABLED () {
                            str
                                << (host < vchunk.DDiskStates.size()
                                        ? ToString(vchunk.DDiskStates[host]
                                                       .OperationalBlockCount)
                                        : TString("-"));
                        }
                        TABLED () {
                            str
                                << (host < vchunk.PBuffers.size()
                                        ? ToString(vchunk.PBuffers[host]
                                                       .CurrentRecords)
                                        : TString("-"));
                        }
                        TABLED () {
                            str
                                << (host < vchunk.PBuffers.size()
                                        ? ToString(vchunk.PBuffers[host]
                                                       .CurrentBytes)
                                        : TString("-"));
                        }
                        TABLED () {
                            str
                                << (host < vchunk.PBuffers.size()
                                        ? ToString(vchunk.PBuffers[host]
                                                       .CurrentLockedRecords)
                                        : TString("-"));
                        }
                    }
                }
            }
        }
    }
}

void RenderVChunkPage(
    IOutputStream& str,
    const THeaderInfo& header,
    const TMonSnapshot& snapshot,
    const TCgiFilters& filters)
{
    HTML (str) {
        TAG (TH3) {
            str << "VChunk";
        }
    }

    const ui64 maxIndex = snapshot.TotalVChunks ? snapshot.TotalVChunks - 1 : 0;
    const TString currentValue =
        filters.VChunk ? ToString(*filters.VChunk) : TString();
    str << "<form method='get' style='margin-bottom:1em;'>"
           "<input type='hidden' name='TabletID' value='"
        << header.TabletId
        << "'>"
           "<input type='hidden' name='page' value='vchunk'>"
           "VChunk index (0.."
        << maxIndex << ", total " << snapshot.TotalVChunks
        << "): <input name='vchunk' value='" << currentValue
        << "' style='width:90px;'> "
           "<button type='submit'>Show</button>"
           "</form>";

    if (!filters.VChunk) {
        HTML (str) {
            DIV_CLASS ("alert alert-info") {
                str << "Enter a vchunk index above to see its details.";
            }
        }
        return;
    }

    bool found = false;
    for (const auto& dbg: snapshot.Dbgs) {
        for (const auto& vchunk: dbg.VChunks) {
            if (vchunk.Index == *filters.VChunk) {
                RenderOneVChunk(str, dbg.Index, vchunk);
                found = true;
            }
        }
    }
    if (!found) {
        HTML (str) {
            DIV_CLASS ("alert alert-warning") {
                str << "VChunk #" << *filters.VChunk << " not found.";
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString RenderMonPage(const TMonPageData& data)
{
    TStringStream str;
    // Simple auto-refresh (full reload). Phase 2 replaces this with AJAX.
    str << "<script>setTimeout(function(){location.reload();}, 5000);</script>";

    RenderHeader(str, data.Header);
    RenderMenu(str, data.Header, data.Page);
    RenderRuntimeBanner(str, data);

    switch (data.Page) {
        case EMonPage::Overview:
            if (data.Runtime) {
                RenderOverview(str, *data.Runtime);
            }
            break;
        case EMonPage::Hosts:
            if (data.Runtime) {
                RenderHosts(str, *data.Runtime);
            }
            break;
        case EMonPage::Configs:
            RenderConfigs(str, data.Db);
            break;
        case EMonPage::Connections:
            if (data.Runtime) {
                RenderConnections(str, *data.Runtime);
            }
            break;
        case EMonPage::VChunk:
            if (data.Runtime) {
                RenderVChunkPage(str, data.Header, *data.Runtime, data.Filters);
            }
            break;
    }
    return str.Str();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
