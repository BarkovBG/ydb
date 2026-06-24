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
    TString s(in);
    SubstGlobal(s, "&", "&amp;");
    SubstGlobal(s, "<", "&lt;");
    SubstGlobal(s, ">", "&gt;");
    SubstGlobal(s, "\"", "&quot;");
    return s;
}

const char* OperationName(EOperation op)
{
    switch (op) {
        case EOperation::ReadFromPBuffer: return "ReadFromPBuffer";
        case EOperation::ReadFromDDisk: return "ReadFromDDisk";
        case EOperation::WriteToPBuffer: return "WriteToPBuffer";
        case EOperation::WriteToManyPBuffers: return "WriteToManyPBuffers";
        case EOperation::WriteToDDisk: return "WriteToDDisk";
        case EOperation::Flush: return "Flush";
        case EOperation::FlushCrossNode: return "FlushCrossNode";
        case EOperation::Erase: return "Erase";
        case EOperation::BarrierErase: return "BarrierErase";
        case EOperation::Count_: return "?";
    }
    return "?";
}

const char* HostStateName(EHostState s)
{
    switch (s) {
        case EHostState::Online: return "Online";
        case EHostState::TemporaryOffline: return "TemporaryOffline";
        case EHostState::Offline: return "Offline";
    }
    return "?";
}

const char* HealthName(EHostHealthView h)
{
    switch (h) {
        case EHostHealthView::Online: return "Online";
        case EHostHealthView::Sufferer: return "Sufferer";
        case EHostHealthView::TemporaryOffline: return "TemporaryOffline";
        case EHostHealthView::Offline: return "Offline";
    }
    return "?";
}

TString OptLsn(const std::optional<ui64>& v)
{
    return v ? ToString(*v) : TString("-");
}

void RenderHeader(IOutputStream& str, const THeaderInfo& h)
{
    HTML(str) {
        TAG(TH3) { str << "partition_direct tablet " << h.TabletId; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "TabletId"; } TABLED() { str << h.TabletId; } }
                TABLER() { TABLED() { str << "Generation"; } TABLED() { str << h.Generation; } }
                TABLER() { TABLED() { str << "DiskId"; } TABLED() { str << HtmlEscape(h.DiskId); } }
                TABLER() { TABLED() { str << "State"; } TABLED() { str << HtmlEscape(h.State); } }
                TABLER() { TABLED() { str << "Uptime"; } TABLED() { str << h.Uptime.ToString(); } }
            }
        }
    }
}

void RenderRuntimeBanner(IOutputStream& str, const TMonPageData& d)
{
    if (d.Runtime || !d.RuntimeError) {
        return;
    }
    HTML(str) {
        DIV_CLASS("alert alert-warning") {
            str << "Runtime state unavailable: " << HtmlEscape(*d.RuntimeError);
        }
    }
}

void RenderOverview(IOutputStream& str, const TMonPageData& d)
{
    HTML(str) {
        TAG(TH3) { str << "Overview"; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                if (d.Runtime) {
                    const auto& r = *d.Runtime;
                    TABLER() { TABLED() { str << "DirectBlockGroups"; } TABLED() { str << r.Dbgs.size(); } }
                    size_t vchunks = 0;
                    for (const auto& dbg: r.Dbgs) {
                        vchunks += dbg.VChunks.size();
                    }
                    TABLER() { TABLED() { str << "VChunks"; } TABLED() { str << vchunks; } }
                    TABLER() { TABLED() { str << "LSN counter"; } TABLED() { str << r.LsnCounter; } }
                    TABLER() { TABLED() { str << "Global safe barrier"; } TABLED() { str << OptLsn(r.GlobalSafeBarrier); } }
                } else {
                    TABLER() { TABLED() { str << "Runtime"; } TABLED() { str << "unavailable"; } }
                }
            }
        }
    }
}

void RenderLocalDb(IOutputStream& str, const TDbContents& db)
{
    HTML(str) {
        TAG(TH3) { str << "Local DB"; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { str << "StorageConfig"; }
                    TABLED() { str << HtmlEscape(db.StorageConfigText); }
                }
                TABLER() {
                    TABLED() { str << "VolumeConfig"; }
                    TABLED() {
                        str << (db.HasVolumeConfig ? "" : "(none)");
                        str << "<pre>" << HtmlEscape(db.VolumeConfigText) << "</pre>";
                    }
                }
                TABLER() {
                    TABLED() { str << "DirectBlockGroupsConnections"; }
                    TABLED() {
                        str << (db.HasConnections ? "" : "(none)");
                        str << "<pre>" << HtmlEscape(db.ConnectionsText) << "</pre>";
                    }
                }
            }
        }
        TAG(TH4) { str << "VChunkConfigs (persisted overrides)"; }
        TABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "VChunkIndex"; }
                    TABLEH() { str << "Config"; }
                }
            }
            TABLEBODY() {
                for (const auto& row: db.VChunkConfigs) {
                    TABLER() {
                        TABLED() { str << row.VChunkIndex; }
                        TABLED() { str << "<pre>" << HtmlEscape(row.Summary) << "</pre>"; }
                    }
                }
            }
        }
    }
}

void RenderConnections(
    IOutputStream& str,
    const TMonSnapshot& r,
    const TCgiFilters& f)
{
    HTML(str) {
        TAG(TH3) { str << "Direct Block Groups"; }
        for (const auto& dbg: r.Dbgs) {
            if (f.Dbg && *f.Dbg != dbg.Index) {
                continue;
            }
            TAG(TH4) { str << "DBG #" << dbg.Index; }
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Host"; }
                        TABLEH() { str << "DDisk id"; }
                        TABLEH() { str << "PBuffer id"; }
                        TABLEH() { str << "DDisk session"; }
                        TABLEH() { str << "PBuffer connected"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& c: dbg.Connections) {
                        TABLER() {
                            TABLED() { str << (int)c.HostIndex; }
                            TABLED() { str << HtmlEscape(c.DDiskId); }
                            TABLED() { str << HtmlEscape(c.PBufferId); }
                            TABLED() { str << HtmlEscape(c.DDiskSession); }
                            TABLED() { str << (c.PBufferConnected ? "yes" : "no"); }
                        }
                    }
                }
            }
        }
    }
}

void RenderHosts(IOutputStream& str, const TMonSnapshot& r, const TCgiFilters& f)
{
    HTML(str) {
        TAG(TH3) { str << "Hosts"; }
        for (const auto& dbg: r.Dbgs) {
            if (f.Dbg && *f.Dbg != dbg.Index) {
                continue;
            }
            TAG(TH4) { str << "DBG #" << dbg.Index << " hosts"; }
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Host"; }
                        TABLEH() { str << "State"; }
                        TABLEH() { str << "Health"; }
                        TABLEH() { str << "PBuffer used"; }
                        TABLEH() { str << "Errors"; }
                        for (size_t op = 0; op < OperationCount; ++op) {
                            TABLEH() { str << OperationName(static_cast<EOperation>(op)); }
                        }
                    }
                }
                TABLEBODY() {
                    for (const auto& h: dbg.Hosts) {
                        TABLER() {
                            TABLED() { str << (int)h.Index; }
                            TABLED() { str << HostStateName(h.State); }
                            TABLED() { str << HealthName(h.Health); }
                            TABLED() { str << h.PBufferUsedSize; }
                            TABLED() { str << h.Errors.ErrorCount; }
                            for (size_t op = 0; op < OperationCount; ++op) {
                                TABLED() { str << h.InflightByOp[op]; }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderVChunks(IOutputStream& str, const TMonSnapshot& r, const TCgiFilters& f)
{
    HTML(str) {
        TAG(TH3) { str << "VChunks"; }
        for (const auto& dbg: r.Dbgs) {
            if (f.Dbg && *f.Dbg != dbg.Index) {
                continue;
            }
            for (const auto& vc: dbg.VChunks) {
                if (f.VChunk && *f.VChunk != vc.Index) {
                    continue;
                }
                TAG(TH4) { str << "DBG #" << dbg.Index << " vchunk #" << vc.Index; }
                TABLE_CLASS("table table-condensed") {
                    TABLEBODY() {
                        TABLER() { TABLED() { str << "Safe barrier"; } TABLED() { str << OptLsn(vc.SafeBarrier); } }
                        TABLER() { TABLED() { str << "Inflight"; } TABLED() { str << vc.Counts.Inflight; } }
                        TABLER() { TABLED() { str << "Flush pending"; } TABLED() { str << vc.Counts.FlushPending; } }
                        TABLER() { TABLED() { str << "Erase pending"; } TABLED() { str << vc.Counts.ErasePending; } }
                        TABLER() { TABLED() { str << "Erase belated"; } TABLED() { str << vc.Counts.EraseBelated; } }
                    }
                }
                TAG(TH5) { str << "Roles / DDisk / PBuffer per host"; }
                TABLE_CLASS("table table-condensed") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { str << "Host"; }
                            TABLEH() { str << "PBuffer role"; }
                            TABLEH() { str << "DDisk role"; }
                            TABLEH() { str << "Enabled"; }
                            TABLEH() { str << "Watermark"; }
                            TABLEH() { str << "DDisk state"; }
                            TABLEH() { str << "OperBlocks"; }
                            TABLEH() { str << "PB cur recs"; }
                            TABLEH() { str << "PB cur bytes"; }
                            TABLEH() { str << "PB locked recs"; }
                        }
                    }
                    TABLEBODY() {
                        for (size_t host = 0; host < vc.HostCount; ++host) {
                            TABLER() {
                                TABLED() { str << host; }
                                TABLED() { str << (host < vc.Roles.size() ? vc.Roles[host].PBufferRole : TString("-")); }
                                TABLED() { str << (host < vc.Roles.size() ? vc.Roles[host].DDiskRole : TString("-")); }
                                TABLED() { str << (host < vc.Roles.size() && vc.Roles[host].Enabled ? "yes" : "no"); }
                                TABLED() { str << (host < vc.Roles.size() ? OptLsn(vc.Roles[host].Watermark) : TString("-")); }
                                TABLED() { str << (host < vc.DDiskStates.size() ? vc.DDiskStates[host].State : TString("-")); }
                                TABLED() { str << (host < vc.DDiskStates.size() ? ToString(vc.DDiskStates[host].OperationalBlockCount) : TString("-")); }
                                TABLED() { str << (host < vc.PBuffers.size() ? ToString(vc.PBuffers[host].CurrentRecords) : TString("-")); }
                                TABLED() { str << (host < vc.PBuffers.size() ? ToString(vc.PBuffers[host].CurrentBytes) : TString("-")); }
                                TABLED() { str << (host < vc.PBuffers.size() ? ToString(vc.PBuffers[host].CurrentLockedRecords) : TString("-")); }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderBarriers(IOutputStream& str, const TMonSnapshot& r)
{
    HTML(str) {
        TAG(TH3) { str << "Barriers"; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "LSN generator"; } TABLED() { str << r.LsnCounter; } }
                TABLER() { TABLED() { str << "Global safe barrier"; } TABLED() { str << OptLsn(r.GlobalSafeBarrier); } }
            }
        }
        TABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "DBG"; }
                    TABLEH() { str << "VChunk"; }
                    TABLEH() { str << "Safe barrier"; }
                }
            }
            TABLEBODY() {
                for (const auto& dbg: r.Dbgs) {
                    for (const auto& vc: dbg.VChunks) {
                        TABLER() {
                            TABLED() { str << dbg.Index; }
                            TABLED() { str << vc.Index; }
                            TABLED() { str << OptLsn(vc.SafeBarrier); }
                        }
                    }
                }
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
    RenderRuntimeBanner(str, data);
    RenderOverview(str, data);
    RenderLocalDb(str, data.Db);
    if (data.Runtime) {
        RenderConnections(str, *data.Runtime, data.Filters);
        RenderHosts(str, *data.Runtime, data.Filters);
        RenderVChunks(str, *data.Runtime, data.Filters);
        RenderBarriers(str, *data.Runtime);
    }
    return str.Str();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
