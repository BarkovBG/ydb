#!/bin/bash
# Run inside the QEMU guest against the NBS disk. Prints one FIO_JSONL line
# per job side (read/write) so the serial log is enough to collect results.
set -euo pipefail

DEV=${DEV:-/dev/vdb}
OUT=${OUT:-/tmp/fio-campaign}
REPEATS=${REPEATS:-3}
RUNTIME=${RUNTIME:-30}
COOLDOWN=${COOLDOWN:-5}

mkdir -p "$OUT"

COMMON=(
    --filename="$DEV"
    --direct=1
    --ioengine=libaio
    --group_reporting
    --time_based
    --iodepth=32
    --numjobs=1
    --lat_percentiles=1
    --percentile_list=50:90:95:99:99.9
    --output-format=json
)

emit_jsonl() {
    python3 - "$1" "$2" "$3" <<'PY'
import json
import sys

path, job, rep = sys.argv[1], sys.argv[2], int(sys.argv[3])
data = json.load(open(path))
entry = data["jobs"][0]
err = int(entry.get("error") or 0)
job_opts = entry.get("job options") or data.get("global options") or {}
verify = job_opts.get("verify")


def percentile(blob, prefix):
    # fio 3.16 puts percentile_list under lat_ns, not clat_ns
    # (clat_ns only has min/max/mean/stddev).
    hist = blob.get("lat_ns") or blob.get("clat_ns") or {}
    pct = hist.get("percentile") or {}
    for key, value in pct.items():
        if str(key).startswith(prefix):
            return int(value)
    return None


def emit(rw, blob):
    iops = float(blob.get("iops") or 0)
    io_bytes = int(blob.get("io_bytes") or 0)
    if iops == 0 and io_bytes == 0:
        return
    p50 = percentile(blob, "50")
    p99 = percentile(blob, "99")
    if p50 == 0 and p99 == 0 and iops == 0:
        return
    bw_bytes = int(blob.get("bw_bytes") or 0)
    if bw_bytes == 0:
        bw_bytes = int(float(blob.get("bw") or 0) * 1024)
    rec = {
        "job": job,
        "repeat": rep,
        "rw": rw,
        "err": err,
        "dropped": int(blob.get("drop_ios") or 0),
        "short": int(blob.get("short_ios") or 0),
        "iops": iops,
        "bw_bytes": bw_bytes,
        "lat_ns_p50": percentile(blob, "50"),
        "lat_ns_p99": percentile(blob, "99"),
        "verify": verify or None,
    }
    print("FIO_JSONL " + json.dumps(rec, separators=(",", ":")), flush=True)


read_blob = entry.get("read") or {}
write_blob = entry.get("write") or {}
emit("read", read_blob)
emit("write", write_blob)
if float(read_blob.get("iops") or 0) == 0 and float(write_blob.get("iops") or 0) == 0:
    emit("read", read_blob)
    rec = {
        "job": job,
        "repeat": rep,
        "rw": "none",
        "err": err,
        "dropped": 0,
        "short": 0,
        "iops": 0,
        "bw_bytes": 0,
        "lat_ns_p50": None,
        "lat_ns_p99": None,
        "verify": verify or None,
    }
    print("FIO_JSONL " + json.dumps(rec, separators=(",", ":")), flush=True)
PY
}

run_job() {
    local name="$1"
    local rep="$2"
    shift 2
    local out="$OUT/${rep}_${name}.json"
    echo "===== START ${name} rep=${rep} ====="
    sudo fio --name="$name" --output="$out" --runtime="$RUNTIME" "${COMMON[@]}" "$@"
    emit_jsonl "$out" "$name" "$rep"
    echo "===== END ${name} rep=${rep} ====="
    sleep "$COOLDOWN"
}

echo "===== WARMUP ====="
sudo fio --name=warmup --filename="$DEV" --direct=1 --ioengine=libaio \
    --iodepth=32 --runtime=15 --time_based --blocksize=4096 --rw=randread \
    --output-format=json --output="$OUT/warmup.json" >/dev/null || true
echo "===== WARMUP DONE ====="

for rep in $(seq 1 "$REPEATS"); do
    echo "===== REPEAT ${rep}/${REPEATS} ====="
    run_job randrw4k "$rep" --blocksize=4096 --rw=randrw --buffered=0
    run_job randwrite4k "$rep" --blocksize=4096 --rw=randwrite
    run_job randread4k "$rep" --blocksize=4096 --rw=randread
    run_job verify_sha1 "$rep" --rw=randwrite --bssplit=4k/20:8k/20:64k/50:1M/10 \
        --verify_fatal=1 --verify_dump=1 --verify_async=2 --do_verify=1 \
        --verify=sha1 --verify_backlog=500
done

echo CAMPAIGN_DONE
ls -l "$OUT"
