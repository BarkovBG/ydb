#!/usr/bin/env python3
"""Compare main vs PR fio JSONL. Writes HTML report and a short markdown verdict."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# IOPS/BW: PR is worse if mean drops by at least this fraction AND is below
# the baseline min. Latency: PR is worse if mean rises by at least this
# fraction AND is above the baseline max.
IOPS_REGRESSION_PCT = 5.0
LAT_P99_REGRESSION_PCT = 10.0
HIGH_CV_PCT = 10.0


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def row_name(rec: dict[str, Any]) -> str:
    job = rec["job"]
    rw = rec.get("rw") or "read"
    if job == "randrw4k":
        return f"randrw4k {rw}"
    return job


def metrics_of(rec: dict[str, Any]) -> dict[str, float | None]:
    p50 = rec.get("lat_ns_p50")
    p99 = rec.get("lat_ns_p99")
    return {
        "iops": float(rec.get("iops") or 0),
        "bw_bytes": float(rec.get("bw_bytes") or 0),
        "lat_p50_us": None if p50 is None else float(p50) / 1000.0,
        "lat_p99_us": None if p99 is None else float(p99) / 1000.0,
    }


def summarize(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) >= 2 else 0.0
    return {
        "n": float(len(values)),
        "mean": mean,
        "stdev": stdev,
        "min": min(values),
        "max": max(values),
        "cv_pct": 0.0 if mean == 0 else 100.0 * stdev / abs(mean),
    }


def fmt_iops(v: float) -> str:
    if v >= 1000:
        return f"{v / 1000:.1f}k"
    return f"{v:.0f}"


def fmt_bw(v: float) -> str:
    mib = v / (1024 * 1024)
    if mib >= 1:
        return f"{mib:.0f} MiB/s"
    return f"{v / 1024:.0f} KiB/s"


def fmt_lat(v: float) -> str:
    if v >= 1000:
        return f"{v / 1000:.2f} ms"
    return f"{v:.0f} µs"


def fmt_stat(stat: dict[str, float] | None, kind: str) -> str:
    if stat is None:
        return "—"
    mean = stat["mean"]
    if kind == "iops":
        core = fmt_iops(mean)
    elif kind == "bw":
        core = fmt_bw(mean)
    else:
        core = fmt_lat(mean)
    return f"{core} ± {stat['stdev']:.0f} [{stat['min']:.0f}–{stat['max']:.0f}]"


def delta_pct(pr: float, main: float) -> float | None:
    if main == 0:
        return None
    return 100.0 * (pr - main) / main


def decide_metric(
    name: str,
    main: dict[str, float] | None,
    pr: dict[str, float] | None,
) -> str:
    if main is None or pr is None:
        return "missing"
    d = delta_pct(pr["mean"], main["mean"])
    if d is None:
        return "missing"
    higher_is_better = name in ("iops", "bw_bytes")
    if main["cv_pct"] > HIGH_CV_PCT or pr["cv_pct"] > HIGH_CV_PCT:
        noisy = True
    else:
        noisy = False
    if higher_is_better:
        threshold = IOPS_REGRESSION_PCT
        regression = d <= -threshold and pr["mean"] < main["min"]
        improvement = d >= threshold and pr["mean"] > main["max"]
    else:
        threshold = LAT_P99_REGRESSION_PCT if name == "lat_p99_us" else IOPS_REGRESSION_PCT
        regression = d >= threshold and pr["mean"] > main["max"]
        improvement = d <= -threshold and pr["mean"] < main["min"]
    if regression:
        return "regression"
    if noisy and not improvement:
        return "inconclusive"
    if improvement:
        return "faster"
    return "ok"


def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def render_html(
    *,
    title: str,
    verdict: str,
    reason: str,
    rows: list[dict[str, Any]],
    meta: dict[str, str],
    raw: list[dict[str, Any]],
) -> str:
    banner_class = {
        "PASS": "ok",
        "FAIL": "crit",
        "INCONCLUSIVE": "imp",
        "ERRORS": "crit",
    }.get(verdict, "imp")
    body_rows = []
    for row in rows:
        cells = [
            html_escape(row["name"]),
            html_escape(row["err"]),
            html_escape(row["iops_main"]),
            html_escape(row["iops_pr"]),
            html_escape(row["iops_delta"]),
            html_escape(row["lat99_main"]),
            html_escape(row["lat99_pr"]),
            html_escape(row["lat99_delta"]),
            html_escape(row["decision"]),
        ]
        cls = ""
        if row["decision"] == "regression":
            cls = ' class="bad"'
        elif row["decision"] == "faster":
            cls = ' class="good"'
        body_rows.append("<tr" + cls + ">" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    raw_rows = []
    for rec in sorted(raw, key=lambda r: (r.get("binary"), r.get("job"), r.get("repeat"), r.get("rw"))):
        p99 = rec.get("lat_ns_p99")
        raw_rows.append(
            "<tr>"
            + "".join(
                f"<td>{html_escape(str(x))}</td>"
                for x in (
                    rec.get("binary"),
                    rec.get("repeat"),
                    row_name(rec),
                    rec.get("err"),
                    f"{rec.get('dropped', 0)}/{rec.get('short', 0)}",
                    fmt_iops(float(rec.get("iops") or 0)),
                    fmt_bw(float(rec.get("bw_bytes") or 0)),
                    "—" if rec.get("lat_ns_p50") is None else fmt_lat(float(rec["lat_ns_p50"]) / 1000.0),
                    "—" if p99 is None else fmt_lat(float(p99) / 1000.0),
                )
            )
            + "</tr>"
        )
    meta_items = "".join(
        f"<li><span>{html_escape(k)}</span> <code>{html_escape(v)}</code></li>"
        for k, v in meta.items()
    )
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html_escape(title)}</title>
  <style>
    :root {{
      --bg: #f6f5f1; --fg: #1b1b1b; --muted: #5c5c5c; --line: #d9d6ce;
      --card: #fff; --crit: #8b1e1e; --crit-bg: #f8e8e8; --imp: #8a5a00;
      --imp-bg: #f8efd9; --ok: #1f5c38; --ok-bg: #e7f3ec; --code: #2a2a2a;
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #161616; --fg: #ececec; --muted: #a3a3a3; --line: #2e2e2e;
        --card: #1e1e1e; --crit: #f0a8a8; --crit-bg: #3a1c1c; --imp: #e6c07b;
        --imp-bg: #3a2f18; --ok: #8fd4a8; --ok-bg: #1c2e24; --code: #d4d4d4;
      }}
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font: 15px/1.5 system-ui, sans-serif;
      color: var(--fg);
      background: var(--bg);
    }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 32px 24px 64px; }}
    h1 {{ font-size: 28px; font-weight: 650; margin: 0 0 8px; }}
    h2 {{ font-size: 18px; margin: 32px 0 12px; }}
    p {{ margin: 0 0 12px; }}
    .meta {{ color: var(--muted); font-size: 13px; }}
    code {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.92em;
      color: var(--code);
    }}
    .banner {{
      padding: 12px 14px;
      margin: 16px 0 28px;
      border: 1px solid;
    }}
    .banner.ok {{ background: var(--ok-bg); border-color: var(--ok); }}
    .banner.crit {{ background: var(--crit-bg); border-color: var(--crit); }}
    .banner.imp {{ background: var(--imp-bg); border-color: var(--imp); }}
    .banner strong {{ display: block; margin-bottom: 4px; }}
    table {{ width: 100%; border-collapse: collapse; background: var(--card); }}
    th, td {{ text-align: left; vertical-align: top; padding: 8px 10px; border-bottom: 1px solid var(--line); font-size: 13px; }}
    th {{ font-size: 12px; color: var(--muted); font-weight: 600; }}
    tr.bad td {{ background: var(--crit-bg); }}
    tr.good td {{ background: var(--ok-bg); }}
    ul.kv {{ padding: 0; list-style: none; }}
    ul.kv li {{ margin: 0 0 6px; }}
    ul.kv span {{ color: var(--muted); display: inline-block; min-width: 140px; }}
  </style>
</head>
<body>
<main>
  <h1>{html_escape(title)}</h1>
  <p class="meta">{html_escape(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))}</p>
  <div class="banner {banner_class}">
    <strong>{html_escape(verdict)}</strong>
    {html_escape(reason)}
  </div>
  <h2>Сводка (mean из 3 прогонов)</h2>
  <table>
    <thead>
      <tr>
        <th>job</th><th>err</th>
        <th>IOPS main</th><th>IOPS PR</th><th>Δ IOPS</th>
        <th>lat p99 main</th><th>lat p99 PR</th><th>Δ p99</th>
        <th>decision</th>
      </tr>
    </thead>
    <tbody>
      {"".join(body_rows)}
    </tbody>
  </table>
  <h2>Правила вердикта</h2>
  <p>IOPS/BW: регрессия, если mean PR ниже main на ≥ {IOPS_REGRESSION_PCT:.0f}% и ниже min main.
  p99: регрессия, если mean PR выше main на ≥ {LAT_P99_REGRESSION_PCT:.0f}% и выше max main.
  CV &gt; {HIGH_CV_PCT:.0f}% без выхода за min/max → inconclusive. Один прогон PR с прошлой сессии не считается стороной A/B.</p>
  <h2>Прогон</h2>
  <ul class="kv">{meta_items}</ul>
  <h2>Все повторы</h2>
  <table>
    <thead>
      <tr>
        <th>binary</th><th>rep</th><th>job</th><th>err</th><th>dropped/short</th>
        <th>IOPS</th><th>BW</th><th>p50</th><th>p99</th>
      </tr>
    </thead>
    <tbody>
      {"".join(raw_rows)}
    </tbody>
  </table>
</main>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl", type=Path)
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--md", type=Path)
    parser.add_argument("--title", default="FIO main vs PR")
    args = parser.parse_args()

    records = load_jsonl(args.jsonl)
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    errors = []
    meta: dict[str, str] = {}
    for rec in records:
        if int(rec.get("err") or 0) != 0 or int(rec.get("dropped") or 0) or int(rec.get("short") or 0):
            errors.append(rec)
        name = row_name(rec)
        binary = rec.get("binary") or "?"
        by_key[(name, binary)].append(rec)
        if rec.get("sha"):
            meta[f"sha {binary}"] = str(rec["sha"])
        if rec.get("actor_id"):
            meta[f"adapter {binary}"] = str(rec["actor_id"])

    names = sorted({name for name, _ in by_key})
    table_rows: list[dict[str, Any]] = []
    decisions: list[str] = []
    md_lines = [
        "| job | err | IOPS main | IOPS PR | Δ IOPS | lat p99 main | lat p99 PR | Δ p99 | decision |",
        "|---|---|---|---|---|---|---|---|---|",
    ]

    for name in names:
        main_recs = by_key.get((name, "main"), [])
        pr_recs = by_key.get((name, "pr"), [])
        main_metrics = [metrics_of(r) for r in main_recs]
        pr_metrics = [metrics_of(r) for r in pr_recs]

        def col(key: str, side: list[dict[str, float | None]]) -> dict[str, float] | None:
            vals = [float(m[key]) for m in side if m[key] is not None]
            return summarize(vals)

        iops_m, iops_p = col("iops", main_metrics), col("iops", pr_metrics)
        p99_m, p99_p = col("lat_p99_us", main_metrics), col("lat_p99_us", pr_metrics)
        iops_d = decide_metric("iops", iops_m, iops_p)
        p99_d = decide_metric("lat_p99_us", p99_m, p99_p)
        job_decision = "regression" if "regression" in (iops_d, p99_d) else (
            "inconclusive" if "inconclusive" in (iops_d, p99_d) else (
                "faster" if iops_d == "faster" and p99_d in ("ok", "faster") else "ok"
            )
        )
        decisions.append(job_decision)
        err_note = "0"
        if any(int(r.get("err") or 0) for r in main_recs + pr_recs):
            err_note = "nonzero"
        def dlabel(a, b, kind):
            if a is None or b is None:
                return "—"
            d = delta_pct(b["mean"], a["mean"])
            if d is None:
                return "—"
            sign = "+" if d >= 0 else ""
            return f"{sign}{d:.1f}%"
        row = {
            "name": name,
            "err": err_note,
            "iops_main": fmt_iops(iops_m["mean"]) if iops_m else "—",
            "iops_pr": fmt_iops(iops_p["mean"]) if iops_p else "—",
            "iops_delta": dlabel(iops_m, iops_p, "iops"),
            "lat99_main": fmt_lat(p99_m["mean"]) if p99_m else "—",
            "lat99_pr": fmt_lat(p99_p["mean"]) if p99_p else "—",
            "lat99_delta": dlabel(p99_m, p99_p, "lat"),
            "decision": job_decision,
        }
        table_rows.append(row)
        md_lines.append(
            f"| {row['name']} | {row['err']} | {row['iops_main']} | {row['iops_pr']} | "
            f"{row['iops_delta']} | {row['lat99_main']} | {row['lat99_pr']} | "
            f"{row['lat99_delta']} | {row['decision']} |"
        )

    if errors:
        verdict = "ERRORS"
        reason = "Есть err≠0, dropped или short — сначала это, не перф."
    elif any(d == "regression" for d in decisions):
        verdict = "FAIL"
        reason = "Регрессия по правилу mean+выход за min/max baseline."
    elif not names:
        verdict = "INCONCLUSIVE"
        reason = "Не хватает прогонов, чтобы сравнить."
    else:
        noisy = [row["name"] for row in table_rows if row["decision"] == "inconclusive"]
        verdict = "PASS"
        if noisy:
            reason = (
                "Регрессии нет. IOPS/BW в шуме. Шумные p99 (CV>10%): "
                + ", ".join(noisy)
                + " — не считаем порчей."
            )
        else:
            reason = "Перф PR не хуже main за пределами шума трёх прогонов."

    args.html.parent.mkdir(parents=True, exist_ok=True)
    args.html.write_text(
        render_html(
            title=args.title,
            verdict=verdict,
            reason=reason,
            rows=table_rows,
            meta=meta,
            raw=records,
        ),
        encoding="utf-8",
    )
    md = f"**{verdict}** — {reason}\n\n" + "\n".join(md_lines) + "\n"
    if args.md:
        args.md.write_text(md, encoding="utf-8")
    sys.stdout.write(md)
    return 0 if verdict in ("PASS", "INCONCLUSIVE") else 1


if __name__ == "__main__":
    raise SystemExit(main())
