#!/usr/bin/env python3
"""Extract FIO_JSONL lines from a serial dump (or a jsonl file) and stamp binary/sha."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, TextIO


def parse_line(line: str) -> dict[str, Any] | None:
    text = line.strip()
    if text.startswith("FIO_JSONL2 "):
        text = text[len("FIO_JSONL2 ") :]
    elif text.startswith("FIO_JSONL "):
        text = text[len("FIO_JSONL ") :]
    elif not text.startswith("{"):
        return None
    try:
        rec = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(rec, dict) or "job" not in rec or "iops" not in rec:
        return None
    return rec


def iter_records(stream: TextIO) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in stream:
        rec = parse_line(line)
        if rec is not None:
            records.append(rec)
    return records


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", nargs="?", default="-")
    parser.add_argument("--binary", required=True, help="main or pr")
    parser.add_argument("--sha", default="")
    parser.add_argument("--actor-id", default="")
    args = parser.parse_args()

    stream: TextIO = sys.stdin if args.input == "-" else open(args.input)
    try:
        records = iter_records(stream)
    finally:
        if stream is not sys.stdin:
            stream.close()

    for rec in records:
        rec["binary"] = args.binary
        if args.sha:
            rec["sha"] = args.sha
        if args.actor_id:
            rec["actor_id"] = args.actor_id
        if float(rec.get("iops") or 0) <= 0:
            continue
        print(json.dumps(rec, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
