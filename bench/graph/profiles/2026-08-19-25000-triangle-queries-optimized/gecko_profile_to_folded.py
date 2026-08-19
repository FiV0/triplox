#!/usr/bin/env python3
"""Extract and symbolize a time window from a Samply Gecko profile."""

from __future__ import annotations

import collections
import gzip
import json
import pathlib
import re
import subprocess
import sys


ADDRESS = re.compile(r"^0x[0-9a-f]+$")


def clean_frame(name: str) -> str:
    return name.replace(";", ":").replace("\n", " ")


def resource_library(profile: dict, thread: dict, func_index: int) -> dict | None:
    resource_index = thread["funcTable"]["resource"][func_index]
    if resource_index is None:
        return None
    library_index = thread["resourceTable"]["lib"][resource_index]
    if library_index is None:
        return None
    return profile["libs"][library_index]


def load_symbols(profile: dict, binary: pathlib.Path) -> dict[str, str]:
    addresses: set[str] = set()
    for thread in profile["threads"]:
        if thread.get("processName") != "triplox":
            continue
        for func_index, name_index in enumerate(thread["funcTable"]["name"]):
            name = thread["stringArray"][name_index]
            library = resource_library(profile, thread, func_index)
            if library is not None and library["name"] == "triplox" and ADDRESS.fullmatch(name):
                addresses.add(name)

    ordered = sorted(addresses, key=lambda address: int(address, 16))
    if not ordered:
        return {}
    result = subprocess.run(
        ["addr2line", "-Cfpe", binary, *ordered],
        check=True,
        capture_output=True,
        text=True,
    )
    symbols = {}
    for address, line in zip(ordered, result.stdout.splitlines(), strict=True):
        function = line.split(" at ", maxsplit=1)[0]
        if function != "??":
            symbols[address] = function
    return symbols


def frame_name(profile: dict, thread: dict, func_index: int, symbols: dict[str, str]) -> str:
    raw = thread["stringArray"][thread["funcTable"]["name"][func_index]]
    library = resource_library(profile, thread, func_index)
    if library is not None and ADDRESS.fullmatch(raw):
        if library["name"] == "triplox":
            raw = symbols.get(raw, f"triplox!{raw}")
        else:
            raw = f'{library["name"]}!{raw}'
    return clean_frame(raw)


def convert(
    source: pathlib.Path,
    start_ms: float,
    end_ms: float,
    binary: pathlib.Path,
    destination: pathlib.Path,
) -> None:
    with gzip.open(source, "rt", encoding="utf-8") as profile_file:
        profile = json.load(profile_file)

    symbols = load_symbols(profile, binary)
    wall_start = float(profile["meta"]["startTime"])
    profile_start = min(
        float(thread["processStartupTime"])
        for thread in profile["threads"]
        if float(thread["processStartupTime"]) > 0
    )
    folded: collections.Counter[str] = collections.Counter()
    selected = 0

    for thread in profile["threads"]:
        if thread.get("processName") != "triplox":
            continue
        samples = thread["samples"]
        current_time = 0.0
        weights = samples.get("weight") or [1] * samples["length"]

        for index in range(samples["length"]):
            delta = samples["timeDeltas"][index]
            current_time = delta if index == 0 else current_time + delta
            wall_time = wall_start + current_time - profile_start
            if wall_time < start_ms or wall_time >= end_ms:
                continue

            stack_index = samples["stack"][index]
            if stack_index is None:
                continue
            frames = []
            while stack_index is not None:
                frame_index = thread["stackTable"]["frame"][stack_index]
                func_index = thread["frameTable"]["func"][frame_index]
                frames.append(
                    "unknown"
                    if func_index is None
                    else frame_name(profile, thread, func_index, symbols)
                )
                stack_index = thread["stackTable"]["prefix"][stack_index]
            frames.reverse()

            root = clean_frame(f'thread:{thread["name"]}:{thread["tid"]}')
            folded[";".join([root, *frames])] += weights[index]
            selected += weights[index]

    with destination.open("w", encoding="utf-8") as folded_file:
        for stack, weight in sorted(folded.items()):
            folded_file.write(f"{stack} {weight:g}\n")
    print(json.dumps({"output": str(destination), "samples": selected, "stacks": len(folded)}))


if __name__ == "__main__":
    if len(sys.argv) != 6:
        raise SystemExit(
            f"usage: {sys.argv[0]} PROFILE START_MS END_MS BINARY OUTPUT"
        )
    convert(
        pathlib.Path(sys.argv[1]),
        float(sys.argv[2]),
        float(sys.argv[3]),
        pathlib.Path(sys.argv[4]),
        pathlib.Path(sys.argv[5]),
    )
