#!/usr/bin/env python3
"""
KOIOS Fractal Map — Ingest Pipeline
Extracts YAML metadata from DAR/CT markdown files, normalizes scores,
builds fractal graph (nodes/edges/timeline/alerts), outputs JSON.
"""

import json
import re
import os
import math
from datetime import datetime, timezone
from collections import defaultdict
from itertools import combinations

import yaml

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DAR_FILE = os.path.join(BASE_DIR, "000_Critical_DAR (1).md")
CT_FILE = os.path.join(BASE_DIR, "000_critical_coffeetalks (1).md")
CONTRACT_FILE = os.path.join(BASE_DIR, "DAR_CT_fractal_map_contract_v1.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "koios_fractal_data.json")

# ---------------------------------------------------------------------------
# LOAD CONTRACT
# ---------------------------------------------------------------------------
with open(CONTRACT_FILE, "r", encoding="utf-8") as f:
    CONTRACT = json.load(f)

LINE_ALIAS_MAP = CONTRACT.get("line_alias_map", {})
STATUS_MULTIPLIER = CONTRACT.get("transform_rules", {}).get("status_multiplier", {})
CANONICAL_LINES = CONTRACT.get("canonical_enums", {}).get("fractal_lines", [])

# ---------------------------------------------------------------------------
# KNOWN YAML FIELDS (order matters for splitting)
# ---------------------------------------------------------------------------
DAR_FIELDS = [
    "id", "type", "date", "day_index", "daily_score", "rpg_score",
    "status", "fractal_lines_active", "key_achievements", "blockers",
    "rpg_breakdown",
]

CT_FIELDS = [
    "id", "type", "title", "date", "day_index", "importance",
    "fractal_lines", "tags", "key_insights", "connections", "archetype",
]

# ---------------------------------------------------------------------------
# YAML PARSER
# ---------------------------------------------------------------------------

def unescape_markdown(text: str) -> str:
    """Remove markdown escapes from text extracted from table cells."""
    text = text.replace("\\#", "#")
    text = text.replace("\\[", "[")
    text = text.replace("\\]", "]")
    text = text.replace("\\_", "_")
    text = text.replace("\\+", "+")
    text = text.replace("\\>", ">")
    text = text.replace("\\<", "<")
    text = text.replace("\\*", "*")
    # \- inside list context → -
    text = text.replace("\\-", "-")
    # Handle escaped quotes if any
    text = text.replace('\\"', '"')
    return text


def split_yaml_line(raw: str, fields: list[str]) -> str:
    """Split a single-line YAML blob into multi-line YAML using known field names."""
    # Sort fields longest-first so "archetype" matches before "type",
    # "fractal_lines_active" before "fractal_lines", etc.
    sorted_fields = sorted(fields, key=len, reverse=True)
    # Build alternation with word boundary: each field must NOT be preceded by
    # a word character (so "type:" won't match inside "archetype:")
    field_alts = "|".join(re.escape(f) for f in sorted_fields)
    pattern = rf'(?<![a-zA-Z_])(?=(?:{field_alts})\s*:)'
    parts = re.split(pattern, raw)

    lines = []
    for part in parts:
        part = part.strip()
        if part:
            lines.append(part)
    return "\n".join(lines)


def fix_yaml_lists(yaml_str: str) -> str:
    """Fix YAML list items that are on the same line as the key."""
    # Pattern: key_achievements:  - "item1"  - "item2"
    # → key_achievements:\n  - "item1"\n  - "item2"
    def fix_inline_list(match):
        key = match.group(1)
        items_str = match.group(2)
        items = re.findall(r'-\s*"([^"]*)"', items_str)
        if items:
            result = f"{key}:\n"
            for item in items:
                result += f'  - "{item}"\n'
            return result.rstrip()
        return match.group(0)

    yaml_str = re.sub(
        r'(\w+):\s*((?:\s*-\s*"[^"]*")+)',
        fix_inline_list,
        yaml_str,
    )
    return yaml_str


def fix_rpg_breakdown(yaml_str: str) -> str:
    """Fix rpg_breakdown inline object: 'rpg_breakdown:  KOIOS: 4  Praca: 2' → proper YAML."""
    match = re.search(r"rpg_breakdown:\s*(.+?)$", yaml_str, re.MULTILINE)
    if not match:
        return yaml_str
    value = match.group(1).strip()
    if value == "null" or value == "None" or not value:
        return yaml_str
    # Check if it's already multi-line (has newlines after)
    if "\n  " in value:
        return yaml_str
    # Parse inline key-value pairs like "KOIOS: 4  Praca: 2  Stan: 1"
    pairs = re.findall(r'([A-Za-zżźćńółęąśŻŹĆŃÓŁĘĄŚ/]+):\s*(-?\d+(?:\.\d+)?)', value)
    if pairs:
        replacement = "rpg_breakdown:\n"
        for key, val in pairs:
            replacement += f"  {key}: {val}\n"
        yaml_str = re.sub(
            r"rpg_breakdown:\s*.+?$",
            replacement.rstrip(),
            yaml_str,
            count=1,
            flags=re.MULTILINE,
        )
    return yaml_str


def extract_yaml_blocks(filepath: str, doc_type: str) -> list[dict]:
    """Extract YAML blocks from markdown table cells."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    content = unescape_markdown(content)

    # Pattern to find table cells containing YAML-like data with id field
    # Match content between | ... | that contains id: "DAR#..." or id: "CT#..."
    prefix = "DAR" if doc_type == "DAR" else "CT"
    fields = DAR_FIELDS if doc_type == "DAR" else CT_FIELDS

    # Find all table cells with YAML data
    pattern = rf'\|([^|]*?id:\s*"{prefix}#[^"]*?"[^|]*?)\|'
    matches = re.findall(pattern, content, re.DOTALL)

    results = []
    for raw_cell in matches:
        raw = raw_cell.strip()
        # Remove --- delimiters
        raw = re.sub(r"^-{3,}", "", raw).strip()
        raw = re.sub(r"-{3,}$", "", raw).strip()

        # Split into proper YAML lines
        yaml_text = split_yaml_line(raw, fields)
        # Fix inline lists
        yaml_text = fix_yaml_lists(yaml_text)
        # Fix rpg_breakdown
        if doc_type == "DAR":
            yaml_text = fix_rpg_breakdown(yaml_text)

        try:
            data = yaml.safe_load(yaml_text)
            if isinstance(data, dict) and "id" in data:
                results.append(data)
                continue
        except yaml.YAMLError:
            pass

        # Fallback: regex extraction
        fallback = regex_extract_fields(raw, fields, doc_type)
        if fallback:
            results.append(fallback)

    # Filter out template/legend entries (DAR#XXX, CT#XXX) and ensure day_index is int
    filtered = []
    for entry in results:
        entry_id = entry.get("id", "")
        if "XXX" in entry_id:
            continue
        # Ensure day_index is integer
        di = entry.get("day_index")
        if di is not None:
            try:
                entry["day_index"] = int(di)
            except (ValueError, TypeError):
                continue  # skip entries with non-numeric day_index
        filtered.append(entry)

    return filtered


def regex_extract_fields(raw: str, fields: list[str], doc_type: str) -> dict | None:
    """Fallback: extract fields via regex when YAML parsing fails."""
    data = {}

    # id
    m = re.search(r'id:\s*"([^"]+)"', raw)
    if m:
        data["id"] = m.group(1)
    else:
        return None

    # type
    m = re.search(r'type:\s*"([^"]+)"', raw)
    if m:
        data["type"] = m.group(1)

    # title (CT only)
    if doc_type == "CT":
        m = re.search(r'title:\s*"([^"]+)"', raw)
        if m:
            data["title"] = m.group(1)

    # date
    m = re.search(r'date:\s*"(\d{4}-\d{2}-\d{2})"', raw)
    if m:
        data["date"] = m.group(1)

    # day_index
    m = re.search(r"day_index:\s*(\d+)", raw)
    if m:
        data["day_index"] = int(m.group(1))

    # daily_score (DAR)
    if doc_type == "DAR":
        m = re.search(r'daily_score:\s*("?[^"]*?"?|[\d.]+/\d+|[\d.]+)', raw)
        if m:
            val = m.group(1).strip('"')
            data["daily_score"] = val

    # rpg_score (DAR)
    if doc_type == "DAR":
        m = re.search(r'rpg_score:\s*("?[^"]*?"?|\d+)', raw)
        if m:
            val = m.group(1).strip('"')
            try:
                data["rpg_score"] = int(val)
            except ValueError:
                data["rpg_score"] = val

    # importance (CT)
    if doc_type == "CT":
        m = re.search(r"importance:\s*([\d.]+)", raw)
        if m:
            data["importance"] = float(m.group(1))

    # status (DAR)
    if doc_type == "DAR":
        m = re.search(r'status:\s*"([^"]+)"', raw)
        if m:
            data["status"] = m.group(1)

    # archetype (CT)
    if doc_type == "CT":
        m = re.search(r'archetype:\s*"([^"]+)"', raw)
        if m:
            data["archetype"] = m.group(1)

    # Array fields
    array_fields_dar = ["fractal_lines_active", "key_achievements", "blockers"]
    array_fields_ct = ["fractal_lines", "tags", "key_insights", "connections"]
    arr_fields = array_fields_dar if doc_type == "DAR" else array_fields_ct

    for field in arr_fields:
        # Try to find [...] style
        m = re.search(rf'{field}:\s*\[([^\]]*)\]', raw)
        if m:
            items_str = m.group(1)
            items = re.findall(r'"([^"]*)"', items_str)
            data[field] = items
        else:
            # Try list style:  - "item"
            m = re.search(rf'{field}:\s*((?:\s*-\s*"[^"]*")+)', raw)
            if m:
                items = re.findall(r'"([^"]*)"', m.group(1))
                data[field] = items
            else:
                data[field] = []

    # rpg_breakdown (DAR)
    if doc_type == "DAR":
        m = re.search(r"rpg_breakdown:\s*null", raw)
        if m:
            data["rpg_breakdown"] = None
        else:
            pairs = re.findall(
                r'(?:rpg_breakdown:.*?)([A-Za-zżźćńółęąśŻŹĆŃÓŁĘĄŚ/]+):\s*(-?\d+(?:\.\d+)?)',
                raw,
            )
            if pairs:
                data["rpg_breakdown"] = {k: float(v) for k, v in pairs}
            else:
                data["rpg_breakdown"] = None

    return data if "id" in data else None


# ---------------------------------------------------------------------------
# NORMALIZATION
# ---------------------------------------------------------------------------

def resolve_line_alias(line: str) -> str:
    """Map line name to canonical form."""
    return LINE_ALIAS_MAP.get(line, line)


def normalize_daily_score(raw) -> float | None:
    """Normalize daily_score to 0.0–1.0."""
    if raw is None:
        return None
    raw_str = str(raw)
    # Try "X.X/10" pattern
    m = re.search(r"(\d+\.?\d*)\s*/\s*10", raw_str)
    if m:
        return max(0.0, min(1.0, float(m.group(1)) / 10.0))
    # Try embedded number in N/A string
    m = re.search(r"(\d+\.?\d*)", raw_str)
    if m and "N/A" in raw_str:
        val = float(m.group(1))
        if val <= 10:
            return max(0.0, min(1.0, val / 10.0))
        return None
    # Try bare number
    try:
        val = float(raw)
        if val <= 10:
            return max(0.0, min(1.0, val / 10.0))
    except (ValueError, TypeError):
        pass
    return None


def normalize_rpg_score(raw) -> float | None:
    """Normalize rpg_score to 0.0–1.0 using formula (value+20)/40."""
    if raw is None:
        return None
    try:
        val = float(raw)
        return max(0.0, min(1.0, (val + 20) / 40.0))
    except (ValueError, TypeError):
        return None


def normalize_importance(raw) -> float:
    """Normalize importance (1-5) to 0.0–1.0."""
    try:
        return max(0.0, min(1.0, float(raw) / 5.0))
    except (ValueError, TypeError):
        return 0.5  # default medium


# ---------------------------------------------------------------------------
# GRAPH CONSTRUCTION
# ---------------------------------------------------------------------------

def build_graph(dars: list[dict], cts: list[dict]) -> dict:
    """Build the fractal map output structure."""

    # -- Collect per-line per-day scores and references --
    line_day_scores = defaultdict(lambda: defaultdict(float))  # line → {day → score}
    line_evidence = defaultdict(list)      # line → [source_ids]
    edge_counts = defaultdict(int)         # (lineA, lineB) → co-occurrence count
    edge_evidence = defaultdict(list)      # (lineA, lineB) → [source_ids]
    timeline_data = {}                     # day_index → {date, daily_score_norm, rpg_norm, line_scores}
    all_day_indices = set()

    # Process DARs
    for dar in dars:
        src_id = dar.get("id", "?")
        day = dar.get("day_index")
        if day is None:
            continue
        all_day_indices.add(day)

        date_str = dar.get("date", "")
        status = dar.get("status", "OPERATIONAL")
        multiplier = STATUS_MULTIPLIER.get(status, 1.0)

        daily_norm = normalize_daily_score(dar.get("daily_score"))
        rpg_norm = normalize_rpg_score(dar.get("rpg_score"))

        lines_active = [resolve_line_alias(l) for l in dar.get("fractal_lines_active", [])]
        rpg_bd = dar.get("rpg_breakdown")

        # If we have rpg_breakdown, use per-line scores
        if rpg_bd and isinstance(rpg_bd, dict):
            for line_raw, pts in rpg_bd.items():
                line = resolve_line_alias(line_raw)
                try:
                    score = float(pts) * multiplier
                except (ValueError, TypeError):
                    score = 0.0
                line_day_scores[line][day] += score
                if src_id not in line_evidence[line]:
                    line_evidence[line].append(src_id)
        else:
            # Pre-RPG system: distribute daily_score equally across active lines
            if daily_norm is not None and lines_active:
                per_line = (daily_norm * 10.0 * multiplier) / len(lines_active)
                for line in lines_active:
                    line_day_scores[line][day] += per_line
                    if src_id not in line_evidence[line]:
                        line_evidence[line].append(src_id)
            else:
                for line in lines_active:
                    if src_id not in line_evidence[line]:
                        line_evidence[line].append(src_id)

        # Key achievements → boost lines
        achievements = dar.get("key_achievements", [])
        if achievements and lines_active:
            bonus = len(achievements) * 0.5 * multiplier / len(lines_active)
            for line in lines_active:
                line_day_scores[line][day] += bonus

        # Blockers → small penalty
        blockers = dar.get("blockers", [])
        if blockers and lines_active:
            penalty = len(blockers) * 0.2 / len(lines_active)
            for line in lines_active:
                line_day_scores[line][day] -= penalty

        # Edges: co-activation
        for a, b in combinations(sorted(set(lines_active)), 2):
            edge_counts[(a, b)] += 1
            if src_id not in edge_evidence[(a, b)]:
                edge_evidence[(a, b)].append(src_id)

        # Timeline entry
        if day not in timeline_data:
            timeline_data[day] = {
                "day_index": day,
                "date": date_str,
                "line_scores": {},
                "daily_score_norm_0_1": daily_norm,
                "rpg_score_norm_0_1": rpg_norm,
            }
        else:
            if daily_norm is not None:
                timeline_data[day]["daily_score_norm_0_1"] = daily_norm
            if rpg_norm is not None:
                timeline_data[day]["rpg_score_norm_0_1"] = rpg_norm

    # Process CTs
    for ct in cts:
        src_id = ct.get("id", "?")
        day = ct.get("day_index")
        if day is None:
            continue
        all_day_indices.add(day)

        importance = normalize_importance(ct.get("importance", 3))
        lines = [resolve_line_alias(l) for l in ct.get("fractal_lines", [])]

        # Each CT contributes importance-weighted score to its lines
        for line in lines:
            line_day_scores[line][day] += importance * 5.0  # scale to similar range as DAR
            if src_id not in line_evidence[line]:
                line_evidence[line].append(src_id)

        # Edges from CT fractal_lines
        for a, b in combinations(sorted(set(lines)), 2):
            edge_counts[(a, b)] += 1
            if src_id not in edge_evidence[(a, b)]:
                edge_evidence[(a, b)].append(src_id)

        # Edges from connections (CT→CT references contribute to line edges)
        # Timeline: ensure day exists
        if day not in timeline_data:
            timeline_data[day] = {
                "day_index": day,
                "date": ct.get("date", ""),
                "line_scores": {},
                "daily_score_norm_0_1": None,
                "rpg_score_norm_0_1": None,
            }

    # -- Fill timeline line_scores --
    for day in sorted(all_day_indices):
        if day in timeline_data:
            for line in line_day_scores:
                score = line_day_scores[line].get(day, 0.0)
                if score != 0.0:
                    timeline_data[day]["line_scores"][line] = round(score, 3)

    # -- Build nodes --
    nodes = []
    sorted_days = sorted(all_day_indices)
    max_day = max(sorted_days) if sorted_days else 0
    min_day = min(sorted_days) if sorted_days else 0

    for line in sorted(set(list(line_day_scores.keys()) + CANONICAL_LINES)):
        day_scores = line_day_scores.get(line, {})
        if not day_scores and line not in [l for ld in line_day_scores for l in [ld]]:
            continue  # skip lines with zero data

        total_score = sum(day_scores.values())

        # Last 7 days
        recent_days = [d for d in sorted_days if d > max_day - 7]
        score_7d = sum(day_scores.get(d, 0.0) for d in recent_days)

        # Previous 7 days
        prev_days = [d for d in sorted_days if max_day - 14 < d <= max_day - 7]
        score_prev_7d = sum(day_scores.get(d, 0.0) for d in prev_days)

        # Momentum
        mean_recent = score_7d / max(len(recent_days), 1)
        mean_prev = score_prev_7d / max(len(prev_days), 1)
        momentum_7d = mean_recent - mean_prev

        # Volatility & Stability
        recent_scores = [day_scores.get(d, 0.0) for d in recent_days]
        if len(recent_scores) > 1:
            mean_rs = sum(recent_scores) / len(recent_scores)
            variance = sum((s - mean_rs) ** 2 for s in recent_scores) / len(recent_scores)
            stddev = math.sqrt(variance)
            max_possible = max(abs(s) for s in recent_scores) if recent_scores else 1.0
            volatility_7d = stddev / max(max_possible, 0.001)
        else:
            volatility_7d = 0.0
        stability_7d = 1.0 - min(volatility_7d, 1.0)

        # Status
        if momentum_7d > 0.5:
            status = "up"
        elif momentum_7d < -0.5:
            status = "down"
        else:
            status = "flat"

        nodes.append({
            "id": line,
            "label": line,
            "total_score": round(total_score, 3),
            "score_7d": round(score_7d, 3),
            "momentum_7d": round(momentum_7d, 3),
            "volatility_7d": round(volatility_7d, 3),
            "stability_7d": round(stability_7d, 3),
            "status": status,
            "evidence_refs": line_evidence.get(line, []),
        })

    # -- Build edges --
    max_cooccurrence = max(edge_counts.values()) if edge_counts else 1
    edges = []
    for (a, b), count in sorted(edge_counts.items()):
        strength = round(count / max_cooccurrence, 3)
        edges.append({
            "from": a,
            "to": b,
            "strength": strength,
            "relation_type": "co_activation",
            "lag_days": 0,
            "evidence_refs": edge_evidence.get((a, b), []),
        })

    # -- Build timeline --
    timeline = [timeline_data[d] for d in sorted(timeline_data.keys())]

    # -- Build alerts --
    alerts = []
    alert_id = 0
    for node in nodes:
        if node["momentum_7d"] < -0.08:
            alert_id += 1
            alerts.append({
                "id": f"alert_{alert_id:03d}",
                "severity": "warning",
                "line": node["id"],
                "message": f"Linia {node['id']} traci momentum (7d: {node['momentum_7d']:.2f})",
                "rule": "line_decline",
            })
        if node["volatility_7d"] > 0.65:
            alert_id += 1
            alerts.append({
                "id": f"alert_{alert_id:03d}",
                "severity": "warning",
                "line": node["id"],
                "message": f"Wysoka zmienność w linii {node['id']} ({node['volatility_7d']:.2f})",
                "rule": "high_volatility",
            })

    # -- Assemble output --
    output = {
        "map_meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "range": {
                "from_day_index": min_day,
                "to_day_index": max_day,
            },
            "source_counts": {
                "DAR": len(dars),
                "CT": len(cts),
            },
        },
        "nodes": nodes,
        "edges": edges,
        "timeline": timeline,
        "alerts": alerts,
    }

    return output


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("KOIOS Fractal Map — Ingest Pipeline")
    print("=" * 60)

    # Extract
    print(f"\n[1/4] Parsing DARs from: {os.path.basename(DAR_FILE)}")
    dars = extract_yaml_blocks(DAR_FILE, "DAR")
    print(f"  Found {len(dars)} DAR entries:")
    for d in dars:
        print(f"    {d.get('id', '?'):>10}  day={d.get('day_index', '?'):>3}  score={d.get('daily_score', 'N/A')}")

    print(f"\n[2/4] Parsing CTs from: {os.path.basename(CT_FILE)}")
    cts = extract_yaml_blocks(CT_FILE, "CT")
    print(f"  Found {len(cts)} CT entries:")
    for c in cts:
        print(f"    {c.get('id', '?'):>10}  day={c.get('day_index', '?'):>3}  imp={c.get('importance', '?')}  {c.get('title', '')[:40]}")

    # Build
    print(f"\n[3/4] Building fractal graph...")
    output = build_graph(dars, cts)
    print(f"  Nodes: {len(output['nodes'])}")
    for n in output["nodes"]:
        print(f"    {n['id']:>12}  total={n['total_score']:>8.2f}  7d={n['score_7d']:>7.2f}  mom={n['momentum_7d']:>+6.2f}  [{n['status']}]")
    print(f"  Edges: {len(output['edges'])}")
    print(f"  Timeline entries: {len(output['timeline'])}")
    print(f"  Alerts: {len(output['alerts'])}")

    # Write
    print(f"\n[4/4] Writing output to: {os.path.basename(OUTPUT_FILE)}")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  Done! ({os.path.getsize(OUTPUT_FILE)} bytes)")

    print("\n" + "=" * 60)
    print("Ingest complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
