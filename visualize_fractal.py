#!/usr/bin/env python3
"""
KOIOS Fractal Map — Interactive Visualization
Reads koios_fractal_data.json, builds a networkx graph,
renders an interactive Plotly HTML file.
"""

import json
import os
import math

import networkx as nx
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "koios_fractal_data.json")
OUTPUT_HTML = os.path.join(BASE_DIR, "fractal_map.html")

# ---------------------------------------------------------------------------
# COLOR MAP (one per fractal line)
# ---------------------------------------------------------------------------
COLOR_MAP = {
    "KOIOS": "#1E90FF",       # DodgerBlue
    "Praca": "#A0A0A0",       # Grey
    "Stan": "#2ECC71",        # Green
    "Relacje": "#E74C3C",     # Red
    "Studia": "#9B59B6",      # Purple
    "Uczelnia": "#8E44AD",    # DarkPurple
    "Finanse": "#F39C12",     # Orange/Gold
    "Mieszkanie": "#D2691E",  # Chocolate
    "Dziewczyna": "#FF69B4",  # HotPink
    "Rodzina": "#3CB371",     # MediumSeaGreen
    "Kudelkowo": "#8B4513",   # SaddleBrown
    "Zdrowie": "#00CED1",     # DarkTurquoise
    "META": "#FFD700",        # Gold
}

STATUS_EMOJI = {"up": "\u2191", "down": "\u2193", "flat": "\u2192"}

# ---------------------------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------------------------

def load_data():
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# BUILD GRAPH
# ---------------------------------------------------------------------------

def build_nx_graph(data: dict) -> nx.Graph:
    G = nx.Graph()

    for node in data["nodes"]:
        G.add_node(
            node["id"],
            label=node["label"],
            total_score=node["total_score"],
            score_7d=node["score_7d"],
            momentum_7d=node["momentum_7d"],
            volatility_7d=node["volatility_7d"],
            stability_7d=node["stability_7d"],
            status=node["status"],
            evidence_refs=node["evidence_refs"],
        )

    for edge in data["edges"]:
        G.add_edge(
            edge["from"],
            edge["to"],
            strength=edge["strength"],
            relation_type=edge["relation_type"],
            evidence_refs=edge["evidence_refs"],
        )

    return G

# ---------------------------------------------------------------------------
# VISUALIZATION
# ---------------------------------------------------------------------------

def determine_symbol(evidence_refs: list) -> str:
    """DAR-dominant → square, CT-dominant → circle, mixed → diamond."""
    dar_count = sum(1 for ref in evidence_refs if ref.startswith("DAR"))
    ct_count = sum(1 for ref in evidence_refs if ref.startswith("CT"))
    if dar_count > ct_count:
        return "square"
    elif ct_count > dar_count:
        return "circle"
    else:
        return "diamond"


def create_figure(G: nx.Graph, data: dict) -> go.Figure:
    # Layout
    pos = nx.spring_layout(G, k=2.5, iterations=100, seed=42)

    # -- Edge traces --
    edge_traces = []
    for u, v, edata in G.edges(data=True):
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        strength = edata.get("strength", 0.5)
        width = max(1, strength * 8)
        opacity = 0.3 + strength * 0.4

        edge_traces.append(
            go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode="lines",
                line=dict(width=width, color=f"rgba(150,180,220,{opacity})"),
                hoverinfo="text",
                text=f"{u} \u2194 {v}<br>Strength: {strength:.2f}<br>Refs: {', '.join(edata.get('evidence_refs', [])[:5])}",
                showlegend=False,
            )
        )

    # -- Node trace (one per node for individual colors) --
    node_traces = []
    scores = [G.nodes[n].get("total_score", 0) for n in G.nodes()]
    max_score = max(scores) if scores else 1
    min_score = min(scores) if scores else 0
    score_range = max(max_score - min_score, 0.001)

    for node_id in G.nodes():
        ndata = G.nodes[node_id]
        x, y = pos[node_id]
        score = ndata.get("total_score", 0)
        momentum = ndata.get("momentum_7d", 0)
        status = ndata.get("status", "flat")
        evidence = ndata.get("evidence_refs", [])

        # Size: scaled between 25 and 70
        normalized = (score - min_score) / score_range
        size = 25 + normalized * 45

        color = COLOR_MAP.get(node_id, "#FFFFFF")
        symbol = determine_symbol(evidence)
        emoji = STATUS_EMOJI.get(status, "")

        # Hover text
        hover_parts = [
            f"<b>{node_id}</b>",
            f"Total Score: {score:.1f}",
            f"7d Score: {ndata.get('score_7d', 0):.1f}",
            f"Momentum: {momentum:+.2f} {emoji}",
            f"Stability: {ndata.get('stability_7d', 0):.2f}",
            f"Status: {status.upper()}",
            f"Sources: {len(evidence)} ({', '.join(evidence[:6])}{'...' if len(evidence) > 6 else ''})",
        ]
        hover_text = "<br>".join(hover_parts)

        node_traces.append(
            go.Scatter(
                x=[x],
                y=[y],
                mode="markers+text",
                marker=dict(
                    size=size,
                    color=color,
                    symbol=symbol,
                    line=dict(width=2, color="#ffffff"),
                    opacity=0.9,
                ),
                text=node_id,
                textposition="top center",
                textfont=dict(size=11, color="#e0e0e0", family="Arial Black"),
                hoverinfo="text",
                hovertext=hover_text,
                name=node_id,
                showlegend=True,
            )
        )

    # -- Assemble figure --
    fig = go.Figure(data=edge_traces + node_traces)

    # Meta info
    meta = data.get("map_meta", {})
    range_info = meta.get("range", {})
    counts = meta.get("source_counts", {})
    subtitle = (
        f"Days {range_info.get('from_day_index', '?')}-{range_info.get('to_day_index', '?')} | "
        f"{counts.get('DAR', 0)} DARs | {counts.get('CT', 0)} CTs"
    )

    fig.update_layout(
        title=dict(
            text=f"KOIOS Fractal Map \u2014 Genesis Sprint<br><sup>{subtitle}</sup>",
            font=dict(size=22, color="#e0e0e0", family="Arial Black"),
            x=0.5,
        ),
        plot_bgcolor="#1a1a2e",
        paper_bgcolor="#16213e",
        font=dict(color="#e0e0e0"),
        showlegend=True,
        legend=dict(
            bgcolor="rgba(22,33,62,0.8)",
            bordercolor="#394867",
            borderwidth=1,
            font=dict(size=11),
            title=dict(text="Linie Fraktala", font=dict(size=13)),
        ),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, visible=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, visible=False),
        hovermode="closest",
        margin=dict(l=20, r=20, t=80, b=20),
        width=1200,
        height=800,
        # Annotations for alerts
        annotations=_build_alert_annotations(data.get("alerts", [])),
    )

    return fig


def _build_alert_annotations(alerts: list) -> list:
    """Build corner annotations from alerts."""
    if not alerts:
        return []

    texts = []
    for a in alerts[:5]:  # max 5 alerts shown
        severity_icon = {"critical": "\u26a0\ufe0f", "warning": "\u26a0", "info": "\u2139"}.get(
            a.get("severity", "info"), "\u2139"
        )
        texts.append(f"{severity_icon} {a['message']}")

    return [
        dict(
            text="<br>".join(texts),
            xref="paper",
            yref="paper",
            x=0.01,
            y=0.01,
            showarrow=False,
            font=dict(size=10, color="#f0ad4e"),
            bgcolor="rgba(26,26,46,0.9)",
            bordercolor="#f0ad4e",
            borderwidth=1,
            align="left",
        )
    ]


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("KOIOS Fractal Map — Visualization")
    print("=" * 60)

    print(f"\n[1/3] Loading data from: {os.path.basename(DATA_FILE)}")
    data = load_data()
    print(f"  Nodes: {len(data['nodes'])}, Edges: {len(data['edges'])}")

    print(f"\n[2/3] Building graph...")
    G = build_nx_graph(data)
    print(f"  NetworkX graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    print(f"\n[3/3] Generating interactive visualization...")
    fig = create_figure(G, data)
    fig.write_html(OUTPUT_HTML, include_plotlyjs=True, full_html=True)
    print(f"  Output: {OUTPUT_HTML} ({os.path.getsize(OUTPUT_HTML)} bytes)")

    print("\n" + "=" * 60)
    print(f"Done! Open {os.path.basename(OUTPUT_HTML)} in a browser.")
    print("=" * 60)


if __name__ == "__main__":
    main()
