"""
transect_report.py
==================
Generates a PDF report showing the buffered transect output for every survey,
grouped by site.  Clean surveys show the fixed buffer in a 3-column grid.
Affected surveys (Enarau, PCA 2024_11) show buggy vs fixed side by side.

Output
------
    Outputs/transect_report.pdf

Run with:
    pixi run python DSC_Analysis/transect_report.py
"""

import os
import math
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from shapely.geometry import Polygon
from shapely.ops import linemerge
from datetime import date

# ---------------------------------------------------------------------------
OUTPUT_DIR  = "Outputs/Analysis"
REPORT_PATH = "Outputs/transect_report.pdf"
BUFFER_DIST = 500
SIMPLIFY    = 50
GRID_COLS   = 3      # columns per row for clean surveys

SITE_ORDER = ["NABOISHO", "MNC", "OMC", "PCA", "ENARAU"]
PALETTE    = {"fixed": "#6dbf84", "buggy": "#e07070", "line": "#222222",
              "hole": "white",    "hole_edge": "#0044cc"}


# ---------------------------------------------------------------------------
def site_of(name):
    return name.split("_Survey_")[0]


def load(survey_name):
    path    = os.path.join(OUTPUT_DIR, f"DSC_Analysis_{survey_name}_orig_transects.gpkg")
    gdf     = gpd.read_file(path)
    utm_crs = gdf.estimate_utm_crs()
    gdf     = gdf.to_crs(utm_crs)

    def hole_count(buf):
        parts = buf.geoms if buf.geom_type == "MultiPolygon" else [buf]
        return sum(len(list(p.interiors)) for p in parts)

    records = []
    for _, row in gdf.iterrows():
        g      = row.geometry
        merged = linemerge(g) if g.geom_type == "MultiLineString" else g
        b_bug  = g.buffer(BUFFER_DIST, cap_style="flat")
        b_fix  = merged.simplify(SIMPLIFY).buffer(BUFFER_DIST, resolution=5, cap_style="flat")
        records.append({
            "name":    row["name"],
            "n_parts": len(list(g.geoms)) if hasattr(g, "geoms") else 1,
            "line":    g,
            "buggy":   b_bug,
            "fixed":   b_fix,
            "h_buggy": hole_count(b_bug),
            "h_fixed": hole_count(b_fix),
            "crs":     utm_crs,
        })
    return records


def draw_buf(ax, buf, fc, ec, crs):
    gpd.GeoDataFrame({"geometry": [buf]}, crs=crs).plot(
        ax=ax, facecolor=fc, edgecolor=ec, linewidth=0.8, alpha=0.85)


def draw_holes(ax, buf, crs):
    parts = buf.geoms if buf.geom_type == "MultiPolygon" else [buf]
    for hole in [h for p in parts for h in p.interiors]:
        gpd.GeoDataFrame({"geometry": [Polygon(hole)]}, crs=crs).plot(
            ax=ax, facecolor=PALETTE["hole"], edgecolor=PALETTE["hole_edge"], linewidth=0.6)


def draw_line(ax, geom, crs):
    gpd.GeoDataFrame({"geometry": [geom]}, crs=crs).plot(
        ax=ax, color=PALETTE["line"], linewidth=1.1)


def style_ax(ax, title):
    ax.set_title(title, fontsize=7.5, pad=3)
    ax.set_aspect("equal")
    ax.tick_params(labelsize=5)
    ax.set_xlabel("")
    ax.set_ylabel("")


def survey_page_clean(pdf, survey_name, records):
    """Grid layout (GRID_COLS per row) for unaffected surveys."""
    n    = len(records)
    cols = GRID_COLS
    rows = math.ceil(n / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(4.5 * cols, 4 * rows), squeeze=False)
    fig.patch.set_facecolor("#fafafa")
    fig.suptitle(survey_name.replace("_", "  "), fontsize=13, fontweight="bold")

    for idx, r in enumerate(records):
        ax = axes[idx // cols][idx % cols]
        draw_buf(ax, r["fixed"], PALETTE["fixed"], "#1a6b35", r["crs"])
        draw_line(ax, r["line"], r["crs"])
        style_ax(ax, r["name"])

    # Hide unused axes
    for idx in range(n, rows * cols):
        axes[idx // cols][idx % cols].set_visible(False)

    plt.tight_layout()
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def survey_page_affected(pdf, survey_name, records):
    """Two-column layout (buggy | fixed) for affected surveys."""
    n = len(records)
    fig, axes = plt.subplots(n, 2, figsize=(10, 3.8 * n), squeeze=False)
    fig.patch.set_facecolor("#fafafa")
    fig.suptitle(survey_name.replace("_", "  "), fontsize=13, fontweight="bold")

    for i, r in enumerate(records):
        crs = r["crs"]

        ax = axes[i][0]
        draw_buf(ax, r["buggy"], PALETTE["buggy"], "#aa2222", crs)
        draw_holes(ax, r["buggy"], crs)
        draw_line(ax, r["line"], crs)
        style_ax(ax, f"{r['name']}  BEFORE — {r['h_buggy']} hole(s)")
        if r["h_buggy"] > 0:
            ax.legend(handles=[mpatches.Patch(
                facecolor=PALETTE["hole"], edgecolor=PALETTE["hole_edge"],
                linewidth=0.6, label="interior hole")], fontsize=6, loc="lower right")

        ax = axes[i][1]
        draw_buf(ax, r["fixed"], PALETTE["fixed"], "#1a6b35", crs)
        draw_line(ax, r["line"], crs)
        style_ax(ax, f"{r['name']}  AFTER — {r['h_fixed']} hole(s)")

    plt.tight_layout()
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
all_files = sorted(
    f.replace("DSC_Analysis_", "").replace("_orig_transects.gpkg", "")
    for f in os.listdir(OUTPUT_DIR) if f.endswith("_orig_transects.gpkg")
)

by_site = {s: [f for f in all_files if site_of(f) == s] for s in SITE_ORDER}
by_site = {s: v for s, v in by_site.items() if v}

os.makedirs("Outputs", exist_ok=True)

# ---------------------------------------------------------------------------
with PdfPages(REPORT_PATH) as pdf:

    # ── Cover ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("#f5f5f5")
    ax  = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")

    ax.text(0.5, 0.88, "DSC Analysis — Transect Buffer Report",
            ha="center", va="center", fontsize=22, fontweight="bold",
            transform=ax.transAxes)
    ax.text(0.5, 0.82, f"Generated: {date.today().strftime('%B %d, %Y')}",
            ha="center", va="center", fontsize=11, color="#666666",
            transform=ax.transAxes)
    ax.axhline(0.78, xmin=0.08, xmax=0.92, color="#cccccc", linewidth=0.8)

    summary_lines = [
        f"  {'Site':<12}  {'Surveys':>7}  {'Transects':>10}  {'Status'}",
        f"  {'-'*12}  {'-'*7}  {'-'*10}  {'-'*20}",
    ]
    for site, surveys in by_site.items():
        try:
            recs         = load(surveys[0])
            residual     = sum(r["h_fixed"] for r in recs)
            was_affected = any(r["h_buggy"] > 0 for r in recs)
            if residual > 0:
                status = f"residual holes ({residual})"
            elif was_affected:
                status = "clean ✓  (linemerge applied)"
            else:
                status = "clean ✓"
            summary_lines.append(f"  {site:<12}  {len(surveys):>7}  {len(recs):>10}  {status}")
        except Exception:
            summary_lines.append(f"  {site:<12}  {len(surveys):>7}  {'—':>10}  (could not load)")

    ax.text(0.08, 0.72, "\n".join(summary_lines),
            ha="left", va="top", fontsize=10, family="monospace",
            transform=ax.transAxes, linespacing=1.7)

    ax.text(0.5, 0.28,
            "Affected surveys (Enarau, PCA 2024_11) had transects stored as GPS segment chains\n"
            "in EarthRanger.  linemerge() stitches them into a single line before buffering,\n"
            "eliminating interior holes.  All other sites were unaffected.",
            ha="center", va="center", fontsize=10, color="#444444",
            transform=ax.transAxes, linespacing=1.6)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

    # ── Survey pages — detect affected per survey ────────────────────────────
    for site, surveys in by_site.items():
        for survey_name in surveys:
            try:
                records  = load(survey_name)
                affected = any(r["h_buggy"] > 0 for r in records)
                if affected:
                    survey_page_affected(pdf, survey_name, records)
                else:
                    survey_page_clean(pdf, survey_name, records)
            except Exception as e:
                print(f"  [skip] {survey_name}: {e}")

print(f"Report saved → {REPORT_PATH}")
