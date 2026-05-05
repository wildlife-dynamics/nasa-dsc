"""
verify_outputs.py
=================
Reads the actual _transects.gpkg files written by DSC_Analysis.py and checks
whether the buffered polygons have interior holes.  A clean output (0 holes)
confirms the linemerge fix is working correctly in the pipeline.

Output
------
    Terminal  — summary table per survey
    Outputs/verify_outputs.pdf  — one page per site, grid of actual buffers

Run with:
    pixi run python DSC_Analysis/verify_outputs.py
"""

import os
import math
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from shapely.geometry import Polygon
from datetime import date

OUTPUT_DIR  = "Outputs/Analysis"
REPORT_PATH = "test-outputs/verify_outputs.pdf"
SITE_ORDER  = ["NABOISHO", "MNC", "OMC", "PCA", "ENARAU"]
GRID_COLS   = 3


def site_of(name):
    return name.split("_Survey_")[0]


def hole_count(geom):
    parts = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    return sum(len(list(p.interiors)) for p in parts)


def load_transects(survey_name):
    path = os.path.join(OUTPUT_DIR, f"DSC_Analysis_{survey_name}_transects.gpkg")
    gdf  = gpd.read_file(path)
    return gdf


# ---------------------------------------------------------------------------
# Discover surveys
# ---------------------------------------------------------------------------
all_surveys = sorted(
    f.replace("DSC_Analysis_", "").replace("_transects.gpkg", "")
    for f in os.listdir(OUTPUT_DIR)
    if f.endswith("_transects.gpkg") and "_orig_" not in f
)

by_site = {s: [f for f in all_surveys if site_of(f) == s] for s in SITE_ORDER}
by_site = {s: v for s, v in by_site.items() if v}

# ---------------------------------------------------------------------------
# Terminal summary
# ---------------------------------------------------------------------------
print()
print(f"  {'Survey':<35}  {'Transect':<15}  {'Holes':>5}  Status")
print(f"  {'-'*35}  {'-'*15}  {'-'*5}  {'-'*10}")

any_holes = False
for site, surveys in by_site.items():
    for survey_name in surveys:
        try:
            gdf = load_transects(survey_name)
            for _, row in gdf.iterrows():
                h      = hole_count(row.geometry)
                status = "clean ✓" if h == 0 else f"HOLES ✗"
                if h > 0:
                    any_holes = True
                print(f"  {survey_name:<35}  {row['name']:<15}  {h:>5}  {status}")
        except FileNotFoundError:
            print(f"  {survey_name:<35}  {'—':<15}  {'—':>5}  (no output file)")

print()
if any_holes:
    print("  ✗  Some transects still have holes — linemerge fix may not be applied.")
else:
    print("  ✓  All transects are clean — linemerge fix confirmed working.")
print()

# ---------------------------------------------------------------------------
# PDF — actual pipeline output buffers
# ---------------------------------------------------------------------------
os.makedirs("test-outputs", exist_ok=True)

with PdfPages(REPORT_PATH) as pdf:

    # Cover
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("#f5f5f5")
    ax  = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    ax.text(0.5, 0.88, "DSC Analysis — Pipeline Output Verification",
            ha="center", va="center", fontsize=20, fontweight="bold",
            transform=ax.transAxes)
    ax.text(0.5, 0.82, f"Generated: {date.today().strftime('%B %d, %Y')}",
            ha="center", va="center", fontsize=11, color="#666666",
            transform=ax.transAxes)
    ax.axhline(0.78, xmin=0.08, xmax=0.92, color="#cccccc", linewidth=0.8)
    ax.text(0.08, 0.72,
            "Reads _transects.gpkg directly from Outputs/Analysis/.\n"
            "Checks each buffered polygon for interior holes.\n"
            "0 holes = linemerge fix is working correctly in the pipeline.",
            ha="left", va="top", fontsize=11, color="#333333",
            transform=ax.transAxes, linespacing=1.8)

    # Summary table
    lines = [
        f"  {'Site':<12}  {'Surveys':>7}  {'Status'}",
        f"  {'-'*12}  {'-'*7}  {'-'*25}",
    ]
    for site, surveys in by_site.items():
        total_holes = 0
        n_loaded    = 0
        for s in surveys:
            try:
                gdf = load_transects(s)
                total_holes += sum(hole_count(r.geometry) for _, r in gdf.iterrows())
                n_loaded += 1
            except FileNotFoundError:
                pass
        if n_loaded == 0:
            status = "(no output files)"
        elif total_holes == 0:
            status = "clean ✓"
        else:
            status = f"HOLES ✗  ({total_holes} total)"
        lines.append(f"  {site:<12}  {n_loaded:>7}  {status}")

    ax.text(0.08, 0.52, "\n".join(lines),
            ha="left", va="top", fontsize=10, family="monospace",
            transform=ax.transAxes, linespacing=1.7)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

    # One page per survey — grid of actual buffer polygons
    for site, surveys in by_site.items():
        for survey_name in surveys:
            try:
                gdf = load_transects(survey_name)
            except FileNotFoundError:
                continue

            utm_crs = gdf.estimate_utm_crs()
            gdf     = gdf.to_crs(utm_crs)
            records = [(row["name"], row.geometry, hole_count(row.geometry))
                       for _, row in gdf.iterrows()]

            n    = len(records)
            cols = GRID_COLS
            rows = math.ceil(n / cols)

            fig, axes = plt.subplots(rows, cols, figsize=(4.5 * cols, 4 * rows), squeeze=False)
            fig.patch.set_facecolor("#fafafa")
            fig.suptitle(survey_name.replace("_", "  "), fontsize=13, fontweight="bold")

            for idx, (name, geom, holes) in enumerate(records):
                ax  = axes[idx // cols][idx % cols]
                fc  = "#6dbf84" if holes == 0 else "#e07070"
                ec  = "#1a6b35" if holes == 0 else "#aa2222"

                gpd.GeoDataFrame({"geometry": [geom]}, crs=utm_crs).plot(
                    ax=ax, facecolor=fc, edgecolor=ec, linewidth=0.8, alpha=0.85)

                # Highlight holes if any
                parts = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
                for hole in [h for p in parts for h in p.interiors]:
                    gpd.GeoDataFrame({"geometry": [Polygon(hole)]}, crs=utm_crs).plot(
                        ax=ax, facecolor="white", edgecolor="#0044cc", linewidth=0.7)

                label = f"{name}  ✓" if holes == 0 else f"{name}  ✗ {holes} hole(s)"
                ax.set_title(label, fontsize=7.5, pad=3)
                ax.set_aspect("equal")
                ax.tick_params(labelsize=5)
                ax.set_xlabel("")
                ax.set_ylabel("")

            for idx in range(n, rows * cols):
                axes[idx // cols][idx % cols].set_visible(False)

            plt.tight_layout()
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

print(f"Report saved → {REPORT_PATH}")
