"""
buffer_example.py
=================
Reproduces and verifies the fix for the scrambled-buffer vertices bug for any
survey in the pipeline output directory.

Usage
-----
Run for a specific survey:
    pixi run python DSC_Analysis/buffer_example.py ENARAU_Survey_2025_02
    pixi run python DSC_Analysis/buffer_example.py PCA_Survey_2024_11

Run for all surveys found in the output directory:
    pixi run python DSC_Analysis/buffer_example.py --all

If no argument is given, defaults to ENARAU_Survey_2025_02.

What this script does
---------------------
1. Loads the _orig_transects.gpkg for the given survey from Outputs/Analysis/.
2. Projects to UTM so the 500 m buffer distance is in metres.
3. Buffers each transect twice:
     - Buggy: raw MultiLineString buffered directly — junction gaps become holes.
     - Fixed: linemerge() applied first, then buffered — no gaps, no holes.
4. Prints a summary table to the terminal showing hole counts before and after.
5. Writes both versions to a GeoPackage for visual inspection in QGIS.
6. Saves a side-by-side PNG plot (buggy left, fixed right) for every transect.

Background — the bug
---------------------
Some transects are stored in EarthRanger as a MultiLineString of individual
2-point GPS segments (one segment per GPS step).  When Shapely buffers a
MultiLineString with cap_style='flat', it wraps each segment in its own
flat-capped rectangle and then unions all rectangles.  At every bend junction,
the two rectangles share exactly one point but diverge at the bend angle,
leaving a tiny triangular gap.  Shapely records these gaps as interior holes in
the final polygon.  With many segments per transect, the holes accumulate and
produce the scrambled / zig-zagging appearance visible in QGIS.

Background — the fix
---------------------
shapely.ops.linemerge() stitches the GPS segment chain back into a single
connected LineString.  Buffering a single LineString with a flat cap produces
one clean rectangular strip — no junction gaps, no interior holes.
"""

import os
import sys
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from shapely.geometry import Polygon
from shapely.ops import linemerge

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR  = "Outputs/Analysis"
BUFFER_DIST = 500   # metres — must match DSC_Analysis.py


# ---------------------------------------------------------------------------
# parse_args()
#
# Reads the survey name(s) from the command line.
# Returns a list of survey names to process.
# ---------------------------------------------------------------------------

def parse_args():
    if len(sys.argv) < 2:
        # Default to Enarau if nothing is passed
        return ["ENARAU_Survey_2025_02"]
    if sys.argv[1] == "--all":
        # Discover all surveys that have an orig_transects.gpkg in the output dir
        files = [
            f for f in os.listdir(OUTPUT_DIR)
            if f.endswith("_orig_transects.gpkg")
        ]
        surveys = sorted(
            f.replace("DSC_Analysis_", "").replace("_orig_transects.gpkg", "")
            for f in files
        )
        if not surveys:
            print(f"No _orig_transects.gpkg files found in {OUTPUT_DIR}/")
            sys.exit(1)
        return surveys
    return [sys.argv[1]]


# ---------------------------------------------------------------------------
# run(survey_name)
#
# Runs the full buffer comparison for a single survey.
# Prints results to terminal, writes a GeoPackage, and saves a PNG plot.
# ---------------------------------------------------------------------------

def run(survey_name):
    orig_path = os.path.join(OUTPUT_DIR, f"DSC_Analysis_{survey_name}_orig_transects.gpkg")

    if not os.path.exists(orig_path):
        print(f"  [SKIP] {survey_name} — orig_transects.gpkg not found.")
        return

    os.makedirs("Outputs", exist_ok=True)

    # ── Step 1: Load and project ─────────────────────────────────────────────
    # Read the raw transect lines (WGS84) and project to the local UTM zone
    # so that the 500 m buffer distance is in metres.
    orig    = gpd.read_file(orig_path)
    utm_crs = orig.estimate_utm_crs()
    gdf_utm = orig.to_crs(utm_crs)

    # ── Step 2: Inspect input geometry ───────────────────────────────────────
    # Show geometry type and number of sub-parts per transect.
    # Transects with many parts are the ones likely affected by the bug.
    print()
    print("=" * 65)
    print(f"SURVEY: {survey_name}")
    print("=" * 65)
    print("INPUT GEOMETRIES  (EarthRanger source, projected to UTM)")
    print("-" * 65)
    for _, row in gdf_utm.iterrows():
        g       = row.geometry
        n_parts = len(list(g.geoms)) if hasattr(g, "geoms") else 1
        print(f"  {row['name']:15s}  {g.geom_type:20s}  {n_parts:2d} part(s)")

    # ── Step 3: Buffer both ways and count interior holes ────────────────────
    # Buggy:  buffer raw MultiLineString — junction gaps → interior holes.
    # Fixed:  linemerge() first → single LineString → clean strip, no holes.
    print()
    print(f"BUFFER RESULTS  ({BUFFER_DIST} m, cap_style='flat')")
    print("-" * 65)
    print(f"  {'name':15s}  {'parts':5s}  {'buggy holes':11s}  {'fixed holes':11s}  {'verdict'}")
    print(f"  {'-'*15}  {'-'*5}  {'-'*11}  {'-'*11}  {'-'*15}")

    results = []
    any_buggy = False

    for _, row in gdf_utm.iterrows():
        g       = row.geometry
        n_parts = len(list(g.geoms)) if hasattr(g, "geoms") else 1

        buf_buggy = g.buffer(BUFFER_DIST, cap_style="flat")
        buf_fixed = (
            linemerge(g).buffer(BUFFER_DIST, cap_style="flat")
            if g.geom_type == "MultiLineString"
            else g.buffer(BUFFER_DIST, cap_style="flat")
        )

        # Count interior holes across all parts — handles both Polygon and MultiPolygon
        h_buggy = sum(len(list(p.interiors)) for p in (buf_buggy.geoms if buf_buggy.geom_type == "MultiPolygon" else [buf_buggy]))
        h_fixed = sum(len(list(p.interiors)) for p in (buf_fixed.geoms  if buf_fixed.geom_type  == "MultiPolygon" else [buf_fixed]))
        verdict = "SCRAMBLED ✗" if h_buggy > 0 else "clean ✓"
        if h_buggy > 0:
            any_buggy = True

        print(f"  {row['name']:15s}  {n_parts:5d}  {str(h_buggy):11s}  {str(h_fixed):11s}  {verdict}")
        results.append({
            "name":      row["name"],
            "n_parts":   n_parts,
            "buggy":     buf_buggy,
            "fixed":     buf_fixed,
            "orig_line": g,
            "h_buggy":   h_buggy,
            "h_fixed":   h_fixed,
        })

    if not any_buggy:
        print()
        print("  No holes found — this survey is not affected by the bug.")

    # ── Step 4: Write GeoPackage ─────────────────────────────────────────────
    # Three layers per transect:
    #   {name}_buggy  — holed polygon (before fix)
    #   {name}_fixed  — clean polygon (after fix)
    #   {name}_line   — original centreline for reference
    out_gpkg = f"Outputs/{survey_name}_buffer_comparison.gpkg"
    rows = []
    for r in results:
        rows.append({"label": f"{r['name']}_buggy", "geometry": r["buggy"]})
        rows.append({"label": f"{r['name']}_fixed", "geometry": r["fixed"]})
        rows.append({"label": f"{r['name']}_line",  "geometry": r["orig_line"]})

    gpd.GeoDataFrame(rows, crs=utm_crs).to_crs("EPSG:4326").to_file(out_gpkg)
    print()
    print(f"GeoPackage → {out_gpkg}")

    # ── Step 5: Side-by-side plot ─────────────────────────────────────────────
    # Each row = one transect.
    # Left  = buggy (red, interior holes outlined in blue).
    # Right = fixed (green, clean strip).
    # Original centreline overlaid in dark grey on both sides.
    n   = len(results)
    fig, axes = plt.subplots(n, 2, figsize=(12, 3 * n))
    fig.suptitle(
        f"{survey_name} — buggy buffer (left)  vs  fixed with linemerge (right)",
        fontsize=12, fontweight="bold"
    )

    for i, r in enumerate(results):
        for col, (buf, title, fc, ec, hole_source) in enumerate([
            (r["buggy"], f"{r['name']}  BEFORE — {r['h_buggy']} hole(s)", "#f7a8a8", "#cc0000", r["buggy"]),
            (r["fixed"], f"{r['name']}  AFTER  — {r['h_fixed']} hole(s)", "#a8d8a8", "#006600", r["fixed"]),
        ]):
            ax = axes[i][col] if n > 1 else axes[col]

            gpd.GeoDataFrame({"geometry": [buf]},          crs=utm_crs).plot(ax=ax, facecolor=fc,      edgecolor=ec,      linewidth=1.2, alpha=0.7)
            gpd.GeoDataFrame({"geometry": [r["orig_line"]]}, crs=utm_crs).plot(ax=ax, color="#333333", linewidth=1.5)

            parts = hole_source.geoms if hole_source.geom_type == "MultiPolygon" else [hole_source]
            for hole in [h for p in parts for h in p.interiors]:
                gpd.GeoDataFrame({"geometry": [Polygon(hole)]}, crs=utm_crs).plot(
                    ax=ax, facecolor="white", edgecolor="blue", linewidth=0.8)

            ax.set_title(title, fontsize=9)
            ax.set_aspect("equal")
            ax.tick_params(labelsize=6)

            if len([h for p in parts for h in p.interiors]) > 0:
                patch = mpatches.Patch(facecolor="white", edgecolor="blue", linewidth=0.8, label="interior hole")
                ax.legend(handles=[patch], fontsize=7, loc="lower right")

    plt.tight_layout()
    out_png = f"Outputs/{survey_name}_buffer_comparison.png"
    plt.savefig(out_png, dpi=150, bbox_inches="tight")
    print(f"Plot      → {out_png}")
    # Only open an interactive window when running a single survey;
    # skip it in --all mode to avoid blocking between surveys.
    if len(parse_args()) == 1:
        plt.show()
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    for survey in parse_args():
        run(survey)
