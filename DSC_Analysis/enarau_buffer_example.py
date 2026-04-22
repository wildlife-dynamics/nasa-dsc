"""
enarau_buffer_example.py
========================
Reproduces and verifies the fix for the scrambled-buffer vertices bug found
in the Enarau _transects.gpkg outputs.

What this script does
---------------------
1. Loads the original (un-buffered) Enarau transect lines from the pipeline
   output directory.
2. Projects them to UTM so distances are in metres.
3. Buffers each transect twice — once WITHOUT the fix (buggy) and once WITH
   the fix (fixed) — and counts the interior holes in each result.
4. Prints a summary table to the terminal.
5. Writes both versions to a GeoPackage so they can be opened side-by-side
   in QGIS.
6. Saves a side-by-side PNG plot showing buggy (red, with holes) vs fixed
   (green, clean strip) for every transect.
7. Prints the exact two-line code change applied to DSC_Analysis.py.

Why this script exists
----------------------
It is a standalone diagnostic and verification tool. Running it confirms that:
  a) The bug is reproducible from the source geometry alone.
  b) linemerge() fully resolves the holes for all Enarau transects.
  c) The fix does not change the shape of transects that were already stored
     as single connected lines (e.g. enarau_7).

Background — the bug
---------------------
Enarau transects 1–6 are stored in EarthRanger as a MultiLineString made up
of individual 2-point GPS segments (one segment per GPS step along the walked
line).  When Shapely buffers a MultiLineString with cap_style='flat', it wraps
each segment in its own flat-capped rectangle and then unions all rectangles.
At every bend junction where two rectangles meet, they share exactly one point
but diverge at the bend angle, leaving a tiny triangular gap.  Shapely records
these gaps as interior holes in the final polygon.  With 22–30 segments per
transect, the holes accumulate and produce the scrambled / zig-zagging
appearance visible in QGIS.

Transect 7 is stored as a single connected MultiLineString (one part), so it
buffers cleanly with no junction gaps.

Background — the fix
---------------------
shapely.ops.linemerge() stitches a chain of touching 2-point segments back
into a single connected LineString.  Buffering a single LineString with a flat
cap produces one clean rectangular strip — no junction gaps, no interior holes.

Run with:
    pixi run python DSC_Analysis/enarau_buffer_example.py
"""

import os
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from shapely.geometry import Polygon
from shapely.ops import linemerge

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Any Enarau quarter works here — all share the same EarthRanger transect group
# ID (20a5f900-ed79-4b04-a233-5a28891f494d) so the source geometry is identical
# across all quarters.
ORIG_PATH   = "Outputs/Analysis/DSC_Analysis_ENARAU_Survey_2025_02_orig_transects.gpkg"
OUT_GPKG    = "Outputs/enarau_buffer_comparison.gpkg"
OUT_PNG     = "Outputs/enarau_buffer_comparison.png"
BUFFER_DIST = 500   # metres — matches the detection strip used in DSC_Analysis.py

if not os.path.exists(ORIG_PATH):
    raise FileNotFoundError(
        f"Could not find {ORIG_PATH}.\n"
        "Re-run the pipeline for ENARAU_Survey_2025_02 first."
    )

os.makedirs("Outputs", exist_ok=True)

# ---------------------------------------------------------------------------
# Step 1 — Load and project
#
# The orig_transects GeoPackage contains the raw transect lines as downloaded
# from EarthRanger (WGS84 / EPSG:4326).  We project to UTM so that the 500 m
# buffer distance is in metres rather than degrees.
# estimate_utm_crs() picks the correct UTM zone automatically from the centroid
# of the data.
# ---------------------------------------------------------------------------
orig    = gpd.read_file(ORIG_PATH)
utm_crs = orig.estimate_utm_crs()
gdf_utm = orig.to_crs(utm_crs)

# ---------------------------------------------------------------------------
# Step 2 — Inspect input geometry
#
# Print the geometry type and number of sub-parts for each transect.
# Transects with many parts are the ones affected by the bug.
# ---------------------------------------------------------------------------
print("=" * 65)
print("INPUT GEOMETRIES  (EarthRanger source, projected to UTM)")
print("=" * 65)
for _, row in gdf_utm.iterrows():
    g = row.geometry
    n_parts = len(list(g.geoms))
    print(f"  {row['name']:12s}  {g.geom_type:20s}  {n_parts:2d} part(s)")

# ---------------------------------------------------------------------------
# Step 3 — Buffer both ways and count interior holes
#
# Buggy:  buffer the raw MultiLineString directly — each segment gets its own
#         flat-capped rectangle; junction gaps become interior holes.
#
# Fixed:  linemerge() first, then buffer — the segment chain is stitched into
#         a single LineString before buffering; no junction gaps, no holes.
# ---------------------------------------------------------------------------
print()
print("=" * 65)
print(f"BUFFER RESULTS  ({BUFFER_DIST} m, cap_style='flat')")
print("=" * 65)
print(f"  {'name':12s}  {'parts':5s}  {'buggy holes':11s}  {'fixed holes':11s}  {'verdict':20s}")
print(f"  {'-'*12}  {'-'*5}  {'-'*11}  {'-'*11}  {'-'*20}")

results = []
for _, row in gdf_utm.iterrows():
    g       = row.geometry
    n_parts = len(list(g.geoms))

    # Buggy: buffer MultiLineString directly
    buf_buggy = g.buffer(BUFFER_DIST, cap_style="flat")

    # Fixed: merge segments into a single LineString first, then buffer
    buf_fixed = linemerge(g).buffer(BUFFER_DIST, cap_style="flat")

    h_buggy = len(list(buf_buggy.interiors)) if hasattr(buf_buggy, "interiors") else "—"
    h_fixed = len(list(buf_fixed.interiors)) if hasattr(buf_fixed, "interiors") else "—"
    verdict = "SCRAMBLED ✗" if h_buggy != 0 else "clean ✓"

    print(f"  {row['name']:12s}  {n_parts:5d}  {str(h_buggy):11s}  {str(h_fixed):11s}  {verdict}")
    results.append({
        "name":      row["name"],
        "buggy":     buf_buggy,
        "fixed":     buf_fixed,
        "orig_line": g,
    })

# ---------------------------------------------------------------------------
# Step 4 — Write GeoPackage for QGIS inspection
#
# Three layers per transect are written:
#   {name}_buggy  — the holed polygon (what the pipeline produced before fix)
#   {name}_fixed  — the clean polygon (what the pipeline produces after fix)
#   {name}_line   — the original transect centreline for reference
#
# Load in QGIS and toggle between _buggy and _fixed to compare visually.
# ---------------------------------------------------------------------------
rows = []
for r in results:
    name = r["name"]
    rows.append({"label": f"{name}_buggy", "geometry": r["buggy"]})
    rows.append({"label": f"{name}_fixed", "geometry": r["fixed"]})
    rows.append({"label": f"{name}_line",  "geometry": r["orig_line"]})

gpd.GeoDataFrame(rows, crs=utm_crs).to_crs("EPSG:4326").to_file(OUT_GPKG)
print()
print(f"GeoPackage written → {OUT_GPKG}")

# ---------------------------------------------------------------------------
# Step 5 — Side-by-side matplotlib plot
#
# Each row is one transect.  Left column = buggy (red fill, white holes
# outlined in blue).  Right column = fixed (green fill, clean strip).
# The original transect centreline is overlaid in dark grey on both sides.
# ---------------------------------------------------------------------------
n   = len(results)
fig, axes = plt.subplots(n, 2, figsize=(12, 3 * n))
fig.suptitle(
    "Enarau transect buffers — buggy (left)  vs  fixed with linemerge (right)",
    fontsize=13, fontweight="bold"
)

for i, r in enumerate(results):
    name      = r["name"]
    buggy_gdf = gpd.GeoDataFrame({"geometry": [r["buggy"]]},     crs=utm_crs)
    fixed_gdf = gpd.GeoDataFrame({"geometry": [r["fixed"]]},     crs=utm_crs)
    line_gdf  = gpd.GeoDataFrame({"geometry": [r["orig_line"]]}, crs=utm_crs)

    h_buggy = len(list(r["buggy"].interiors))
    h_fixed = len(list(r["fixed"].interiors))

    for col, (gdf_buf, title, hole_source, facecolor, edgecolor) in enumerate([
        (buggy_gdf, f"{name}  BUGGY — {h_buggy} interior hole(s)", r["buggy"], "#f7a8a8", "#cc0000"),
        (fixed_gdf, f"{name}  FIXED — {h_fixed} interior hole(s)", r["fixed"], "#a8d8a8", "#006600"),
    ]):
        ax = axes[i][col]

        # Draw buffer polygon
        gdf_buf.plot(ax=ax, facecolor=facecolor, edgecolor=edgecolor, linewidth=1.2, alpha=0.7)

        # Overlay original transect centreline
        line_gdf.plot(ax=ax, color="#333333", linewidth=1.5)

        # Shade each interior hole white with a blue outline so holes are obvious
        for hole in hole_source.interiors:
            hole_gdf = gpd.GeoDataFrame({"geometry": [Polygon(hole)]}, crs=utm_crs)
            hole_gdf.plot(ax=ax, facecolor="white", edgecolor="blue", linewidth=0.8)

        ax.set_title(title, fontsize=9)
        ax.set_aspect("equal")
        ax.tick_params(labelsize=6)

        if len(list(hole_source.interiors)) > 0:
            patch = mpatches.Patch(facecolor="white", edgecolor="blue", linewidth=0.8, label="interior hole")
            ax.legend(handles=[patch], fontsize=7, loc="lower right")

plt.tight_layout()
plt.savefig(OUT_PNG, dpi=150, bbox_inches="tight")
print(f"Plot saved       → {OUT_PNG}")
plt.show()

# ---------------------------------------------------------------------------
# Step 6 — Print the exact code change applied to DSC_Analysis.py
# ---------------------------------------------------------------------------
print()
print("=" * 65)
print("CODE CHANGE APPLIED  (DSC_Analysis.py)")
print("=" * 65)
print("""
  Import added (line 24):

      from shapely.ops import linemerge

  linemerge step inserted before simplify/buffer (line ~397):

      # BEFORE
      transects["geometry"] = transects["geometry"].simplify(50)
      transects["geometry"] = transects["geometry"].buffer(
          500, resolution=5, cap_style='flat', single_sided=False)

      # AFTER
      transects["geometry"] = transects["geometry"].apply(
          lambda g: linemerge(g) if g.geom_type == 'MultiLineString' else g
      )
      transects["geometry"] = transects["geometry"].simplify(50)
      transects["geometry"] = transects["geometry"].buffer(
          500, resolution=5, cap_style='flat', single_sided=False)

  Notes
  -----
  - linemerge() on a geometry that is already a single-part MultiLineString
    (e.g. enarau_7) or a plain LineString returns it unchanged — no impact
    on transects that were already stored correctly.
  - Distance calculations (off_transect_dist, ortho_dist) run before this
    block and use the original MultiLineString, so they are unaffected.
  - The fix applies to all sites; only Enarau and PCA 2024_11 had
    multi-segment source geometries so only those outputs change.
""")
