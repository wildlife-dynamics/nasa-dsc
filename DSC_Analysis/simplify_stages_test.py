"""
simplify_stages_test.py
=======================
Shows three stages for every Enarau transect side by side:

  Col 1 — Before (raw MultiLineString buffered, holes visible)
  Col 2 — linemerge only (no simplify)
  Col 3 — linemerge + simplify 50 m  (current pipeline)
  Col 4 — linemerge + simplify 100 m
  Col 5 — linemerge + simplify 150 m
  Col 6 — linemerge + simplify 200 m

Run with:
    pixi run python DSC_Analysis/simplify_stages_test.py
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from shapely.geometry import Polygon
from shapely.ops import linemerge

ORIG_PATH   = "Outputs/Analysis/DSC_Analysis_ENARAU_Survey_2025_02_orig_transects.gpkg"
BUFFER_DIST = 500
TOLERANCES  = [50, 100, 150, 200]

orig    = gpd.read_file(ORIG_PATH)
utm_crs = orig.estimate_utm_crs()
gdf_utm = orig.to_crs(utm_crs)

rows = list(gdf_utm.iterrows())
n    = len(rows)
cols = 2 + len(TOLERANCES)   # before | linemerge-only | tol×4

fig, axes = plt.subplots(n, cols, figsize=(3.5 * cols, 3 * n))
fig.suptitle(
    "Enarau — Before  |  linemerge only  |  linemerge + simplify (50 / 100 / 150 / 200 m)",
    fontsize=11, fontweight="bold"
)

col_titles = ["BEFORE\n(raw, holes)", "linemerge\n(no simplify)"] + [f"simplify\n{t} m" for t in TOLERANCES]

for j, title in enumerate(col_titles):
    ax = axes[0][j] if n > 1 else axes[j]
    ax.set_title(title, fontsize=8, fontweight="bold", pad=4)

for i, (_, row) in enumerate(rows):
    g_raw    = row.geometry
    g_merged = linemerge(g_raw) if g_raw.geom_type == "MultiLineString" else g_raw

    stages = []

    # Stage 1 — before: raw buffer, keep holes
    buf_before = g_raw.buffer(BUFFER_DIST, cap_style="flat")
    stages.append(("before", buf_before, "#f7a8a8", "#cc0000"))

    # Stage 2 — linemerge only, no simplify
    buf_merged = g_merged.buffer(BUFFER_DIST, resolution=5, cap_style="flat")
    stages.append(("linemerge", buf_merged, "#ffe0a0", "#b86000"))

    # Stages 3–6 — linemerge + increasing simplify tolerance
    for tol in TOLERANCES:
        buf = g_merged.simplify(tol).buffer(BUFFER_DIST, resolution=5, cap_style="flat")
        stages.append((f"tol={tol}", buf, "#a8d8a8", "#006600"))

    for j, (label, buf, fc, ec) in enumerate(stages):
        ax = axes[i][j] if n > 1 else axes[j]

        gpd.GeoDataFrame({"geometry": [buf]},   crs=utm_crs).plot(
            ax=ax, facecolor=fc, edgecolor=ec, linewidth=0.9, alpha=0.8)
        gpd.GeoDataFrame({"geometry": [g_raw]}, crs=utm_crs).plot(
            ax=ax, color="#333333", linewidth=1.0)

        # Highlight holes (only relevant for the before column)
        parts = buf.geoms if buf.geom_type == "MultiPolygon" else [buf]
        holes = [h for p in parts for h in p.interiors]
        for hole in holes:
            gpd.GeoDataFrame({"geometry": [Polygon(hole)]}, crs=utm_crs).plot(
                ax=ax, facecolor="white", edgecolor="blue", linewidth=0.7)

        if i == 0:
            pass  # titles already set above
        ax.set_ylabel(row["name"] if j == 0 else "", fontsize=8)
        ax.set_aspect("equal")
        ax.tick_params(labelsize=5)
        ax.set_xlabel("")

        if holes and j == 0:
            patch = mpatches.Patch(facecolor="white", edgecolor="blue", linewidth=0.7, label=f"{len(holes)} hole(s)")
            ax.legend(handles=[patch], fontsize=6, loc="lower right")

plt.tight_layout()
out = "Outputs/enarau_simplify_stages.png"
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"Saved → {out}")
plt.show()
