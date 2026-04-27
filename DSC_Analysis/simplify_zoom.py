"""
Zoomed stages comparison for enarau_2 and enarau_4 only.

Run with:
    pixi run python DSC_Analysis/simplify_zoom.py
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from shapely.geometry import Polygon
from shapely.ops import linemerge

ORIG_PATH   = "Outputs/Analysis/DSC_Analysis_ENARAU_Survey_2025_02_orig_transects.gpkg"
BUFFER_DIST = 500
TOLERANCES  = [50, 100, 150, 200]
TRANSECTS   = {"enarau_2", "enarau_4"}

orig    = gpd.read_file(ORIG_PATH)
utm_crs = orig.estimate_utm_crs()
gdf_utm = orig.to_crs(utm_crs)

rows = [(_, row) for _, row in gdf_utm.iterrows() if row["name"] in TRANSECTS]
n    = len(rows)
cols = 2 + len(TOLERANCES)

fig, axes = plt.subplots(n, cols, figsize=(5 * cols, 5 * n))
fig.suptitle(
    "enarau_2 & enarau_4 — Before  |  linemerge only  |  simplify 50 / 100 / 150 / 200 m",
    fontsize=13, fontweight="bold"
)

col_titles = ["BEFORE\n(raw, holes)", "linemerge\n(no simplify)"] + [f"simplify\n{t} m" for t in TOLERANCES]
for j, title in enumerate(col_titles):
    axes[0][j].set_title(title, fontsize=10, fontweight="bold", pad=6)

for i, (_, row) in enumerate(rows):
    g_raw    = row.geometry
    g_merged = linemerge(g_raw) if g_raw.geom_type == "MultiLineString" else g_raw

    stages = [
        (g_raw.buffer(BUFFER_DIST, cap_style="flat"),                                           "#f7a8a8", "#cc0000"),
        (g_merged.buffer(BUFFER_DIST, resolution=5, cap_style="flat"),                          "#ffe0a0", "#b86000"),
    ] + [
        (g_merged.simplify(tol).buffer(BUFFER_DIST, resolution=5, cap_style="flat"),            "#a8d8a8", "#006600")
        for tol in TOLERANCES
    ]

    for j, (buf, fc, ec) in enumerate(stages):
        ax = axes[i][j]

        gpd.GeoDataFrame({"geometry": [buf]},   crs=utm_crs).plot(ax=ax, facecolor=fc, edgecolor=ec, linewidth=1.0, alpha=0.8)
        gpd.GeoDataFrame({"geometry": [g_raw]}, crs=utm_crs).plot(ax=ax, color="#333333", linewidth=1.2)

        parts = buf.geoms if buf.geom_type == "MultiPolygon" else [buf]
        holes = [h for p in parts for h in p.interiors]
        for hole in holes:
            gpd.GeoDataFrame({"geometry": [Polygon(hole)]}, crs=utm_crs).plot(
                ax=ax, facecolor="white", edgecolor="blue", linewidth=0.8)

        ax.set_ylabel(row["name"] if j == 0 else "", fontsize=10)
        ax.set_aspect("equal")
        ax.tick_params(labelsize=7)

        if holes:
            patch = mpatches.Patch(facecolor="white", edgecolor="blue", linewidth=0.8, label=f"{len(holes)} hole(s)")
            ax.legend(handles=[patch], fontsize=7, loc="lower right")

plt.tight_layout()
out = "Outputs/enarau_2_4_simplify_zoom.png"
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"Saved → {out}")
plt.show()
