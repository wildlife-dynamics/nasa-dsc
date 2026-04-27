"""
Quick visual test — compare simplify() tolerances for Enarau transects.
Shows the buffered result at 50 m, 100 m, 150 m, and 200 m side by side.

Run with:
    pixi run python DSC_Analysis/simplify_test.py
"""

import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.ops import linemerge

ORIG_PATH   = "Outputs/Analysis/DSC_Analysis_ENARAU_Survey_2025_02_orig_transects.gpkg"
BUFFER_DIST = 500
TOLERANCES  = [50, 100, 150, 200]

orig    = gpd.read_file(ORIG_PATH)
utm_crs = orig.estimate_utm_crs()
gdf_utm = orig.to_crs(utm_crs)

rows = list(gdf_utm.iterrows())
n    = len(rows)
cols = len(TOLERANCES)

fig, axes = plt.subplots(n, cols, figsize=(4 * cols, 3 * n))
fig.suptitle("Enarau — simplify tolerance comparison (after linemerge, before buffer)",
             fontsize=12, fontweight="bold")

for i, (_, row) in enumerate(rows):
    g      = linemerge(row.geometry) if row.geometry.geom_type == "MultiLineString" else row.geometry

    for j, tol in enumerate(TOLERANCES):
        ax  = axes[i][j] if n > 1 else axes[j]
        buf = g.simplify(tol).buffer(BUFFER_DIST, resolution=5, cap_style="flat")

        gpd.GeoDataFrame({"geometry": [buf]},        crs=utm_crs).plot(
            ax=ax, facecolor="#a8d8a8", edgecolor="#006600", linewidth=1.0, alpha=0.8)
        gpd.GeoDataFrame({"geometry": [row.geometry]}, crs=utm_crs).plot(
            ax=ax, color="#333333", linewidth=1.2)

        ax.set_title(f"{row['name']}  tol={tol} m", fontsize=8)
        ax.set_aspect("equal")
        ax.tick_params(labelsize=6)

plt.tight_layout()
out = "Outputs/enarau_simplify_tolerance_test.png"
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"Saved → {out}")
plt.show()
