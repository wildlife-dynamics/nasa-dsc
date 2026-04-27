"""
Experiment: linemerge + varying simplify tolerances for ENARAU_Survey_2025_02.
Writes a GeoPackage with one layer per tolerance so you can compare in QGIS.

Run with:
    pixi run python DSC_Analysis/simplify_experiment.py
"""

import geopandas as gpd
from shapely.ops import linemerge

ORIG_PATH   = "Outputs/Analysis/DSC_Analysis_ENARAU_Survey_2025_02_orig_transects.gpkg"
OUT_GPKG    = "Outputs/enarau_2025_02_simplify_experiment.gpkg"
BUFFER_DIST = 500
TOLERANCES  = [50, 100, 150, 200]

orig    = gpd.read_file(ORIG_PATH)
utm_crs = orig.estimate_utm_crs()
gdf_utm = orig.to_crs(utm_crs)

layers = []

for tol in TOLERANCES:
    gdf = gdf_utm.copy()
    gdf["geometry"] = gdf["geometry"].apply(
        lambda g: linemerge(g) if g.geom_type == "MultiLineString" else g
    )
    gdf["geometry"] = gdf["geometry"].simplify(tol)
    gdf["geometry"] = gdf["geometry"].buffer(BUFFER_DIST, resolution=5, cap_style="flat")
    gdf = gdf.to_crs("EPSG:4326")
    gdf.to_file(OUT_GPKG, layer=f"simplify_{tol}m", driver="GPKG")
    print(f"  simplify_{tol}m → written")

# Also write the original centrelines for reference
orig.to_file(OUT_GPKG, layer="orig_lines", driver="GPKG")
print(f"\nGeoPackage → {OUT_GPKG}")
print("Open in QGIS and toggle between simplify_50m / 100m / 150m / 200m layers.")
