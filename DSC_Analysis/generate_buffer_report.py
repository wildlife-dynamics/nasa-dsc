"""
generate_buffer_report.py
=========================
Generates a multi-page PDF report documenting the scrambled buffer vertices
bug found in Enarau and PCA Nov 2024 transects, the root cause, and the fix.

What this script produces
-------------------------
  Outputs/buffer_fix_report.pdf  — a 4-page PDF containing:

  Page 1 — Cover / Summary
      Plain-English description of the issue, root cause, fix applied, and
      the list of affected surveys.

  Page 2 — Interior Hole Count table
      One row per transect for each affected survey.  Shows the number of GPS
      segments in the source geometry, the hole count before the fix (buggy),
      the hole count after the fix (fixed), and a FIXED / residual status.
      Built live by buffering the orig_transects both ways and counting holes —
      not read from a saved file, so numbers always reflect current geometry.

  Page 3 — Enarau before / after plots
      Side-by-side visual for every Enarau transect.  Left = buggy (red, with
      white/blue interior holes).  Right = fixed (green, clean strip).  Uses
      the 2025_02 quarter as representative (all quarters share the same source
      geometry group ID so all quarters look identical).

  Page 4 — PCA 2024_11 before / after plots
      Same layout as page 3 but for the PCA November 2024 survey, which used a
      different EarthRanger transect group ID and was also stored as GPS segments.

How hole counts are computed
-----------------------------
load_transects() reads the _orig_transects.gpkg for a given survey, projects
to UTM, and buffers each transect twice:
  - buf_buggy: raw MultiLineString buffered directly (reproduces the bug)
  - buf_fixed: linemerge() applied first, then buffered (applies the fix)
Interior holes are counted with len(list(geom.interiors)).

Why only two surveys are shown
-------------------------------
All other sites (NABOISHO, MNC, OMC) and all other PCA quarters store their
transects as single connected lines (one-part MultiLineString), so they
produce zero holes with or without linemerge.  Only Enarau (all quarters) and
PCA 2024_11 were affected.

Run with:
    pixi run python DSC_Analysis/generate_buffer_report.py
"""

import os
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from shapely.geometry import Polygon
from shapely.ops import linemerge
from datetime import date

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR  = "Outputs/Analysis"   # where pipeline writes _orig_transects.gpkg files
REPORT_PATH = "Outputs/buffer_fix_report.pdf"
BUFFER_DIST = 500                  # metres — must match DSC_Analysis.py

os.makedirs("Outputs", exist_ok=True)
TODAY = date.today().strftime("%B %d, %Y")


# ---------------------------------------------------------------------------
# load_transects()
#
# Reads the orig_transects GeoPackage for a given survey, projects to UTM,
# and buffers each transect both ways (buggy and fixed).  Returns a list of
# dicts — one per transect — containing the geometries and hole counts needed
# by both the table and the plot pages.
#
# Parameters
# ----------
# site_prefix  : used only to identify the survey in calling code (not used
#                inside the function — survey_name fully identifies the file)
# survey_name  : e.g. "ENARAU_Survey_2025_02" — used to build the file path
# ---------------------------------------------------------------------------

def load_transects(site_prefix, survey_name):
    orig_path = os.path.join(OUTPUT_DIR, f"DSC_Analysis_{survey_name}_orig_transects.gpkg")
    orig      = gpd.read_file(orig_path)
    utm_crs   = orig.estimate_utm_crs()
    orig_utm  = orig.to_crs(utm_crs)

    results = []
    for _, row in orig_utm.iterrows():
        g       = row.geometry
        n_parts = len(list(g.geoms)) if hasattr(g, "geoms") else 1

        # Buggy: buffer MultiLineString directly — junction gaps become holes
        buf_buggy = g.buffer(BUFFER_DIST, cap_style="flat")

        # Fixed: stitch segments into one LineString first, then buffer
        buf_fixed = (
            linemerge(g).buffer(BUFFER_DIST, cap_style="flat")
            if g.geom_type == "MultiLineString"
            else g.buffer(BUFFER_DIST, cap_style="flat")
        )

        h_buggy = len(list(buf_buggy.interiors)) if hasattr(buf_buggy, "interiors") else 0
        h_fixed = len(list(buf_fixed.interiors)) if hasattr(buf_fixed, "interiors") else 0

        results.append({
            "name":    row["name"],
            "n_parts": n_parts,   # number of GPS segments in source geometry
            "buggy":   buf_buggy,
            "fixed":   buf_fixed,
            "line":    g,         # original centreline for overlay
            "h_buggy": h_buggy,
            "h_fixed": h_fixed,
            "crs":     utm_crs,
        })
    return results


# ---------------------------------------------------------------------------
# Build PDF
# ---------------------------------------------------------------------------

with PdfPages(REPORT_PATH) as pdf:

    # ── PAGE 1: Cover / Summary ──────────────────────────────────────────────
    # Plain-English description of the issue, root cause, fix, and scope.
    # Written as a matplotlib figure so it sits naturally alongside the plot
    # pages in the same PDF.
    # ────────────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("#f8f8f8")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")

    # Title and date
    ax.text(0.5, 0.88, "DSC Analysis — Transect Buffer Fix Report",
            ha="center", va="center", fontsize=20, fontweight="bold",
            transform=ax.transAxes)
    ax.text(0.5, 0.82, f"Prepared: {TODAY}",
            ha="center", va="center", fontsize=11, color="#555555",
            transform=ax.transAxes)
    ax.axhline(0.78, xmin=0.05, xmax=0.95, color="#cccccc", linewidth=1)

    summary = (
        "Issue\n"
        "─────\n"
        "The buffered transect polygons for Enarau (all quarters) and PCA November 2024\n"
        "displayed scrambled / zig-zagging geometry when opened in QGIS. On inspection,\n"
        "the polygons contained between 3 and 322 interior holes — artefacts of how the\n"
        "transect lines were stored in EarthRanger.\n\n"
        "Root Cause\n"
        "──────────\n"
        "EarthRanger stored these transects as a MultiLineString of individual 2-point GPS\n"
        "segments (one segment per GPS step) rather than a single connected line. When each\n"
        "segment is buffered with a flat cap, the two rectangles at every bend junction leave\n"
        "a small triangular gap. Shapely records these gaps as interior holes in the final\n"
        "union polygon. With 22–82 segments per transect, the holes accumulate and produce\n"
        "the scrambled appearance.\n\n"
        "Fix Applied\n"
        "───────────\n"
        "A linemerge() call was added to DSC_Analysis.py immediately before the simplify /\n"
        "buffer step. linemerge() stitches the GPS segment chain back into a single connected\n"
        "LineString. Buffering a single line with a flat cap produces one clean rectangular\n"
        "strip with no junction gaps and no interior holes.\n\n"
        "Code change (DSC_Analysis.py, before the simplify/buffer block):\n\n"
        "    transects[\"geometry\"] = transects[\"geometry\"].apply(\n"
        "        lambda g: linemerge(g) if g.geom_type == 'MultiLineString' else g\n"
        "    )\n\n"
        "Affected Files\n"
        "──────────────\n"
        "  • ENARAU — all 6 quarters (2024_11 through 2026_02)  →  enarau_1–6 fixed\n"
        "  • PCA    — 2024_11 only (different source transect group in EarthRanger)\n"
        "  • All other sites and quarters were unaffected (transects already stored\n"
        "    as single connected lines)."
    )

    ax.text(0.08, 0.73, summary,
            ha="left", va="top", fontsize=10, family="monospace",
            transform=ax.transAxes, linespacing=1.6)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

    # ── PAGE 2: Interior Hole Count table ────────────────────────────────────
    # One row per transect.  Columns: Survey, Transect, GPS Segments,
    # Holes (buggy), Holes (fixed), Status.
    # Header row: dark blue.  Status column: green = FIXED, orange = residual.
    # Alternating row shading for readability.
    # ────────────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.patch.set_facecolor("#f8f8f8")
    ax.axis("off")
    ax.set_title("Interior Hole Count — Before and After Fix",
                 fontsize=14, fontweight="bold", pad=20)

    headers   = ["Survey", "Transect", "GPS Segments", "Holes (buggy)", "Holes (fixed)", "Status"]
    rows_data = []

    for survey_name, site_prefix in [
        ("ENARAU_Survey_2025_02", "enarau"),
        ("PCA_Survey_2024_11",    "pardamat"),
    ]:
        try:
            results = load_transects(site_prefix, survey_name)
            for r in results:
                status = "FIXED ✓" if r["h_fixed"] == 0 else "residual"
                rows_data.append([
                    survey_name.replace("_Survey_", " "),
                    r["name"],
                    str(r["n_parts"]),
                    str(r["h_buggy"]),
                    str(r["h_fixed"]),
                    status,
                ])
        except FileNotFoundError:
            pass

    col_widths = [0.22, 0.13, 0.13, 0.15, 0.14, 0.10]
    table = ax.table(
        cellText=rows_data,
        colLabels=headers,
        colWidths=col_widths,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.5)

    for (row, col), cell in table.get_celld().items():
        if row == 0:
            # Header row
            cell.set_facecolor("#2c5f8a")
            cell.set_text_props(color="white", fontweight="bold")
        elif row % 2 == 0:
            # Alternating row shading
            cell.set_facecolor("#f0f4f8")
        # Status column colouring
        if col == 5 and row > 0:
            val = rows_data[row - 1][5]
            cell.set_facecolor("#c8e6c9" if "FIXED" in val else "#ffccbc")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

    # ── PAGES 3+: Before / after plots per site ──────────────────────────────
    # One page per affected survey.  Each row = one transect.
    # Left column  = buggy buffer (red fill, interior holes outlined in blue).
    # Right column = fixed buffer (green fill, clean strip).
    # Original transect centreline overlaid in dark grey on both sides.
    # ────────────────────────────────────────────────────────────────────────
    for survey_name, site_prefix, page_title in [
        ("ENARAU_Survey_2025_02", "enarau",
         "Enarau Survey 2025_02 — representative quarter (all quarters identical)"),
        ("PCA_Survey_2024_11", "pardamat",
         "PCA Survey 2024_11"),
    ]:
        try:
            results = load_transects(site_prefix, survey_name)
        except FileNotFoundError:
            continue

        n    = len(results)
        fig, axes = plt.subplots(n, 2, figsize=(11, 3.2 * n))
        fig.suptitle(page_title, fontsize=13, fontweight="bold", y=1.01)

        for i, r in enumerate(results):
            crs = r["crs"]

            for col, (buf, label, fc, ec, hole_source) in enumerate([
                # Left column — buggy
                (r["buggy"], f"{r['name']}  BEFORE — {r['h_buggy']} hole(s)",
                 "#f7a8a8", "#cc0000", r["buggy"]),
                # Right column — fixed
                (r["fixed"], f"{r['name']}  AFTER  — {r['h_fixed']} hole(s)",
                 "#a8d8a8", "#006600", r["fixed"]),
            ]):
                ax = axes[i][col] if n > 1 else axes[col]

                # Draw buffer polygon
                gpd.GeoDataFrame({"geometry": [buf]}, crs=crs).plot(
                    ax=ax, facecolor=fc, edgecolor=ec, linewidth=1.0, alpha=0.7)

                # Overlay original transect centreline
                gpd.GeoDataFrame({"geometry": [r["line"]]}, crs=crs).plot(
                    ax=ax, color="#222222", linewidth=1.2)

                # Highlight each interior hole in white with a blue border
                for hole in hole_source.interiors:
                    gpd.GeoDataFrame({"geometry": [Polygon(hole)]}, crs=crs).plot(
                        ax=ax, facecolor="white", edgecolor="#0000cc", linewidth=0.7)

                ax.set_title(label, fontsize=8)
                ax.set_aspect("equal")
                ax.tick_params(labelsize=6)

                if len(list(hole_source.interiors)) > 0:
                    patch = mpatches.Patch(
                        facecolor="white", edgecolor="#0000cc",
                        linewidth=0.7, label="interior hole"
                    )
                    ax.legend(handles=[patch], fontsize=6, loc="lower right")

        plt.tight_layout()
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

print(f"Report saved → {REPORT_PATH}")
