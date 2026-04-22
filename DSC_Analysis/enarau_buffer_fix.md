# Transect Buffer — Scrambled Vertices Issue

## What Is the Error?

When the DSC pipeline writes `_transects.gpkg`, the buffered transect polygons
for **Enarau** (all quarters) and **PCA November 2024** appear scrambled or
zig-zagging when opened in QGIS. Instead of a clean elongated strip around each
transect line, the polygon looks like a tangled mess of crossing lines.

This is not a display glitch. The polygon geometry itself is broken. Each polygon
contains multiple **interior holes** — enclosed voids punched out of the interior
of the buffer strip. QGIS renders those holes as transparent cutouts, and with
many small holes scattered across the polygon the fill pattern looks scrambled.

| Site / Survey       | Transect    | Holes in buffer |
|---------------------|-------------|-----------------|
| ENARAU (all quarters) | enarau_1  | 5               |
| ENARAU (all quarters) | enarau_2  | 16              |
| ENARAU (all quarters) | enarau_3  | 11              |
| ENARAU (all quarters) | enarau_4  | 13              |
| ENARAU (all quarters) | enarau_5  | 9               |
| ENARAU (all quarters) | enarau_6  | 30              |
| ENARAU (all quarters) | enarau_7  | 0  ✓            |
| PCA 2024_11           | pardamat_1 | 87             |
| PCA 2024_11           | pardamat_2 | **322**        |
| PCA 2024_11           | pardamat_6 | 168            |
| All other sites       | all        | 0  ✓            |

---

## What Is a Hole?

In GIS, a polygon can have an outer boundary (exterior ring) and one or more
inner boundaries (interior rings). An interior ring defines a void — an area
that is excluded from the polygon's fill. A simple rectangle has no interior
rings. A donut shape has one.

A correctly buffered transect should be a simple elongated rectangle — one
exterior ring, zero interior rings. What we have instead is a polygon whose
exterior ring encloses the full strip area but whose interior rings punch out
many small voids across it:

```
Correct buffer          Buggy buffer
(0 interior rings)      (many interior rings)

 ┌────────────┐          ┌────────────┐
 │            │          │ ░░░  ░░░░ │
 │            │          │░░░ ░░ ░░░ │   ← holes rendered
 │            │          │ ░░░░  ░░░ │     as transparent
 └────────────┘          └────────────┘
```

Because the holes are small and numerous, the rendered result looks like
crossing diagonal lines — the "scrambled" appearance.

---

## Root Cause — How the Transects Are Stored

The key difference between affected and unaffected transects is how the geometry
is stored in EarthRanger.

### Unaffected sites — single connected line

NABOISHO, MNC, OMC, and all PCA quarters except 2024_11 store each transect as
a **MultiLineString with 1 part** — effectively a single connected line drawn
from start to finish in one stroke:

```
MultiLineString (1 part)
└── LineString: A ──────────────────────────── B
```

When this is buffered with a flat cap, the result is one clean rectangle:

```
 ┌──────────────────────────────────────────┐
 │                                          │
 └──────────────────────────────────────────┘
```

### Affected sites — GPS segment chain

Enarau (enarau_1 to enarau_6) and PCA 2024_11 store each transect as a
**MultiLineString with 22–82 parts**, where every part is a 2-point segment
connecting two consecutive GPS coordinates:

```
MultiLineString (25 parts)
├── LineString: A ──── B
├── LineString: B ──── C
├── LineString: C ──── D
├── LineString: D ──── E
    ... (25 segments total)
```

This happens because the transect was either:
- Traced from a GPS track file recorded in the field (one point per GPS ping),
  or
- Drawn in EarthRanger by clicking individual points rather than as a continuous
  path.

The segments share endpoints exactly (B is both the end of segment 1 and the
start of segment 2), but geometrically they are separate objects.

### Why 1-part transects buffer cleanly

A single LineString has a continuous direction along its length. The flat-cap
buffer wraps the whole line in one rectangular strip. At every internal bend the
buffer widens smoothly on the outside and narrows on the inside — no gaps:

```
       bend
        ╱
───────╱
      ╱────── one continuous rectangle, no gaps at the bend
```

### Why multi-segment transects create holes

When each 2-point segment is buffered separately and the results are unioned,
each segment gets its own flat-capped rectangle. At a bend junction, two
rectangles meet at exactly one shared point. On the **outside** of the bend the
rectangles overlap (fine). On the **inside** of the bend they leave a small
**triangular gap** because each rectangle ends flat at the shared point and the
two flat ends point in different directions:

```
  segment A rectangle        segment B rectangle

   ──────────────────┐                 ┌──────────────────
                     │   TRIANGULAR    │
   ──────────────────┘      GAP        └──────────────────
                       ↑         ↑
                   flat end   flat end
                   of A       of B
```

This triangular gap is fully enclosed by the surrounding rectangles of
neighbouring segments, so Shapely records it as an interior hole in the unioned
polygon. With 22–82 segments per transect, there are 22–82 potential gaps, and
the final polygon ends up with 3 to 322 interior holes.

**enarau_7 is unaffected** because it happens to be stored as a 1-part
MultiLineString — a single connected line — just like the unaffected sites.

---

## Comparison Across All Sites

| Site       | Survey          | Parts per transect | Holes | Affected? |
|------------|-----------------|--------------------|-------|-----------|
| NABOISHO   | all quarters    | 1                  | 0     | No        |
| MNC        | all quarters    | 1                  | 0     | No        |
| OMC        | all quarters    | 1                  | 0     | No        |
| PCA        | 2025_02–2026_02 | 1                  | 0     | No        |
| PCA        | **2024_11**     | **28–82**          | 3–322 | **Yes**   |
| ENARAU     | all quarters    | 1 (enarau_7 only)  | 0     | No        |
| ENARAU     | all quarters    | **22–30** (1–6)    | 5–30  | **Yes**   |

The root cause is purely in the source geometry stored in EarthRanger — not in
the analysis code. PCA 2024_11 uses a different transect group ID
(`0e6a223a-43d8-4bf0-9e7f-7bbf49f180ca`) from all other PCA quarters
(`83367ac7-b949-4b4c-b249-719ba63c10d2`), which is why only that quarter is
affected.

---

## The Fix

`shapely.ops.linemerge()` stitches a chain of touching 2-point segments back
into a single connected LineString before buffering. One line, one rectangle,
no junction gaps, no holes.

**Added to `DSC_Analysis.py` before the simplify/buffer block:**

```python
from shapely.ops import linemerge   # added at top of file

# Merge MultiLineString segment chains into a single LineString before buffering to
# prevent interior holes from forming at bend junctions (flat-cap buffer artefact).
transects["geometry"] = transects["geometry"].apply(
    lambda g: linemerge(g) if g.geom_type == 'MultiLineString' else g
)
transects["geometry"] = transects["geometry"].simplify(50)
transects["geometry"] = transects["geometry"].buffer(500, resolution=5, cap_style='flat', single_sided=False)
```

This is safe for all sites:
- For 1-part MultiLineStrings (all unaffected sites), `linemerge()` returns an
  equivalent LineString and the buffer result is identical.
- For multi-segment chains (Enarau, PCA 2024_11), `linemerge()` stitches the
  chain and eliminates all holes.
- Distance calculations (`off_transect_dist`, `ortho_dist`) run before this
  block and use the original geometry — they are unaffected.

---

## Reproducing and Verifying

Use `buffer_example.py` to reproduce the bug and verify the fix for any survey:

```bash
# Single survey
pixi run python DSC_Analysis/buffer_example.py ENARAU_Survey_2025_02
pixi run python DSC_Analysis/buffer_example.py PCA_Survey_2024_11

# All surveys at once
pixi run python DSC_Analysis/buffer_example.py --all
```

Each run prints a hole-count table to the terminal and writes a side-by-side
PNG plot and a GeoPackage to `Outputs/`.

Use `generate_buffer_report.py` to produce the full PDF report:

```bash
pixi run python DSC_Analysis/generate_buffer_report.py
# → Outputs/buffer_fix_report.pdf
```
