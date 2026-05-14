# DSC Analysis Pipeline

Processes Distance Sampling Count (DSC) survey data from one or more EarthRanger conservancy sites. For each survey it downloads patrol events and transect lines, computes observer-to-transect and animal-to-transect distances, labels transects with satellite NDVI and terrain slope, and writes analysis-ready CSVs and GeoPackages for use in R distance sampling models.

---

## Background

Distance sampling is a standard wildlife census method. Rangers walk pre-defined transect lines and record every animal group they detect, noting the angle and distance to each sighting. The key assumption is that detection probability decreases with perpendicular distance from the transect — animals close to the line are nearly always seen, those far away are increasingly missed. Fitting a detection function to the distribution of perpendicular distances gives a density estimate that accounts for this drop-off.

This pipeline takes raw EarthRanger events and produces the perpendicular distances, enriched with satellite-derived covariates (NDVI, slope), needed to fit those models.

### What rangers record in the field

Each wildlife sighting is logged as an EarthRanger event of type `distancecountwildlife_rep` and captures:

| Field | Description |
|---|---|
| GPS position | Observer's location at the moment of the sighting |
| Radial angle | Angle in degrees, clockwise from the transect bearing, to the animal |
| Distance to centre | Straight-line distance in metres from the observer to the animal |
| Species | Species observed |
| Total count | Number of animals in the group |
| Juveniles | Number of juveniles in the group |

Rangers also log a patrol-start event (`distancecountpatrol_rep`) at the beginning of each transect leg, recording the transect ID and number of observers. Because this information is on the start event only — not on each sighting — the pipeline propagates it forward and backward to every observation row within the same patrol.

---

## How it works

### Animal position projection

The observer's GPS position is not where the animal is. The pipeline converts each sighting into an estimated animal position using:

```
animal_x = observer_x + dist_to_centre × sin(radial_angle)
animal_y = observer_y + dist_to_centre × sin(radial_angle)
```

Both calculations are done in a projected UTM coordinate system (not geographic WGS84) so that distances are in metres and trigonometry is Euclidean. The appropriate UTM zone is selected automatically from the transects' centroid using `estimate_utm_crs()`.

The original observer GPS geometry is kept as `orig_geometry` for QA — you can load both in QGIS to verify that projected positions look sensible relative to the transect.

### Distance calculations

Distances are computed twice, once before and once after the projection step:

**1. `off_transect_dist`** — distance from the observer's GPS position to the transect centreline. This is a data quality check: if an observer recorded a sighting on transect A but their GPS puts them far from transect A's line, something is wrong (wrong transect ID entered, GPS error, etc.). Events with no matching transect are dropped.

**2. `ortho_dist`** — perpendicular distance from the *projected animal position* to the transect centreline. This is the distance that goes into the detection function in R. It reflects where the animal actually was, not where the observer stood.

### Transect buffering and intersection check

After computing distances, transects are buffered to create a 500 m detection strip on each side of the centreline. Before buffering:

1. **`linemerge`** — EarthRanger stores transect tracks as a chain of 2-point GPS segments (one per GPS fix), not as a single connected LineString. If you buffer each 2-point segment individually with `cap_style='flat'`, the flat end caps leave triangular gaps at every bend where segments meet. `linemerge` stitches the chain into a single LineString first so the buffer produces one clean, continuous strip.

2. **Simplify** — a Douglas-Peucker simplification is applied (default 50 m tolerance, 25 m for ENARAU) to smooth out GPS noise before buffering.

3. **Buffer** — 500 m flat-capped on both sides using `buffer(500, cap_style='flat', single_sided=False)`.

Each sighting's projected animal position is then tested against its transect's buffered polygon to produce the `intersects_transect` boolean column. Observations outside the strip are retained in the output (for model truncation decisions in R) but flagged.

### NDVI composite (NASA HLS)

NDVI is used as a vegetation density covariate in the detection function — denser vegetation reduces detectability. The pipeline builds a single cloud-free mean NDVI image per survey from NASA's Harmonized Landsat-Sentinel (HLS) dataset on Google Earth Engine.

Two sensor collections are merged:

| Collection | Sensor | Revisit |
|---|---|---|
| `NASA/HLS/HLSL30/v002` | Landsat 8/9 | ~16 days |
| `NASA/HLS/HLSS30/v002` | Sentinel-2 A/B | ~5 days |

Together they achieve ~2–3 day revisit at 30 m resolution, which maximises the chance of getting cloud-free coverage over the survey area. Before merging, each image is pixel-level cloud/shadow/water masked using the Fmask quality band (lower 4 bits = 0 means clear). Optical bands are scaled from integer DN to reflectance (×0.0001). The merged collection is reduced to a per-pixel mean, then NDVI = (NIR − Red) / (NIR + Red) is computed.

If `ndvi_window_days` is set when calling `run_analysis`, only images within ±N days of the survey start date are included. This keeps NDVI representative of conditions at the time of the survey rather than a long-term seasonal average. Without it, the full archive is used.

NDVI and slope are sampled over each transect polygon using `ee.Reducer.mean()` in a single `reduceRegions` call per image — one GEE request covers all transects at once.

### Terrain slope (SRTM)

Terrain slope is computed from the USGS SRTMGL1 30 m digital elevation model using `ee.Terrain.slope()`. It is included as a covariate because slope affects both animal distribution and ranger detectability. Mean slope is sampled over each buffered transect polygon, then joined onto every observation row on that transect.

### Survey metadata

Patrol-start events (`distancecountpatrol_rep`) are pulled from the same event batch and processed separately from wildlife sightings. They record who was on each patrol leg, which transect was walked, and how many observers there were. These are written to `_metadata.csv` — one row per patrol leg — and are not included in the main analysis file.

### Full pipeline flow

```
EarthRanger
  ├── Transect lines  (get_spatial_features_group)
  └── Patrol events   (get_patrol_events → summary only)
            │
            ▼ get_events in batches of 25 (full payload with event_details)
            │
            ├──► distancecountpatrol_rep  ──► _metadata.csv
            │
            └──► distancecountwildlife_rep
                      │
                      ▼
            Resolve event_details enum values → human-readable titles
            Unpack event_details dict → flat columns
            Forward/back-fill transect_id + num_observers per patrol
            Rename columns via eventColumnTransform
                      │
                      ▼ Reproject to UTM
                      │
                      ├── off_transect_dist  (observer GPS → transect centreline)
                      │
                      ├── Project observer → animal position
                      │     animal_x = obs_x + dist × sin(angle)
                      │     animal_y = obs_y + dist × sin(angle)
                      │
                      ├── ortho_dist  (animal position → transect centreline)
                      │
                      └── intersects_transect  (animal within 500m buffer?)
                                │
                                ▼ Reproject transects → WGS84
                                │
                      Google Earth Engine
                        ├── HLS NDVI composite  → mean NDVI per transect
                        └── SRTM slope          → mean slope per transect
                                │
                                ▼ Join NDVI + slope onto events
                                │
                      Write to Outputs/Analysis/
```

---

## Setup

### 1. Install dependencies

```bash
pixi install
```

Key dependencies managed by pixi:

| Package | Purpose |
|---|---|
| `geopandas` | Spatial operations: CRS reprojection, geometry construction, distance calculations |
| `earthengine-api` | Google Earth Engine Python client for NDVI and slope extraction |
| `ecoscope-workflows-ext-ecoscope` | EarthRanger and EarthEngine connection management |
| `ecoscope-workflows-ext-custom` | `process_events_details` — resolves EarthRanger event field enums to human-readable titles |
| `pydantic` | Config validation — catches misconfigured `config.json` at startup with clear error messages |
| `python-dotenv` | Loads credentials from `.env` into the environment |

### 2. Create a `.env` file

Place a `.env` file in the project root (`nasa-dsc/`). Each EarthRanger site uses its own credential prefix matching `credentialsKey` in `config.json`:

```env
# EarthRanger credentials — one pair per site
NABOISHO_USERNAME=your_username
NABOISHO_PASSWORD=your_password

PCA_USERNAME=your_username
PCA_PASSWORD=your_password

ENARAU_USERNAME=your_username
ENARAU_PASSWORD=your_password

MNC_USERNAME=your_username
MNC_PASSWORD=your_password

OMC_USERNAME=your_username
OMC_PASSWORD=your_password

# Google Earth Engine service account
EE_SERVICE_ACCOUNT=your-service-account@project.iam.gserviceaccount.com
EE_PRIVATE_KEY_FILE=/path/to/private-key.json
EE_PROJECT=your-gee-project-id
```

The `EE_PRIVATE_KEY_FILE` must point to the JSON key file downloaded from the Google Cloud service account. EarthEngine is initialised once at startup before any surveys run — if it fails, the whole pipeline exits.

### 3. Configure surveys

Edit `config.json` to add or update surveys. See [Config reference](#config-reference) below.

---

## Running

```bash
pixi run run-analysis
```

Or directly from the project root:

```bash
python DSC_Analysis/DSC_Analysis.py
```

Surveys across all sites run sequentially. A failure in one survey is caught, logged with a full traceback, and the pipeline moves on to the next. Progress is printed after each survey:

```
2026-05-14 09:01:23 - INFO - Starting pipeline: 12 surveys across 5 site(s)
2026-05-14 09:04:11 - INFO - Progress: 1/12 surveys complete
...
2026-05-14 09:47:03 - INFO - Progress: 12/12 surveys complete
```

---

## Outputs

All files are written to `Outputs/Analysis/` with the survey name embedded in the filename.

| File | Description |
|---|---|
| `DSC_Analysis_{name}_metadata.csv` | One row per patrol leg. Contains team members, observer count, transect ID, and timestamps. Sourced from patrol-start events (`distancecountpatrol_rep`). |
| `DSC_Analysis_{name}_analysis_data.csv` | One row per wildlife sighting. Contains all distance measurements, species, counts, NDVI, slope, and the `intersects_transect` flag. This is the primary input for R distance sampling models. |
| `DSC_Analysis_{name}_events.gpkg` | Spatial layer of projected animal positions with key distance fields. Load in QGIS alongside `_transects.gpkg` to visually QA sightings. |
| `DSC_Analysis_{name}_transects.gpkg` | Buffered transect polygons (500 m) with mean NDVI and slope per transect. |
| `DSC_Analysis_{name}_orig_transects.gpkg` | Original unmodified transect centrelines from EarthRanger, before linemerge, simplification, and buffering. Useful for verifying raw transect geometry. |

### Key columns in `_analysis_data.csv`

| Column | Description |
|---|---|
| `transect_id` | Transect name as recorded by the ranger on the patrol-start event |
| `patrol_serial_number` | Unique patrol identifier — groups all events from the same patrol leg |
| `survey_id` | Survey name from `config.json`, identifies which survey run produced this row |
| `species` | Species observed |
| `totalcount` | Total animals in the group |
| `num_juveniles` | Juvenile count within the group |
| `num_observers` | Number of observers on the patrol leg (propagated from patrol-start event) |
| `radialangle` | Angle (degrees clockwise from transect bearing) from observer to animal |
| `dist_to_centre` | Straight-line distance (m) from observer to animal |
| `off_transect_dist` | Distance (m) from observer GPS to the transect centreline — data quality check |
| `ortho_dist` | Perpendicular distance (m) from projected animal position to the transect — primary DSC input |
| `intersects_transect` | `True` if the projected animal position falls within the 500 m detection strip |
| `orig_geometry` | Observer's GPS position (WKT) — preserved for QA comparison against projected position |
| `geometry` | Projected animal position (WKT) |
| `NDVI_HSL` | Mean NDVI of the transect polygon from the HLS composite |
| `img_date_hsl_ndvi` | Survey start date used as the reference date for the NDVI temporal window |
| `slope` | Mean terrain slope (degrees) of the transect polygon from SRTM |

---

## Config reference

`config.json` is a list of connection blocks. Each block defines one EarthRanger server and all the surveys to run against it. Multiple sites can be configured in the same file and are processed in order.

```json
[
  {
    "earthranger": {
      "server": "https://site.pamdas.org",
      "patrolType": "<patrol-type-uuid>",
      "credentialsKey": "SITE_KEY",
      "exportTimezone": "Africa/Nairobi",
      "eventColumnTransform": {
        "time": "time",
        "event_type": "event_type",
        "serial_number": "serial_number",
        "patrol_id": "patrol_id",
        "patrol_serial_number": "patrol_serial_number",
        "event_details__distancecountpatrol_transectid": "transect_id",
        "event_details__distancecountpatrol_numberofobservers": "num_observers",
        "event_details__distancecountwildlife_species": "species",
        "event_details__distancecountwildlife_totalcount": "totalcount",
        "event_details__distancecountwildlife_numberofjuveniles": "num_juveniles",
        "event_details__distancecountwildlife_radialangle": "radialangle",
        "event_details__distancecountwildlife_distancetocentre": "dist_to_centre"
      }
    },
    "surveys": [
      {
        "surveyName": "SITE_Survey_2026_02",
        "since": "2026-02-17T03:00:00+0000",
        "until": "2026-02-18T10:00:00+0000",
        "erSpatialTransectsGroupId": "<spatial-feature-group-uuid>",
        "simplifyTolerance": 50
      }
    ]
  }
]
```

### `earthranger` fields

| Field | Required | Description |
|---|---|---|
| `server` | Yes | Base URL of the EarthRanger instance (e.g. `https://naboisho.pamdas.org`) |
| `patrolType` | Yes | UUID of the patrol type to query. Found in the EarthRanger admin panel under patrol types. |
| `credentialsKey` | Yes | Prefix used to look up `<KEY>_USERNAME` and `<KEY>_PASSWORD` from `.env`. Must match exactly. |
| `exportTimezone` | No | IANA timezone for converting event timestamps in outputs. Defaults to `UTC`. Use `Africa/Nairobi` for Kenya sites. |
| `eventColumnTransform` | Yes | Maps raw flattened `event_details__*` column names to output column names. The keys use double-underscore notation: `event_details__<event_type_schema_key>`. |

### `surveys` fields

| Field | Required | Description |
|---|---|---|
| `surveyName` | Yes | Arbitrary label used in all output filenames for this survey. Convention: `{SITE}_{Survey}_{YYYY}_{MM}`. |
| `since` / `until` | Yes | ISO 8601 datetime window for querying patrol events. Use UTC offset (`+0000`) for consistency. The window should start slightly before the first patrol and end after the last patrol. |
| `erSpatialTransectsGroupId` | Yes | UUID of the EarthRanger spatial features group containing the transect lines. Find it via the EarthRanger spatial features API or admin panel. |
| `simplifyTolerance` | No | Douglas-Peucker simplification tolerance in metres applied to transect geometry before buffering. Defaults to `50`. Use `25` for sites with tighter transect curves (e.g. ENARAU). Too high a value will cut corners off curved transects. |

---

## Adding a new site or survey

**New survey on an existing site:**

1. Add an entry to the `surveys` list under the relevant `earthranger` block in `config.json`.
2. Set `since`/`until` to bracket the survey window.
3. Confirm `erSpatialTransectsGroupId` is correct — this UUID can change if the spatial feature group was deleted and recreated in EarthRanger between surveys.

**New site:**

1. Add a new top-level object to `config.json` with its own `earthranger` block and `surveys` list.
2. Add `<KEY>_USERNAME` and `<KEY>_PASSWORD` to `.env`, using the same key as `credentialsKey`.
3. Get the `patrolType` UUID from the EarthRanger admin for that server.
4. Get the `erSpatialTransectsGroupId` UUID for the transect feature group on that server.
5. Check the `event_details__*` key names in `eventColumnTransform` — field schema keys can differ between EarthRanger instances.

---

## Troubleshooting

**`Missing credentials for 'SITE'`** — the `credentialsKey` in `config.json` does not match the prefix in `.env`. Both must be identical (case-sensitive).

**`No patrol events found for survey`** — the `since`/`until` window does not cover any patrols of the configured `patrolType`. Check the patrol type UUID and time window in EarthRanger.

**`No matching spatial transect: <name>`** — the transect ID recorded by the ranger does not match any feature name in the EarthRanger spatial features group. Usually a data entry error in the field — the ranger typed a transect ID that doesn't exist.

**GEE authentication error** — the service account key file path or project ID in `.env` is wrong, or the service account does not have Earth Engine API access enabled in Google Cloud.

**NDVI is `NaN` for some transects** — no cloud-free HLS imagery was available over that transect within the configured time window. Widen `ndvi_window_days` or remove it to use the full archive.
