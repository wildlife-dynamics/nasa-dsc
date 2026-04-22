# =============================================================================
# DSC Analysis Pipeline
# =============================================================================
# This script runs a Distance Sampling Count (DSC) analysis across one or more
# EarthRanger sites. For each survey it:
#   1. Downloads the spatial transect lines and patrol events from EarthRanger
#   2. Cleans and filters events to wildlife observation records only
#   3. Projects each observed animal's position using its radial angle and
#      distance-to-centre measurement (standard DSC field protocol)
#   4. Calculates each event's off-transect and orthogonal distances, and flags
#      whether the projected position falls within a 500m buffer of the transect
#   5. Labels each transect with NDVI (from NASA HLS) and terrain slope (SRTM)
#      via a single Google Earth Engine composite call
#   6. Writes the enriched events and transects to CSV and GeoPackage
#
# Configuration is read from config.json. Credentials are loaded from .env.
# =============================================================================

# --- Standard library ---
import os                                # file path construction and directory creation
import json                              # parsing config.json
import logging                           # structured logging throughout the pipeline
from math import sin, radians            # trigonometry for projecting animal positions from radial angle
from shapely.ops import linemerge        # merges MultiLineString segments into a single LineString before buffering
from datetime import datetime            # type annotation for survey date fields in pydantic models
from typing import Dict, Optional, List  # type hints used in function signatures and pydantic models

# --- Environment ---
from dotenv import load_dotenv           # reads EarthRanger and EarthEngine credentials from a .env file into os.environ

# --- Data manipulation ---
import pandas as pd                      # tabular operations: event merges, column transforms, chunked API calls
import geopandas as gpd                  # spatial operations: CRS projection, geometry construction, distance calculations

# --- Google Earth Engine ---
import ee                                # GEE Python API — used to build the NDVI composite and extract slope per transect

# --- Ecoscope ---
import ecoscope                          # wildlife movement analytics library; must be initialised with ecoscope.init() before use
from ecoscope.io.eetools import label_gdf_with_img        # samples a single GEE image over all features in a GeoDataFrame using ee.Image.reduceRegions() (one GEE call for all transects)
from ecoscope.io.earthranger_utils import normalize_column # unpacks a nested JSON column (e.g. event_details) into individual flat columns on the DataFrame

# --- EarthRanger / EarthEngine connections ---
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection  # manages OAuth/token auth and HTTP session to an EarthRanger server
from ecoscope_workflows_ext_ecoscope.connections import EarthEngineConnection  # initialises the GEE Python session using a Google service account key file
from ecoscope_workflows_ext_custom.tasks.io import process_events_details      # resolves event_details enum values and maps field keys to human-readable titles using the EarthRanger event type schema

# --- Config validation ---
from pydantic import BaseModel, Field, TypeAdapter
# BaseModel   — defines the schema for each config section and validates types on load
# Field       — maps camelCase JSON keys to snake_case Python attributes via aliases
# TypeAdapter — parses the top-level config list (List[ConnectionConfig]) without a wrapper model


# =============================================================================
# Initialisation
# =============================================================================

ecoscope.init()  # registers ecoscope extensions and prepares internal state before any ecoscope calls

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()  # populates os.environ from .env — must run before any os.getenv() calls


# =============================================================================
# Configuration Models
# =============================================================================
# These pydantic models mirror the structure of config.json and enforce types
# at load time, so misconfigured surveys fail immediately with a clear error.

class SurveyConfig(BaseModel):
    name: str = Field(alias='surveyName')           # human-readable survey label, used in output filenames
    since: datetime                                  # start of the patrol event query window (ISO 8601)
    until: datetime                                  # end of the patrol event query window (ISO 8601)
    group_id: str = Field(alias='erSpatialTransectsGroupId')  # UUID of the spatial feature group containing the transect lines


class EarthRangerConfig(BaseModel):
    server: str                                                                          # base URL of the EarthRanger instance
    patrol_type: str = Field(alias='patrolType')                                         # UUID of the patrol type to query events from
    credentials_key: str = Field(alias='credentialsKey')                                 # prefix used to look up USERNAME/PASSWORD env vars (e.g. NABOISHO → NABOISHO_USERNAME)
    export_tz: str = Field('UTC', alias='exportTimezone')                                # IANA timezone for converting event timestamps before export
    event_column_transform: Dict[str, str] = Field(default_factory=dict, alias='eventColumnTransform')  # maps raw EarthRanger column names to output column names


class ConnectionConfig(BaseModel):
    earthranger: EarthRangerConfig
    surveys: List[SurveyConfig]


# =============================================================================
# Helpers
# =============================================================================

def load_configuration(config_path: str = 'config.json') -> List[ConnectionConfig]:
    # Reads config.json and validates every field against the pydantic models above.
    # Raises a clear validation error if any field is missing, mistyped, or malformed.
    with open(config_path, 'r') as f:
        data = json.load(f)
    return TypeAdapter(List[ConnectionConfig]).validate_python(data)


def transform_df_columns(df: pd.DataFrame, column_map_dict: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    # Subsets the DataFrame to only the columns listed as keys in column_map_dict,
    # then renames them to the corresponding values. This standardises the raw
    # EarthRanger event schema into the expected output column names.
    # Columns present in the map but missing from the DataFrame are warned and skipped.
    if not column_map_dict:
        return df.copy()

    missing_cols = set(column_map_dict.keys()) - set(df.columns)
    if missing_cols:
        logger.warning(f"Columns in mapping not found in DataFrame: {missing_cols}")
        column_map_dict = {k: v for k, v in column_map_dict.items() if k not in missing_cols}

    if not column_map_dict:
        logger.warning("No valid columns to transform after filtering missing columns")
        return df.copy()

    existing_cols = list(set(column_map_dict.keys()) & set(df.columns))
    return df[existing_cols].rename(columns=column_map_dict)


def chunk_df(df: pd.DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    # Splits a DataFrame into smaller chunks for paginated API calls.
    # The EarthRanger get_events endpoint has a practical limit on how many
    # event IDs can be requested at once, so we batch them in groups of chunk_size.
    return [df.iloc[i: i + chunk_size].copy() for i in range(0, len(df), chunk_size)]


def calculate_distance(rows: pd.DataFrame, transects: gpd.GeoDataFrame, dist: str = "dist") -> pd.DataFrame:
    # Called per transect group (via groupby). Looks up the transect geometry by name
    # and computes the Euclidean distance from each event point to that line geometry.
    # Returns dist=-1 if no matching transect is found so the caller can filter those out.
    patrol_transect_geo = transects[transects["name"] == rows.name]
    if patrol_transect_geo.empty:
        logger.warning(f"No matching spatial transect: {rows.name}")
        rows[dist] = -1
        return rows
    if len(patrol_transect_geo) > 1:
        logger.warning(f"More than one matching transect found for: {rows.name}")
    rows[dist] = rows["geometry"].distance(patrol_transect_geo.iloc[0]["geometry"], align=False)
    return rows


def point_pos(x0: float, y0: float, d: float, theta: float):
    # Converts a DSC field observation (observer position + radial angle + distance)
    # into the estimated (x, y) position of the observed animal in projected coordinates.
    # theta is the radial angle in degrees measured clockwise from the transect bearing.
    theta_rad = radians(theta)
    return x0 + d * sin(theta_rad), y0 + d * sin(theta_rad)


# def repair_transect_buffer(geom):
#     # EarthRanger sometimes stores transect lines as a MultiLineString of individual
#     # 2-point GPS segments rather than a single connected LineString.  When these are
#     # buffered with cap_style='flat', the union of many flat-capped rectangles leaves
#     # triangular gaps at bend junctions, which become interior holes in the output
#     # polygon and render as scrambled vertices in QGIS.
#     #
#     # Step 1 — linemerge: stitches a chain of touching segments into a single
#     # LineString so the buffer produces one clean strip.  If linemerge cannot fully
#     # resolve the chain (e.g. the source geometry has micro-loops or reversed segments),
#     # the result is still a MultiLineString with fewer parts, and step 2 handles
#     # any residual holes.
#     #
#     # Step 2 — fill holes: drops any interior rings that survive after buffering.
#     # These are always artefacts of tangled source geometry, not intentional voids.
#     from shapely.geometry import Polygon as ShapelyPolygon
#     if geom.geom_type == 'MultiLineString':
#         geom = linemerge(geom)
#     if geom.geom_type == 'Polygon' and len(list(geom.interiors)) > 0:
#         return ShapelyPolygon(geom.exterior)
#     return geom


def do_events_intersect_transect(events: pd.DataFrame, transects: gpd.GeoDataFrame) -> pd.DataFrame:
    # For each transect group, checks whether each event's projected animal position
    # falls within the 500m buffered transect polygon. This is used downstream in
    # distance sampling models to determine which observations are within the detection strip.
    try:
        patrol_transect = transects[transects["name"] == events.name]
        events["intersects_transect"] = events["geometry"].intersects(patrol_transect.iloc[0]["geometry"])
    except Exception:
        events["intersects_transect"] = False
    return events


def unpack_event_details(df: pd.DataFrame) -> pd.DataFrame:
    # Expands the event_details dict column into flat columns without any prefix.
    # Must be called after process_events_details(map_to_titles=True) so the dict keys
    # are already human-readable display names (e.g. "Transect ID", "Team Members").
    # For sites where the schema endpoint returns 404 the raw internal key names are kept.
    details = pd.DataFrame(
        df['event_details'].apply(lambda x: x if isinstance(x, dict) else {}).tolist(),
        index=df.index
    )
    return pd.concat([df.drop(columns=['event_details']), details], axis=1)




def build_hls_ndvi_image(aoi: ee.FeatureCollection, since: datetime, ndvi_window_days: Optional[int]) -> ee.Image:
    # Constructs a single cloud-free mean NDVI composite image from NASA's Harmonized
    # Landsat-Sentinel (HLS) dataset for use in labelling survey transects.
    #
    # HLS combines two sensors at 30m resolution:
    #   - HLS_L30: Landsat 8/9, ~16 day revisit
    #   - HLS_S30: Sentinel-2 A/B, ~5 day revisit
    # Together they provide ~2-3 day revisit, maximising the chance of cloud-free coverage.
    #
    # Each image is pixel-level cloud masked using the Fmask quality band before merging.
    # The merged collection is reduced to a single mean composite, which fills any remaining
    # cloud gaps and avoids the duplicate-timestamp issue that arises when both sensors
    # acquire on the same day.
    #
    # If ndvi_window_days is set, the collection is restricted to a symmetric window around
    # the survey start date, ensuring NDVI reflects vegetation conditions at the time of the
    # survey rather than a long-term seasonal average. Set to None for the full archive.

    SharedBands = ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'Fmask']
    L30Bands    = ['B2',   'B3',    'B4',  'B5',  'B6',    'B7',    'Fmask']  # Landsat band names
    S30Bands    = ['B2',   'B3',    'B4',  'B8A', 'B11',   'B12',   'Fmask']  # Sentinel-2 band names

    def maskHLS(ee_image):
        # Fmask encodes cloud, cloud shadow, water, and snow in the lower 4 bits.
        # bitwiseAnd('1111') isolates those bits; .eq(0) keeps only clear pixels.
        # Optical bands are scaled by 0.0001 to convert from integer DN to reflectance (0–1).
        qaMask = ee_image.select('Fmask').bitwiseAnd(int('1111', 2)).eq(0)
        opticalBands = ee_image.select(['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2']).multiply(0.0001)
        return (ee_image
                .updateMask(qaMask)
                .addBands(opticalBands, None, True)
                .copyProperties(ee_image, ['system:time_start', 'system:index', 'SPACECRAFT_ID', 'CLOUD_COVER']))

    l30 = (ee.ImageCollection('NASA/HLS/HLSL30/v002')
           .filter(ee.Filter.lt('CLOUD_COVERAGE', 30))  # scene-level cloud filter before pixel masking
           .filterBounds(aoi)
           .select(L30Bands, SharedBands)
           .map(maskHLS))

    s30 = (ee.ImageCollection('NASA/HLS/HLSS30/v002')
           .filter(ee.Filter.lt('CLOUD_COVERAGE', 30))
           .filterBounds(aoi)
           .select(S30Bands, SharedBands)
           .map(maskHLS))

    if ndvi_window_days is not None:
        # Restrict both collections to a symmetric window around the survey date.
        # ±30 days (default) is broad enough to guarantee some cloud-free imagery
        # in most African savanna environments while remaining seasonally representative.
        survey_date_ee = ee.Date(since.isoformat())
        date_filter = ee.Filter.date(
            survey_date_ee.advance(-ndvi_window_days, 'day'),
            survey_date_ee.advance(ndvi_window_days, 'day')
        )
        l30 = l30.filter(date_filter)
        s30 = s30.filter(date_filter)

    # Compute per-pixel NDVI = (NIR - Red) / (NIR + Red), then reduce to a single mean
    # composite across all available cloud-free images in the window.
    return (l30.merge(s30)
            .map(lambda img: img.addBands(img.normalizedDifference(['NIR', 'Red']).rename('NDVI_HSL')))
            .select('NDVI_HSL')
            .mean())


# =============================================================================
# Main Analysis
# =============================================================================

def run_analysis(
    er_connection: EarthRangerConnection,
    survey_config: SurveyConfig,
    er_config: EarthRangerConfig,
    ndvi_window_days: Optional[int] = None,  # recommended: 30 (±30 days around survey date for survey-representative NDVI)
):
    # Orchestrates the full DSC analysis for a single survey:
    # transect download → event download → cleaning → distance geometry →
    # EE labelling → output export.
    logger.info(f"--- Starting Analysis for Survey: {survey_config.name} ---")

    survey_name          = survey_config.name
    since_filter         = survey_config.since
    until_filter         = survey_config.until
    transects_group_id   = survey_config.group_id
    export_tz            = er_config.export_tz
    event_column_transform = er_config.event_column_transform

    # Download the pre-defined transect lines from EarthRanger's spatial features.
    # These define where each patrol was supposed to walk and are used as the reference
    # geometry for all subsequent distance calculations.
    transects = er_connection.get_spatial_features_group(spatial_features_group_id=transects_group_id).set_crs(epsg=4326)
    original_transects = transects.copy()  # preserve unmodified transects for the output GeoPackage

    # Download all patrol events of the configured patrol type within the survey window.
    # The initial query returns summary records only; full event_details are fetched below.
    patrol_events = er_connection.get_patrol_events(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(),
        patrol_type=er_config.patrol_type,
    ).set_index('id')

    if patrol_events.empty:
        logger.warning(f"No patrol events found for survey '{survey_name}'. Skipping.")
        return

    # Fetch full events (with event_details) in batches of 25.
    # get_patrol_events returns summary records only; get_events fetches the complete
    # event payload including nested event_details (custom field data).
    events = pd.concat([
        er_connection.get_events(
            event_ids=chunk.index.astype(str).values.flatten().tolist(),
            include_details=True
        )
        for chunk in chunk_df(patrol_events, chunk_size=25)
    ])

    # Filter → select → resolve → unpack
    survey_events = events[events['event_type'] == 'distancecountpatrol_rep'].copy()
    survey_events['latitude']    = survey_events['location'].apply(lambda v: v.get('latitude')  if isinstance(v, dict) else None)
    survey_events['longitude']   = survey_events['location'].apply(lambda v: v.get('longitude') if isinstance(v, dict) else None)
    survey_events['reported_by'] = survey_events['reported_by'].apply(lambda v: v.get('name', '') if isinstance(v, dict) else str(v) if pd.notna(v) else '')
    survey_events = survey_events[['serial_number', 'event_type', 'time', 'latitude', 'longitude', 'reported_by', 'event_details']]
    survey_events = process_events_details(survey_events, client=er_connection, map_to_titles=True)
    survey_events = unpack_event_details(survey_events)
    survey_events['Team Members'] = survey_events['Team Members'].apply(lambda v: ', '.join(v) if isinstance(v, list) else v)

    # Merge event_details back into patrol_events for the main wildlife analysis pipeline.
    patrol_events = pd.merge(
        left=patrol_events,
        right=events[['event_details']],
        how='left',
        left_index=True,
        right_index=True,
    )

    # Convert timestamps to the survey's local timezone, unpack the event_details JSON
    # column into individual flat columns, then rename/subset to the expected output schema.
    patrol_events['time'] = patrol_events['time'].dt.tz_convert(export_tz)
    normalize_column(patrol_events, "event_details")
    patrol_events = transform_df_columns(df=patrol_events, column_map_dict=event_column_transform)

    # transect_id and num_observers are recorded once per patrol leg (on the patrol start event),
    # not on every wildlife observation. Forward and back-fill within each patrol serial number
    # to propagate these values to all observation rows.
    for col in ['transect_id', 'num_observers']:
        patrol_events[[col]] = (
            patrol_events.groupby('patrol_serial_number', as_index=False, group_keys=False)[[col]]
            .apply(lambda x: x.bfill().ffill())
        )

    # Keep only wildlife observation events (distance count records).
    # Other event types (patrol start/end, incidentals) are not used in the DSC model.
    patrol_events = patrol_events[patrol_events['event_type'] == 'distancecountwildlife_rep']
    patrol_events['survey_id'] = survey_name

    # Reproject both layers to the local UTM CRS for accurate metric distance calculations.
    # estimate_utm_crs() selects the appropriate UTM zone based on the transects' centroid.
    utm_crs = transects.estimate_utm_crs()
    transects     = transects.to_crs(utm_crs)
    patrol_events = patrol_events.to_crs(utm_crs)

    # Calculate the distance from each observer position to the transect centreline.
    # This is the raw off-transect distance before applying the radial angle correction.
    # Events with no matching transect (dist=-1) are dropped.
    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(calculate_distance, transects=transects, dist="off_transect_dist", include_groups=False)
        .reset_index()
    )
    patrol_events = patrol_events[patrol_events["off_transect_dist"] != -1]

    # Project each observation from the observer's GPS position to the estimated animal position
    # using the recorded radial angle (degrees) and distance-to-centre (metres).
    # The original observer geometry is preserved for QA purposes.
    patrol_events["orig_geometry"] = patrol_events["geometry"]
    patrol_events["geometry"] = gpd.points_from_xy(
        *zip(*patrol_events.apply(
            lambda x: point_pos(x["geometry"].x, x["geometry"].y, x["dist_to_centre"], x["radialangle"]),
            axis=1
        )),
        crs=utm_crs
    )

    # Recalculate distance from the projected animal position to the transect.
    # This orthogonal distance is the key input for the distance sampling detection function.
    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(calculate_distance, transects=transects, dist="ortho_dist", include_groups=False)
        .reset_index()
    )
    patrol_events = patrol_events[patrol_events["ortho_dist"] != -1]

    # Merge MultiLineString segment chains into a single LineString before buffering to
    # prevent interior holes from forming at bend junctions (flat-cap buffer artefact).
    # Then simplify and buffer by 500m flat-capped to create the detection strip.
    transects["geometry"] = transects["geometry"].apply(
        lambda g: linemerge(g) if g.geom_type == 'MultiLineString' else g
    )
    transects["geometry"] = transects["geometry"].simplify(50)
    transects["geometry"] = transects["geometry"].buffer(500, resolution=5, cap_style='flat', single_sided=False)

    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(do_events_intersect_transect, transects=transects, include_groups=True)
        .set_index("id")
    )

    # Reproject transects back to WGS84 for GEE (which expects geographic coordinates).
    transects = transects.to_crs(epsg=4326)
    transects['survey_date'] = since_filter

    # Build the HLS NDVI composite for the survey period and sample it over all transects
    # in a single GEE reduceRegions call. Do the same for SRTM-derived terrain slope.
    # Both are assigned via .values to bypass pandas index alignment — label_gdf_with_img
    # internally calls .explode() which can misalign the index and cause a row explosion
    # when merging. Using .values is safe because GEE preserves feature order.
    aoi         = ee.FeatureCollection(transects.__geo_interface__)
    ndvi_image  = build_hls_ndvi_image(aoi=aoi, since=since_filter, ndvi_window_days=ndvi_window_days)
    slope_image = ee.Terrain.slope(ee.Image("USGS/SRTMGL1_003").select("elevation"))

    transects['NDVI_HSL']        = label_gdf_with_img(gdf=transects, img=ndvi_image,  region_reducer=ee.Reducer.mean(), scale=30.0)['mean'].values
    transects['img_date_hsl_ndvi'] = since_filter
    transects['slope']           = label_gdf_with_img(gdf=transects, img=slope_image, region_reducer=ee.Reducer.mean(), scale=30.0)['mean'].values

    transects = transects[["name", "img_date_hsl_ndvi", "NDVI_HSL", "slope", "geometry"]]

    # Join NDVI and slope from transects onto events so every observation row carries
    # the environmental covariates of its transect.
    patrol_events = patrol_events.merge(
        transects.drop(columns=["geometry"]),
        left_on="transect_id",
        right_on="name",
        how="left"
    ).drop(columns=["name"])

    # Write all five output files for this survey:
    #   _survey_metadata.csv   — one row per transect leg, shows which transects were surveyed
    #   _analysis_data.csv     — full enriched event table for use in R/distance sampling models
    #   _events.gpkg           — spatial event layer (projected animal positions + key attributes)
    #   _transects.gpkg        — buffered transects with NDVI and slope labels
    #   _orig_transects.gpkg   — original unmodified transect centrelines
    output_dir = os.path.join('.', 'Outputs', 'Analysis')
    os.makedirs(output_dir, exist_ok=True)

    survey_events.to_csv(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_metadata.csv'), index=False)

    csv_columns = [
        "transect_id", "level_1", "title", "patrol_id", "patrol_serial_number",
        "totalcount", "num_juveniles", "event_type", "num_observers", "dist_to_centre",
        "geometry", "species", "time", "serial_number", "radialangle", "survey_id",
        "off_transect_dist", "orig_geometry", "ortho_dist", "intersects_transect",
        "img_date_hsl_ndvi", "NDVI_HSL", "slope"
    ]
    # Only include columns that exist in the dataframe (guards against optional fields)
    csv_columns = [c for c in csv_columns if c in patrol_events.columns]
    patrol_events[csv_columns].to_csv(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_analysis_data.csv'), index=False)
    patrol_events[["serial_number", "transect_id", "dist_to_centre", "ortho_dist", "intersects_transect", "geometry"]].to_file(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_events.gpkg'), index=False)
    transects.to_file(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_transects.gpkg'), index=False)
    original_transects.to_file(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_orig_transects.gpkg'), index=False)

    logger.info(f"--- Finished Analysis for Survey: {survey_config.name} ---")


# =============================================================================
# Entry Point
# =============================================================================

def main():
    # Initialise Earth Engine first — all subsequent EE calls (including inside run_analysis)
    # rely on the session being active. Uses a service account key file for non-interactive auth.
    try:
        EarthEngineConnection(
            service_account=os.getenv("EE_SERVICE_ACCOUNT") or "",
            private_key_file=os.getenv("EE_PRIVATE_KEY_FILE") or "",
            ee_project=os.getenv("EE_PROJECT") or "",
        ).get_client()
        logger.info("Successfully connected to EarthEngine")
    except Exception as e:
        logger.error(f"Failed to connect to EarthEngine: {e}")
        return

    try:
        configs = load_configuration(os.path.join(os.path.dirname(__file__), 'config.json'))
    except Exception as e:
        logger.error(f"Failed to load or parse config.json: {e}")
        return

    # Count total surveys upfront so we can log a progress indicator after each one.
    all_surveys = [(conn_config, survey) for conn_config in configs for survey in conn_config.surveys]
    total = len(all_surveys)
    completed = 0
    logger.info(f"Starting pipeline: {total} surveys across {len(configs)} site(s)")

    for conn_config in configs:
        er_config = conn_config.earthranger
        logger.info(f"Processing server: {er_config.server}")

        # Credentials are stored in .env as <KEY>_USERNAME / <KEY>_PASSWORD
        # so each site can have its own login without hardcoding values.
        er_username = os.getenv(f"{er_config.credentials_key}_USERNAME")
        er_password = os.getenv(f"{er_config.credentials_key}_PASSWORD")

        if not er_username or not er_password:
            logger.error(
                f"Missing credentials for '{er_config.credentials_key}'. "
                f"Set {er_config.credentials_key}_USERNAME and {er_config.credentials_key}_PASSWORD in .env."
            )
            completed += len(conn_config.surveys)
            continue

        try:
            er_io = EarthRangerConnection(
                server=er_config.server,
                username=er_username,
                password=er_password,
                tcp_limit=5,        # max concurrent TCP connections to the EarthRanger API
                sub_page_size=4000, # number of records per paginated API response
            ).get_client()
            logger.info(f"Successfully connected to EarthRanger at {er_config.server}")
        except Exception as e:
            logger.error(f"Failed to connect to EarthRanger at {er_config.server}: {e}")
            completed += len(conn_config.surveys)
            continue

        # Run the analysis for each survey under this connection. Errors in one survey
        # are caught and logged so the remaining surveys still run.
        for survey_config in conn_config.surveys:
            try:
                run_analysis(er_connection=er_io, survey_config=survey_config, er_config=er_config)
            except Exception as e:
                logger.error(f"Analysis failed for survey '{survey_config.name}': {e}", exc_info=True)
            finally:
                completed += 1
                logger.info(f"Progress: {completed}/{total} surveys complete")


if __name__ == "__main__":
    main()
