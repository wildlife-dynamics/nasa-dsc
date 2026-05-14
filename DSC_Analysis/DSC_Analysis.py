import os
import json
import logging
from math import sin, radians
from shapely.ops import linemerge
from datetime import datetime
from typing import Dict, Optional, List

from dotenv import load_dotenv

import pandas as pd
import geopandas as gpd
import ee

import ecoscope
from ecoscope.io.eetools import label_gdf_with_img
from ecoscope.io.earthranger_utils import normalize_column

from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection
from ecoscope_workflows_ext_ecoscope.connections import EarthEngineConnection
from ecoscope_workflows_ext_custom.tasks.io import process_events_details

from pydantic import BaseModel, Field, TypeAdapter


ecoscope.init()  # must be called before any ecoscope function

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()  # must run before any os.getenv() calls


class SurveyConfig(BaseModel):
    name: str = Field(alias='surveyName')
    since: datetime
    until: datetime
    group_id: str = Field(alias='erSpatialTransectsGroupId')  # UUID of the EarthRanger spatial feature group containing the transect lines
    simplify_tolerance: int = Field(50, alias='simplifyTolerance')  # Douglas-Peucker tolerance in metres applied before buffering


class EarthRangerConfig(BaseModel):
    server: str
    patrol_type: str = Field(alias='patrolType')
    credentials_key: str = Field(alias='credentialsKey')  # prefix for env vars: <KEY>_USERNAME / <KEY>_PASSWORD
    export_tz: str = Field('UTC', alias='exportTimezone')
    event_column_transform: Dict[str, str] = Field(default_factory=dict, alias='eventColumnTransform')


class ConnectionConfig(BaseModel):
    earthranger: EarthRangerConfig
    surveys: List[SurveyConfig]


def load_configuration(config_path: str = 'config.json') -> List[ConnectionConfig]:
    with open(config_path, 'r') as f:
        data = json.load(f)
    return TypeAdapter(List[ConnectionConfig]).validate_python(data)


def transform_df_columns(df: pd.DataFrame, column_map_dict: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    # Subsets to mapped columns and renames them to the expected output schema.
    # Columns present in the map but absent from the DataFrame are warned and skipped.
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
    # EarthRanger's get_events endpoint has a practical limit on concurrent event IDs.
    return [df.iloc[i: i + chunk_size].copy() for i in range(0, len(df), chunk_size)]


def calculate_distance(rows: pd.DataFrame, transects: gpd.GeoDataFrame, dist: str = "dist") -> pd.DataFrame:
    # Called per transect group. Returns dist=-1 if no matching transect is found
    # so the caller can filter those rows out.
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
    # Converts observer position + radial angle + distance into estimated animal position
    # in projected coordinates. theta is degrees clockwise from the transect bearing.
    theta_rad = radians(theta)
    return x0 + d * sin(theta_rad), y0 + d * sin(theta_rad)


def do_events_intersect_transect(events: pd.DataFrame, transects: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        patrol_transect = transects[transects["name"] == events.name]
        events["intersects_transect"] = events["geometry"].intersects(patrol_transect.iloc[0]["geometry"])
    except Exception:
        events["intersects_transect"] = False
    return events


def unpack_event_details(df: pd.DataFrame) -> pd.DataFrame:
    # Expands the event_details dict column into flat columns without any prefix.
    # Must be called after process_events_details(map_to_titles=True) so keys are human-readable.
    details = pd.DataFrame(
        df['event_details'].apply(lambda x: x if isinstance(x, dict) else {}).tolist(),
        index=df.index
    )
    return pd.concat([df.drop(columns=['event_details']), details], axis=1)


def build_hls_ndvi_image(aoi: ee.FeatureCollection, since: datetime, ndvi_window_days: Optional[int]) -> ee.Image:
    # Builds a cloud-free mean NDVI composite from NASA HLS (Harmonized Landsat-Sentinel).
    # HLS_L30 (Landsat 8/9, ~16-day revisit) and HLS_S30 (Sentinel-2, ~5-day revisit) are
    # merged to achieve ~2-3 day revisit, maximising cloud-free coverage at 30m resolution.
    # Each image is pixel-level cloud masked using the Fmask quality band before merging.

    SharedBands = ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'Fmask']
    L30Bands    = ['B2',   'B3',    'B4',  'B5',  'B6',    'B7',    'Fmask']
    S30Bands    = ['B2',   'B3',    'B4',  'B8A', 'B11',   'B12',   'Fmask']

    def maskHLS(ee_image):
        # Fmask lower 4 bits encode cloud/shadow/water/snow; .eq(0) keeps only clear pixels.
        # Optical bands are scaled by 0.0001 to convert DN to reflectance (0–1).
        qaMask = ee_image.select('Fmask').bitwiseAnd(int('1111', 2)).eq(0)
        opticalBands = ee_image.select(['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2']).multiply(0.0001)
        return (ee_image
                .updateMask(qaMask)
                .addBands(opticalBands, None, True)
                .copyProperties(ee_image, ['system:time_start', 'system:index', 'SPACECRAFT_ID', 'CLOUD_COVER']))

    l30 = (ee.ImageCollection('NASA/HLS/HLSL30/v002')
           .filter(ee.Filter.lt('CLOUD_COVERAGE', 30))
           .filterBounds(aoi)
           .select(L30Bands, SharedBands)
           .map(maskHLS))

    s30 = (ee.ImageCollection('NASA/HLS/HLSS30/v002')
           .filter(ee.Filter.lt('CLOUD_COVERAGE', 30))
           .filterBounds(aoi)
           .select(S30Bands, SharedBands)
           .map(maskHLS))

    if ndvi_window_days is not None:
        # Restrict to a symmetric window around the survey date so NDVI reflects
        # vegetation conditions at survey time rather than a long-term seasonal average.
        survey_date_ee = ee.Date(since.isoformat())
        date_filter = ee.Filter.date(
            survey_date_ee.advance(-ndvi_window_days, 'day'),
            survey_date_ee.advance(ndvi_window_days, 'day')
        )
        l30 = l30.filter(date_filter)
        s30 = s30.filter(date_filter)

    return (l30.merge(s30)
            .map(lambda img: img.addBands(img.normalizedDifference(['NIR', 'Red']).rename('NDVI_HSL')))
            .select('NDVI_HSL')
            .mean())


def run_analysis(
    er_connection: EarthRangerConnection,
    survey_config: SurveyConfig,
    er_config: EarthRangerConfig,
    ndvi_window_days: Optional[int] = None,
):
    logger.info(f"--- Starting Analysis for Survey: {survey_config.name} ---")

    survey_name            = survey_config.name
    since_filter           = survey_config.since
    until_filter           = survey_config.until
    transects_group_id     = survey_config.group_id
    export_tz              = er_config.export_tz
    event_column_transform = er_config.event_column_transform

    transects          = er_connection.get_spatial_features_group(spatial_features_group_id=transects_group_id).set_crs(epsg=4326)
    original_transects = transects.copy()  # preserved for the output GeoPackage before buffering

    patrol_events = er_connection.get_patrol_events(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(),
        patrol_type=er_config.patrol_type,
    ).set_index('id')

    if patrol_events.empty:
        logger.warning(f"No patrol events found for survey '{survey_name}'. Skipping.")
        return

    # get_patrol_events returns summary records only; get_events fetches the full payload
    # including nested event_details (custom field data), batched to avoid API limits.
    events = pd.concat([
        er_connection.get_events(
            event_ids=chunk.index.astype(str).values.flatten().tolist(),
            include_details=True
        )
        for chunk in chunk_df(patrol_events, chunk_size=25)
    ])

    survey_events = events[events['event_type'] == 'distancecountpatrol_rep'].copy()
    survey_events['latitude']    = survey_events['location'].apply(lambda v: v.get('latitude')  if isinstance(v, dict) else None)
    survey_events['longitude']   = survey_events['location'].apply(lambda v: v.get('longitude') if isinstance(v, dict) else None)
    survey_events['reported_by'] = survey_events['reported_by'].apply(lambda v: v.get('name', '') if isinstance(v, dict) else str(v) if pd.notna(v) else '')
    survey_events = survey_events[['serial_number', 'event_type', 'time', 'latitude', 'longitude', 'reported_by', 'event_details']]
    survey_events = process_events_details(survey_events, client=er_connection, map_to_titles=True)
    survey_events = unpack_event_details(survey_events)
    survey_events['Team Members'] = survey_events['Team Members'].apply(lambda v: ', '.join(v) if isinstance(v, list) else v)

    patrol_events = pd.merge(
        left=patrol_events,
        right=events[['event_details']],
        how='left',
        left_index=True,
        right_index=True,
    )

    patrol_events['time'] = patrol_events['time'].dt.tz_convert(export_tz)
    normalize_column(patrol_events, "event_details")
    patrol_events = transform_df_columns(df=patrol_events, column_map_dict=event_column_transform)

    # transect_id and num_observers are recorded once per patrol leg on the patrol-start event,
    # not on each wildlife sighting. Propagate them to every row within the same patrol.
    for col in ['transect_id', 'num_observers']:
        patrol_events[[col]] = (
            patrol_events.groupby('patrol_serial_number', as_index=False, group_keys=False)[[col]]
            .apply(lambda x: x.bfill().ffill())
        )

    patrol_events = patrol_events[patrol_events['event_type'] == 'distancecountwildlife_rep']
    patrol_events['survey_id'] = survey_name

    utm_crs       = transects.estimate_utm_crs()
    transects     = transects.to_crs(utm_crs)
    patrol_events = patrol_events.to_crs(utm_crs)

    # First pass: distance from observer GPS position to the transect centreline.
    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(calculate_distance, transects=transects, dist="off_transect_dist", include_groups=False)
        .reset_index()
    )
    patrol_events = patrol_events[patrol_events["off_transect_dist"] != -1]

    # Project each sighting from the observer's position to the animal's estimated position
    # using the recorded radial angle and distance-to-centre. Original GPS geometry is preserved for QA.
    patrol_events["orig_geometry"] = patrol_events["geometry"]
    patrol_events["geometry"] = gpd.points_from_xy(
        *zip(*patrol_events.apply(
            lambda x: point_pos(x["geometry"].x, x["geometry"].y, x["dist_to_centre"], x["radialangle"]),
            axis=1
        )),
        crs=utm_crs
    )

    # Second pass: orthogonal distance from projected animal position to the transect.
    # This is the key input for the distance sampling detection function.
    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(calculate_distance, transects=transects, dist="ortho_dist", include_groups=False)
        .reset_index()
    )
    patrol_events = patrol_events[patrol_events["ortho_dist"] != -1]

    # linemerge stitches MultiLineString GPS segments into a single LineString before buffering.
    # Without it, flat-cap buffers on individual segments leave triangular gaps at bends.
    transects["geometry"] = transects["geometry"].apply(
        lambda g: linemerge(g) if g.geom_type == 'MultiLineString' else g
    )
    transects["geometry"] = transects["geometry"].simplify(survey_config.simplify_tolerance)
    transects["geometry"] = transects["geometry"].buffer(500, resolution=5, cap_style='flat', single_sided=False)

    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(do_events_intersect_transect, transects=transects, include_groups=True)
        .set_index("id")
    )

    transects = transects.to_crs(epsg=4326)
    transects['survey_date'] = since_filter

    aoi         = ee.FeatureCollection(transects.__geo_interface__)
    ndvi_image  = build_hls_ndvi_image(aoi=aoi, since=since_filter, ndvi_window_days=ndvi_window_days)
    slope_image = ee.Terrain.slope(ee.Image("USGS/SRTMGL1_003").select("elevation"))

    # .values bypasses pandas index alignment — label_gdf_with_img calls .explode() internally
    # which can misalign the index and cause a row explosion when merging.
    transects['NDVI_HSL']          = label_gdf_with_img(gdf=transects, img=ndvi_image,  region_reducer=ee.Reducer.mean(), scale=30.0)['mean'].values
    transects['img_date_hsl_ndvi'] = since_filter
    transects['slope']             = label_gdf_with_img(gdf=transects, img=slope_image, region_reducer=ee.Reducer.mean(), scale=30.0)['mean'].values

    transects = transects[["name", "img_date_hsl_ndvi", "NDVI_HSL", "slope", "geometry"]]

    patrol_events = patrol_events.merge(
        transects.drop(columns=["geometry"]),
        left_on="transect_id",
        right_on="name",
        how="left"
    ).drop(columns=["name"])

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
    csv_columns = [c for c in csv_columns if c in patrol_events.columns]  # guards against optional fields
    patrol_events[csv_columns].to_csv(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_analysis_data.csv'), index=False)
    patrol_events[["serial_number", "transect_id", "dist_to_centre", "ortho_dist", "intersects_transect", "geometry"]].to_file(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_events.gpkg'), index=False)
    transects.to_file(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_transects.gpkg'), index=False)
    original_transects.to_file(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_orig_transects.gpkg'), index=False)

    logger.info(f"--- Finished Analysis for Survey: {survey_config.name} ---")


def main():
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

    all_surveys = [(conn_config, survey) for conn_config in configs for survey in conn_config.surveys]
    total = len(all_surveys)
    completed = 0
    logger.info(f"Starting pipeline: {total} surveys across {len(configs)} site(s)")

    for conn_config in configs:
        er_config = conn_config.earthranger
        logger.info(f"Processing server: {er_config.server}")

        # Credentials are namespaced per-site: <KEY>_USERNAME / <KEY>_PASSWORD in .env
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
                sub_page_size=4000,
            ).get_client()
            logger.info(f"Successfully connected to EarthRanger at {er_config.server}")
        except Exception as e:
            logger.error(f"Failed to connect to EarthRanger at {er_config.server}: {e}")
            completed += len(conn_config.surveys)
            continue

        # Errors in one survey are caught and logged so remaining surveys still run.
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
