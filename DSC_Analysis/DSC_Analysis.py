
# Standard library
import os                          # file path construction and directory creation
import logging                     # structured logging throughout the pipeline
from math import sin, radians      # trigonometry for computing animal position from radial angle
from datetime import datetime      # type used in pydantic models for survey date fields
from typing import Dict, Optional, List  # type hints for function signatures and models

# Third-party: environment and config
from dotenv import load_dotenv     # loads EarthRanger and EarthEngine credentials from .env file

# Third-party: data manipulation
import pandas as pd                # tabular data handling for patrol events and merges
import geopandas as gpd            # spatial data handling for transects and event geometries

# Third-party: Google Earth Engine
import ee                          # GEE Python API — used for NDVI composite and slope extraction

# Third-party: ecoscope
import ecoscope                                                          # wildlife movement analytics library, initialised before use
from ecoscope.io.eetools import label_gdf_with_img                       # samples a GEE image over a GeoDataFrame using reduceRegions (single GEE call)
from ecoscope.io.earthranger_utils import normalize_column               # unpacks nested dict columns (e.g. event_details) into flat columns

# Third-party: ecoscope workflow connections
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection  # manages auth and HTTP session to an EarthRanger server
from ecoscope_workflows_ext_ecoscope.connections import EarthEngineConnection  # initialises the GEE session using a service account

# Third-party: pydantic
from pydantic import BaseModel, Field, TypeAdapter  # config validation: BaseModel defines schema, Field maps JSON aliases, TypeAdapter parses lists

# --- Initialise ---

ecoscope.init()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()


# --- Pydantic Models for Configuration ---

class SurveyConfig(BaseModel):
    name: str = Field(alias='surveyName')
    since: datetime
    until: datetime
    group_id: str = Field(alias='erSpatialTransectsGroupId')


class EarthRangerConfig(BaseModel):
    server: str
    patrol_type: str = Field(alias='patrolType')
    credentials_key: str = Field(alias='credentialsKey')
    export_tz: str = Field('UTC', alias='exportTimezone')
    event_column_transform: Dict[str, str] = Field(default_factory=dict, alias='eventColumnTransform')


class ConnectionConfig(BaseModel):
    earthranger: EarthRangerConfig
    surveys: List[SurveyConfig]


# --- Helpers ---

def load_configuration(config_path: str = 'config.json') -> List[ConnectionConfig]:
    with open(config_path, 'r') as f:
        import json
        data = json.load(f)
    adapter = TypeAdapter(List[ConnectionConfig])
    return adapter.validate_python(data)


def transform_df_columns(df: pd.DataFrame, column_map_dict: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Subset and rename DataFrame columns based on a mapping dict. Returns df unchanged if no map provided."""
    if not column_map_dict:
        return df.copy()

    missing_cols = set(column_map_dict.keys()) - set(df.columns)
    if missing_cols:
        logger.warning(f"Columns in mapping not found in DataFrame: {missing_cols}")
        column_map_dict = {k: v for k, v in column_map_dict.items() if k not in missing_cols}

    if not column_map_dict:
        logger.warning("No valid columns to transform")
        return df.copy()

    existing_cols = list(set(column_map_dict.keys()) & set(df.columns))
    return df[existing_cols].rename(columns=column_map_dict)


def chunk_df(df: pd.DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    """Split a DataFrame into a list of chunks of at most chunk_size rows."""
    return [df.iloc[i: i + chunk_size].copy() for i in range(0, len(df), chunk_size)]


def calculate_distance(rows, transects: gpd.GeoDataFrame, dist: str = "dist") -> pd.DataFrame:
    """Calculate the distance from each event in a group to its named transect geometry."""
    patrol_transect_geo = transects[transects["name"] == rows.name]
    if patrol_transect_geo.empty:
        logger.warning(f"No matching spatial transect: {rows.name}")
        rows[dist] = -1
        return rows
    if len(patrol_transect_geo) > 1:
        logger.warning(f"More than one matching transect: {rows.name}")
    rows[dist] = rows["geometry"].distance(patrol_transect_geo.iloc[0]["geometry"], align=False)
    return rows


def point_pos(x0: float, y0: float, d: float, theta: float):
    """Compute the (x, y) position of an animal given observer position, distance, and radial angle."""
    theta_rad = radians(theta)
    return x0 + d * sin(theta_rad), y0 + d * sin(theta_rad)


def do_events_intersect_transect(events, transects: gpd.GeoDataFrame) -> pd.DataFrame:
    """Flag whether each event's geometry falls within the buffered transect polygon."""
    try:
        patrol_transect = transects[transects["name"] == events.name]
        events["intersects_transect"] = events["geometry"].intersects(patrol_transect.iloc[0]["geometry"])
    except Exception:
        events["intersects_transect"] = False
    return events


def build_hls_ndvi_image(aoi: ee.FeatureCollection, since: datetime, ndvi_window_days: Optional[int]) -> ee.Image:
    """
    Build a single mean NDVI composite image from the HLS L30 (Landsat) and S30 (Sentinel-2) collections.

    Both collections are cloud-filtered (<30% cloud cover), spatially filtered to the AOI,
    cloud-masked at pixel level using the Fmask band, and optionally restricted to a date
    window around the survey date. The merged collection is reduced to a single mean composite
    to fill cloud gaps and avoid duplicate-timestamp issues from same-day L30+S30 acquisitions.

    ndvi_window_days: days either side of survey date to filter. None = full archive.
    """
    SharedBands = ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'Fmask']
    L30Bands = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'Fmask']
    S30Bands = ['B2', 'B3', 'B4', 'B8A', 'B11', 'B12', 'Fmask']

    def maskHLS(ee_image):
        qaMask = ee_image.select('Fmask').bitwiseAnd(int('1111', 2)).eq(0)
        opticalBands = ee_image.select(['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2']).multiply(0.0001)
        return ee_image.updateMask(qaMask).addBands(opticalBands, None, True).copyProperties(
            ee_image, ['system:time_start', 'system:index', 'SPACECRAFT_ID', 'CLOUD_COVER'])

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


# --- Main Analysis ---

def run_analysis(
    er_connection: EarthRangerConnection,
    survey_config: SurveyConfig,
    er_config: EarthRangerConfig,
    ndvi_window_days: Optional[int] = 30,
):
    """
    Runs the DSC analysis for a single survey.

    ndvi_window_days: days either side of survey date to filter the HLS collection.
    Set to None to search the full archive (slower, may hit GEE limits).
    """
    logger.info(f"--- Starting Analysis for Survey: {survey_config.name} ---")

    survey_name = survey_config.name
    since_filter = survey_config.since
    until_filter = survey_config.until
    transects_group_id = survey_config.group_id
    export_tz = er_config.export_tz
    event_column_transform = er_config.event_column_transform

    # Download spatial transects
    transects = er_connection.get_spatial_features_group(spatial_features_group_id=transects_group_id).set_crs(epsg=4326)
    original_transects = transects.copy()

    # Download patrol events
    patrol_events = er_connection.get_patrol_events(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(),
        patrol_type=er_config.patrol_type,
    ).set_index('id')

    if patrol_events.empty:
        logger.warning(f"No patrol events found for survey '{survey_name}'. Skipping.")
        return

    # Re-fetch full event details in chunks
    patrol_events2 = pd.concat([
        er_connection.get_events(
            event_ids=chunk.index.astype(str).values.flatten().tolist(),
            include_details=True
        )
        for chunk in chunk_df(patrol_events, chunk_size=25)
    ])
    patrol_events = pd.merge(
        left=patrol_events,
        right=patrol_events2[['event_details']],
        how='left',
        left_index=True,
        right_index=True,
    )

    patrol_events['time'] = patrol_events['time'].dt.tz_convert(export_tz)
    normalize_column(patrol_events, "event_details")
    patrol_events = transform_df_columns(df=patrol_events, column_map_dict=event_column_transform)

    # Forward/back-fill transect_id and num_observers within each patrol
    for col in ['transect_id', 'num_observers']:
        patrol_events[[col]] = (
            patrol_events.groupby('patrol_serial_number', as_index=False, group_keys=False)[[col]]
            .apply(lambda x: x.bfill().ffill())
        )

    patrol_events = patrol_events[patrol_events['event_type'] == 'distancecountwildlife_rep']
    patrol_events['survey_id'] = survey_name

    # Project to UTM for distance calculations
    utm_crs = transects.estimate_utm_crs()
    transects = transects.to_crs(utm_crs)
    patrol_events = patrol_events.to_crs(utm_crs)

    # Calculate distance from each event to its transect centreline
    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(calculate_distance, transects=transects, dist="off_transect_dist", include_groups=False)
        .reset_index()
    )
    patrol_events = patrol_events[patrol_events["off_transect_dist"] != -1]

    # Project animal positions using radial angle and distance to centre
    patrol_events["orig_geometry"] = patrol_events["geometry"]
    patrol_events["geometry"] = gpd.points_from_xy(
        *zip(*patrol_events.apply(
            lambda x: point_pos(x["geometry"].x, x["geometry"].y, x["dist_to_centre"], x["radialangle"]),
            axis=1
        )),
        crs=utm_crs
    )

    # Calculate orthogonal distance from projected position to transect
    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(calculate_distance, transects=transects, dist="ortho_dist", include_groups=False)
        .reset_index()
    )
    patrol_events = patrol_events[patrol_events["ortho_dist"] != -1]

    # Buffer transects and flag whether each event intersects
    transects["geometry"] = transects["geometry"].simplify(50)
    transects["geometry"] = transects["geometry"].buffer(500, resolution=5, cap_style='flat', single_sided=False)

    patrol_events = (
        patrol_events.groupby(["transect_id"])
        .apply(do_events_intersect_transect, transects=transects, include_groups=True)
        .set_index("id")
    )

    # Reproject transects to WGS84 and label with EE-derived NDVI and slope
    transects = transects.to_crs(epsg=4326)
    transects['survey_date'] = since_filter

    aoi = ee.FeatureCollection(transects.__geo_interface__)
    ndvi_image = build_hls_ndvi_image(aoi=aoi, since=since_filter, ndvi_window_days=ndvi_window_days)
    slope_image = ee.Terrain.slope(ee.Image("USGS/SRTMGL1_003").select("elevation"))

    # Assign by .values to avoid index-alignment row explosion from label_gdf_with_img's internal explode
    transects['NDVI_HSL'] = label_gdf_with_img(gdf=transects, img=ndvi_image, region_reducer=ee.Reducer.mean(), scale=30.0)['mean'].values
    transects['img_date_hsl_ndvi'] = since_filter
    transects['slope'] = label_gdf_with_img(gdf=transects, img=slope_image, region_reducer=ee.Reducer.mean(), scale=30.0)['mean'].values

    transects = transects[["name", "img_date_hsl_ndvi", "NDVI_HSL", "slope", "geometry"]]

    patrol_events = patrol_events.merge(
        transects.drop(columns=["geometry"]),
        left_on="transect_id",
        right_on="name",
        how="left"
    ).drop(columns=["name"])

    # Write outputs
    output_dir = os.path.join('.', 'Outputs', 'Analysis')
    os.makedirs(output_dir, exist_ok=True)

    patrol_events.to_csv(os.path.join(output_dir, f'DSC_Analysis_{survey_name}_analysis_data.csv'), index=False)
    patrol_events[["serial_number", "transect_id", "dist_to_centre", "ortho_dist", "intersects_transect", "geometry"]].to_file(
        os.path.join(output_dir, f'DSC_Analysis_{survey_name}_events.gpkg'), index=False)
    transects.to_file(os.path.join(output_dir, f'DSC_Analysis_{survey_name}_transects.gpkg'), index=False)
    original_transects.to_file(os.path.join(output_dir, f'DSC_Analysis_{survey_name}_orig_transects.gpkg'), index=False)

    logger.info(f"--- Finished Analysis for Survey: {survey_config.name} ---")


def main():
    # Initialise Earth Engine using service account credentials from .env
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

    for conn_config in configs:
        er_config = conn_config.earthranger
        logger.info(f"Processing server: {er_config.server}")

        er_username = os.getenv(f"{er_config.credentials_key}_USERNAME")
        er_password = os.getenv(f"{er_config.credentials_key}_PASSWORD")

        if not er_username or not er_password:
            logger.error(
                f"Missing credentials for key '{er_config.credentials_key}'. "
                f"Please set {er_config.credentials_key}_USERNAME and {er_config.credentials_key}_PASSWORD in your .env file."
            )
            continue

        try:
            er_io = EarthRangerConnection(
                server=er_config.server,
                username=er_username,
                password=er_password,
                tcp_limit=5,
                sub_page_size=4000,
            ).get_client()
            logger.info(f"Successfully connected to EarthRanger at {er_config.server}")
        except Exception as e:
            logger.error(f"Failed to connect to EarthRanger at {er_config.server}: {e}")
            continue

        for survey_config in conn_config.surveys:
            try:
                run_analysis(er_connection=er_io, survey_config=survey_config, er_config=er_config)
            except Exception as e:
                logger.error(f"An error occurred during analysis for survey '{survey_config.name}': {e}", exc_info=True)


if __name__ == "__main__":
    main()
