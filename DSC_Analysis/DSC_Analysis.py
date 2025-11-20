
import os
import json
import logging
from typing import Tuple, Union, Dict, Optional, List
from dotenv import load_dotenv
from math import sin, cos, radians, pi
import pandas as pd
import geopandas as gpd
import ee
import ecoscope
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection
from ecoscope_workflows_ext_ecoscope.connections import EarthEngineConnection
from ecoscope.io.eetools import label_gdf_with_temporal_image_collection_by_feature, label_gdf_with_img
from datetime import datetime
from pydantic import BaseModel, Field, TypeAdapter

# Initialize ecoscope
ecoscope.init()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# load environment variables
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

def load_configuration(config_path: str = 'config.json') -> List[ConnectionConfig]:
    """
    Loads, validates, and returns the list of connection configurations.
    """
    with open(config_path, 'r') as f:
        data = json.load(f)
    
    adapter = TypeAdapter(List[ConnectionConfig])
    return adapter.validate_python(data)


def transform_df_columns(df: pd.DataFrame = None, column_map_dict: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    A function to first subset a dataframe's columns based on the keys in the supplied dictionary, then
    secondly to re-name the columns to the values provided in the dictionary. If the user doesn't supply
    a column_map_dict then just pass back the original dataframe.
    """
    df_transformed = df.copy()
    if column_map_dict:
        try:
            missing_cols = set(column_map_dict.keys()) - set(df.columns)
            if missing_cols:
                logging.warning(f"The following columns from mapping are not in DataFrame: {missing_cols}")
                column_map_dict = {k: v for k, v in column_map_dict.items() if k not in missing_cols}

            if column_map_dict:
                existing_cols = list(set(column_map_dict.keys()) & set(df.columns))
                df_transformed = df_transformed[existing_cols]
                df_transformed = df_transformed.rename(columns=column_map_dict)
            else:
                logging.warning("No valid columns to transform")
        except Exception as e:
            logging.error(f"Error during column transformation: {str(e)}")
            df_transformed = df.copy()
    return df_transformed

def run_analysis(er_connection: EarthRangerConnection, survey_config: SurveyConfig, er_config: EarthRangerConfig):
    """
    Runs the DSC analysis for a single survey.
    """
    logger.info(f"--- Starting Analysis for Survey: {survey_config.name} ---")

    # Config variables
    survey_name = survey_config.name
    since_filter = survey_config.since
    until_filter = survey_config.until
    transects_group_id = survey_config.group_id
    er_patrol_type = er_config.patrol_type
    export_tz = er_config.export_tz
    event_column_transform = er_config.event_column_transform

    # Download the Spatial Transects
    transects = er_connection.get_spatial_features_group(transects_group_id).set_crs(epsg=4326)
    original_transects = transects.copy()

    # Download events linked with the patrol type
    patrol_events = er_connection.get_patrol_events(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(), 
        patrol_type=er_patrol_type,
    ).set_index('id')

    if patrol_events.empty:
        logger.warning(f"No patrol events found for survey '{survey_name}'. Skipping.")
        return

    # Re-query for event details
    df_chunk_size = 25
    def chunk_df(df, chunk_size):
        return [df.iloc[i : i + chunk_size].copy() for i in range(0, len(df), chunk_size)]
    
    patrol_events2 = pd.concat([er_connection.get_events(event_ids=chunk.index.astype(str).values.flatten().tolist(), include_details=True)
                                        for chunk in chunk_df(patrol_events, df_chunk_size)])
    patrol_events = pd.merge(left=patrol_events,
                                right=patrol_events2[['event_details']], 
                                how='left', 
                                left_index=True, 
                                right_index=True,
                            )
    
    patrol_events['time'] = patrol_events['time'].dt.tz_convert(export_tz)
    ecoscope.io.earthranger_utils.normalize_column(patrol_events, "event_details")
    patrol_events = transform_df_columns(df=patrol_events, column_map_dict=event_column_transform)

    patrol_events[['transect_id']] = patrol_events.groupby('patrol_serial_number', as_index=False, group_keys=False)[['transect_id']].apply(lambda x: x.bfill().ffill())
    patrol_events[['num_observers']] = patrol_events.groupby('patrol_serial_number', as_index=False, group_keys=False)[['num_observers']].apply(lambda x: x.bfill().ffill())
    
    patrol_events = patrol_events[patrol_events['event_type']=='distancecountwildlife_rep']
    patrol_events['survey_id'] = survey_name

    utm_crs = transects.estimate_utm_crs()
    transects = transects.to_crs(utm_crs)
    patrol_events = patrol_events.to_crs(utm_crs)

    def calculate_distance(rows, dist="dist"):
        patrol_transect_geo = transects[transects["name"]==rows.name]
        if patrol_transect_geo.empty:
             logger.warning("No matching spatial transect: " + rows.name)
             rows[dist] = -1
             return rows
        if len(patrol_transect_geo) > 1:
             logger.warning("More than one matching transect: " + rows.name)
        
        rows[dist] = rows["geometry"].distance(patrol_transect_geo.iloc[0]["geometry"], align=False)
        return rows
    
    patrol_events = patrol_events.groupby(["transect_id"]).apply(calculate_distance, include_groups=False, dist="off_transect_dist").reset_index()
    patrol_events = patrol_events[patrol_events["off_transect_dist"] != -1] # Filter out events with no matching transect

    patrol_events["orig_geometry"] = patrol_events["geometry"]

    def point_pos(x0, y0, d, theta):
        theta_rad = radians(theta)
        return x0 + d*sin(theta_rad), y0 + d*cos(theta_rad)

    patrol_events["geometry"] = gpd.points_from_xy(*zip(
         *patrol_events.apply(lambda x: 
            point_pos(
                x0=x["geometry"].x,
                y0=x["geometry"].y,
                d=x["dist_to_centre"],
                theta=x["radialangle"],
                ),
                axis=1)), crs=utm_crs)
    
    patrol_events = patrol_events.groupby(["transect_id"]).apply(calculate_distance, include_groups=False, dist="ortho_dist").reset_index()
    patrol_events = patrol_events[patrol_events["ortho_dist"] != -1]

    transects["geometry"] = transects["geometry"].simplify(50)
    transects["geometry"] = transects["geometry"].buffer(500, resolution=5, cap_style='flat', single_sided=False)

    def do_events_intersect_transect(events):
        try:
            patrol_transect = transects[transects["name"]==events.name]
            events["intersects_transect"] = events["geometry"].intersects(patrol_transect.iloc[0]["geometry"])
        except Exception:
            events["intersects_transect"] = False
        return events

    patrol_events = patrol_events.groupby(["transect_id"]).apply(do_events_intersect_transect, include_groups=True).set_index("id")

    transects = transects.to_crs(epsg=4326)
    transects['survey_date'] = since_filter

    def ndvi_composite():
        def ndvi(ee_image):
            return ee_image.addBands(ee_image.normalizedDifference(['NIR', 'Red']).rename('NDVI_HSL'))
        
        def maskHLS(ee_image):
            qaMask = ee_image.select('Fmask').bitwiseAnd(int('1111', 2)).eq(0) 
            opticalBands = ee_image.select(['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2']).multiply(0.0001)
            return ee_image.updateMask(qaMask).addBands(opticalBands, None, True).copyProperties(
                ee_image, ['system:time_start', 'system:index', 'SPACECRAFT_ID', 'CLOUD_COVER'])
        
        SharedBands = ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'Fmask']
        L30Bands = ['B2','B3','B4','B5','B6','B7','Fmask']
        S30Bands = ['B2','B3','B4','B8A','B11','B12','Fmask']

        HLS_L30_COLLECTION = ee.ImageCollection('NASA/HLS/HLSL30/v002').filter(ee.Filter.lt('CLOUD_COVERAGE', 30))
        HLS_S30_COLLECTION = ee.ImageCollection('NASA/HLS/HLSS30/v002').filter(ee.Filter.lt('CLOUD_COVERAGE', 30))

        hlsMerged = HLS_L30_COLLECTION.select(L30Bands, SharedBands).merge(HLS_S30_COLLECTION.select(S30Bands, SharedBands))
        return hlsMerged.map(ndvi).select('NDVI_HSL')

    ndvi_composite_params = {
        "time_col_name": "survey_date",
        "n_before": 0,
        "n_after": 0,
        "n": "images",
        "img_coll": ndvi_composite().filterBounds(ee.FeatureCollection(transects.__geo_interface__)),
        "region_reducer": ee.Reducer.mean(),
        "scale": 30.0,
    }
    transects = transects.merge(label_gdf_with_temporal_image_collection_by_feature(gdf=transects, **ndvi_composite_params),
                                     left_index=True, right_index=True)
    
    transects['img_date_hsl_ndvi'] = pd.to_datetime(transects['img_date']).dt.tz_localize('UTC')
    transects.drop(columns=['img_date'], inplace=True)
    
    transects = transects.merge(label_gdf_with_img(gdf=transects,
                                     img = ee.Terrain.slope(ee.Image("USGS/SRTMGL1_003").select("elevation")),
                                     region_reducer= ee.Reducer.mean(),
                                     scale= 30.0,
                                     ),
                                     left_index=True, right_index=True)
    
    transects = transects.rename(columns={"mean":"slope"}) 
    transects = transects[["name", "img_date_hsl_ndvi", "NDVI_HSL", "slope", "geometry"]]

    patrol_events = patrol_events.merge(transects.drop(columns=["geometry"]), 
                                        left_on="transect_id", right_on="name",
                                        how="left").drop(columns=["name"])

    # Create output directories if they don't exist
    output_dir = os.path.join('.', 'Outputs', 'Analysis')
    os.makedirs(output_dir, exist_ok=True)

    patrol_events.to_csv(os.path.join(output_dir, f'DSC_Analysis_{survey_name}_analysis_data.csv'), index=False)
    
    patrol_events[["serial_number", "transect_id", "dist_to_centre", "ortho_dist", "intersects_transect", "geometry"]].to_file(
         os.path.join(output_dir, f'DSC_Analysis_{survey_name}_events.gpkg'), index=False)
    
    transects.to_file(os.path.join(output_dir, f'DSC_Analysis_{survey_name}_transects.gpkg'), index=False)
    original_transects.to_file(os.path.join(output_dir, f'DSC_Analysis_{survey_name}_orig_transects.gpkg'), index=False)

    logger.info(f"--- Finished Analysis for Survey: {survey_config.name} ---")


def main():
    # Initialize EarthEngine
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

    # Load configurations from JSON file
    try:
        configs = load_configuration(os.path.join(os.path.dirname(__file__), 'config.json'))
    except Exception as e:
        logger.error(f"Failed to load or parse config.json: {e}")
        return

    for conn_config in configs:
        er_config = conn_config.earthranger
        logger.info(f"Processing server: {er_config.server}")

        # Get credentials from .env file
        user_env_var = f"{er_config.credentials_key}_USERNAME"
        pass_env_var = f"{er_config.credentials_key}_PASSWORD"
        er_username = os.getenv(user_env_var)
        er_password = os.getenv(pass_env_var)

        if not er_username or not er_password:
            logger.error(f"Missing credentials for key '{er_config.credentials_key}'. "
                         f"Please set {user_env_var} and {pass_env_var} in your .env file.")
            continue

        # Setup connection to EarthRanger
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

        # Process each survey for the current connection
        for survey_config in conn_config.surveys:
            try:
                run_analysis(er_connection=er_io, survey_config=survey_config, er_config=er_config)
            except Exception as e:
                logger.error(f"An error occurred during analysis for survey '{survey_config.name}': {e}", exc_info=True)

if __name__ == "__main__":
    main()
