import os
import json
import logging
from typing import Tuple, Union, Dict, Optional
from dotenv import load_dotenv
from math import sin, cos, radians, pi
import pandas as pd
import geopandas as gpd
import ecoscope
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection
from ecoscope_workflows_ext_ecoscope.connections import EarthEngineConnection

# Initialize ecoscope
ecoscope.init()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# load environment variables
load_dotenv()


def transform_df_columns(df: pd.DataFrame = None, column_map_dict: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    A function to first subset a dataframe's columns based on the keys in the supplied dictionary, then
    secondly to re-name the columns to the values provided in the dictionary. If the user doesn't supply
    a column_map_dict then just pass back the original dataframe.

    Args:
        df (pd.DataFrame): Input DataFrame
        column_map_dict (Dict[str, str], optional): Dictionary mapping old column names to new ones

    Returns:
        pd.DataFrame: DataFrame with transformed columns
    """

    # Start with a copy of the original DataFrame
    df_transformed = df.copy()

    # Only proceed with transformation if column_map_dict is provided and valid
    if column_map_dict:
        try:
            # Validate column_map_dict keys exist in DataFrame
            missing_cols = set(column_map_dict.keys()) - set(df.columns)
            if missing_cols:
                logging.warning(f"The following columns from mapping are not in DataFrame: {missing_cols}")
                # Remove missing columns from mapping
                column_map_dict = {k: v for k, v in column_map_dict.items() if k not in missing_cols}

            if column_map_dict:  # If there are still valid columns to transform
                # Select only the columns that exist in the mapping
                existing_cols = list(set(column_map_dict.keys()) & set(df.columns))
                df_transformed = df_transformed[existing_cols]

                # Rename columns using the mapping
                df_transformed = df_transformed.rename(columns=column_map_dict)
            else:
                logging.warning("No valid columns to transform")

        except Exception as e:
            logging.error(f"Error during column transformation: {str(e)}")
            df_transformed = df.copy()  # Reset to original if there's an error

    return df_transformed


def main():

    # Setup connection to EarthRanger
    er_server = os.getenv('ER_SERVER')
    er_username = os.getenv('ER_USERNAME')
    er_password = os.getenv('ER_PASSWORD')
    er_patrol_type = os.getenv('ER_PATROL_TYPE')
    survey_name = os.getenv('SURVEY_NAME')
    since_filter = pd.to_datetime(os.getenv('SINCE'))
    until_filter = pd.to_datetime(os.getenv('UNTIL'))
    export_tz = os.getenv('EXPORT_TIME_ZONE')
    transects_group_id = os.getenv('ER_SPATIAL_TRANSECTS_GROUPID')
    event_column_transform = json.loads(os.getenv("EVENT_COLUMN_TRANSFORM"))
  

    # Check missing variables
    if not er_server or not er_username or not er_password:
            raise ValueError("Missing EarthRanger credentials. Please check your .env file.")

    logger.info("Environment variables loaded successfully.")
    logger.info(f"Connecting to EarthRanger at {er_server}...")
    
    er_io = EarthRangerConnection(
        server = er_server,
        username = er_username,
        password = er_password,
        tcp_limit = 5,
        sub_page_size = 4000,
    ).get_client()

    logger.info(("Successfully connected to EarthRanger.")

    # Initialize EarthEngine
    EarthEngineConnection(
        service_account=os.getenv("EE_SERVICE_ACCOUNT") or "",
        private_key_file=os.getenv("EE_PRIVATE_KEY_FILE") or "",
        ee_project=os.getenv("EE_PROJECT") or "",
    ).get_client()
    logger.info("Successfully connected to EarthEngine")

    # Download patrol events within a given time frame
    patrols_df = er_io.get_patrols(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(),
        patrol_type=er_patrol_type,
    )

    # Download events linked with the patrol type
    # TODO: Request that event_details, event_category be passed back from get_patrol_events()
    # TODO: the event ID should be the index here
    # 'id', 'serial_number', 'event_type', 'priority', 'title', 'state',
    # 'contains', 'updated_at', 'created_at', 'geojson', 'is_collection',
    # 'patrol_id', 'patrol_serial_number', 'patrol_segment_id',
    # 'patrol_start_time', 'patrol_type', 'geometry', 'time'
    patrol_events = er_io.get_patrol_events(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(), 
        patrol_type=er_patrol_type,
    ).set_index('id')
    

    # Because the er_io.get_patrol_events() function does not return the event details
    # we need to re-query the API using each event ID. But these can overwhelm the http query limit
    # so we chunk the request
    #   'id', 'location', 'time', 'end_time', 'message', 'provenance',
    #   'event_type', 'event_category', 'priority', 'priority_label',
    #   'attributes', 'comment', 'title', 'reported_by', 'state',
    #   'is_contained_in', 'sort_at', 'patrol_segments', 'geometry',
    #   'updated_at', 'created_at', 'icon_id', 'serial_number', 'url',
    #   'image_url', 'geojson', 'is_collection', 'event_details',
    #   'related_subjects', 'patrols'
    # We need to join these two tables based on the id column and keep ['event_details'] columns
    df_chunk_size = 25
    def chunk_df(df, chunk_size):
            chunks = [df.iloc[i : i + chunk_size].copy() for i in range(0, len(df), chunk_size)]
            return chunks
    patrol_events2 = pd.concat([er_io.get_events(event_ids=chunk.index.astype(str).values.flatten().tolist())
                                        for chunk in chunk_df(patrol_events, df_chunk_size)])
    patrol_events = pd.merge(left=patrol_events,
                                right=patrol_events2[['event_details']], 
                                how='left', 
                                left_index=True, 
                                right_index=True,
                            )
    
    # convert the event times to local time
    patrol_events['time'] = patrol_events['time'].dt.tz_convert(export_tz)

    # unpack the event_details into their own columns
    ecoscope.io.earthranger_utils.normalize_column(patrol_events, "event_details")

    # transform the patrol event columns
    patrol_events = transform_df_columns(df=patrol_events, column_map_dict=event_column_transform)

    # ensure each row has the correct transect_id and num_of_observers
    # TODO: figure out why .loc[[]] notation is not working for assigning slices
    patrol_events[['transect_id']] = patrol_events.groupby('patrol_serial_number', as_index=False, group_keys=False)[['transect_id']].apply(lambda x: x.bfill().ffill()) # .reset_index(level=0, drop=True)
    patrol_events[['num_observers']] = patrol_events.groupby('patrol_serial_number', as_index=False, group_keys=False)[['num_observers']].apply(lambda x: x.bfill().ffill()) #.reset_index(level=0, drop=True)
    
    # subset the DF to just the wildlife sightings and drop the metadata
    patrol_events = patrol_events[patrol_events['event_type']=='distancecountwildlife_rep']

    # set the name of the survey
    patrol_events['survey_id'] = survey_name

    # Download the Spatial Transects
    # TODO: the crs should be set in er_io.get_spatial_features_group
    sf_group_df = er_io.get_spatial_features_group(transects_group_id).set_crs(4326) 

    # Project the transects to UTM coordinates
    utm_crs = sf_group_df.estimate_utm_crs()
    sf_group_df = sf_group_df.to_crs(utm_crs)

    # Project the events to the same UTM coordinates
    patrol_events = patrol_events.to_crs(utm_crs)

    # Make a copy of the original coords
    patrol_events["orig_geometry"] = patrol_events["geometry"]

    # Project the point to the observed location   
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
    
    # calculate the orthogonal distance
    def calculate_distance(rows):
        # find the geometry of the matching transect
        patrol_transect_geo = sf_group_df[sf_group_df["name"]==rows.name]
        if patrol_transect_geo.empty:
             raise Exception("No matching spatial transect: " + rows.name)
        if len(patrol_transect_geo) > 1:
             raise Exception("More than one matching transect: " + rows.name)
        
        rows["corr_dist"] = rows["geometry"].distance(patrol_transect_geo.iloc[0]["geometry"], align=False)
        return rows
    patrol_events = patrol_events.groupby(["transect_id"]).apply(calculate_distance, include_groups=False).reset_index()

    # export to csv
    patrol_events.to_csv(os.path.join('.', 'Outputs', 'Analysis', 'DSC_Analysis_' + survey_name + '.csv'), index=False)
    
    # export to gpkg
    patrol_events[["serial_number", "transect_id", "dist_to_centre", "corr_dist",  "geometry"]].to_file(os.path.join('.', 'Outputs', 'Analysis', 'DSC_Analysis_' + survey_name + '_events_calc.gpkg'), index=False)
    sf_group_df.to_file(os.path.join('.', 'Outputs', 'Analysis', 'DSC_Analysis_' + survey_name + '_transects.gpkg'), index=False)


if __name__ == "__main__":
    main()