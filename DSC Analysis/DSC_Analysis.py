import os
import json
import logging
from typing import Tuple, Union, Dict, Optional
from dotenv import load_dotenv
from math import sin, cos, radians, pi
import pandas as pd
import geopandas as gpd
import ee
import ecoscope
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection
from ecoscope_workflows_ext_ecoscope.connections import EarthEngineConnection
from ecoscope.io.eetools import label_gdf_with_temporal_image_collection_by_feature, label_gdf_with_img

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

    logger.info("Successfully connected to EarthRanger.")

    # Initialize EarthEngine
    EarthEngineConnection(
        service_account=os.getenv("EE_SERVICE_ACCOUNT") or "",
        private_key_file=os.getenv("EE_PRIVATE_KEY_FILE") or "",
        ee_project=os.getenv("EE_PROJECT") or "",
    ).get_client()
    logger.info("Successfully connected to EarthEngine")

    # Download the Spatial Transects
    # TODO: the crs should be set in er_io.get_spatial_features_group
    transects = er_io.get_spatial_features_group(transects_group_id).set_crs(4326)
    original_transects = transects.copy()

    # Download events linked with the patrol type
    # TODO: Request that event_details, event_category be passed back from get_patrol_events()
    # TODO: the event ID should be the index
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
    patrol_events2 = pd.concat([er_io.get_events(event_ids=chunk.index.astype(str).values.flatten().tolist(), include_details=True)
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

    # Project the transects to UTM coordinates
    utm_crs = transects.estimate_utm_crs()
    transects = transects.to_crs(utm_crs)

    # Project the events to the same UTM coordinates
    patrol_events = patrol_events.to_crs(utm_crs)

     # calculate the orthogonal distance
    def calculate_distance(rows, dist="dist"):
        # find the geometry of the matching transect
        patrol_transect_geo = transects[transects["name"]==rows.name]
        if patrol_transect_geo.empty:
             raise Exception("No matching spatial transect: " + rows.name)
        if len(patrol_transect_geo) > 1:
             raise Exception("More than one matching transect: " + rows.name)
        
        rows[dist] = rows["geometry"].distance(patrol_transect_geo.iloc[0]["geometry"], align=False)
        return rows
    
    # calculate the off-transect dist
    patrol_events = patrol_events.groupby(["transect_id"]).apply(calculate_distance, include_groups=False, dist="off_transect_dist").reset_index()

    # Make a copy of the original coords for posterity
    patrol_events["orig_geometry"] = patrol_events["geometry"]

    # Project the point to the observed location using the measured distance and direction
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
    
    # Calculate the orthgonal distance of the pprojected point to the transect
    patrol_events = patrol_events.groupby(["transect_id"]).apply(calculate_distance, include_groups=False, dist="ortho_dist").reset_index()


    ###---------------- Transect Polygon Creation ----------------###

    # Simplify the transect geometries
    transects["geometry"] = transects["geometry"].simplify(50)

    # Transform the polyline transects into buffered polygons (500m aside) with flat ends
    transects["geometry"] = transects["geometry"].buffer(500, resolution=5, cap_style='flat', single_sided=False)

    # Calculate the intersection of the event with each transect polygon
    # patrol_events["intersects_transect"] = sf_group_df["geometry"].intersects(patrol_events["geometry"])
    def do_events_intersect_transect(events):
        try:
            patrol_transect = transects[transects["name"]==events.name]
            print(patrol_transect)
            print(events)
            events["intersects_transect"] = events["geometry"].intersects(patrol_transect.iloc[0]["geometry"])
        except:
            events["intersects_transect"] = False

        return events

    patrol_events = patrol_events.groupby(["transect_id"]).apply(do_events_intersect_transect, include_groups=True).set_index("id")

    ###---------------- GEE Labeling ----------------###

    # Project to WGS84 GCS
    transects = transects.to_crs(4326)

    # add a time column using the 'Since' time
    transects['survey_date'] = since_filter

    # # Annotate transects with EarthEngine Data - NDVI
    # params = {
    #     "time_col_name": "survey_date",
    #     "n_before": 0, # requesting the 1 image after a feature's time value
    #     "n_after": 0, # requesting the 1 image after a feature's time value
    #     "n": "images",
    #     "img_coll": ee.ImageCollection("MODIS/061/MCD43A4").select(["Nadir_Reflectance_Band2", "Nadir_Reflectance_Band1"]),
    #     "region_reducer": ee.Reducer.mean(),
    #     "scale": 200.0, 
    # }
    # transects = transects.merge(label_gdf_with_temporal_image_collection_by_feature(gdf=transects, **params),
    #                                  left_index=True, right_index=True)
    
    # transects['NDVI_MODIS'] = transects.apply(lambda x: (x["Nadir_Reflectance_Band2"]-x["Nadir_Reflectance_Band1"])/(x["Nadir_Reflectance_Band2"] + x["Nadir_Reflectance_Band1"]), axis=1)
    # transects['img_date_modis_ndvi'] = pd.to_datetime(transects['img_date']).dt.tz_localize('UTC')
    # transects.drop(columns=['img_date'], inplace=True)
    
    # Annotate with cloud masked NDVI product
    def ndvi_composite():

        def ndvi(ee_image):
            return ee_image.addBands(ee_image.normalizedDifference(['NIR', 'Red']).rename('NDVI_HSL'))
        
        # Define function for masking clouds and applying scaling factors for HLS products
        def maskHLS(ee_image):
            # HLS uses 'Fmask' for quality assessment
            #3 Bit 0 - Cirrus | Bit 1 - Cloud | Bit 2 - Adjacent to Cloud/Shadow | Bit 3 - Cloud Shadow
            qaMask = ee_image.select('Fmask').bitwiseAnd(int('1111', 2)).eq(0) 

            # Apply the HLS scaling factor (0.0001) to relevant bands.
            opticalBands = ee_image.select(['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2']) \
                                    .multiply(0.0001)

            # Return the masked image with scaled optical bands.
            return ee_image.updateMask(qaMask).addBands(opticalBands, None, True).copyProperties(
                ee_image, ['system:time_start', 'system:index', 'SPACECRAFT_ID', 'CLOUD_COVER'])
        
        # List the desired bands these two collections share in common
        SharedBands = ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'Fmask']
        L30Bands = ['B2','B3','B4','B5','B6','B7','Fmask']
        S30Bands = ['B2','B3','B4','B8A','B11','B12','Fmask']

        # HLS Collections (v2.0)

        # Landsat 8/9
        HLS_L30_COLLECTION = ee.ImageCollection('NASA/HLS/HLSL30/v002') \
            .filter(ee.Filter.lt('CLOUD_COVERAGE', 30))  # Don't mess with images with >30% cloud
        
        # Sentinel-2
        HLS_S30_COLLECTION = ee.ImageCollection('NASA/HLS/HLSS30/v002') \
            .filter(ee.Filter.lt('CLOUD_COVERAGE', 30))  # Don't mess with images with >30% cloud

        #Combine HLSL30 and HLSS30 collections
        hlsMerged = HLS_L30_COLLECTION.select(L30Bands, SharedBands) \
            .merge(HLS_S30_COLLECTION.select(S30Bands, SharedBands))

        # Apply the NDVI calculation and generate a mean pixel composite
        ndviComposite = hlsMerged.map(ndvi).select('NDVI_HSL') # .mean()

        return ndviComposite

    ndvi_composite_params = {
        "time_col_name": "survey_date",
        "n_before": 0, # requesting the images before a feature's time value
        "n_after": 0, # requesting the images after a feature's time value
        "n": "images",
        "img_coll": ndvi_composite().filterBounds(ee.FeatureCollection(transects.__geo_interface__)),
        "region_reducer": ee.Reducer.mean(),
        "scale": 30.0,
    }
    transects = transects.merge(label_gdf_with_temporal_image_collection_by_feature(gdf=transects, **ndvi_composite_params),
                                     left_index=True, right_index=True)
    
    transects['img_date_hsl_ndvi'] = pd.to_datetime(transects['img_date']).dt.tz_localize('UTC')
    transects.drop(columns=['img_date'], inplace=True)
    
    print(transects.columns)
    
    # Annotate transects with EarthEngine Data - elevation
    transects = transects.merge(label_gdf_with_img(gdf=transects,
                                     img = ee.Terrain.slope(ee.Image("USGS/SRTMGL1_003").select("elevation")),
                                     region_reducer= ee.Reducer.mean(),
                                     scale= 30.0,
                                     ),
                                     left_index=True, right_index=True)
    
    # rename columns that we want to keep
    transects = transects.rename(columns={"mean":"slope"}) 

    # subselect columns
    transects = transects[["name", "img_date_hsl_ndvi", "NDVI_HSL", "slope", "geometry"]]

    # Join the transect data onto each event to retrieve the EE annotations
    patrol_events = patrol_events.merge(transects.drop(columns=["geometry"]), 
                                        left_on="transect_id", right_on="name",
                                        how="left").drop(columns=["name"])


    ##---------------- EXPORT ----------------##

    # export to csv
    patrol_events.to_csv(os.path.join('.', 'Outputs', 'Analysis', 'DSC_Analysis_' + survey_name + '_analysis_data.csv'), index=False)
    
    # export to gpkg
    patrol_events[["serial_number", "transect_id", "dist_to_centre", "ortho_dist", "intersects_transect", "geometry"]].to_file(
         os.path.join('.', 'Outputs', 'Analysis', 'DSC_Analysis_' + survey_name + '_events.gpkg'), index=False)
    
    transects.to_file(os.path.join('.', 'Outputs', 'Analysis', 'DSC_Analysis_' + survey_name + '_transects.gpkg'), index=False)

    original_transects.to_file(os.path.join('.', 'Outputs', 'Analysis', 'DSC_Analysis_' + survey_name + '_orig_transects.gpkg'), index=False)


if __name__ == "__main__":
    main()