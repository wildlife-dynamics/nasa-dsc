import os
import json
import logging
from typing import Tuple, Union, Dict, Optional
from dotenv import load_dotenv
import pandas as pd
import ecoscope
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection

# Initialize ecoscope
ecoscope.init()

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
    event_column_transform = json.loads(os.getenv("EVENT_COLUMN_TRANSFORM"))

    # Check missing variables
    if not er_server or not er_username or not er_password:
            raise ValueError("Missing EarthRanger credentials. Please check your .env file.")

    print("Environment variables loaded successfully.")
    print(f"Connecting to EarthRanger at {er_server}...")
    
    er_io = EarthRangerConnection(
        server = er_server,
        username = er_username,
        password = er_password,
        tcp_limit = 5,
        sub_page_size = 4000,
    ).get_client()

    print("Successfully connected to EarthRanger.")

    # Download patrol events within a given time frame
    patrols_df = er_io.get_patrols(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(),
        patrol_type=er_patrol_type,
    )
    print(patrols_df.head())
    print(patrols_df.columns)


    # download events linked with the patrol type
    patrol_events = er_io.get_patrol_events(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(), 
        patrol_type=er_patrol_type,
    )

    def chunk_df(df, chunk_size):
            chunks = [df.iloc[i : i + chunk_size].copy() for i in range(0, len(df), chunk_size)]
            return chunks

    df_chunk_size = 50
    patrol_events = pd.concat([er_io.get_events(event_ids=chunk['id'].astype(str).values.flatten().tolist())
                                        for chunk in chunk_df(patrol_events, df_chunk_size)]).reset_index()
    
     # convert the event times to local time
    patrol_events['time'] = patrol_events['time'].dt.tz_convert(export_tz)
    
    # pull out the patrol ID
    patrol_events['patrol_id'] = patrol_events['patrols'].apply(lambda x: x[0])

    # create patrol_serial_number column
    patrol_events['patrol_serial_number'] = patrol_events['patrol_id'].map(dict(zip(patrols_df['id'].to_list(), patrols_df['serial_number'].to_list())))
    patrol_events['patrol_serial_number'] = patrol_events['patrol_serial_number'].astype(int)

    # unpack the event_details into their own columns
    ecoscope.io.earthranger_utils.normalize_column(patrol_events, "event_details")

    # transform the patrol event columns
    patrol_events = transform_df_columns(df=patrol_events, column_map_dict=event_column_transform)

    # ensure each row has the correct transect_id and num_of_observers
    patrol_events['transect_id'] = patrol_events.groupby('patrol_serial_number')['transect_id'].apply(lambda x: x.bfill().ffill()).reset_index(drop=True)
    patrol_events['num_observers'] = patrol_events.groupby('patrol_serial_number')['num_observers'].apply(lambda x: x.bfill().ffill()).reset_index(drop=True)

    # subset the DF to just the wildlife sightings and drop the metadata
    patrol_events = patrol_events[patrol_events['event_type']=='distancecountwildlife_rep']

    # set the name of the survey
    patrol_events['survey_id'] = survey_name

    # export to csv
    patrol_events.to_csv(os.path.join('.', 'Outputs', 'Analysis', 'dsc_analysis' + survey_name + '.csv'))



if __name__ == "__main__":
    main()