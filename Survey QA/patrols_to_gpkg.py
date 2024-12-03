import os
import sys
import ast
import getpass
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import ecoscope
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import helper as helper
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection
import ecoscope.plotting as plotting 

# Initialize ecoscope
ecoscope.init()

# Load environment variables
load_dotenv()


def main():

    # Setup a connection to EarthRanger
    er_server = os.getenv('ER_SERVER') 
    er_username = os.getenv('ER_USERNAME')
    er_password = os.getenv('ER_PASSWORD')
    er_patrol_type = os.getenv('ER_PATROL_TYPE')
    survey_number = os.getenv('SURVEY_NUMBER')
    since_filter = pd.to_datetime(os.getenv('SINCE'))
    until_filter = pd.to_datetime(os.getenv('UNTIL'))
    er_patrol_serials_filter = ast.literal_eval(os.getenv("ER_PATROL_SERIALS_FILTER"))
    er_subject_names_filter = ast.literal_eval(os.getenv("ER_SUBJECT_FILTER"))
    relocs_columns = ast.literal_eval(os.getenv("RELOCS_COLUMNS"))
    traj_columns = ast.literal_eval(os.getenv("TRAJ_COLUMNS"))
    event_columns = ast.literal_eval(os.getenv("EVENT_COLUMNS"))

    # Output DIR
    output_dir = os.path.join('Outputs', 'Patrols_to_GPKG', er_server.strip("https://"), str(survey_number))

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    er_io = EarthRangerConnection(
        server=er_server,
        username=er_username,
        password=er_password,
        tcp_limit=5,
        sub_page_size=5000,
    ).get_client()

    # get a dataframe of patrols based on the parameters
    patrols_df = er_io.get_patrols(
        since=since_filter.isoformat(),
        until=until_filter.isoformat(),
        patrol_type=er_patrol_type,
    )

    # filter based on serial number
    if er_patrol_serials_filter:
        patrols_df = patrols_df[patrols_df['serial_number'].isin(er_patrol_serials_filter)]

    if not patrols_df.empty:

        #-------------------Relocs/Traj

        # download observations related to the patrols
        patrol_relocs = er_io.get_patrol_observations(
            patrols_df=patrols_df,
            include_patrol_details=True,
            include_subject_details=True,
        )

        # filter based on subject_name
        if er_subject_names_filter:
            patrol_relocs = patrol_relocs[patrol_relocs['extra__subject__name'].isin(er_subject_names_filter)]

        # make sure serial number is int type
        patrol_relocs['patrol_serial_number'] = patrol_relocs['patrol_serial_number'].astype(int)

        # Export relocs to GPKG
        if not patrol_relocs.empty:
            patrol_relocs.groupby(['patrol_serial_number'])[relocs_columns].apply(
                lambda t: helper.export_gpkg(df=t, dir=output_dir, outname="patrol_" + str(t.name)+ ".gpkg", lyrname= 'relocs'),
                include_groups=True,
                )
        
        # convert relocs to trajectory
        patrol_relocs["groupby_col"] = patrol_relocs["patrol_serial_number"]
        patrol_traj = ecoscope.base.Trajectory.from_relocations(patrol_relocs)

         # make sure serial number is int type
         # TODO: why is the conversion to a trajectory changing the data type? 
        patrol_traj['extra__patrol_serial_number'] = patrol_traj['extra__patrol_serial_number'].astype(int)

        # subset columns
        patrol_traj = patrol_traj[traj_columns] 

        # Export each trajectory as a GPKG lyr per patrol_id
        if not patrol_traj.empty:
            patrol_traj.groupby(['extra__patrol_serial_number'])[traj_columns].apply(
                lambda t: helper.export_gpkg(df=t, dir=output_dir, outname="patrol_" + str(t.name)+ ".gpkg", lyrname='traj'),
                include_groups=True,
                )

        #-------------------Events
        
        # download events linked with the patrol type
        patrol_events = er_io.get_patrol_events(
            since=since_filter.isoformat(),
            until=until_filter.isoformat(), 
            patrol_type=er_patrol_type,
        )

        # use the event ids to pull the full event details
        # TODO: move this function to ecoscope-core library
        def chunk_df(df, chunk_size):
            chunks = [df.iloc[i : i + chunk_size].copy() for i in range(0, len(df), chunk_size)]
            return chunks
        
        df_chunk_size = 1 # until this is deployed https://allenai.atlassian.net/browse/ERA-10527, then =50
        patrol_events = pd.concat([er_io.get_events(event_ids=chunk['id'].astype(str).values.flatten().tolist())
                                        for chunk in chunk_df(patrol_events, df_chunk_size)]).reset_index()
        
        # pull out the patrol ID
        patrol_events['patrol_id'] = patrol_events['patrols'].apply(lambda x: x[0])

        # create patrol_serial_number column

        patrol_events['patrol_serial_number'] = patrol_events['patrol_id'].map(dict(zip(patrols_df['id'].to_list(), patrols_df['serial_number'].to_list())))
        patrol_events['patrol_serial_number'] = patrol_events['patrol_serial_number'].astype(int)

        # unpack the event_details into their own columns
        ecoscope.io.earthranger_utils.normalize_column(patrol_events, "event_details")

        # subset columns
        patrol_events = patrol_events[event_columns] 

        # Export each set of event as a GPKG lyr per patrol_id
        if not patrol_events.empty:
            patrol_events.groupby(['patrol_serial_number'])[event_columns].apply(
                lambda t: helper.export_gpkg(df=t, dir=output_dir, outname="patrol_" + str(t.name)+ ".gpkg", lyrname='events'),
                include_groups=True,
                )

        
if __name__ == "__main__":
    main()

    

