import os
import sys
import ast

import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import helper

import ecoscope
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection


load_dotenv()
ecoscope.init()


def main():
    config = {
        "er_server": os.getenv("ER_SERVER"),
        "er_username": os.getenv("ER_USERNAME"),
        "er_password": os.getenv("ER_PASSWORD"),
        "er_patrol_type": os.getenv("ER_PATROL_TYPE"),
        "since_filter": pd.to_datetime(os.getenv("SINCE")),
        "until_filter": pd.to_datetime(os.getenv("UNTIL")),
        "er_patrol_serials_filter": ast.literal_eval(os.getenv("ER_PATROL_SERIALS_FILTER")),
        "er_subject_names_filter": ast.literal_eval(os.getenv("ER_SUBJECT_FILTER")),
        "output_dir": os.getenv("OUTPUT_DIR"),
        "er_output_mode": os.getenv("OUTPUT_MODE", "both").lower(),
    }

    # Create output directory
    os.makedirs(config["output_dir"], exist_ok=True)

    # Validate OUTPUT_MODE
    if config["er_output_mode"] not in ["individual", "grouped", "both"]:
        print(f"Warning: Invalid OUTPUT_MODE '{config['er_output_mode']}'. Using default 'both'.")
        config["er_output_mode"] = "both"

    # Initialize Earth Ranger connection
    er_io = EarthRangerConnection(
        server=config["er_server"],
        username=config["er_username"],
        password=config["er_password"],
        tcp_limit=5,
        sub_page_size=5000,
    ).get_client()

    # Get patrols
    patrols_df = er_io.get_patrols(
        since=config["since_filter"].isoformat(),
        until=config["until_filter"].isoformat(),
        patrol_type=config["er_patrol_type"],
    )

    # Filter based on serial number
    if config["er_patrol_serials_filter"]:
        patrols_df = patrols_df[patrols_df["serial_number"].isin(config["er_patrol_serials_filter"])]

    if not patrols_df.empty:
        # Get patrol observations
        patrol_relocs = er_io.get_patrol_observations(
            patrols_df=patrols_df,
            include_patrol_details=True,
            include_subject_details=True,
        )

        # Filter based on subject_name
        if config["er_subject_names_filter"]:
            patrol_relocs = patrol_relocs[patrol_relocs["extra__subject__name"].isin(config["er_subject_names_filter"])]

        # Turn the relocations into trajectory segments and union the segments into single polylines
        def create_trajectory(x):
            if len(x) > 1:
                geo = ecoscope.base.Trajectory.from_relocations(x)["geometry"].unary_union
                if geo:
                    return gpd.GeoSeries({"geometry": geo}, crs=4326)

        patrol_polylines = patrol_relocs.groupby("patrol_serial_number").apply(create_trajectory, include_groups=True)

        # Export based on OUTPUT_MODE
        if config["er_output_mode"] in ["individual", "both"]:
            # Export individual geopackages for each patrol
            for patrol_serial in patrol_polylines.index:
                try:
                    patrol_gdf = patrol_polylines.loc[[patrol_serial]]
                    helper.export_gpkg(
                        df=patrol_gdf,
                        dir=config["output_dir"],
                        outname=f"{patrol_serial}_Polyline.gpkg",
                        lyrname="patrol_polyline",
                    )
                    print(f"Exported individual geopackage for patrol: {patrol_serial}")
                except Exception as e:
                    print(f"Error exporting patrol {patrol_serial}: {e}")

        if config["er_output_mode"] in ["grouped", "both"]:
            # Export the grouped geopackage
            helper.export_gpkg(
                df=patrol_polylines,
                dir=config["output_dir"],
                outname="Patrol_Polylines.gpkg",
                lyrname="patrol_polylines",
            )
            print("Exported grouped geopackage with all patrols")


if __name__ == "__main__":
    main()
