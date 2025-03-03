import os
from dotenv import load_dotenv
import geopandas as gpd
import ecoscope
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection

# Initialize ecoscope
ecoscope.init()

# load environment variables
load_dotenv()


def main():
    # Setup connection to EarthRanger
    er_server = os.getenv("ER_SERVER")
    er_username = os.getenv("ER_USERNAME")
    er_password = os.getenv("ER_PASSWORD")
    er_sf_group_id = os.getenv("SPATIAL_FEATURES_GROUP_ID")

    # Check missing variables
    if not er_server or not er_username or not er_password:
        raise ValueError("Missing EarthRanger credentials. Please check your .env file.")

    print("Environment variables loaded successfully.")
    print(f"Connecting to EarthRanger at {er_server}...")

    er_io = EarthRangerConnection(
        server=er_server,
        username=er_username,
        password=er_password,
        tcp_limit=5,
        sub_page_size=4000,
    ).get_client()

    print("Successfully connected to EarthRanger.")

    try:
        # get spatial features group
        sf_group_df = er_io.get_spatial_features_group(er_sf_group_id)

        # convert to geopandas dataframe
        gdf = gpd.GeoDataFrame(sf_group_df, geometry="geometry", crs="EPSG:4326")
        print(f"Successfully fetched {len(gdf)} spatial features.")
    except Exception as e:
        print(f"Error fetching spatial features: {str(e)}")
        if er_sf_group_id is None:
            print("Spatial features group ID is not set. Check your SPATIAL_FEATURES_GROUP_ID environment variable.")
        else:
            print(f"Could not fetch spatial features group with ID: {er_sf_group_id}")
        return None


if __name__ == "__main__":
    main()
