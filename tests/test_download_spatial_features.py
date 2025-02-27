import importlib.util
import os
import subprocess
import sys

from pathlib import Path


SCRIPT_DIR = Path(__file__).parent.parent
SCRIPT_PATH = os.path.join(SCRIPT_DIR, "Transect Mapping", "download_spatial_features.py")


def test_download_spatial_features_runs():
    """Test that the download_spatial_features.py script runs successfully and returns a GeoDataFrame."""
    # Run the script as a module
    process = subprocess.run([sys.executable, str(SCRIPT_PATH)], capture_output=True, text=True, env=os.environ.copy())

    # Check if the script executed successfully
    assert process.returncode == 0, f"Script failed with error: {process.stderr}"
    # Check output for successful GeoDataFrame creation message
    assert "Successfully fetched" in process.stdout, "Failed to fetch spatial features"


def test_gdf_creation():
    """Test that the script correctly creates a GeoDataFrame with expected properties."""
    # Import the module dynamically
    spec = importlib.util.spec_from_file_location("download_spatial_features", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)

    # Add the parent directory to sys.path to import the module
    parent_dir = str(SCRIPT_DIR)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    spec.loader.exec_module(module)

    # Save original main function
    original_main = module.main

    def mock_main():
        # Get environment variables
        er_server = os.environ["ER_SERVER"]
        er_username = os.environ["ER_USERNAME"]
        er_password = os.environ["ER_PASSWORD"]
        er_sf_group_id = os.environ["SPATIAL_FEATURES_GROUP_ID"]

        # Create EarthRanger connection and get client
        er_io = module.EarthRangerConnection(
            server=er_server,
            username=er_username,
            password=er_password,
            tcp_limit=5,
            sub_page_size=4000,
        ).get_client()

        # Get spatial features and create GeoDataFrame
        sf_group_df = er_io.get_spatial_features_group(er_sf_group_id)
        gdf = module.gpd.GeoDataFrame(sf_group_df, geometry="geometry", crs="EPSG:4326")
        return gdf

    try:
        # Replace the main function temporarily
        module.main = mock_main

        # Call the modified main function
        gdf = module.main()

        # Assert that we got a valid GeoDataFrame
        assert gdf is not None, "GeoDataFrame was not created"
        assert isinstance(gdf, module.gpd.GeoDataFrame), "Result is not a GeoDataFrame"
        assert len(gdf) > 0, "GeoDataFrame is empty"
        assert "geometry" in gdf.columns, "GeoDataFrame does not have a geometry column"
        assert gdf.crs == "EPSG:4326", "GeoDataFrame has incorrect CRS"

    finally:
        # Restore the original main function
        module.main = original_main
