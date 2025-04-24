import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.parent
SCRIPT_PATH = os.path.join(SCRIPT_DIR, "Transect Mapping", "polylines_from_patrols.py")


def test_polylines_from_patrols(tmp_path):
    env = os.environ.copy()
    env["ER_PATROL_TYPE"] = "0ef3bf48-b44c-4a4e-a145-7ab2e38c9a57"
    env["SINCE"] = "2023-10-05"
    env["UNTIL"] = "2024-09-25"
    env["ER_PATROL_SERIALS_FILTER"] = "[]"
    env["ER_SUBJECT_FILTER"] = "[]"
    env["OUTPUT_MODE"] = "both"
    env["OUTPUT_DIR"] = str(tmp_path)

    process = subprocess.run([sys.executable, str(SCRIPT_PATH)], capture_output=True, text=True, env=env)

    assert process.returncode == 0
    assert os.path.exists(os.path.join(tmp_path, "23420_Polyline.gpkg"))
    assert os.path.exists(os.path.join(tmp_path, "24359_Polyline.gpkg"))
    assert os.path.exists(os.path.join(tmp_path, "Patrol_Polylines.gpkg"))
