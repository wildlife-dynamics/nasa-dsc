import os
import sys
import ast
import getpass
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import ecoscope
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerConnection
import ecoscope.plotting as plotting 

# Initialize ecoscope
ecoscope.init()

# Load environment variables
load_dotenv()

# Output DIR
output_dir = 'Outputs/'

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)



