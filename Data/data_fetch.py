import kagglehub
import os

os.environ["KAGGLEHUB_CACHE"] = r'C:\Users\victo\Documents\Portfolio\Airlines_project\Data\raw'

# Download latest version
path = kagglehub.dataset_download("rohitgrewal/airlines-flights-data")

print("Path to dataset files:", path)