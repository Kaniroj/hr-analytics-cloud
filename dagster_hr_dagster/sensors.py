from dagster import sensor, RunRequest
from dagster_hr_dagster.hr_analytics_job import hr_pipeline
import os

@sensor(job=hr_pipeline)
def new_file_sensor(context):
    watch_dir = "data/incoming"
    for file in os.listdir(watch_dir):
        if file.endswith(".csv"):
            context.log.info(f"Detected new file: {file}")
            return RunRequest(run_key=file)
