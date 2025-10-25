from dagster import job, op
import dlt
from dlt import pipeline
from dlt.pipeline import Pipeline
from dlt.extract import source

# توابع و منابع DLT از pipeline اصلی (در پوشه dlt/)
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "dlt"))
from pipeline import jobsearch_source  # این تابع در فایل pipeline.py تو است

@op
def run_dlt_pipeline():
    """اجرای pipeline DLT و بازگرداندن نتیجه"""
    pipe = dlt.pipeline(
        pipeline_name="jobtech_to_duckdb",
        destination="duckdb",
        dataset_name="staging",
    )
    load_info = pipe.run(jobsearch_source())
    print("✅ داده‌ها با موفقیت از JobTech API گرفته و در DuckDB ذخیره شدند.")
    print(load_info)
    return load_info

@job
def hr_pipeline():
    """Job اصلی Dagster برای اجرای pipeline DLT"""
    run_dlt_pipeline()
