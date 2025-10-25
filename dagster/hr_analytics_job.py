from dagster import job, op
import subprocess
import dlt
import sys
import os

# اضافه کردن مسیر DLT برای ایمپورت
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "dlt"))
from pipeline import jobsearch_source

# مسیر پروژه DBT
DBT_PROJECT_PATH = os.path.join(os.path.dirname(__file__), "..", "dbt")

@op
def run_dlt_pipeline():
    """اجرای pipeline DLT و بازگرداندن نتیجه"""
    pipe = dlt.pipeline(
        pipeline_name="jobtech_to_duckdb",
        destination="duckdb",
        dataset_name="staging",
    )
    load_info = pipe.run(jobsearch_source())
    print("✅ داده‌ها از JobTech API گرفته شدند و در DuckDB ذخیره شدند.")
    return load_info

@op
def run_dbt_transformations():
    """اجرای dbt run برای تبدیل داده‌ها در DuckDB"""
    print("🚀 اجرای DBT transformations...")
    try:
        subprocess.run(
            ["dbt", "run"],
            cwd=DBT_PROJECT_PATH,
            check=True,
            shell=True
        )
        print("✅ DBT transformations با موفقیت انجام شدند.")
    except subprocess.CalledProcessError as e:
        print("❌ خطا در اجرای DBT:", e)
        raise

@job
def hr_pipeline():
    """Pipeline کامل شامل DLT + DBT"""
    run_dbt_transformations(run_dlt_pipeline())
