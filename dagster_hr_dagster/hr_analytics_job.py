from dagster import job, op
import subprocess
import sys
import os

# مرحله ۱: اجرای DLT pipeline
@op
def run_dlt_pipeline():
    print("🚀 Running DLT pipeline...")
    result = subprocess.run(
        [sys.executable, "dlt/pipeline.py"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"DLT pipeline failed:\n{result.stderr}")
    return "✅ DLT pipeline completed successfully"

# مرحله ۲: اجرای DBT transformations
@op
def run_dbt_transformations(context, dlt_result: str):
    context.log.info("🏗️ Running DBT transformations...")
    context.log.info(f"Previous step output: {dlt_result}")

    dbt_dir = os.path.join(os.getcwd(), "dbt")
    result = subprocess.run(
        ["dbt", "run"],
        cwd=dbt_dir,
        capture_output=True,
        text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"DBT run failed:\n{result.stderr}")
    return "✅ DBT models executed successfully"

# تعریف job اصلی Dagster
@job
def hr_pipeline():
    run_dbt_transformations(run_dlt_pipeline())
