from pathlib import Path
import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster_dlt import DagsterDltResource, dlt_assets
import dlt
import sys

# اضافه کردن مسیر dlt pipeline
sys.path.insert(0, str(Path(__file__).parents[1] / "data_extract_load"))
from load_hr_data import hr_data_source

# مسیر دیتابیس
DB_PATH = Path(__file__).parents[1] / "data_warehouse/hr_data.duckdb"

# تعریف dlt resource
dlt_resource = DagsterDltResource()

@dlt_assets(
    dlt_source=hr_data_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="hr_pipeline",
        dataset_name="staging",
        destination=dlt.destinations.duckdb(str(DB_PATH))
    ),
)
def dlt_load(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

# تعریف پروژه‌ی dbt
DBT_PROJECT_PATH = Path(__file__).parents[1] / "dbt"
dbt_project = DbtProject(project_dir=DBT_PROJECT_PATH)
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROJECT_PATH,
)

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# تعریف jobها
job_dlt = dg.define_asset_job("job_dlt", selection=dg.AssetSelection.keys("dlt_hr_data_source_hr_data_resource"))
job_dbt = dg.define_asset_job("job_dbt", selection=dg.AssetSelection.all())

# سنسور: وقتی dlt تموم شد → dbt اجرا شه
@dg.asset_sensor(asset_key=dg.AssetKey("dlt_hr_data_source_hr_data_resource"), job_name="job_dbt")
def dlt_to_dbt_sensor():
    yield dg.RunRequest()

# تعریف نهایی
defs = dg.Definitions(
    assets=[dlt_load, dbt_models],
    resources={"dlt": dlt_resource, "dbt": dbt_resource},
    jobs=[job_dlt, job_dbt],
    sensors=[dlt_to_dbt_sensor],
)
