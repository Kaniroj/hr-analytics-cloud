from dagster import Definitions
from dagster_dbt import load_assets_from_dbt_project

from dagster_hr_dagster.hr_analytics_job import hr_pipeline
from dagster_hr_dagster.schedules import daily_hr_pipeline
from dagster_hr_dagster.sensors import new_file_sensor

# مسیر به پروژه‌ی dbt
DBT_PROJECT_PATH = "dbt"
DBT_PROFILES_PATH = "dbt"

# بارگذاری مدل‌های dbt به‌صورت asset
dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_PATH,
    DBT_PROFILES_PATH,
)

# ساخت مجموعه‌ی Dagster Definitions (نسخه‌ی مدرن‌تر از @repository)
defs = Definitions(
    assets=[dbt_assets],
    jobs=[hr_pipeline],
    schedules=[daily_hr_pipeline],
    sensors=[new_file_sensor],
)
