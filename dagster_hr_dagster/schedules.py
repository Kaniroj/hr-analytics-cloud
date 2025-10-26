from dagster import ScheduleDefinition
from dagster_hr_dagster.hr_analytics_job import hr_pipeline

daily_hr_pipeline = ScheduleDefinition(
    job=hr_pipeline,
    cron_schedule="0 2 * * *",  # هر روز ساعت 02:00 شب
)

