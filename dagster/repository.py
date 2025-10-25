from hr_analytics_job import hr_pipeline
from dagster import repository

@repository
def hr_repo():
    return [hr_pipeline]
