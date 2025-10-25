from dagster import repository
from hr_analytics_job import hr_pipeline

@repository
def hr_repository():
    return [hr_pipeline]
