
import os
import requests
import dlt
from datetime import datetime, timedelta

# سه رشته‌ی شغلی انتخابی برای نمونه
OCCUPATION_TERMS = os.getenv("OCCUPATION_TERMS", "data,it|vård,omsorg|bygg").split("|")

# تابعی برای گرفتن آگهی‌ها از JobTech API
def fetch_job_ads(search_term: str, days_back: int = 7, limit: int = 500):
    """
    دریافت آگهی‌ها از API در بازه زمانی مشخص.
    """
    base_url = "https://jobsearch.api.jobtechdev.se/search"
    params = {
        "q": search_term,
        "limit": limit,
        "published-after": (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    response = requests.get(base_url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("hits", [])

# منبع (resource) برای dlt
@dlt.resource(name="job_ads", write_disposition="append")
def job_ads():
    """
    داده‌های آگهی‌های شغلی را از JobTech API استخراج می‌کند.
    """
    for term in OCCUPATION_TERMS:
        results = fetch_job_ads(term)
        for ad in results:
            yield {
                "occupation_field": term,
                "ad_id": ad.get("id"),
                "headline": ad.get("headline"),
                "employer": (ad.get("employer") or {}).get("name"),
                "municipality": (ad.get("workplace_addresses") or [{}])[0].get("municipality"),
                "employment_type": ad.get("employment_type"),
                "publication_date": ad.get("publication_date"),
                "source_url": ad.get("webpage_url"),
            }

# پیکربندی dlt و مقصد DuckDB
@dlt.pipeline(
    pipeline_name="jobtech_to_duckdb",
    destination="duckdb",
    dataset_name="landing"
)
def pipeline():
    pipeline.run(job_ads())

if __name__ == "__main__":
    pipeline()
    print("✅ داده‌ها با موفقیت در warehouse.duckdb ذخیره شدند.")
