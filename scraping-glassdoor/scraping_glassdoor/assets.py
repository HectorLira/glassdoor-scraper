import time
# from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import sqlite3
from dagster import asset, define_asset_job, get_dagster_logger, Output

from bs4 import BeautifulSoup
from selenium import webdriver

logger = get_dagster_logger()


URL_LINK = "https://www.glassdoor.com.mx/Empleo/data-analyst-empleos-SRCH_KO0,12.htm"

@asset
def html_code() -> str:

    browser = webdriver.Chrome()
    browser.get(URL_LINK)

    html = browser.page_source
    time.sleep(2)

    browser.close()

    return html


@asset
def parsed_html_code(html_code: str) -> Any:
    return BeautifulSoup(html_code)


@asset
def formatted_df(parsed_html_code: Any) -> Output[pd.DataFrame]:

    company_names = []
    company_ratings = []
    job_titles = []
    job_links = []
    job_ids = []
    locations = []
    salary_ranges = []
    is_fast_candidacies = []
    listing_ages = []

    job_cards = parsed_html_code.find_all("div", attrs={"class": "jobCard"})

    for job in job_cards:
    
        company_info = job.find("div", attrs={"class": "EmployerProfile_employerInfo__EehaI"})
        company_rating_info = company_info.find("span", attrs={"class": "EmployerProfile_employerRating__lq_ZL"})
        
        company_name = company_info.find_next(text=True)
        try:
            company_rating = company_rating_info.text.replace("★", "").strip()
        except:
            company_rating = None
        
        job_title_info = job.find("a", attrs={"class": "JobCard_seoLink__r4HUE"})
        job_title = job_title_info.text
        job_link = job_title_info.get("href")
        job_id = job_title_info.get("id").replace("job-title-", "")
        
        location = job.find("div", attrs={"class": "JobCard_location__DX0MJ"}).text
        
        salary_range_info = job.find("div", attrs={"class": "JobCard_salaryEstimate__TLvO7"})
        try:
            salary_range = salary_range_info.text.replace("(Est. del empleador)", "").replace("\xa0", "")
        except:
            salary_range = None
            
        try:
            is_fast_candidacy = job.find("div", attrs={"class": "JobCard_easyApply__DU2gA"}).text
        except:
            is_fast_candidacy = None
        
        listing_age = job.find("div", attrs={"class": "JobCard_listingAge__JPA03"}).text.replace("\xa0d", "")
        
        company_names.append(str(company_name))
        company_ratings.append(str(company_rating))
        job_titles.append(str(job_title))
        job_links.append(str("https://www.glassdoor.com.mx" + job_link))
        job_ids.append(str(job_id))
        locations.append(str(location))
        salary_ranges.append(str(salary_range))
        is_fast_candidacies.append(str(is_fast_candidacy))
        listing_ages.append(str(listing_age))

    df = pd.DataFrame(data={
        "company_name": company_names,
        "company_rating": company_ratings,
        "job_title": job_titles,
        "job_link": job_links,
        "job_id": job_ids,
        "location": locations,
        "salary_range": salary_ranges,
        "is_fast_candidacy": is_fast_candidacies,
        "listing_age": listing_ages,
    })

    for col in df.columns:
        logger.info(f"{col}: {df[col].dtype}")

    return Output(
        df,
        metadata={"jobs_found": len(df)}
    )


@asset
def new_clean_data(formatted_df: pd.DataFrame) -> pd.DataFrame:

    df = formatted_df.copy()

    df["salary_range_min"] = (
        df["salary_range"]
        .str[:2]
        .str.replace("k", "")
    )
    df["salary_range_min"] = pd.to_numeric(df['salary_range_min'], errors='coerce')

    df["salary_range_max"] = (
        df["salary_range"]
        .str[-4:-2]
    )
    df["salary_range_max"] = pd.to_numeric(df['salary_range_max'], errors='coerce')

    df["listing_age_days"] = np.where(
        df["listing_age"].str.contains("d"),
        df["listing_age"].str[:2],
        df["listing_age"]
    )

    df["listing_age_days"] = np.where(
        df["listing_age"].str.contains("h"),
        1,
        df["listing_age_days"]
    )

    df["listing_age_days"] = np.where(
        df["listing_age"].str.contains("\+"),
        30,
        df["listing_age_days"]
    )

    df["listing_age_days"] = pd.to_numeric(df['listing_age_days'], errors='coerce')
    df["is_fast_candidacy"] = df["is_fast_candidacy"] == "Candidatura rápida"
    df["job_id"] = df["job_id"].astype("str")

    cols = [
        "job_id",
        "company_name",
        "company_rating",
        "job_title",
        "job_link",
        "location",
        "salary_range_min",
        "salary_range_max",
        "is_fast_candidacy",
        "listing_age_days"
    ]

    return df[cols]


@asset
def previously_stored_jobs() -> pd.DataFrame:
    conn = sqlite3.connect("db_glassdoor")

    previous_jobs = pd.read_sql_query("SELECT * FROM job_roles", conn)

    conn.close()

    return previous_jobs


@asset
def deduplicate_jobs(
    new_clean_data: pd.DataFrame,
    previously_stored_jobs: pd.DataFrame
) -> Output[pd.DataFrame]:
    
    all_jobs = pd.concat([
        new_clean_data.assign(status="new"),
        previously_stored_jobs.assign(status="previous")
    ])
    all_jobs["duplicated"] = all_jobs["job_id"].duplicated(keep=False)

    deduplicated_df = (
        all_jobs
        .query("(~duplicated) and status=='new'")
        .drop(columns=["duplicated", "status"])
        .reset_index(drop=True)
        # .assign(created_at=str(date.today())[:10])
    )

    return Output(
        deduplicated_df,
        metadata={
            "new_jobs_found": len(deduplicated_df)
        }
    )


@asset
def store_new_jobs(deduplicate_jobs: pd.DataFrame):
    if len(deduplicate_jobs) == 0:
        logger.info("Skipping data upload to database.")
    else:
        conn = sqlite3.connect("db_glassdoor")

        deduplicate_jobs.to_sql(
            'job_roles',
            con=conn,
            if_exists='append',
            index=False,
            chunksize=100,
            method="multi"
        )

        conn.close()

    return


glassdoor_scraper_job = define_asset_job(
    "glassdoor_scraper_job",
    selection = [
        html_code,
        parsed_html_code,
        formatted_df,
        new_clean_data,
        previously_stored_jobs,
        deduplicate_jobs,
        store_new_jobs,
    ]
)
