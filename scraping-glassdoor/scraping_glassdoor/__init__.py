from dagster import Definitions, load_assets_from_modules
# from dagster import (
#     Definitions,
#     load_assets_from_modules,
#     define_asset_job,
#     ScheduleDefinition
# )

from . import assets

all_assets = load_assets_from_modules([assets], group_name="glassdoor_scraper_job")

defs = Definitions(
    assets = all_assets,
    jobs = [assets.glassdoor_scraper_job]
    # schedules = ScheduleDefinition(
    #     name = "glassdoor_scraper_schedule",
    #     job = define_asset_job(
    #         name = "glassdoor_scraper_job",
    #         selection = [
    #             html_code,
    #             parsed_html_code,
    #             formatted_df,
    #             new_clean_data,
    #             previously_stored_jobs,
    #             deduplicate_jobs,
    #             store_new_jobs,
    #         ]),
    #     cron_schedule = "0 3 * * *"
    # )
)
