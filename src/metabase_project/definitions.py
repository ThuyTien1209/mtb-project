import dagster as dg

from .defs.assets import assets
from .defs.jobs import jobs
from .defs.resources.notifier import NftyResource


all_assets = dg.load_assets_from_modules([assets])

defs = dg.Definitions(
    assets=all_assets,
    resources={
        'nfty': NftyResource()
    },
    jobs=[jobs.extract_data_job, jobs.transform_data_job, jobs.load_data_job],
    schedules=[jobs.extract_schedule, jobs.transform_schedule, jobs.load_schedule]
)
