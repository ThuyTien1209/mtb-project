import dagster as dg

from .defs.assets import assets
from .defs.jobs import jobs
from .defs.resources.notifier import NftyResource
from .defs.resources.filter import FilterResource
from .defs.resources.email import EmailResource
from .defs.resources.client import ClientResource

all_assets = dg.load_assets_from_modules([assets])

defs = dg.Definitions(
    assets=all_assets,
    
    resources={
        'nfty': NftyResource(),
        'filter': FilterResource(),
        'email': EmailResource(),
        'client': ClientResource()
    },
    
    jobs=[jobs.full_job_affiliate,
          jobs.full_job_facebook,
          jobs.full_job_organic,
          jobs.full_job_twitter,
          jobs.full_job_google,
          
          jobs.etl_affiliate,
          jobs.etl_facebook,
          jobs.etl_organic,
          jobs.etl_twitter,
          jobs.etl_google],
    
    schedules=[jobs.extract_schedule, 
               jobs.transform_schedule, 
               jobs.load_schedule]
)
