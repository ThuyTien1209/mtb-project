import dagster as dg

extract_data_job = dg.define_asset_job(name='extract_data_job', selection=['metabase_data'])
transform_data_job = dg.define_asset_job(name='transform_data_job', selection=['cleaned_data'])
load_data_job = dg.define_asset_job(name='load_data_job', selection=['gsheet_data'])


full_job_affiliate = dg.define_asset_job(name='full_job_affiliate', selection=['metabase_data', 'cleaned_data', 'gsheet_affiliate', 'email_affiliate'])
full_job_facebook = dg.define_asset_job(name='full_job_facebook', selection=['metabase_data', 'cleaned_data', 'gsheet_facebook', 'email_facebook'])
full_job_organic = dg.define_asset_job(name='full_job_organic', selection=['metabase_data', 'cleaned_data', 'gsheet_organic', 'email_organic'])
full_job_twitter = dg.define_asset_job(name='full_job_twitter', selection=['metabase_data', 'cleaned_data', 'gsheet_twitter', 'email_twitter'])
full_job_google = dg.define_asset_job(name='full_job_google', selection=['metabase_data', 'cleaned_data', 'gsheet_google', 'email_google'])

# no sending email
etl_affiliate = dg.define_asset_job(name='etl_affiliate', selection=['metabase_data', 'cleaned_data', 'gsheet_affiliate'])
etl_facebook = dg.define_asset_job(name='etl_facebook', selection=['metabase_data', 'cleaned_data', 'gsheet_facebook'])
etl_organic = dg.define_asset_job(name='etl_organic', selection=['metabase_data', 'cleaned_data', 'gsheet_organic'])
etl_twitter = dg.define_asset_job(name='etl_twitter', selection=['metabase_data', 'cleaned_data', 'gsheet_twitter'])
etl_google = dg.define_asset_job(name='etl_google', selection=['metabase_data', 'cleaned_data', 'gsheet_google'])


extract_schedule = dg.ScheduleDefinition(job=extract_data_job, cron_schedule='0 6 * * *', execution_timezone='Asia/Ho_Chi_Minh')
transform_schedule = dg.ScheduleDefinition(job=transform_data_job, cron_schedule='0 6 * * *', execution_timezone='Asia/Ho_Chi_Minh')
load_schedule = dg.ScheduleDefinition(job=load_data_job, cron_schedule='0 6 * * *', execution_timezone='Asia/Ho_Chi_Minh')