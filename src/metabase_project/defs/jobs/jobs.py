import dagster as dg

extract_data_job = dg.define_asset_job(name='extract_data_job', selection=['metabase_data'])
transform_data_job = dg.define_asset_job(name='transform_data_job', selection=['cleaned_data'])
load_data_job = dg.define_asset_job(name='load_data_job', selection=['gsheet_data'])

extract_schedule = dg.ScheduleDefinition(job=extract_data_job, cron_schedule='0 6 * * *', execution_timezone='Asia/Ho_Chi_Minh')
transform_schedule = dg.ScheduleDefinition(job=transform_data_job, cron_schedule='0 6 * * *', execution_timezone='Asia/Ho_Chi_Minh')
load_schedule = dg.ScheduleDefinition(job=load_data_job, cron_schedule='0 6 * * *', execution_timezone='Asia/Ho_Chi_Minh')