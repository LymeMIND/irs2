
from rq import get_current_job
from step_main_pipeline import run_pipeline
def create_flow_pipeline(project_code,pipeline_id,process, mount_point,run_dry=False,resetquery=False,one_sample=False ):

    job = get_current_job()
    job_id = job.get_id()
    # job_id='None'

    run_pipeline(project_code, pipeline_id,mount_point,job_id,run_dry,resetquery,one_sample)
    print('#############################################')
    print('WORKFLOW OVER')
    print('#############################################')
    return 0
