from step_main_pipeline import run_pipeline
def create_flow_pipeline(project_code,pipeline_id,process, mount_point):

    print(pipeline_id)
    print(process)
    run_pipeline(project_code, pipeline_id,mount_point,run_dry=False,resetquery=False,one_sample=False)
    return 0
