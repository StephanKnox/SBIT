"""Module with entry point"""
import argparse
import importlib
from my_sbit_project.utils.common import parse_wkf_args
from my_sbit_project.workflows.GenericBackfillWkf import GenericBackfillWkf


def main():
    parser = argparse.ArgumentParser(description="SBIT environment setup")

    arg_specs = [
        ("--env", dict(required=True, help="DB name for a project")),
        ("--app", dict(required=True, help="Job(Class) name ")),
        ("--app_cfg", dict(required=True, help="Path to job parameter file")),
        ("--dt_from", dict(help="Backfill arg: Date from which to backfill")),
        ("--dt_to", dict(help="Backfill arg: Date up to which to backfill")),
        ("--dt_filter_col", dict(help="Backfill arg: Date column to filter on")),
    ]

    for arg_name, arg_params in arg_specs:
        parser.add_argument(arg_name, **arg_params)

    known_args, unknown_args = parser.parse_known_args()
    app = known_args.app

    backfill_wkf_args = {k:v for k,v in vars(known_args).items()}
    
    
    # Import the specified module dynamically
    module_name = f'my_sbit_project.workflows.{app}'
    try:
        module = importlib.import_module(module_name, package='my_sbit_project')
        print(f'{module_name} imported successfully')
    except ImportError as e:
        print(f'Error importing module {module_name}: {e}')
        raise

    # Get the workflow class and instantiate it
    workflow_class = getattr(module, app.split('.')[-1])
    
    wkf_instance = workflow_class(**backfill_wkf_args)
    backfill_wkf_args["wkf_instance"] = wkf_instance
    

    # Launch the workflow with error handling
    try:
        #print(workflow_instance)
        # TODO
        # to change to a generic backfill class call
        ##backfill_wkf = BackfillWkfFactory.get_backfill_wkf(app, backfill_wkf_args)
        ##backfill_wkf.launch()
        backfill_wkf = GenericBackfillWkf(**backfill_wkf_args)
        backfill_wkf.launch()
    except Exception as err:
        #err_msg = f'{err=}, {type(err)=} {traceback.format_exc()}'
        #pause_and_alert_on_max_retries(workflow_instance, err_msg)
        raise

if __name__ == '__main__':
    main()