"""Module with entry point"""
import argparse
from my_sbit_project.setup_env.setup import SetupHelper


def main():
    parser = argparse.ArgumentParser(description="SBIT environment setup")

    arg_specs = [
        ("--env", dict(required=True, type=str, help="DB name for a project")),
        ("--param", dict(required=True, type=str, help="Path to job parameter file")),
    ]

    for arg_name, arg_params in arg_specs:
        parser.add_argument(arg_name, **arg_params)

    known_args, unknown_args = parser.parse_known_args()
    env, param = known_args.env, known_args.param

    print(f'Entry point executed with parameters: {known_args}')
    job = SetupHelper(env, param)
    job.run()

if __name__ == '__main__':
    main()
    