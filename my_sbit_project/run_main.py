"""Module with entry point"""
import argparse


def main():
    parser = argparse.ArgumentParser(description="SBIT workflow launcher")

    arg_specs = [
        ("--workflow", dict(required=True, type=str, help="Workflow to be executed")),
    ]

    for arg_name, arg_params in arg_specs:
        parser.add_argument(arg_name, **arg_params)

    known_args, unknown_args = parser.parse_known_args()

    print(f'Entry point executed with parameters: {known_args}')

if __name__ == '__main__':
    main()