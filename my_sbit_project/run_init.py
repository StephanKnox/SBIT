"""Module with entry point"""
import argparse


def main():
    parser = argparse.ArgumentParser(description="SBIT environment setup")

    arg_specs = [
        ("--env", dict(required=True, type=str, help="DB name for a project")),
    ]

    for arg_name, arg_params in arg_specs:
        parser.add_argument(arg_name, **arg_params)

    known_args, unknown_args = parser.parse_known_args()

    print(f'Entry point executed with parameters: {known_args}')

if __name__ == '__main__':
    main()