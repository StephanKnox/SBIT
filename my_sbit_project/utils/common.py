import yaml


def read_yaml(config_file):
    """Read configuration yaml"""
    with open(config_file, 'r') as cfg:
        config = yaml.safe_load(cfg)
    return config