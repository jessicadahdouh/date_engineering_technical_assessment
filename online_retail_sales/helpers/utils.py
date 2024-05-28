from online_retail_sales.pipeline.base import PipelineBuilder
import importlib
import yaml


def load_config(config_path: str) -> dict:
    """Loads a YAML configuration file and returns a Python dictionary.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: A Python dictionary containing the configuration parameters.

    Raises:
        FileNotFoundError: If the configuration file cannot be found.
        ValueError: If the configuration file is not a valid YAML file.
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def execute_pipeline(config_section, module_name):
    module = importlib.import_module(module_name)

    builder = PipelineBuilder()

    for step in config_section:
        _class = step['class']
        _kwargs = step.get('kwargs', {})

        obj = getattr(module, _class)

        builder.add_step(obj(), **_kwargs)

    pipeline = builder.build()

    df = pipeline.execute()
    return df
