from online_retail_sales.helpers.utils import load_config, execute_pipeline
from pathlib import Path


if __name__ == "__main__":
    __version__ = "1.0.0"
    print("Version: ", __version__)

    print("Executing pipeline...")

    module_name = 'online_retail_sales.helpers.steps'

    current_directory = Path.cwd()
    print("Current Directory:", current_directory)

    config_folder = current_directory / 'configs' / 'config.yml'
    config = load_config(config_path=config_folder)

    pipeline_steps = config['pipeline_steps']

    df = execute_pipeline(pipeline_steps, module_name)
    print(f"The final shape of the df is: \n rows: {df.shape[0]}\n columns: {df.shape[1]}")
