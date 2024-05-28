import pandas as pd


def load_df_from_path(*args, **kwargs) -> pd.DataFrame:
    from_file = kwargs["from_file"] if kwargs["from_file"] else "csv"
    match from_file:
        case "csv":
            return pd.read_csv(*args, **kwargs)
        case "xlsx":
            return pd.read_excel(*args, **kwargs)
        case "json":
            return pd.read_json(*args, **kwargs)
        case _:
            raise ValueError(f"Unsupported file format: {from_file}")
