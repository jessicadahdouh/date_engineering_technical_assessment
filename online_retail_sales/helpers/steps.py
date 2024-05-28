from online_retail_sales.helpers.db_helpers import close_connection, connect_to_db
from online_retail_sales.helpers.settings import get_connection_string
from online_retail_sales.pipeline.base import PipelineStep
from typing import Optional
from sqlalchemy import text
import sqlalchemy.engine
import pandas as pd


db_engine: sqlalchemy.engine.Engine = None


class LoadDataframeFromPath(PipelineStep):
    """
    Step to read data from a file into a DataFrame.
    """

    def __init__(self) -> None:
        super().__init__("LoadDataframeFromPath")

    def function(self, data: Optional[pd.DataFrame], file_type: str = "csv", **kwargs) -> pd.DataFrame:
        """
        Read data from a file.

        Args:
            data (Optional[pd.DataFrame]): Input DataFrame (unused).
            file_type (str): Type of the file ('csv', 'excel', 'json'). Default is 'csv'.
            **kwargs: Additional keyword arguments passed to the corresponding pandas read function.

        Returns:
            pd.DataFrame: DataFrame with the data read from the file.
        """
        match file_type:
            case "csv":
                return pd.read_csv(**kwargs)
            case "xlsx":
                return pd.read_excel(**kwargs)
            case "json":
                return pd.read_json(**kwargs)
            case _:
                raise ValueError(f"Unsupported file format: {'file_type'}")


class InitializeDatabaseConnection(PipelineStep):
    """
    Step to initialize the database connection.
    """

    def __init__(self) -> None:
        super().__init__("InitializeDatabaseConnection")

    def function(self, data: Optional[pd.DataFrame] = None, **kwargs) -> pd.DataFrame:
        """
        Initialize the database connection.

        Args:
            data (Optional[pd.DataFrame]): Input DataFrame (unused).

        Returns:
            pd.DataFrame: An empty DataFrame, since this step does not produce data.
        """
        global db_engine
        connection_string = get_connection_string()
        db_engine = connect_to_db(connection_string)
        print("Database connection initialized.")
        return data


class SaveDataframeToDatabase(PipelineStep):
    """
    Step to save data to a database.
    """

    def __init__(self) -> None:
        super().__init__("SaveDataframeToDatabase")

    def function(self, data: Optional[pd.DataFrame], table_name: str, schema_name: str, if_exists: str = 'append',
                 *kwargs) -> None:
        """
        Save data to the database.

        Args:
            data (Optional[pd.DataFrame]): Input DataFrame.
            table_name (str): Name of the table.
            schema_name (str): Name of the schema.
            if_exists (str): Behavior if the table already exists ('replace', 'append', 'fail'). Default is 'append'.
            **kwargs: Additional keyword arguments passed to the pandas to_sql function.

        Returns:
            pd.DataFrame: The input DataFrame, returned after saving to the database.
        """
        try:
            with db_engine.connect() as conn:
                data.to_sql(table_name, con=conn, schema=schema_name, if_exists=if_exists, index=False, *kwargs)
                return data
        except Exception as e:
            print(f"Error occurred: {str(e)}")
            raise e


class CloseDatabaseConnection(PipelineStep):
    """
    Step to close a database connection.
    """
    def __init__(self) -> None:
        super().__init__("CloseDatabaseConnection")

    def function(self, data: Optional[pd.DataFrame]) -> pd.DataFrame:
        """
        Close the connection to the PostgreSQL database.

        Args:
            data (Optional[pd.DataFrame]): Input DataFrame.

        Raises:
            ValueError: If connection instance is not provided.

        Returns:
            pd.DataFrame: The input DataFrame, returned after closing the database connection.
        """
        if db_engine is None:
            raise ValueError("Connection instance must be provided.")

        try:
            close_connection(db_engine)
            return data
        except Exception as e:
            print(f"Error occurred: {str(e)}")
            raise e


class ReadDataframeFromDatabase(PipelineStep):
    """
    Step to read data from a database into a DataFrame.
    """

    def __init__(self) -> None:
        super().__init__("ReadDataframeFromDatabase")

    def function(self, query: str, table_name: str, schema_name: str, *kwargs) -> Optional[pd.DataFrame]:
        """
        Read data from the database into a DataFrame.

        Args:
            query (str): SQL query to execute.
                        If not provided, a default query will be constructed using table_name and schema_name.
            table_name (str): Name of the table.
            schema_name (str): Name of the schema.
            **kwargs: Additional keyword arguments passed to the pandas read_sql_query function.

        Returns:
            Optional[pd.DataFrame]: DataFrame containing the fetched data.

        Raises:
            Exception: If an error occurs during the database query.
        """
        try:
            if not query:
                query = f"SELECT * FROM {schema_name}.{table_name};"

            with db_engine.connect() as conn:
                result = pd.read_sql_query(query, con=conn, *kwargs)
                return result
        except Exception as e:
            print(f"Error occurred: {str(e)}")
            raise e


class RemoveNulls(PipelineStep):
    """
    Step to remove null values from a DataFrame.
    """

    def __init__(self) -> None:
        super().__init__("RemoveNulls")

    def function(self, data: pd.DataFrame, *kwargs) -> pd.DataFrame:
        """
        This function removes null values from the given DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame from which null values will be removed.
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: The DataFrame with null values removed.
        """
        non_null_df = data.dropna()
        non_null_df.reset_index(inplace=True)
        return non_null_df


class RemoveDuplicates(PipelineStep):
    """
    Step to remove duplicate rows from the DataFrame.
    """
    def __init__(self) -> None:
        super().__init__("RemoveDuplicatesStep")

    def function(self, data: pd.DataFrame, *kwargs) -> pd.DataFrame:
        """
        Remove duplicate rows in the data.

        Args:
            data (pd.DataFrame): The input DataFrame.
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: DataFrame after removing duplicates.
        """
        no_dups = data.drop_duplicates()
        return no_dups


class HandleOutliersZScore(PipelineStep):
    """
    Step to handle outliers in the DataFrame using Z-Score.
    """
    def __init__(self) -> None:
        super().__init__("HandleOutliersStep")

    def function(self, data: pd.DataFrame, method: str = 'remove', z_thresh: float = 3.0, *kwargs) -> pd.DataFrame:
        """
        Handle outliers in the data.

        Args:
            data (pd.DataFrame): Input DataFrame.
            method (str, optional): Method to handle outliers. Default is 'remove'.
            z_thresh (float, optional): Z-score threshold. Default is 3.0.
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: DataFrame after handling outliers.

        Raises:
            ValueError: If an unsupported method is provided.
        """
        import numpy as np
        if method == 'remove':
            z_scores = np.abs((data - data.mean()) / data.std())
            return data[(z_scores < z_thresh).all(axis=1)]
        else:
            raise ValueError(f"Unsupported method: {method}")


class HandleOutliersIQR(PipelineStep):
    """
    Step to handle outliers in the DataFrame using the IQR method.
    """
    def __init__(self) -> None:
        super().__init__("HandleOutliersIQRStep")

    def function(self, data: pd.DataFrame, iqr_factor: float = 1.5, **kwargs) -> pd.DataFrame:
        """
        Handle outliers in the data using the IQR method.

        Args:
            data (pd.DataFrame): Input DataFrame.
            iqr_factor (float, optional): Factor to multiply the IQR to determine the outlier bounds. Default is 1.5.
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: DataFrame after handling outliers.
        """
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - (iqr_factor * IQR)
        upper_bound = Q3 + (iqr_factor * IQR)
        return data[~((data < lower_bound) | (data > upper_bound)).any(axis=1)]


class ValidateData(PipelineStep):
    """
    Step to validate the integrity and consistency of the data.
    """
    def __init__(self) -> None:
        super().__init__("ValidateData")

    def function(self, data: pd.DataFrame, *kwargs) -> pd.DataFrame:
        """
        Validate the data.

        Args:
            data (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame if validation passes.

        Raises:
            ValueError: If the data contains missing values or duplicates after handling.
        """
        if data.isnull().sum().sum() > 0:
            raise ValueError("Data contains missing values after handling")
        if data.duplicated().sum() > 0:
            raise ValueError("Data contains duplicates after handling")
        return data


class DataframeShape(PipelineStep):
    """
    Step to validate the shape of the data.
    """

    def __init__(self) -> None:
        super().__init__("DataframeShape")

    def function(self, data: pd.DataFrame, *kwargs) -> pd.DataFrame:
        """
        Validate the shape of the data.

        Args:
            data (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Input DataFrame.

        Prints:
            str: A message indicating the number of rows and columns in the DataFrame.
        """
        print(f"The data is made up of {data.shape[0]} row(s) and {data.shape[1]} column(s).")
        return data


class ConvertStrToDatetime(PipelineStep):
    """
    Step to convert a column of strings representing dates into datetime data type.
    """
    def __init__(self) -> None:
        super().__init__("ConvertStrToDatetime")

    def function(self, data: pd.DataFrame, date_column: str, *kwargs) -> pd.DataFrame:
        """
        Convert a column of strings representing dates into datetime data type.

        Args:
            data (pd.DataFrame): The dataframe containing the date column.
            date_column (str): The name of the date column to convert from str to datetime.
            **kwargs: Additional keyword arguments passed to pd.to_datetime().

        Returns:
            pd.DataFrame: The input dataframe with the specified date column converted to datetime data type.
        """
        data[date_column] = pd.to_datetime(data[date_column], *kwargs).dt.tz_localize(None)

        return data


class ConvertFloatToStr(PipelineStep):
    """
    Step to convert a column from float to string.
    """

    def __init__(self) -> None:
        super().__init__("ConvertFloatToStr")

    def function(self, data: pd.DataFrame, str_column: str, *kwargs) -> pd.DataFrame:
        """
        Convert the specified column from float to string in the given DataFrame.

        Args:
            data (pd.DataFrame): The DataFrame containing the float column.
            str_column (str): The name of the float column to be converted.
            **kwargs: Additional keyword arguments passed to pd.Series.astype().

        Returns:
            pd.DataFrame: The input DataFrame with the specified column converted from float to string.
        """
        data[str_column] = data[str_column].astype(str, *kwargs)
        return data


class NormalizeData(PipelineStep):
    """
    Step to normalize the DataFrame.
    """

    def __init__(self) -> None:
        super().__init__("NormalizeData")

    def function(self, data: pd.DataFrame, feature_range=(0, 1), *kwargs) -> pd.DataFrame:
        """
        Normalize the data.

        Args:
            data (pd.DataFrame): Input DataFrame.
            feature_range (tuple, optional): Range of transformed feature values. Default is (0, 1).
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: DataFrame after normalization.
        """
        from sklearn.preprocessing import MinMaxScaler

        scaler = MinMaxScaler(feature_range=feature_range)
        data_normalized = scaler.fit_transform(data)
        return pd.DataFrame(data_normalized, columns=data.columns)


class StandardizeData(PipelineStep):
    """
    Step to standardize the DataFrame.
    """

    def __init__(self) -> None:
        super().__init__("StandardizeData")

    def function(self, data: pd.DataFrame, *kwargs) -> pd.DataFrame:
        """
        Standardize the data.

        Args:
            data (pd.DataFrame): Input DataFrame.
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: DataFrame after standardization.
        """
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        data_standardized = scaler.fit_transform(data)
        return pd.DataFrame(data_standardized, columns=data.columns)


class CreateNewTable(PipelineStep):
    """
    Step to create a new table.
    """

    def __init__(self) -> None:
        super().__init__("CreateNewTable")

    def function(self, data: pd.DataFrame, table_name: str, schema_name: str,
                 table_columns: list, distinct: int = 0, if_exists: str = 'append', *kwargs) -> pd.DataFrame:
        """
        Create a new table from the DataFrame.

        Args:
            data (pd.DataFrame): The DataFrame from which to create the new table.
            table_name (str): The name of the new table.
            schema_name (str): The name of the schema for the new table.
            table_columns (list): List of column names to include in the new table.
            distinct (int, optional): If 1, select distinct rows. Default is 0.
            if_exists (str, optional): Action to take if the table already exists. Default is 'append'.
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: The input DataFrame.

        Notes:
            This function creates a new table in the specified database schema with the provided data and table structure.
            If distinct is set to 1, only distinct rows based on the specified table columns will be selected.
        """
        if distinct == 1:
            new_table = data[table_columns].drop_duplicates()
        else:
            new_table = data[table_columns]

        with db_engine.connect() as conn:
            new_table.to_sql(table_name, con=conn, schema=schema_name, if_exists=if_exists, index=False)
        return data


class ExecuteQuery(PipelineStep):
    """
    Step to execute a SQL query using SQLAlchemy.
    """
    def __init__(self) -> None:
        super().__init__("ExecuteQuery")

    def function(self, data: pd.DataFrame, query: str, *kwargs) -> pd.DataFrame:
        """
        Execute the provided SQL query and return the result or perform an operation accordingly.

        Args:
            data (pd.DataFrame): Unused parameter. (This step doesn't modify the input DataFrame.)
            query (str): The SQL query to execute. It can include ALTER, CREATE INDEX, SELECT, INSERT, or other SQL commands.
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: DataFrame containing the result of the SELECT query if applicable. Otherwise, None.

        Notes:
            - If the query is a SELECT statement, the result is returned as a DataFrame.
            - For other types of queries like ALTER, CREATE INDEX, INSERT, or other SQL commands,
                this function performs the operation directly on the database.
            - Ensure that the provided query is valid and appropriate for the desired operation.

        """
        if query.lstrip().upper().startswith('SELECT'):
            with db_engine.connect() as conn:
                result = conn.execute(text(query))
                data = result.fetchall()
                columns = result.keys()
                return pd.DataFrame(data, columns=columns)
        else:
            # For queries other than SELECT, execute directly on the database
            with db_engine.connect() as conn:
                conn.execute(text(query))
            return data
