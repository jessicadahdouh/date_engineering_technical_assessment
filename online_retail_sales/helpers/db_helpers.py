from sqlalchemy import create_engine, engine


def connect_to_db(connection_string) -> engine.Engine:
    """
    Create a SQLAlchemy engine using the provided connection string.

    Parameters:
    connection_string (str): The connection string for the database.

    Returns:
    sqlalchemy.engine.Engine: SQLAlchemy engine object.
    """
    connection_engine = create_engine(connection_string)
    return connection_engine


def close_connection(connection_engine):
    """
    Close the connection of the SQLAlchemy engine.

    Parameters:
    engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object to close.
    """
    connection_engine.dispose()
