from online_retail_sales.pipeline.base import PipelineBuilder
from online_retail_sales.helpers.db_helpers import connect_to_db
from online_retail_sales.helpers.steps import (LoadDataframeFromPath, SaveDataframeToDatabase, CloseDatabaseConnection,
                                               RemoveNulls, RemoveDuplicates, ValidateData, ConvertStrToDatetime,
                                               ConvertFloatToStr)
from online_retail_sales.helpers.settings import (get_connection_string, configure_ingestion_pipeline, configure_processing_pipeline)

configs = configure_ingestion_pipeline()
processing_config = configure_processing_pipeline()
db_connection = connect_to_db(get_connection_string()).connect()


ingest_from_path_to_db_pipeline = PipelineBuilder()\
                                    .add_step(LoadDataframeFromPath(), file_type=configs["file_ext"],
                                              filepath_or_buffer=configs["file_path"])\
                                    .add_step(SaveDataframeToDatabase(), db_engine=db_connection,
                                              schema_name=configs["db_schema_name"],
                                              table_name=configs["db_table_name"])\
                                    .add_step(CloseDatabaseConnection(), db_engine=db_connection)\
                                    .build()

cleaning_pipeline = PipelineBuilder()\
                                    .add_step(RemoveNulls())\
                                    .add_step(RemoveDuplicates())\
                                    .add_step(ValidateData())\
                                    .add_step(ConvertStrToDatetime(), date_column=processing_config["date_column"])\
                                    .add_step(ConvertFloatToStr(), str_column=processing_config["str_column"])\
                                    .build()

transforming_pipeline = PipelineBuilder()\
                                    .add_step(RemoveNulls())\
                                    .add_step(RemoveDuplicates())\
                                    .add_step(ValidateData())\
                                    .add_step(ConvertStrToDatetime(), date_column=processing_config["date_column"])\
                                    .build()
