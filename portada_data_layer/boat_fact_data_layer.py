import json
from pyspark.sql.types import StringType
from portada_data_layer.data_lake_metadata_manager import DataLakeMetadataManager, enable_log_storage, log_process, log_process_context
from portada_data_layer.delta_data_layer import DeltaDataLayerBuilder, DeltaDataLayer, FileSystemTaskExecutor
from portada_data_layer.traced_data_frame import TracedDataFrame
from pyspark.sql import Row, functions as F, DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth
import os
import uuid
import logging

logger = logging.getLogger("delta_data_layer.boat_fact_data_layer")

class BoatFactDataModel(dict):
    CALCULATED_VALUE_FIELD = "calculated_value"
    DEFAULT_VALUE_FIELD = "default_value"
    ORIGINAL_VALUE_FIELD = "original_value"
    STACKED_VALUE = "stacked_value"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        if self.__is_json_structured_filed(key):
            if BoatFactDataModel.STACKED_VALUE in super().__getitem__(key):
                super().__getitem__(key).__getitem__(BoatFactDataModel.STACKED_VALUE).append(value)
            else:
                super().__setitem__(key, {BoatFactDataModel.STACKED_VALUE: [value]})
        else:
            super().__setitem__(key, value)

    def __getitem__(self, key):
        if self.__is_json_structured_filed(key):
            if BoatFactDataModel.STACKED_VALUE in super().__getitem__(key):
                return super().__getitem__(key).__getitem__(BoatFactDataModel.STACKED_VALUE)[-1]
            elif BoatFactDataModel.CALCULATED_VALUE_FIELD in super().__getitem__(key):
                return  super().__getitem__(key).__getitem__(BoatFactDataModel.CALCULATED_VALUE_FIELD)
            elif BoatFactDataModel.ORIGINAL_VALUE_FIELD in super().__getitem__(key):
                return  super().__getitem__(key).__getitem__(BoatFactDataModel.ORIGINAL_VALUE_FIELD)
            elif BoatFactDataModel.DEFAULT_VALUE_FIELD in super().__getitem__(key):
                return super().__getitem__(key).__getitem__(BoatFactDataModel.DEFAULT_VALUE_FIELD)
            else:
                return ""
        return super().__getitem__(key)

    def get(self, key, default=None):
        if key in self:
            return self.__getitem__(key)
        else:
            return default


    def __is_json_structured_filed(self, key):
        return key in self and BoatFactDataModel.is_structured_value(super().__getitem__(key))

    @staticmethod
    def is_structured_value(value):
        return (isinstance(value, dict) and
                (BoatFactDataModel.DEFAULT_VALUE_FIELD in value or
                 BoatFactDataModel.ORIGINAL_VALUE_FIELD in value or
                 BoatFactDataModel.CALCULATED_VALUE_FIELD in value or
                 BoatFactDataModel.STACKED_VALUE in value))

    @staticmethod
    def get_value_of(value):
        if value is None:
            ret = ""
        elif BoatFactDataModel.is_structured_value(value):
            if BoatFactDataModel.STACKED_VALUE in value:
                ret = value[BoatFactDataModel.STACKED_VALUE][-1] if value[BoatFactDataModel.STACKED_VALUE] else None
            elif BoatFactDataModel.CALCULATED_VALUE_FIELD in value:
                ret = value[BoatFactDataModel.CALCULATED_VALUE_FIELD]
            elif BoatFactDataModel.ORIGINAL_VALUE_FIELD in value:
                ret = value[BoatFactDataModel.ORIGINAL_VALUE_FIELD]
            elif BoatFactDataModel.DEFAULT_VALUE_FIELD in value:
                ret = value[BoatFactDataModel.DEFAULT_VALUE_FIELD]
            else:
                ret = ""
        else:
            ret = value
        return str(ret)

    @staticmethod
    def get_obtaining_method_for(value):
        if value is None:
            ret = None
        elif BoatFactDataModel.is_structured_value(value):
            if BoatFactDataModel.STACKED_VALUE in value:
                ret = f"{BoatFactDataModel.STACKED_VALUE}_{len(value[BoatFactDataModel.STACKED_VALUE])-1}"
            elif BoatFactDataModel.CALCULATED_VALUE_FIELD in value:
                ret = BoatFactDataModel.CALCULATED_VALUE_FIELD
            elif BoatFactDataModel.ORIGINAL_VALUE_FIELD in value:
                ret = BoatFactDataModel.ORIGINAL_VALUE_FIELD
            elif BoatFactDataModel.DEFAULT_VALUE_FIELD in value:
                ret = BoatFactDataModel.DEFAULT_VALUE_FIELD
            else:
                ret = "unknown_obtaining_method"
        else:
            ret = BoatFactDataModel.ORIGINAL_VALUE_FIELD
        return str(ret)

class BoatFactDataLayerBuilder(DeltaDataLayerBuilder):
    def __init__(self, json_config=None):
        super().__init__(json_config=json_config)

    """
    Builder to configure Spark + Delta Lake with a fluid and flexible API.
    Example:
        data_layer_builder = (
            BoatFactDataLayerBuilder()
            .protocol("hdfs://localhost:9000")
            .table_path("/datalake/bronze")
            .app_name("MyDeltaApp")
            .config("spark.sql.shuffle.partitions", "8")
        )
        layer = data_layer_builder.build()
        layer.open_spark()
        ...
        layer.close()
    """

    def build(self) -> "BoatFactDataLayer":
        """Constructs and returns a DeltaDataLayer initialized with this constructor."""
        delta_layer = BoatFactDataLayer(builder=self)
        return delta_layer

class BoatFactDataLayer(DeltaDataLayer):
    def __init__(self, builder: BoatFactDataLayerBuilder = None):
        super().__init__(builder=builder)
        self._register_udfs()
        self._current_process_level=0

    def _register_udfs(self):
        """Register UDF to extract values from structured fields as boat fact data model."""
        @F.udf(returnType=StringType())
        def get_value_of(value):
            if isinstance(value, Row):
                value = value.asDict(recursive=True)
            return BoatFactDataModel.get_value_of(value)

        @F.udf(returnType=StringType())
        def get_obtaining_method_for(value):
            if isinstance(value, Row):
                value = value.asDict(recursive=True)
            return BoatFactDataModel.get_obtaining_method_for(value)

        self.get_value_of_udf = get_value_of
        self.get_obtaining_method_for_udf = get_value_of

    # =====================================================
    # Ingestion process of entries
    # =====================================================
    @log_process_context
    @enable_log_storage
    def ingest(self, *container_path, local_path: str):
        """
        Process a JSON input file:
        1) Copy the original file to the FileSystem (HDFS/S3/file)
        2) Read its contents
        3) Save the entries sorted and without duplicates
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Ingest file not found: {local_path}")

        logger.info(f"Starting ingestion process for {local_path}")

        #Copy original file from local to data lake and get data
        data, dest_path = self.copy_ingested_raw_entries(*container_path, local_path=local_path, return_dest_path=True)

        # Classificació i desduplicació
        try:
            self.save_raw_entries(*container_path, data={"source_path":dest_path,"data_json_array":data})
            logger.info("Classification/Deduplication process completed successfully.")
        except Exception as e:
            logger.error(f"Error during classification/deduplication: {e}")
            raise

        self._current_process_level+=1
        logger.info("Ingestion process completed successfully.")

    @log_process_context
    @log_process
    def copy_ingested_raw_entries(self, *container_path, local_path: str, return_dest_path=False):
        # Lectura del fitxer JSON original
        try:
            with open(local_path) as f:
                data = json.load(f)
            logger.info(f"Read {len(data)} entries from loca file.")
        except Exception as e:
            logger.error(f"Error reading file {local_path}: {e}")
            raise

        #Copia del fitxer original (bronze)
        fs_exec = FileSystemTaskExecutor(self.get_configuration())
        try:
            dest_path = fs_exec.copy_from_local(*container_path,
                                                file_name_dest=fs_exec.date_random_file_name_generator("json"),
                                                src_path=local_path, remove_local=True)
            metadata = DataLakeMetadataManager(self.get_configuration())
            metadata.log_storage(data_layer=self, source_path=local_path, target_path=dest_path)
            logger.info(f"File copied to Hadoop file system in container: {container_path} ({local_path} -> {dest_path})")
        except Exception as e:
            logger.error(f"Error copying original file: {e}")
            raise
        if return_dest_path:
            return data, dest_path
        return data

    @log_process_context
    @log_process
    def save_raw_entries(self, *container_path, df=None, data: dict | list =None):
        """
        Save an array of ship entries (JSON) adding or updating them in files organized by:  date_path / publication_name / y / m / d / publication_edition
        """

        if not df and data is None:
            raise ValueError("A DataFrame or JSON list must be passed.")

        if df is None:
            if isinstance(data, dict):
                data_json_array = data["data_json_array"]
                source_path = data["source_path"]
            else:
                data_json_array = data
                source_path = "UNKNOWN"
            df = TracedDataFrame(
                df=self.spark.read.json(self.spark.sparkContext.parallelize([json.dumps(obj) for obj in data_json_array])),
                source_name=source_path,
            )

        if not self.is_initialized():
            error_msg = "BoatFactDataLayer instance is not initializer. start_spark() method must be called first."
            logger.error(error_msg)
            raise ValueError(error_msg)

        #Get value from boat fact data model format
        uuid_udf = F.udf(lambda: str(uuid.uuid4()))
        df = df.withColumn("entry_id", uuid_udf())
        df = df.withColumn("publication_name_value", self.get_value_of_udf(F.col("publication_name")))
        df = df.withColumn("publication_edition_value", self.get_value_of_udf(F.col("publication_edition")))
        df = df.withColumn("parsed_text_value", self.get_value_of_udf(F.col("parsed_text")))
        df = df.withColumn("publication_date_millis_value", self.get_value_of_udf(F.col("publication_date")))
        df = df.withColumn("publication_date_value", (F.col("publication_date_millis_value") / 1000).cast("timestamp"))
        df = df.withColumn("publication_date_year_value", year(col("publication_date_value")))
        df = df.withColumn("publication_date_month_value", month(col("publication_date_value")))
        df = df.withColumn("publication_date_day_value", dayofmonth(col("publication_date_value")))
        df = df.drop("publication_date_value")

        grouped = df.select(
            "publication_name_value",
            "publication_date_year_value",
            "publication_date_month_value",
            "publication_date_day_value",
            "publication_edition_value"
        ).distinct()

        metadata = DataLakeMetadataManager(self.get_configuration())
        base_path = f"{self._resolve_path(*container_path, process_level_dir=self.raw_subdir)}"
        regs = 0
        for row in grouped.collect():
            pub_name = row["publication_name_value"]
            year_ = row["publication_date_year_value"]
            month_ = row["publication_date_month_value"]
            day_ = row["publication_date_day_value"]
            edition = row["publication_edition_value"]
            full_path = os.path.join(base_path, pub_name, f"{year_:04d}", f"{month_:02d}", f"{day_:02d}", edition)

            subset = df.filter(
                (col("publication_name_value") == pub_name) &
                (col("publication_date_year_value") == year_) &
                (col("publication_date_month_value") == month_) &
                (col("publication_date_day_value") == day_) &
                (col("publication_edition_value") == edition)
            )

            # If file exists, load it and detect duplicates
            if self.json_file_exist(full_path):
                existing_df = self.read_json(full_path)
                merged_df = subset.unionByName(existing_df, allowMissingColumns=True).dropDuplicates(["parsed_text_value"])
                duplicates = subset.count() + existing_df.count() - merged_df.count()
                regs += merged_df.count()
                if duplicates > 0:
                    duplicated_df = subset.join(merged_df, on="entry_id", how="left_anti")
                    metadata.log_duplicates(
                        data_layer=self,
                        action=DataLakeMetadataManager.DELETE_DUPLICATES_ACTION,
                        publication=pub_name,
                        date=f"{year_:04d}_{month_:02d}_{day_:02d}",
                        edition=edition,
                        duplicates_df=duplicated_df,
                        from_table_full_path=full_path
                    )
                    self.write_json(full_path, df=merged_df, mode="overwrite")
                    # merged_df.coalesce(1).write.mode("overwrite").json(full_path)

            else:
                regs += subset.count()
                self.write_json(full_path, df=subset, mode="overwrite")
                # subset.coalesce(1).write.mode("overwrite").json(full_path)

        logger.info(f"{regs} entries was saved")


    def read_raw_entries(self, *container_path, publication_name: str = None, y: int | str = None, m: int | str = None, d: int | str = None, edition: str = None):
        base_path = f"{self._resolve_path(*container_path, process_level_dir=self.raw_subdir)}"
        if isinstance(y, int):
            y = f"{y:04d}"
        if isinstance(m, int):
            m = f"{m:02d}"
        if isinstance(d, int):
            d = f"{d:02d}"
        base_dir = os.path.join(base_path, publication_name or "*", y or "*", m or "*", d or "*", edition or "*")
        path = os.path.join(base_dir, "*.json")
        # try:
        #     df = self.spark.read.json(path)
        # except Exception as e:
        #     if "[PATH_NOT_FOUND]" in str(e):
        #         df = None
        #     else:
        #         raise e
        df = self.read_json(path, has_extension=True)
        logger.info(f"{0 if df is None else df.count()} entries was read")
        return df

    def flatten_fields(self, *container_path, df: DataFrame):
        pass

    def clean_fields(self, *container_path, df: DataFrame):
        pass