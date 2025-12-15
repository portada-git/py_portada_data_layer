import datetime
import json

import xmltodict as xmltodict
import yaml
from pyspark.sql.types import StringType, StructType, StructField, ArrayType

from portada_data_layer.boat_fact_model import BoatFactDataModel
from portada_data_layer.data_lake_metadata_manager import DataLakeMetadataManager, enable_storage_log_for_class, \
    block_transformer_method, data_transformer_method, LineageCheckingType, enable_field_lineage_log_for_class
from portada_data_layer.delta_data_layer import DeltaDataLayer, FileSystemTaskExecutor
# from portada_data_layer import PortadaBuilder
from portada_data_layer.traced_data_frame import TracedDataFrame
from pyspark.sql import Row, functions as F
from pyspark.sql.functions import col, year, month, dayofmonth
import os
import uuid
import logging

logger = logging.getLogger("portada_data.delta_data_layer.boat_fact_ingestion")


@enable_storage_log_for_class
@enable_field_lineage_log_for_class
class PortadaIngestion(DeltaDataLayer):
    def __init__(self, builder=None):
        super().__init__(builder=builder)
        self._current_process_level = 0

    # =====================================================
    # Ingestion process of entries
    # =====================================================
    @block_transformer_method
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

        # Copy original file from local to data lake and get data
        data, dest_path = self.copy_ingested_raw_data(*container_path, local_path=local_path, return_dest_path=True)

        # Classificació i desduplicació
        try:
            self.save_raw_data(*container_path, data={"source_path": dest_path, "data_json_array": data})
            logger.info("Classification/Deduplication process completed successfully.")
        except Exception as e:
            logger.error(f"Error during classification/deduplication: {e}")
            raise
        logger.info("Ingestion process completed successfully.")

    @data_transformer_method(description="Copy the original file to the FileSystem (HDFS/S3/file)")
    def copy_ingested_raw_data(self, *container_path, local_path: str, return_dest_path=False):
        """
        Copy a file pointed by a local_path to a destination_path. The destination path is built using container_path.
        the methodology  used to buid destination_path is the following:
           1. If container_path is a string containing a protocol ('file://', 'hdfs://...', ...) its value is used as
           absolute path for destination_path.

           2. If container_path is a dict or a list. Examples: copy_ingested_raw_entries(("portada", "ships"), local_path="ships.json")
           or copy_ingested_raw_entries(["portada", "ships.json"], local_path="ships.json"). The destination_path will be resolved as
           <delta_data_base_path_for_project_and_raw_stage>/portada

           3.  If container_path is only single string or a sequence of strings. Examples:
               - copy_ingested_raw_entries("ships", local_path="ships.json"). This case will be resolved as
               <delta_data_base_path_for_project_and_raw_stage>/ships
               - copy_ingested_raw_entries("portada", "ships", local_path="ships.json") will be resolved as
               <delta_data_base_path_for_project_and_raw_stage>/portada/ships

           4. String, sequence of strings, dict or list with items including dots as separator. Examples:
               - copy_ingested_raw_entries("portada.masters", local_path="ships.json") will be resolved as
               <delta_data_base_path_for_project_and_raw_stage>/portada/masters
               - copy_ingested_raw_entries(("first_folder", "portada.masters"), local_path="ships.json") will be
               resolved as <delta_data_base_path_for_project_and_raw_stage>/first_folder/portada/masters

        :param container_path: to build the destination_path
        :param local_path: where the local file is
        :param return_dest_path: This parameter is a flag to set if the built destination_path is returned
        :return: the copied data if return_dest_path is false. If return_dest_path is true, a tuple with the copied data
        and the destination_path value is returned
        """
        # Lectura del fitxer JSON original
        try:
            with open(local_path) as f:
                data = json.load(f)
        except json.decoder.JSONDecodeError:
            try:
                with open(local_path) as f:
                    data = yaml.safe_load(f)
            except yaml.YAMLError:
                try:
                    with open(local_path) as f:
                        data = xmltodict.parse(f.read())
                except Exception as e:
                    raise e
        except Exception as e:
            logger.error(f"Error reading file {local_path}: {e}")
            raise
        logger.info(f"Read {len(data)} entries from loca file.")

        # Copia del fitxer original (bronze)
        fs_exec = FileSystemTaskExecutor(self.get_configuration())
        try:
            file_extension = os.path.splitext(local_path)[1][1:]
            dest_path = fs_exec.copy_from_local(*container_path,
                                                file_name_dest=fs_exec.date_random_file_name_generator(file_extension),
                                                src_path=local_path, remove_local=True)
            dp = self._resolve_relative_path(dest_path)
            metadata = DataLakeMetadataManager(self.get_configuration())
            metadata.log_storage(data_layer=self, source_path=local_path, target_path=dp, mode="overwrite", new=True)
            logger.info(
                f"File copied to Hadoop file system in container: {container_path} ({local_path} -> {dest_path})")
        except Exception as e:
            logger.error(f"Error copying original file: {e}")
            raise
        if return_dest_path:
            return data, dest_path
        return data

    def save_raw_data(self, *container_path, df=None, data: dict | list = None):
        pass

    def read_raw_data(self, *container_path, **kwargs):
        pass


class NewsExtractionIngestion(PortadaIngestion):
    def __init__(self, builder=None):
        super().__init__(builder=builder)
        self.get_value_of_udf = None
        self.get_obtaining_method_for_udf = None
        self._register_udfs()

    def _register_udfs(self):
        """Register UDF to extract values from structured fields as boat fact data model."""

        # @F.udf(returnType=StringType())
        def get_value_of(value):
            if isinstance(value, Row):
                value = value.asDict(recursive=True)
            return BoatFactDataModel.get_value_of(value)

        # data_layer.get_value_of_udf = get_value_of
        self.register_udfs("get_value_of_udf", get_value_of, StringType())

        # @F.udf(returnType=StringType())
        def get_obtaining_method_for(value):
            if isinstance(value, Row):
                value = value.asDict(recursive=True)
            return BoatFactDataModel.get_obtaining_method_for(value)

        # data_layer.get_obtaining_method_for_udf = get_obtaining_method_for
        self.register_udfs("get_obtaining_method_for_udf", get_obtaining_method_for, StringType())

        @F.udf(StringType())
        def generate_uid():
            return str(uuid.uuid4())

        self.generate_uid_udf = generate_uid

    @data_transformer_method(
        description="Extract values from original files and organize them by news publication metadata.")
    def save_raw_data(self, *container_path, df=None, data: dict | list = None):
        """
        Save an array of ship entries (JSON) adding or updating them in files organized by:  date_path / publication_name / y / m / d / publication_edition
        """
        super().save_raw_data(*container_path, df=df, data=data)
        if not df and data is None:
            raise ValueError("A DataFrame or JSON list must be passed.")

        source_version = -1
        if df is None:
            if isinstance(data, dict):
                data_json_array = data["data_json_array"]
                source_path = self._resolve_relative_path(data["source_path"])
            else:
                data_json_array = data
                source_path = "UNKNOWN"
            df = TracedDataFrame(
                df=self.spark.read.json(
                    self.spark.sparkContext.parallelize([json.dumps(obj) for obj in data_json_array])),
                source_name=source_path,
            )
        elif isinstance(df, TracedDataFrame):
            source_path = df.source_name
            source_version = df.source_version
        else:
            source_path = "UNKNOWN"

        if not self.is_initialized():
            error_msg = "PortadaIngestion instance is not initializer. start_spark() method must be called first."
            logger.error(error_msg)
            raise ValueError(error_msg)

        # uuid_udf = F.udf(generate_uuid, StringType())
        df = df.withColumn("entry_id", self.generate_uid_udf())
        df.persist()
        df = df.withColumn("publication_name_value", F.lower(self.get_value_of_udf(F.col("publication_name"))))
        df = df.withColumn("publication_edition_value", F.lower(self.get_value_of_udf(F.col("publication_edition"))))
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
                existing_df = existing_df.localCheckpoint()
                merged_df = subset.unionByName(existing_df, allowMissingColumns=True).dropDuplicates(
                    ["parsed_text_value"])
                duplicates = subset.count() + existing_df.count() - merged_df.count()
                regs += merged_df.count()
                if duplicates > 0:
                    duplicated_df = existing_df.join(merged_df, on="entry_id", how="left_anti")
                    duplicated_df = subset.join(duplicated_df.select("parsed_text_value"), on="parsed_text_value",
                                                how="left").unionByName(duplicated_df, allowMissingColumns=True)
                    metadata.log_duplicates(
                        data_layer=self,
                        action=DataLakeMetadataManager.DELETE_DUPLICATES_ACTION,
                        publication=pub_name,
                        date={"year": year_, "month": month_, "day": day_},
                        edition=edition,
                        duplicates_df=duplicated_df,
                        source_path=source_path,
                        source_version=source_version,
                        target_path=full_path
                    )
                    self.write_json(full_path, df=merged_df, mode="overwrite")
                    # merged_df.coalesce(1).write.mode("overwrite").json(full_path)

            else:
                regs += subset.count()
                self.write_json(full_path, df=subset, mode="overwrite")
                # subset.coalesce(1).write.mode("overwrite").json(full_path)

        logger.info(f"{regs} entries was saved")

    def read_raw_data(self, *container_path, publication_name: str = None, y: int | str = None, m: int | str = None,
                      d: int | str = None, edition: str = None):
        base_path = f"{self._resolve_path(*container_path, process_level_dir=self.raw_subdir)}"
        if isinstance(y, int):
            y = f"{y:04d}"
        if isinstance(m, int):
            m = f"{m:02d}"
        if isinstance(d, int):
            d = f"{d:02d}"
        base_dir = os.path.join(base_path,
                                publication_name.lower() if publication_name else "*",
                                y or "*",
                                m or "*",
                                d or "*",
                                edition.lower() if edition else "*")
        path = os.path.join(base_dir, "*.json")
        # try:
        #     df = data_layer.spark.read.json(path)
        # except Exception as e:
        #     if "[PATH_NOT_FOUND]" in str(e):
        #         df = None
        #     else:
        #         raise e
        df = self.read_json(path, has_extension=True)
        logger.info(f"{0 if df is None else df.count()} entries was read")
        return df

    def get_missing_dates_from_a_newspaper(self, *container_path, publication_name: str, start_date: str = None,
                                           end_date: str = None):
        publication_name = publication_name.lower()
        p = list(container_path)
        p.append(publication_name)
        p0 = p.copy()
        p1 = p.copy()
        if self.path_exists(p0):
            years = self.subdirs_list(p0)
            years = sorted(years)
            if len(years) > 0:
                year0 = years[0]
                year1 = years[-1]
                p0.append(year0)
                p1.append(year1)
                months = self.subdirs_list(p0)
                month0 = months[0]
                months = self.subdirs_list(p1)
                month1 = months[-1]
                p0.append(month0)
                p1.append(month1)
                days = self.subdirs_list(p0)
                day0 = days[0]
                days = self.subdirs_list(p1)
                day1 = days[-1]

                if start_date is None:
                    start_date = datetime.date(int(year0), int(month0), int(day0))
                else:
                    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
                if end_date is None:
                    end_date = datetime.date(int(year1), int(month1), int(day1))
                else:
                    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
                current_date = start_date
                ret = []
                while current_date <= end_date:
                    dp = list(container_path)
                    dp.append(publication_name)
                    dp.append(f"{current_date.year:04d}")
                    dp.append(f"{current_date.month:02d}")
                    dp.append(f"{current_date.day:02d}")
                    if not self.path_exists(dp):
                        ret.append(current_date.strftime("%Y-%m-%d"))
                    current_date = current_date + datetime.timedelta(days=1)
                return ret
            else:
                raise Exception(f"The container {'/'.join(p0)} is empty.")
        else:
            raise Exception(f"The container {'/'.join(p0)} doesn't exist.")


class KnownEntitiesIngestion(PortadaIngestion):
    __first_container_path = "known_entities"

    def __resolve_container_path(self, *container_path):
        if len(container_path) > 0 and isinstance(container_path[0], list) or isinstance(container_path[0], tuple):
            container_path = container_path[0]
        if len(container_path) > 0 and isinstance(container_path[0], str) and not container_path[0].startswith(
                self.__first_container_path):
            container_path = list(container_path)
            container_path.insert(0, self.__first_container_path)
        if len(container_path) == 0:
            raise Exception("The name of entity or the container_path is necessary")
        return container_path

    def copy_ingested_raw_data(self, *container_path, local_path: str, return_dest_path=False):
        container_path = self.__resolve_container_path(*container_path)
        container_path.insert(1, "original_files")
        return super().copy_ingested_raw_data(*container_path, local_path=local_path,
                                              return_dest_path=return_dest_path)

    def save_raw_data(self, *container_path, df=None, data: dict | list = None):
        super().save_raw_data(*container_path, df=df, data=data)
        container_path = self.__resolve_container_path(*container_path)
        if not df and data is None:
            raise ValueError("A DataFrame or JSON list must be passed.")

        source_version = -1
        if df is None:
            if isinstance(data, dict) and "source_path" in data:
                source_path = self._resolve_relative_path(data["source_path"])
                data = data["data"]
            else:
                data = data
                source_path = "UNKNOWN"
            if isinstance(data, dict) and "names" in data:
                data = data["names"]
            elif isinstance(data, str):
                if data.startswith("{"):
                    data = json.load(data)["names"]
                elif data.startswith("["):
                    data = json.load(data)
                else:
                    try:
                        data = yaml.reader(data)
                    except yaml.YAMLError:
                        try:
                            data = xmltodict.parse(data)
                        except Exception as e:
                            error_msg = "Known entities list must have an accepted format: json, yaml or xml."
                            logger.error(error_msg)
                            raise SyntaxError(error_msg)
            structured_data = [{"name": k, "voices": data[k]} for k in data]
            schema = StructType(
                [StructField("name", StringType(), False), StructField("voices", ArrayType(StringType()))])
            df = TracedDataFrame(
                df=self.spark.createDataFrame(structured_data, schema=schema),
                source_name=source_path,
            )
        elif not isinstance(df, TracedDataFrame):
            source_path = "UNKNOWN"
            df = TracedDataFrame(df, source_name=source_path)

        if not self.is_initialized():
            error_msg = "PortadaIngestion instance is not initializer. start_spark() method must be called first."
            logger.error(error_msg)
            raise ValueError(error_msg)

        base_path = f"{self._resolve_path(*container_path)}"
        self.write_delta(base_path, df=df, mode="overwrite")

    def copy_ingested_entities(self, entity: str, local_path: str, return_dest_path=False):
        return self.copy_ingested_raw_data(entity, local_path=local_path,
                                           return_dest_path=return_dest_path)

    def save_raw_entities(self, entity: str, df=None, data: dict | list = None):
        return self.save_raw_data(entity, df=df, data=data)

    def read_raw_data(self, *container_path):
        container_path= self.__resolve_container_path(*container_path)
        df = self.read_delta(*container_path)
        return df

    def read_raw_entities(self, entity: str):
        return self.read_raw_data(entity)

class BoatFactIngestion(NewsExtractionIngestion):
    __container_path = "ship_entries"

    def copy_ingested_raw_data(self, local_path: str, return_dest_path=False):
        return super().copy_ingested_raw_data(self.__container_path, local_path=local_path,
                                              return_dest_path=return_dest_path)

    def save_raw_data(self, df=None, data: dict | list = None, *args, **kwargs):
        return super().save_raw_data(self.__container_path, df=df, data=data)

    def read_raw_data(self, publication_name: str = None, y: int | str = None, m: int | str = None, d: int | str = None,
                      edition: str = None, *args, **kwargs):
        return super().read_raw_data(self.__container_path, publication_name=publication_name, y=y, m=m, d=d,
                                     edition=edition)

    def get_missing_dates_from_a_newspaper(self, *container_path, publication_name: str, start_date: str = None,
                                           end_date: str = None):
        return super().get_missing_dates_from_a_newspaper(self.__container_path, publication_name=publication_name,
                                                          start_date=start_date, end_date=end_date)
