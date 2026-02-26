import datetime
import json
import re

from pyspark.sql.types import StringType, StructType, StructField, ArrayType

from portada_data_layer.boat_fact_model import BoatFactDataModel
from portada_data_layer.data_lake_metadata_manager import DataLakeMetadataManager, enable_storage_log_for_class, \
    block_transformer_method, data_transformer_method, enable_field_lineage_log_for_class
from portada_data_layer.delta_data_layer import DeltaDataLayer, FileSystemTaskExecutor
from portada_data_layer.traced_data_frame import TracedDataFrame
from pyspark.sql import Row, functions as F
from pyspark.sql.functions import col, year, month, dayofmonth
import os
import uuid
import logging
import xmltodict as xmldoc
import yaml as yamldoc

logger = logging.getLogger("portada_data.delta_data_layer.boat_fact_ingestion")


@enable_storage_log_for_class
@enable_field_lineage_log_for_class
class PortadaIngestion(DeltaDataLayer):
    def __init__(self, builder=None):
        super().__init__(builder=builder)
        self._schema = None
        self._current_process_level = 0

    # =====================================================
    # Ingestion process of entries
    # =====================================================
    @block_transformer_method
    def ingest(self, *container_path, local_path: str, user: str ):
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
        data, dest_path = self.copy_ingested_raw_data(*container_path, local_path=local_path, user=user, return_dest_path=True)

        # Classificació i desduplicació
        try:
            self.save_raw_data(*container_path, data={"source_path": dest_path, "data_json_array": data}, user=user)
            logger.info("Classification/Deduplication process completed successfully.")
        except Exception as e:
            logger.error(f"Error during classification/deduplication: {e}")
            raise
        logger.info("Ingestion process completed successfully.")

    @data_transformer_method(description="Copy the original file to the FileSystem (HDFS/S3/file)")
    def copy_ingested_raw_data(self, *container_path, local_path: str, return_dest_path=False, user: str = None, remove_local: bool = True,  **kwargs):
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
                    data = yamldoc.safe_load(f)
            except yamldoc.YAMLError:
                try:
                    with open(local_path) as f:
                        data = xmldoc.parse(f.read())
                except Exception as e:
                    raise e
                # raise
        except Exception as e:
            logger.error(f"Error reading file {local_path}: {e}")
            raise e
        logger.info(f"Read {len(data)} entries from loca file.")

        # Copia del fitxer original (bronze)
        fs_exec = FileSystemTaskExecutor(self.get_configuration())
        try:
            file_extension = os.path.splitext(local_path)[1][1:]
            if user is None:
                dest_path = fs_exec.copy_from_local(*container_path,
                                                file_name_dest=fs_exec.date_random_file_name_generator(file_extension),
                                                src_path=local_path, remove_local=remove_local)
            else:
                dest_path = fs_exec.copy_from_local(*container_path, user,
                                                file_name_dest=fs_exec.date_random_file_name_generator(file_extension),
                                                src_path=local_path, remove_local=remove_local)
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

    def save_raw_data(self, *container_path, data: dict | list = None, user: str = None, source_path: str = None,  **kwargs):
        pass

    def read_raw_data(self, *container_path, user: str = None, **kwargs):
        pass

    def use_schema(self, json_schema: dict):
        self._schema = json_schema
        return self


class NewsExtractionIngestion(PortadaIngestion):
    def __init__(self, builder=None):
        super().__init__(builder=builder)
        # self.get_single_value_of_udf = None
        # self.get_struct_value_of_udf=None
        self.generate_uid_udf = None
        self._register_udfs()

    def _register_udfs(self):
        """Register UDF to extract values from structured fields as boat fact data model."""

        # # @F.udf(returnType=StringType())
        # def get_single_value_of(value):
        #     if isinstance(value, Row):
        #         value = value.asDict(recursive=True)
        #     elif isinstance(value, str):
        #         if value.startswith("{") or value.startswith("["):
        #             value = json.loads(value)
        #     return BoatFactDataModel.get_single_value_of(value)
        #
        # self.register_udfs("get_single_value_of_udf", get_single_value_of, StringType())

        # def get_struct_value_of(value):
        #     if isinstance(value, Row):
        #         value = value.asDict(recursive=True)
        #     elif isinstance(value, str):
        #         if value.startswith("{") or value.startswith("["):
        #             value = json.loads(value)
        #     return BoatFactDataModel.get_struct_value_of(value)
        #
        # self.register_udfs(
        #     "get_struct_value_of_udf",
        #     get_struct_value_of, StructType(
        #         [
        #             StructField(BoatFactDataModel.DEFAULT_VALUE_FIELD, StringType(), True),
        #             StructField(BoatFactDataModel.ORIGINAL_VALUE_FIELD, StringType(), True),
        #             StructField(BoatFactDataModel.CALCULATED_VALUE_FIELD, StringType(), True),
        #         ]
        #     )
        # )

        # @F.udf(StringType())
        # def generate_uid():
        #     return str(uuid.uuid4())
        #
        # self.generate_uid_udf = generate_uid

    @data_transformer_method(
        description="Extract values from original files and organize them by news publication metadata.")
    def save_raw_data(self, *container_path, data: dict | list = None, user: str = None, source_path: str = None, **kwargs):
        """
        Save an array of ship entries (JSON) adding or updating them in files organized by:  date_path / publication_name / y / m / d / publication_edition
        """
        super().save_raw_data(*container_path, data=data, user=user, **kwargs)
        if data is None:
            raise ValueError("A DataFrame or JSON list must be passed.")

        source_version = -1
        if isinstance(data, dict):
            data_json_array = data["data_json_array"]
            source_path = self._resolve_relative_path(data["source_path"])
            p = re.compile(f"{self.project_name}/{self._process_level_dirs_[self._current_process_level]}/(.*)")
            tn = re.sub(p, "\\g<1>", source_path, 0)
        else:
            data_json_array = data
            if source_path is None:
                tn = "UNKNOWN"
                source_path = "UNKNOWN"
            else:
                source_path = self._resolve_relative_path(source_path)
                p = re.compile(f"{self.project_name}/{self._process_level_dirs_[self._current_process_level]}/(.*)")
                tn = re.sub(p, "\\g<1>", source_path, 0)


        length = len(data_json_array)
        if length == 0:
            return []
        start_counter = self.get_sequence_value("entry_ships", BoatFactDataModel(data_json_array[0])["publication_name"].lower(), increment=len(data_json_array))
        df = TracedDataFrame(
            df=self.spark.read.json(
                self.spark.sparkContext.parallelize([json.dumps(BoatFactDataModel(obj).reformat(i)) for i, obj in enumerate(data_json_array, start_counter)])),
            table_name=tn,
            df_name=source_path,
        )

        if not self.is_initialized():
            error_msg = "PortadaIngestion instance is not initializer. start_spark() method must be called first."
            logger.error(error_msg)
            raise ValueError(error_msg)

        # df = df.withColumn("entry_id", self.generate_uid_udf())
        # df.persist()
        if user is not None:
            df = df.withColumn("uploaded_by", F.lit(user))
        df = df.withColumn("publication_date_value", F.to_date("publication_date", "yyyy-MM-dd"))
        df = df.withColumn("publication_date_year", year(col("publication_date_value")))
        df = df.withColumn("publication_date_month", month(col("publication_date_value")))
        df = df.withColumn("publication_date_day", dayofmonth(col("publication_date_value")))
        df = df.drop("publication_date_value")

        grouped = df.select(
            "publication_name",
            "publication_date_year",
            "publication_date_month",
            "publication_date_day",
            "publication_edition"
        ).distinct()

        metadata = DataLakeMetadataManager(self.get_configuration())
        base_path = f"{self._resolve_path(*container_path, process_level_dir=self.raw_subdir)}"
        regs = 0
        df_list = []
        for row in grouped.collect():
            pub_name = row["publication_name"]
            year_ = row["publication_date_year"]
            month_ = row["publication_date_month"]
            day_ = row["publication_date_day"]
            edition = row["publication_edition"]
            full_path = os.path.join(base_path, pub_name.lower(), f"{year_:04d}", f"{month_:02d}", f"{day_:02d}", edition.lower())

            subset = df.filter(
                (col("publication_name") == pub_name) &
                (col("publication_date_year") == year_) &
                (col("publication_date_month") == month_) &
                (col("publication_date_day") == day_) &
                (col("publication_edition") == edition)
            )

            # If file exists, load it and detect duplicates
            if self.json_file_exist(full_path):
                existing_df = self.read_json(full_path)
                existing_df = existing_df.localCheckpoint()
                merged_df = subset.unionByName(existing_df, allowMissingColumns=True).dropDuplicates(["parsed_text"])
                duplicates = subset.count() + existing_df.count() - merged_df.count()
                regs += merged_df.count()
                if duplicates > 0:
                    duplicated_df = existing_df.join(merged_df, on="entry_id", how="left_anti")
                    duplicated_df = subset.join(duplicated_df.select("parsed_text"), on="parsed_text",
                                                how="left").unionByName(duplicated_df, allowMissingColumns=True)
                    metadata.log_duplicates(
                        data_layer=self,
                        action=DataLakeMetadataManager.DELETE_DUPLICATES_ACTION,
                        publication=pub_name.lower(),
                        date={"year": year_, "month": month_, "day": day_},
                        edition=edition.lower(),
                        duplicates_df=duplicated_df,
                        source_path=source_path,
                        source_version=source_version,
                        target_path=full_path,
                        uploaded_by=user,
                    )
                df_list.append(merged_df)
                self.write_json(full_path, df=merged_df, mode="overwrite")
            else:
                regs += subset.count()
                df_list.append(subset)
                self.write_json(full_path, df=subset, mode="overwrite")

        logger.info(f"{regs} entries was saved")

        return df_list

    def read_raw_data(self, *container_path, user: str = None, publication_name: str = None, y: int | str = None, m: int | str = None,
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
        if df is not None:
            if user is not None:
                df = df.filter(F.col("uploaded_by") == user)
            logger.info(f"{0 if df is None else df.count()} entries was read")
        return df

    def get_missing_dates_from_a_newspaper(self, *container_path, publication_name: str, start_date: str = None,
                                           end_date: str = None, date_and_edition_list: dict | str = None):
        if date_and_edition_list is not None:
            if isinstance(date_and_edition_list, str):
                if date_and_edition_list.startswith("{"):
                    date_and_edition_list = json.load(date_and_edition_list)
                elif date_and_edition_list.startswith("["):
                    date_and_edition_list = json.load(date_and_edition_list)
                else:
                    try:
                        date_and_edition_list = yamldoc.safe_load(date_and_edition_list)
                    except yamldoc.YAMLError:
                        try:
                            date_and_edition_list = xmldoc.parse(date_and_edition_list)
                        except Exception:
                            l = date_and_edition_list.split("\n")
                            date_and_edition_list=[]
                            for date in l:
                                date_and_edition_list.append({date: ["U"]})
            dl = []
            for date , edition_list in date_and_edition_list:
                for edition in edition_list:
                    dl.append((date,edition))
        else:
            dl = None

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
                if dl is None:
                    while current_date <= end_date:
                        dp = list(container_path)
                        dp.append(publication_name)
                        dp.append(f"{current_date.year:04d}")
                        dp.append(f"{current_date.month:02d}")
                        dp.append(f"{current_date.day:02d}")
                        if not self.path_exists(dp):
                            ret.append(current_date.strftime("%Y-%m-%d"))
                        current_date = current_date + datetime.timedelta(days=1)
                else:
                    for d, e in dl:
                        dp = list(container_path)
                        dp.extend(d.split("-"))
                        dp.append(e.lower())
                        if not self.path_exists(dp):
                            ret.append(f"{d} ({e})")
                return ret
            else:
                raise Exception(f"The container {'/'.join(p0)} is empty.")
        else:
            raise Exception(f"The container {'/'.join(p0)} doesn't exist.")


class KnownEntitiesIngestion(PortadaIngestion):
    __first_container_path = "known_entities"
    FLAG_ENTITY = 'flag'
    SHIP_TONS_ENTITY = 'ship_tons'
    TRAVEL_DURATION_ENTITY = 'travel_duration'
    COMODITY_ENTITY = 'comodity'
    SHIP_TYPE_ENTITY = 'ship_type'
    UNIT_ENTITY = 'unit'
    PORT_ENTITY = 'port'
    MASTER_ROLE_ENTITY = 'master_role'


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

    def copy_ingested_raw_data(self, *container_path, local_path: str, return_dest_path=False, remove_local: bool = True):
        container_path = self.__resolve_container_path(*container_path)
        container_path.insert(1, "original_files")
        return super().copy_ingested_raw_data(*container_path, local_path=local_path,
                                              return_dest_path=return_dest_path, remove_local= remove_local)

    @data_transformer_method(description="Copy the original file to the FileSystem (HDFS/S3/file)")
    def save_raw_data(self, *container_path, data: dict | list = None, source_path: str = None, **kwargs):
        super().save_raw_data(*container_path, data=data, **kwargs)
        container_path = self.__resolve_container_path(*container_path)
        if data is None:
            raise ValueError("A DataFrame or JSON list must be passed.")

        if isinstance(data, dict) and "source_path" in data:
            source_path = self._resolve_relative_path(data["source_path"])
            p = re.compile(f"{self.project_name}/{self._process_level_dirs_[self._current_process_level]}/(.*)")
            tn = re.sub(p, "\\g<1>", source_path, 0)
            data = data["data"]
        else:
            data = data
            if source_path is None:
                tn = "UNKNOWN"
                source_path = "UNKNOWN"
            else:
                source_path = self._resolve_relative_path(source_path)
                p = re.compile(f"{self.project_name}/{self._process_level_dirs_[self._current_process_level]}/(.*)")
                tn = re.sub(p, "\\g<1>", source_path, 0)

        if isinstance(data, dict) and "names" in data:
            data = data["names"]
        elif isinstance(data, str):
            if data.startswith("{"):
                data = json.load(data)["names"]
            elif data.startswith("["):
                data = json.load(data)
            else:
                try:
                    data = yamldoc.safe_load(data)
                except yamldoc.YAMLError:
                    try:
                        data = xmldoc.parse(data)
                    except Exception as e:
                        error_msg = "Known entities list must have an accepted format: json, yaml or xmldoc."
                        logger.error(error_msg)
                        raise SyntaxError(error_msg)
                    # error_msg = "Known entities list must have an accepted format: json, yaml or xmldoc."
                    # logger.error(error_msg)
                    # raise SyntaxError(error_msg)
        structured_data = [{"name": k, "voices": data[k]} for k in data]
        schema = StructType(
            [StructField("name", StringType(), False), StructField("voices", ArrayType(StringType()))])
        df = TracedDataFrame(
            df=self.spark.createDataFrame(structured_data, schema=schema),
            table_name=tn,
            df_name=source_path,
        )

        if not self.is_initialized():
            error_msg = "PortadaIngestion instance is not initializer. start_spark() method must be called first."
            logger.error(error_msg)
            raise ValueError(error_msg)

        base_path = f"{self._resolve_path(*container_path)}"
        self.write_delta(base_path, df=df, mode="overwrite")
        return df

    def copy_ingested_entities(self, entity: str, local_path: str, return_dest_path=False):
        return self.copy_ingested_raw_data(entity, local_path=local_path,
                                           return_dest_path=return_dest_path)

    @data_transformer_method(
        description="Extract values from original files and save as delta format.")
    def save_raw_entities(self, entity: str, data: dict | list = None):
        return self.save_raw_data(entity, data=data)

    def read_raw_data(self, *container_path):
        container_path= self.__resolve_container_path(*container_path)
        df = self.read_delta(*container_path)
        return df

    def read_raw_entities(self, entity: str):
        return self.read_raw_data(entity)

class BoatFactIngestion(NewsExtractionIngestion):
    __container_path = "ship_entries"

    def ingest(self, local_path: str, user: str ):
        return super().ingest(self.__container_path, local_path=local_path, user=user)

    def save_raw_data(self, data: dict | list = None, user:str = None, source_path: str = None, **kwargs):
        return super().save_raw_data(self.__container_path, user=user, data=data, source_path=source_path, **kwargs)

    def read_raw_data(self, publication_name: str = None, y: int | str = None, m: int | str = None, d: int | str = None,
                      edition: str = None, user: str = None, **kwargs):
        return super().read_raw_data(self.__container_path, user=user, publication_name=publication_name, y=y, m=m, d=d,
                                     edition=edition)

    def get_missing_dates_from_a_newspaper(self, publication_name: str, start_date: str = None,
                                           end_date: str = None, date_and_edition_list: dict | str = None):
        return super().get_missing_dates_from_a_newspaper(self.__container_path, publication_name=publication_name,
                                                          start_date=start_date, end_date=end_date,
                                                          date_and_edition_list=date_and_edition_list)
