import csv
import datetime
import json
import re
from pyexpat import ExpatError

from pyspark.sql.types import StringType, StructType, StructField, ArrayType, BooleanType

from portada_data_layer.boat_fact_model import BoatFactDataModel
from portada_data_layer.data_lake_metadata_manager import DataLakeMetadataManager, enable_storage_log_for_class, \
    block_transformer_method, data_transformer_method, enable_field_lineage_log_for_class
from portada_data_layer.delta_data_layer import DeltaDataLayer, FileSystemTaskExecutor
from portada_data_layer.portada_delta_common import registry_to_portada_builder, BoatFactConstants
from portada_data_layer.portada_patcher_data_layer import BoatFactPatcherDataLayer, PatchError
from portada_data_layer.traced_data_frame import TracedDataFrame
from pyspark.sql import Row, functions as F
from pyspark.sql.functions import col, year, month, dayofmonth
import os
import uuid
import logging
import xmltodict as xmldoc
import yaml as yamldoc

_RAW_GROUP_KEY_COLS = ("_gk_pub", "_gk_y", "_gk_m", "_gk_d", "_gk_ed")
_RAW_PARTITION_COLS = ("_pub", "_y", "_m", "_d", "_ed")

logger = logging.getLogger("portada_data.delta_data_layer.boat_fact_ingestion")


def _stringify_boolean_schema(data_type):
    """Return a schema like data_type but with every boolean replaced by string."""
    if isinstance(data_type, BooleanType):
        return StringType()
    if isinstance(data_type, StructType):
        return StructType(
            [
                StructField(f.name, _stringify_boolean_schema(f.dataType), f.nullable)
                for f in data_type.fields
            ]
        )
    if isinstance(data_type, ArrayType):
        return ArrayType(
            _stringify_boolean_schema(data_type.elementType),
            containsNull=data_type.containsNull,
        )
    return data_type


def _schema_has_boolean(data_type) -> bool:
    if isinstance(data_type, BooleanType):
        return True
    if isinstance(data_type, StructType):
        return any(_schema_has_boolean(f.dataType) for f in data_type.fields)
    if isinstance(data_type, ArrayType):
        return _schema_has_boolean(data_type.elementType)
    return False


def _cast_booleans_to_string_df(df):
    """
    Cast boolean columns (including nested struct/array fields) to string so unionByName
    stays compatible when IA extraction uses strings and older JSON used booleans.
    """
    if df is None:
        return df
    for field in df.schema.fields:
        if not _schema_has_boolean(field.dataType):
            continue
        target = _stringify_boolean_schema(field.dataType)
        if isinstance(field.dataType, BooleanType):
            df = df.withColumn(field.name, F.col(field.name).cast("string"))
        else:
            df = df.withColumn(
                field.name,
                F.from_json(F.to_json(F.col(field.name)), target),
            )
    return df


def _align_for_union_by_name(df_left, df_right):
    return _cast_booleans_to_string_df(df_left), _cast_booleans_to_string_df(df_right)


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

    @block_transformer_method
    def ingest_fast(self, *container_path, local_path: str, user: str):
        """Like ``ingest`` but uses ``fast_save_raw_data`` for classification/dedup."""
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Ingest file not found: {local_path}")

        logger.info(f"Starting fast ingestion process for {local_path}")
        data, dest_path = self.copy_ingested_raw_data(
            *container_path, local_path=local_path, user=user, return_dest_path=True
        )
        try:
            self.fast_save_raw_data(
                *container_path,
                data={"source_path": dest_path, "data_json_array": data},
                user=user,
            )
            logger.info("Fast classification/deduplication completed successfully.")
        except Exception as e:
            logger.error(f"Error during fast classification/deduplication: {e}")
            raise
        logger.info("Fast ingestion process completed successfully.")

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
                except ExpatError as e:
                    try:
                        with open(local_path) as f:
                            reader = csv.reader(f)
                            data_list = list(reader)
                            fields = [f.lower() for f in data_list[0]]
                            data = []
                            for item in data_list[1:]:
                                row = {}
                                for i, field in enumerate(fields):
                                    row[field] = item[i]
                                data.append(row)
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
                dest_path = fs_exec.copy_from_local("ingested_files", *container_path,
                                                file_name_dest=fs_exec.date_random_file_name_generator(file_extension),
                                                src_path=local_path, remove_local=remove_local)
            else:
                dest_path = fs_exec.copy_from_local("ingested_files", *container_path, user,
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

    def fast_save_raw_data(self, *container_path, data: dict | list = None, user: str = None, source_path: str = None, **kwargs):
        pass

    def read_raw_data(self, *container_path, user: str = None, **kwargs):
        pass

    def use_schema(self, json_schema: dict):
        self._schema = json_schema
        return self


class NewsExtractionIngestion(PortadaIngestion):
    def __init__(self, builder=None):
        super().__init__(builder=builder)
        self.generate_uid_udf = None

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
                # Identifiquem els textos que ja han arribat de nou
                new_texts = subset.select("parsed_text").distinct()

                existing_df = self.read_json(full_path)
                existing_df = existing_df.localCheckpoint()
                # Recuperem els IDs existents per als textos que ja tenim
                # Creem un mapping de parsed_text -> entry_id (l'ID vell)
                id_mapping = existing_df.select(
                    F.col("parsed_text").alias("old_text"),
                    F.col("entry_id").alias("old_id")
                )

                # 2. Ajuntem el subset amb el mapping per recuperar l'ID si el text coincideix
                subset = subset.join(
                    id_mapping,
                    subset.parsed_text == id_mapping.old_text,
                    how="left"
                ).withColumn(
                    "entry_id",
                    F.coalesce(F.col("old_id"), F.col("entry_id"))  # Si hi ha ID vell, l'usem; si no, mantenim el nou
                ).drop("old_text", "old_id")

                # Del dataframe existent, eliminem els que coincideixen amb els nous
                # "Elimina de existing_df tot el que estigui a new_texts"
                existing_df_filtered = existing_df.join(new_texts, on="parsed_text", how="left_anti")

                # 3. Ara la unió és segura: no hi ha duplicats entre els dos DFs
                # I ens assegurem que el que queda és el contingut del subset
                subset_u, existing_u = _align_for_union_by_name(subset, existing_df_filtered)
                merged_df = subset_u.unionByName(existing_u, allowMissingColumns=True)
                duplicates = subset.count() + existing_df.count() - merged_df.count()
                regs += merged_df.count()
                if duplicates > 0:
                    duplicated_df = existing_df.join(merged_df, on="entry_id", how="left_anti")
                    dup_left = subset.join(
                        duplicated_df.select("parsed_text"), on="parsed_text", how="left"
                    )
                    dup_left, dup_right = _align_for_union_by_name(dup_left, duplicated_df)
                    duplicated_df = dup_left.unionByName(dup_right, allowMissingColumns=True)
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
                self._update_state(*container_path, df=merged_df)
            else:
                regs += subset.count()
                df_list.append(subset)
                self.write_json(full_path, df=subset, mode="overwrite")
                self._update_state(*container_path, df=subset)

        logger.info(f"{regs} entries was saved")

        return df_list

    @data_transformer_method(
        description="Fast vectorized save of ship entries organized by publication metadata.")
    def fast_save_raw_data(
        self,
        *container_path,
        data: dict | list = None,
        user: str = None,
        source_path: str = None,
        **kwargs,
    ):
        """Same contract as ``save_raw_data``, without a Spark job per group.

        Merges against existing raw JSON in one pass, writes all touched partitions
        through a staging directory, then promotes them to the canonical layout
        ``publication/yyyy/mm/dd/edition/``. Updates cleaning state once at the end.
        """
        super().fast_save_raw_data(*container_path, data=data, user=user, **kwargs)
        if data is None:
            raise ValueError("A DataFrame or JSON list must be passed.")
        if not self.is_initialized():
            raise ValueError(
                "PortadaIngestion instance is not initializer. start_spark() method must be called first."
            )

        data_json_array, source_path, tn, source_version = self._parse_save_raw_payload(
            data, source_path
        )
        if len(data_json_array) == 0:
            return []

        df = self._build_incoming_raw_df(
            data_json_array, table_name=tn, source_path=source_path, user=user
        )
        base_path = f"{self._resolve_path(*container_path, process_level_dir=self.raw_subdir)}"
        existing = self._read_all_existing_raw(base_path)
        # Break lineage to on-disk JSON before overwrite/promote deletes those part files.
        # Otherwise later actions on merged/duplicates_df raise SparkFileNotFoundException.
        if existing is not None:
            existing = existing.localCheckpoint(eager=True)

        merged, duplicates_df = self._merge_incoming_with_existing_raw(df, existing)
        merged = merged.persist()
        try:
            regs = merged.count()
            if duplicates_df is not None:
                duplicates_df = duplicates_df.localCheckpoint(eager=True)
            self._write_raw_partitions_fast(merged, base_path)
            self._log_duplicates_batch(
                duplicates_df=duplicates_df,
                source_path=source_path,
                source_version=source_version,
                target_path=base_path,
                uploaded_by=user,
            )
            self._update_state(*container_path, df=merged)
        finally:
            merged.unpersist()

        logger.info("%s entries was saved (fast_save_raw_data)", regs)
        return [merged]

    def _parse_save_raw_payload(self, data: dict | list, source_path: str | None):
        source_version = -1
        if isinstance(data, dict):
            data_json_array = data["data_json_array"]
            source_path = self._resolve_relative_path(data["source_path"])
            p = re.compile(
                f"{self.project_name}/{self._process_level_dirs_[self._current_process_level]}/(.*)"
            )
            tn = re.sub(p, "\\g<1>", source_path, 0)
        else:
            data_json_array = data
            if source_path is None:
                tn = "UNKNOWN"
                source_path = "UNKNOWN"
            else:
                source_path = self._resolve_relative_path(source_path)
                p = re.compile(
                    f"{self.project_name}/{self._process_level_dirs_[self._current_process_level]}/(.*)"
                )
                tn = re.sub(p, "\\g<1>", source_path, 0)
        return data_json_array, source_path, tn, source_version

    def _build_incoming_raw_df(
        self,
        data_json_array: list,
        table_name: str,
        source_path: str,
        user: str | None,
    ) -> TracedDataFrame:
        start_counter = self.get_sequence_value(
            "entry_ships",
            BoatFactDataModel(data_json_array[0])["publication_name"].lower(),
            increment=len(data_json_array),
        )
        df = TracedDataFrame(
            df=self.spark.read.json(
                self.spark.sparkContext.parallelize(
                    [
                        json.dumps(BoatFactDataModel(obj).reformat(i))
                        for i, obj in enumerate(data_json_array, start_counter)
                    ]
                )
            ),
            table_name=table_name,
            df_name=source_path,
        )
        if user is not None:
            df = df.withColumn("uploaded_by", F.lit(user))
        df = df.withColumn("publication_date_value", F.to_date("publication_date", "yyyy-MM-dd"))
        df = df.withColumn("publication_date_year", year(col("publication_date_value")))
        df = df.withColumn("publication_date_month", month(col("publication_date_value")))
        df = df.withColumn("publication_date_day", dayofmonth(col("publication_date_value")))
        return df.drop("publication_date_value")

    @staticmethod
    def _with_raw_group_keys(df):
        return (
            df.withColumn("_gk_pub", F.lower(F.col("publication_name")))
            .withColumn("_gk_y", F.col("publication_date_year").cast("int"))
            .withColumn("_gk_m", F.col("publication_date_month").cast("int"))
            .withColumn("_gk_d", F.col("publication_date_day").cast("int"))
            .withColumn("_gk_ed", F.lower(F.col("publication_edition")))
        )

    def _read_all_existing_raw(self, base_path: str):
        path = os.path.join(base_path, "*", "*", "*", "*", "*", "*.json")
        return self.read_json(path, has_extension=True)

    def _merge_incoming_with_existing_raw(self, incoming_df, existing_df):
        incoming = self._with_raw_group_keys(incoming_df)
        if existing_df is None:
            return incoming.drop(*_RAW_GROUP_KEY_COLS), None

        existing = self._with_raw_group_keys(existing_df)
        group_keys = list(_RAW_GROUP_KEY_COLS)
        batch_groups = incoming.select(*group_keys).distinct()
        existing_in_batch = existing.join(batch_groups, on=group_keys, how="inner")

        id_mapping = existing_in_batch.select(
            F.col("_gk_pub").alias("_map_pub"),
            F.col("_gk_y").alias("_map_y"),
            F.col("_gk_m").alias("_map_m"),
            F.col("_gk_d").alias("_map_d"),
            F.col("_gk_ed").alias("_map_ed"),
            F.col("parsed_text").alias("old_text"),
            F.col("entry_id").alias("old_id"),
        )
        incoming = (
            incoming.join(
                id_mapping,
                on=[
                    incoming["_gk_pub"] == id_mapping["_map_pub"],
                    incoming["_gk_y"] == id_mapping["_map_y"],
                    incoming["_gk_m"] == id_mapping["_map_m"],
                    incoming["_gk_d"] == id_mapping["_map_d"],
                    incoming["_gk_ed"] == id_mapping["_map_ed"],
                    incoming["parsed_text"] == id_mapping["old_text"],
                ],
                how="left",
            )
            .withColumn("entry_id", F.coalesce(F.col("old_id"), F.col("entry_id")))
            .drop(
                "_map_pub",
                "_map_y",
                "_map_m",
                "_map_d",
                "_map_ed",
                "old_text",
                "old_id",
            )
        )

        new_texts = incoming.select(*group_keys, "parsed_text")
        existing_kept = existing_in_batch.join(
            new_texts, on=group_keys + ["parsed_text"], how="left_anti"
        )
        replaced = existing_in_batch.join(
            new_texts, on=group_keys + ["parsed_text"], how="inner"
        )

        incoming_u, existing_u = _align_for_union_by_name(incoming, existing_kept)
        merged = incoming_u.unionByName(existing_u, allowMissingColumns=True)
        return merged.drop(*_RAW_GROUP_KEY_COLS), replaced.drop(*_RAW_GROUP_KEY_COLS)

    def _write_raw_partitions_fast(self, merged_df, base_path: str):
        staging_path = f"{base_path.rstrip('/')}/_fast_ingest_staging"
        fs_ex = self._hadoop_fs_executor()
        if fs_ex.path_exists(staging_path):
            fs_ex.delete(staging_path, recursive=True)

        out = (
            merged_df.withColumn("_pub", F.lower(F.col("publication_name")))
            .withColumn(
                "_y", F.format_string("%04d", F.col("publication_date_year").cast("int"))
            )
            .withColumn(
                "_m", F.format_string("%02d", F.col("publication_date_month").cast("int"))
            )
            .withColumn(
                "_d", F.format_string("%02d", F.col("publication_date_day").cast("int"))
            )
            .withColumn("_ed", F.lower(F.col("publication_edition")))
        )
        (
            out.repartition(*_RAW_PARTITION_COLS)
            .write.mode("overwrite")
            .partitionBy(*_RAW_PARTITION_COLS)
            .json(staging_path)
        )
        self._save_log_storage(out, base_path)
        self._promote_staged_json_partitions(fs_ex, staging_path, base_path)
        if fs_ex.path_exists(staging_path):
            fs_ex.delete(staging_path, recursive=True)

    def _hadoop_fs_executor(self) -> FileSystemTaskExecutor:
        fs_ex = FileSystemTaskExecutor(self.get_configuration())
        if fs_ex.spark is None:
            fs_ex.spark = self.spark
        if fs_ex._jvm is None or fs_ex._fs is None:
            fs_ex._fs = fs_ex._init_fs()
        return fs_ex

    def _promote_staged_json_partitions(
        self, fs_ex: FileSystemTaskExecutor, staging_path: str, base_path: str
    ):
        """Move Spark partition dirs ``_pub=x/_y=yyyy/...`` to canonical ``x/yyyy/...`` via Hadoop FS."""
        leaf_dirs = self._list_staging_leaf_dirs(fs_ex, staging_path)
        staging_prefix = staging_path.rstrip("/") + "/"
        base_prefix = base_path.rstrip("/")

        for leaf in leaf_dirs:
            leaf_norm = leaf.rstrip("/")
            if not leaf_norm.startswith(staging_prefix.rstrip("/")):
                # Handle URI forms where toString() may differ slightly; compare by path tail.
                rel = leaf_norm.split("_fast_ingest_staging/", 1)[-1]
            else:
                rel = leaf_norm[len(staging_prefix) :]

            parts = []
            skip = False
            for segment in rel.split("/"):
                if not segment:
                    continue
                if "=" not in segment:
                    skip = True
                    break
                parts.append(segment.split("=", 1)[1])
            if skip or not parts:
                logger.warning("Skipping unexpected staging path during promote: %s", leaf)
                continue

            dest = f"{base_prefix}/{'/'.join(parts)}"
            parent = dest.rsplit("/", 1)[0]
            fs_ex.mkdirs(parent)
            if fs_ex.path_exists(dest):
                fs_ex.delete(dest, recursive=True)
            if not fs_ex.rename(leaf_norm, dest):
                raise RuntimeError(f"Failed to promote staging partition {leaf_norm} -> {dest}")

    def _list_staging_leaf_dirs(self, fs_ex: FileSystemTaskExecutor, staging_path: str) -> list[str]:
        """Return staging directories that contain Spark ``part-*`` data files."""
        leaves: list[str] = []

        def walk(path_str: str) -> None:
            statuses = fs_ex.list_status(path_str)
            has_part = False
            subdirs: list[str] = []
            for st in statuses:
                name = st.getPath().getName()
                child = st.getPath().toString()
                if st.isDirectory():
                    subdirs.append(child)
                elif name.startswith("part-"):
                    has_part = True
            if has_part:
                leaves.append(path_str.rstrip("/"))
                return
            for sub in subdirs:
                walk(sub)

        if fs_ex.path_exists(staging_path):
            walk(staging_path)
        return leaves

    def _log_duplicates_batch(
        self,
        duplicates_df,
        source_path: str,
        source_version: int,
        target_path: str,
        uploaded_by: str | None,
    ):
        if duplicates_df is None or duplicates_df.limit(1).count() == 0:
            return

        metadata = DataLakeMetadataManager(self.get_configuration())
        dup_base = metadata._resolve_path("metadata/duplicates_records")
        (
            duplicates_df.write.partitionBy(
                "publication_name",
                "publication_date_year",
                "publication_date_month",
                "publication_date_day",
                "publication_edition",
            )
            .mode("append")
            .format(metadata.format)
            .save(dup_base)
        )

        # One duplicates_log row per publication/date/edition so filtering works
        # like DataLakeMetadataManager.log_duplicates.
        group_cols = [
            "publication_name",
            "publication_date_year",
            "publication_date_month",
            "publication_date_day",
            "publication_edition",
        ]
        grouped = (
            duplicates_df.groupBy(*group_cols)
            .agg(
                F.count("*").alias("duplicates"),
                F.collect_list("entry_id").alias("duplicate_ids"),
            )
            .collect()
        )

        rel_source = metadata._resolve_relative_path(source_path)
        rel_target = metadata._resolve_relative_path(target_path)
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        entries = []
        for row in grouped:
            pub = (row["publication_name"] or "").lower()
            year = int(row["publication_date_year"])
            month = int(row["publication_date_month"])
            day = int(row["publication_date_day"])
            edition = (row["publication_edition"] or "").lower()
            dup_ids = list(row["duplicate_ids"] or [])
            # Cap ids sent to the driver log; keep a short sample per partition.
            if len(dup_ids) > 100:
                dup_ids = dup_ids[:100]
            dup_filter = (
                f"lower(publication_name)='{pub}' "
                f"AND publication_date_year={year} "
                f"AND publication_date_month={month} "
                f"AND publication_date_day={day} "
                f"AND lower(publication_edition)='{edition}'"
            )
            entries.append(
                Row(
                    log_id=str(uuid.uuid4()),
                    timestamp=ts,
                    process=self.transformer_name,
                    stage=self.current_process_level,
                    source_path=rel_source,
                    source_version=source_version,
                    target_path=rel_target,
                    target_version=-1,
                    action=DataLakeMetadataManager.DELETE_DUPLICATES_ACTION,
                    publication=pub,
                    date=f"{year:04d}-{month:02d}-{day:02d}",
                    edition=edition,
                    uploaded_by=uploaded_by,
                    duplicates=int(row["duplicates"]),
                    duplicate_ids=dup_ids,
                    duplicates_filter=dup_filter,
                )
            )
        if entries:
            metadata._write_log(entries, "duplicates_log", partitionBy=("publication",))

    def _update_state(self, *container_path, df, key_name: str = "entry_id", value: bool = False):
        return super()._update_state(*container_path, df=df, key_name="entry_id",value=False)


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

@registry_to_portada_builder
class ReviewedEntriesIngestion(PortadaIngestion):
    __first_container_path = "reviewed_entries"
    REVIEWED_ENTRY_TYPES = BoatFactConstants.REVIEWED_ENTRY_TYPES

    def __resolve_container_path(self, *container_path, is_cargo = False):
        second_path = self.REVIEWED_ENTRY_TYPES[1] if is_cargo else self.REVIEWED_ENTRY_TYPES[0]
        if len(container_path) > 0 and (isinstance(container_path[0], list) or isinstance(container_path[0], tuple)):
            container_path = container_path[0]
        if len(container_path) > 0 and isinstance(container_path[0], str) and not container_path[0].startswith(
                self.__first_container_path):
            container_path = list(container_path)
            container_path.insert(0, self.__first_container_path)
        if len(container_path) > 1:
            if isinstance(container_path[1], str) and not container_path[1].startswith(
                second_path):
                container_path = list(container_path)
                container_path.insert(1, second_path)
        elif len(container_path) == 1:
            container_path = list(container_path)
            container_path.insert(1, second_path)
        else:   # len(container_path) == 0
            if is_cargo:
                container_path = (self.__first_container_path,self.REVIEWED_ENTRY_TYPES[1])
            else:
                container_path = (self.__first_container_path,self.REVIEWED_ENTRY_TYPES[0])
        return container_path

    def copy_ingested_raw_data(self, *container_path, local_path: str, return_dest_path=False,
                               remove_local: bool = True, is_cargo = False):
        container_path = self.__resolve_container_path(*container_path, is_cargo=is_cargo)
        # container_path.insert(1, "original_files")
        return super().copy_ingested_raw_data(*container_path, local_path=local_path,
                                              return_dest_path=return_dest_path, remove_local=remove_local)

    def save_raw_data(self, *container_path, data: dict | list = None, source_path: str = None, is_cargo=False, **kwargs):
        super().save_raw_data(*container_path, data=data, **kwargs)
        container_path = self.__resolve_container_path(*container_path, is_cargo=is_cargo)
        if data is None:
            raise ValueError("A DataFrame or JSON list must be passed.")

        if isinstance(data, dict) and "source_path" in data:
            source_path = self._resolve_relative_path(data["source_path"])
            p = re.compile(f"{self.project_name}/{self._process_level_dirs_[self._current_process_level]}/(.*)")
            tn = re.sub(p, "\\g<1>", source_path, 0)
            data = data["data_csv"]
        else:
            data = data
            if source_path is None:
                tn = "UNKNOWN"
                source_path = "UNKNOWN"
            else:
                source_path = self._resolve_relative_path(source_path)
                p = re.compile(f"{self.project_name}/{self._process_level_dirs_[self._current_process_level]}/(.*)")
                tn = re.sub(p, "\\g<1>", source_path, 0)

        length = len(data)
        if length == 0:
            return []

        df = TracedDataFrame(
            df=self.spark.createDataFrame(data),
            table_name=tn,
            df_name=source_path,
        )

        if is_cargo:
            df = df.select(
                "temp_key",
                "cargo_merchant_id",
                "cargo_commodity_id",
                "rev_cargo_merchant_name",
                "rev_cargo_commodity",
                "rev_cargo_unit"
            )
        else:
            df = df.select(
                "temp_key",
                "rev_travel_departure_date",
                "rev_travel_duration_value",
                "rev_travel_duration_unit",
                "rev_travel_departure_port",
                "rev_travel_arrival_date",
                "rev_ship_type",
                "rev_ship_name",
                "rev_ship_tons_capacity",
                "rev_ship_tons_unit",
                "rev_ship_flag",
                "rev_master_role",
                "rev_master_name"
            )

        if not self.is_initialized():
            error_msg = "ReviewedEntriesIngestion instance is not initializer. start_spark() method must be called first."
            logger.error(error_msg)
            raise ValueError(error_msg)

        base_path = f"{self._resolve_path(*container_path)}"
        self.write_delta(base_path, df=df, mode="overwrite")
        return df

    def copy_ingested_reviewed_entries(self,local_path: str, return_dest_path=False, is_cargo=False):
        return self.copy_ingested_raw_data(local_path=local_path,
                                           return_dest_path=return_dest_path, is_cargo=is_cargo)

    @data_transformer_method(
        description="Extract values from original files and save as delta format.")
    def save_raw_reviewed_entries(self, data: dict | list = None, is_cargo=False):
        return self.save_raw_data(data=data, is_cargo=is_cargo)

    def read_raw_data(self, *container_path, is_cargo=False):
        container_path= self.__resolve_container_path(*container_path, is_cargo=is_cargo)
        df = self.read_delta(*container_path)
        return df

    def read_raw_reviewed_entries(self, is_cargo=False):
        return self.read_raw_data(is_cargo=is_cargo)


class KnownEntitiesIngestion(PortadaIngestion):
    __first_container_path = "known_entities"
    FLAG_ENTITY = BoatFactConstants.FLAG_ENTITY
    SHIP_TONS_ENTITY = BoatFactConstants.SHIP_TONS_ENTITY
    TRAVEL_DURATION_ENTITY = BoatFactConstants.TRAVEL_DURATION_ENTITY
    COMODITY_ENTITY = BoatFactConstants.COMMODITY_ENTITY
    SHIP_TYPE_ENTITY = BoatFactConstants.SHIP_TYPE_ENTITY
    UNIT_ENTITY = BoatFactConstants.UNIT_ENTITY
    PORT_ENTITY = BoatFactConstants.PORT_ENTITY
    MASTER_ROLE_ENTITY = BoatFactConstants.MASTER_ROLE_ENTITY


    def __resolve_container_path(self, *container_path):
        if len(container_path) > 0 and (isinstance(container_path[0], list) or isinstance(container_path[0], tuple)):
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
        # container_path.insert(1, "original_files")
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
    def __init__(self, builder=None):
        super().__init__(builder=builder)

    def start_session(self):
        super().start_session()
        patcher = BoatFactPatcherDataLayer(cfg_json=self.get_configuration())
        if self.sequencer_params is not None:
            patcher.set_delta_data_version_manager_params(host=self.sequencer_params["host"], port=self.sequencer_params["port"])
            patcher.patch_if_needed()
        elif not self._use_redis_metadata:
            patcher.patch_if_needed()
        else:
            raise ValueError("No redis params or sequencer params set")

    def ingest(self, *container_path, local_path: str, user: str ):
        if len(container_path)>0:
            cp = container_path
        else:
            cp = (self.__container_path,)
        return super().ingest(*cp, local_path=local_path, user=user)

    def ingest_fast(self, *container_path, local_path: str, user: str):
        if len(container_path) > 0:
            cp = container_path
        else:
            cp = (self.__container_path,)
        return super().ingest_fast(*cp, local_path=local_path, user=user)

    def save_raw_data(self, *container_path, data: dict | list = None, user:str = None, source_path: str = None, **kwargs):
        if len(container_path) > 0:
            cp = container_path
        else:
            cp = (self.__container_path,)
        return super().save_raw_data(*cp, user=user, data=data, source_path=source_path, **kwargs)

    def fast_save_raw_data(
        self,
        *container_path,
        data: dict | list = None,
        user: str = None,
        source_path: str = None,
        **kwargs,
    ):
        if len(container_path) > 0:
            cp = container_path
        else:
            cp = (self.__container_path,)
        return super().fast_save_raw_data(
            *cp, user=user, data=data, source_path=source_path, **kwargs
        )

    def read_raw_data(self, *container_path, publication_name: str = None, y: int | str = None, m: int | str = None, d: int | str = None,
                      edition: str = None, user: str = None, **kwargs):
        if len(container_path) > 0:
            cp = container_path
        else:
            cp = (self.__container_path,)
        return super().read_raw_data(*cp, user=user, publication_name=publication_name, y=y, m=m, d=d,
                                     edition=edition)

    def get_missing_dates_from_a_newspaper(self, *container_path, publication_name: str, start_date: str = None,
                                           end_date: str = None, date_and_edition_list: dict | str = None):
        if len(container_path) > 0:
            cp = container_path
        else:
            cp = (self.__container_path,)
        return super().get_missing_dates_from_a_newspaper(*cp, publication_name=publication_name,
                                                          start_date=start_date, end_date=end_date,
                                                          date_and_edition_list=date_and_edition_list)
