import datetime
import inspect
from enum import Enum

from delta import DeltaTable
from pyspark.sql import Row
from datetime import datetime, UTC
from typing import Optional, List, Dict
import uuid
import logging
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, FloatType, LongType, ArrayType, MapType, \
    BooleanType

from portada_data_layer.traced_data_frame import TracedDataFrame, TRANSFORMING_METHODS
from portada_data_layer.delta_data_layer import PathConfigDeltaDataLayer, BaseDeltaDataLayer, DeltaDataLayer
from functools import wraps

logger = logging.getLogger("delta_data_layer.data_lake_metadata_manager")

class LineageCheckingType(Enum):
    NO_CHECKING = 0
    FIELD = 1
    FIELD_AND_VALUE = 2


def __set_enable_storage_log(func, is_enable, data_layer_key: str, *args, **kwargs):
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    if data_layer_key in bound.arguments:
        data_layer = bound.arguments[data_layer_key]
    else:
        raise Exception(f"Parameter {data_layer_key} is needed")
    previous_log_storage = data_layer.log_storage
    data_layer.log_storage = is_enable
    try:
        resultat = func(*args, **kwargs)
        return resultat
    finally:
        # Restore the previous context after executing the method
        data_layer.log_storage = previous_log_storage


def __set_enable_field_lineage_log(func, is_enable, data_layer_key: str, *args, **kwargs):
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    if data_layer_key in bound.arguments:
        data_layer = bound.arguments[data_layer_key]
    else:
        raise Exception(f"Parameter {data_layer_key} is needed")
    previous_save_lineage_on_store = data_layer._save_lineage_on_store
    data_layer._save_lineage_on_store = is_enable
    try:
        resultat = func(*args, **kwargs)
        return resultat
    finally:
        # Restore the previous context after executing the method
        data_layer._save_lineage_on_store = previous_save_lineage_on_store


def __is_block_transformer(func, data_layer_key: str, *args, ** kwargs):
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    if data_layer_key in bound.arguments:
        data_layer = bound.arguments[data_layer_key]
    else:
        raise Exception(f"Parameter {data_layer_key} is needed")
    previous_block_transformer = data_layer._transformer_block_name
    data_layer._transformer_block_name = func.__name__
    try:
        resultat = func(*args, **kwargs)
        return resultat
    finally:
        # Restore the previous context after executing the method
        data_layer._transformer_block_name = previous_block_transformer


def __is_data_transformer(func, data_layer_key: str=None, dataframe_key: str = "df", description="", *args, **kwargs):
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    if data_layer_key in bound.arguments:
        data_layer = bound.arguments[data_layer_key]
    else:
        data_layer = None

    if dataframe_key in bound.arguments:
        dataframe = bound.arguments[dataframe_key] if isinstance(bound.arguments[dataframe_key], TracedDataFrame) else TracedDataFrame(bound.arguments[dataframe_key], table_name="UNKNOWN")
        dataframes = [dataframe]
    else:
        dataframe = None
        dataframes = []

    inici = datetime.now(UTC)
    resultat = None
    num_records = -1
    estat = "OK"
    func_name = func.__name__
    previous_context = data_layer._transformer_process_name if data_layer else ""
    previous_transformer_name = dataframe._transformer_name if dataframe else ""

    new_context = (
        func_name if not previous_context else f"{previous_context}.{func_name}"
    )
    if data_layer:
        data_layer._transformer_process_name = new_context
    if dataframe:
        dataframe._transformer_name = new_context
        dataframe._transformer_description = description
    try:
        if data_layer:
            data_layer.clean_log_process_info()

        resultat = func(*args, **kwargs)

        if isinstance(resultat, DataFrame):
            dataframes = [TracedDataFrame(resultat, table_name="UNKNOWN", transformer_name=previous_transformer_name)]
            num_records = resultat.count()
        if isinstance(resultat, TracedDataFrame):
            dataframes = [resultat]
            num_records = resultat.count()
        elif isinstance(resultat, (list, tuple)):
            if len(resultat) >0 and isinstance(resultat[0], (DataFrame, TracedDataFrame)):
                dataframes = [r if isinstance(r, TracedDataFrame) else TracedDataFrame(r, table_name="UNKNOWN", transformer_name=previous_transformer_name) for r in resultat]
                num_records =0
                for r in dataframes:
                    num_records += dataframes[0].count()
            else:
                num_records = len(resultat)
        final = datetime.now(UTC)
        durada = (final - inici).total_seconds()

        # Get additional information from the class attribute
        extra_info = data_layer.log_process_info if data_layer else {}

        # Combine explicit values with those in the context
        if "num_records" in extra_info:
            num_records = extra_info.get("num_records")
            del extra_info["num_records"]

        # Invoke the metadata manager
        metadata_manager = None
        if data_layer and hasattr(data_layer, "metadata_manager"):
            metadata_manager = data_layer.metadata_manager
        if not metadata_manager:
            metadata_manager = DataLakeMetadataManager(data_layer.get_configuration())

        extra_info_clean = {
            k: (str(v))
            for k, v in {
                **extra_info,
            }.items()
        }

        # data_lineage_list = None
        # if field_lineage != LineageCheckingType.NO_CHECKING:
        #     for dataframe in dataframes:
        #         data_lineage_list = []
        #         for t in dataframe.transformations:
        #             change_types = []
        #             if len(t["source_dataframes"]) > 1:
        #                 change_types.append("new_dataframe")
        #             if len(t["added_columns"]) > 0:
        #                 change_types.append("add_columns")
        #             if len(t["removed_columns"]) > 0:
        #                 change_types.append("remove_columns")
        #             if t["operation"] in TRANSFORMING_METHODS:
        #                 change_types.append("modify_columns")
        #             data_lineage = {
        #                 "dataframe_name": dataframe.name,
        #                 "change_types": change_types,
        #                 "change_action":t["operation"],
        #                 "arguments": t["arguments"],
        #                 "involved_dataframes": [{"name":tr.name, "large_name":tr.large_name} for tr in t["source_dataframes"]],
        #                 "involved_columns": t["involved_columns"],
        #             }
        #             if field_lineage == LineageCheckingType.FIELD_AND_VALUE:
        #                 # guardar el dataframe per forçar una versió
        #                 p, v = metadata_manager._write_dataframe(df=dataframe)
        #                 saved_values = {
        #                     "container_path": p,
        #                     "version_path": v
        #                 }
        #                 data_lineage["saved_values"] = saved_values
        #             data_lineage_list.append(data_lineage)
        if data_layer:
            metadata_manager.log_process(
                data_layer=data_layer,
                description=description,
                status=estat,
                start_time=str(inici),
                end_time=str(final),
                duration=durada,
                num_records=num_records,
                extra_info=extra_info_clean,
            )
        else:
            metadata_manager.log_process(
                process=new_context,
                description=description,
                status=estat,
                start_time=str(inici),
                end_time=str(final),
                duration=durada,
                num_records=num_records,
                extra_info=extra_info_clean,
            )
        return resultat
    finally:
        # Restore the previous context after executing the method
        if data_layer:
            data_layer.clean_log_process_info(True)
            data_layer._transformer_process_name = previous_context

        for dataframe in dataframes:
            dataframe._transformer_name = previous_transformer_name
            dataframe._transformer_description = ""


# def __process_log_context(func, data_layer_key: str, *args, **kwargs):
#     sig = inspect.signature(func)
#     bound = sig.bind(*args, **kwargs)
#     bound.apply_defaults()
#     if data_layer_key in bound.arguments:
#         data_layer = bound.arguments[data_layer_key]
#     else:
#         raise Exception(f"Parameter {data_layer_key} is needed")
#     func_name = func.__name__
#     previous_context = data_layer.transformer_process_name
#     new_context = (
#         func_name if not previous_context else f"{previous_context}.{func_name}"
#     )
#     data_layer.transformer_process_name = new_context
#     try:
#         resultat = func(*args, **kwargs)
#         return resultat
#     finally:
#         # Restore the previous context after executing the method
#         data_layer.transformer_process_name = previous_context


# def __process_log(func, data_layer_key: str, *args, **kwargs):
#     sig = inspect.signature(func)
#     bound = sig.bind(*args, **kwargs)
#     bound.apply_defaults()
#     if data_layer_key in bound.arguments:
#         data_layer = bound.arguments[data_layer_key]
#     else:
#         raise Exception(f"Parameter {data_layer_key} is needed")
#     inici = datetime.now(UTC)
#     resultat = None
#     num_records = -1
#     estat = "OK"
#     try:
#         data_layer.clean_log_process_info()
#         resultat = func(*args, **kwargs)
#         # If the method returns a DataFrame or similar, you may be able to infer num_records:
#         if isinstance(resultat, DataFrame):
#             num_records = resultat.count()
#         elif isinstance(resultat, (list, tuple)):
#             num_records = len(resultat)
#     except Exception as e:
#         estat = f"ERROR: {e}"
#         raise
#     finally:
#         final = datetime.now(UTC)
#         durada = (final - inici).total_seconds()
#         process_name = data_layer.transformer_process_name
#
#         # Get additional information from the class attribute
#         extra_info = data_layer.log_process_info
#
#         # Combine explicit values with those in the context
#         if "num_records" in extra_info:
#             num_records = extra_info.get("num_records")
#             del extra_info["num_records"]
#
#         # Invoke the metadata manager
#         metadata_manager = None
#         if hasattr(data_layer, "metadata_manager"):
#             metadata_manager = data_layer.metadata_manager
#         if not metadata_manager:
#             metadata_manager = DataLakeMetadataManager(data_layer.get_configuration())
#
#         extra_info_clean = {
#             k: (str(v))
#             for k, v in {
#                 "duration": durada,
#                 "start_time": inici.isoformat(),
#                 "end_time": final.isoformat(),
#                 **extra_info,
#             }.items()
#         }
#         metadata_manager.log_process(
#             data_layer=data_layer,
#             status=estat,
#             num_records=num_records,
#             extra_info=extra_info_clean,
#         )
#     return resultat


def disable_storage_log(data_layer_key: str):
    def decorator(func):
        """
        decorator to disable storage logging
        """
        def wrapper(data_layer: DeltaDataLayer, *args, **kwargs):
            return __set_enable_storage_log(func, False, data_layer_key, *args, **kwargs)

        return wrapper

    return decorator


def disable_storage_log_for_method(func):
    """
    decorator to disable storage logging
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return __set_enable_storage_log(func, False, "self", *args, **kwargs)

    return wrapper


def enable_storage_log(data_layer_key: str):
    def decorator(func):
        """
        decorator to disable storage logging
        """
        def wrapper(data_layer: DeltaDataLayer, *args, **kwargs):
            return __set_enable_storage_log(func, True, data_layer_key, *args, **kwargs)

        return wrapper

    return decorator


def enable_storage_log_for_method(func):
    """
    decorator to enable storage logging
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return __set_enable_storage_log(func, True, "self", *args, **kwargs)

    return wrapper


def enable_storage_log_for_class(cls):
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.log_storage = True

    cls.__init__ = new_init
    return cls


def disable_storage_log_for_class(cls):
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.log_storage = False

    cls.__init__ = new_init
    return cls


def block_transformer(data_layer_key: str):
    def decorator(func):
        """
        Decorator to mark a function as a block transformer method.
       """
        def wrapper(*args, **kwargs):
            return __is_block_transformer(func, data_layer_key, *args, **kwargs)

        return wrapper

    return decorator


def block_transformer_method(func):
    """
   Decorator to mark a method as a block transformer method.
   """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return __is_block_transformer(func, "self", *args, **kwargs)

    return wrapper



def data_transformer(data_layer_key: str, dataframe_key: str = "df", description: str = ""):
    def decorator(func):
        """
       Decorator that automatically logs storage process execution using DataLakeMetadataManager.log_process.
       """
        def wrapper(*args, **kwargs):
            return __is_data_transformer(func, data_layer_key, dataframe_key, description, *args, **kwargs)

        return wrapper

    return decorator


def data_transformer_method(dataframe_key: str = "df", description: str = ""):
    def decorator(func):
        """
       Decorator that automatically logs storage process execution using DataLakeMetadataManager.log_process.
       """
        def wrapper(*args, **kwargs):
            return __is_data_transformer(func, "self", dataframe_key, description, *args, **kwargs)

        return wrapper

    return decorator

def disable_field_lineage_log(data_layer_key: str):
    def decorator(func):
        """
        decorator to disable storage logging
        """
        def wrapper(data_layer: DeltaDataLayer, *args, **kwargs):
            return __set_enable_field_lineage_log(func, False, data_layer_key, *args, **kwargs)

        return wrapper

    return decorator


def disable_field_lineage_log_for_method(func):
    """
    decorator to disable storage logging
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return __set_enable_field_lineage_log(func, False, "self", *args, **kwargs)

    return wrapper


def enable_field_lineage_log(data_layer_key: str):
    def decorator(func):
        """
        decorator to disable storage logging
        """
        def wrapper(data_layer: DeltaDataLayer, *args, **kwargs):
            return __set_enable_field_lineage_log(func, True, data_layer_key, *args, **kwargs)

        return wrapper

    return decorator


def enable_field_lineage_log_for_method(func):
    """
    decorator to enable storage logging
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return __set_enable_storage_log(func, True, "self", *args, **kwargs)

    return wrapper


def enable_field_lineage_log_for_class(cls):
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self._save_lineage_on_store = True

    cls.__init__ = new_init
    return cls


def disable_field_lineage_log_for_class(cls):
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self._save_lineage_on_store = False

    cls.__init__ = new_init
    return cls


class DataLakeMetadataManager(PathConfigDeltaDataLayer):
    DELETE_DUPLICATES_ACTION = "delete_duplicates"
    SOURCE_PATH_PARAM = "df_name"
    TARGET_PATH_PARAM = "target_path"
    NUM_RECORDS_PARAM = "num_records"

    """
    Centralized metadata manager for a Data Lake.
    Includes management of specific metadata (processes, duplicates, lineage, errors) and also automatic reading of internal Delta Lake metadata.
    """

    def __init__(self, config: dict, format: str = "parquet"):
        """
        :param config: dict with config values for PathConfigDeltaDataLayer
        :param format: Output format ("delta" recommended, but can be "json")
        """
        super().__init__(config)
        self.format = format

    # ----------------------------------------------------------------------
    # 🔹 PROCESS LOG
    # ----------------------------------------------------------------------
    def log_storage(
            self,
            data_layer: BaseDeltaDataLayer,
            num_records: int = -1,
            mode: str = None,
            new: bool = True,
            table_name: str = None,
            df_name: str = None,
            df_large_name:str = None,
            source_path: str = None,
            source_version: int = -1,
            target_path: str = None,
            target_version: int = -1,
            extra_info: Optional[Dict] = None,
    ):
        """Records a storage data process."""
        log_id =str(uuid.uuid4())
        entry = Row(
            log_id=log_id,
            timestamp=datetime.now(UTC).isoformat(),
            process=data_layer.transformer_name,
            stage=data_layer.current_process_level,
            table_name=table_name,
            df_name=df_name if df_name is not None else table_name,
            df_large_name=df_large_name,
            source_path=source_path,
            source_version=source_version,
            target_path=target_path,
            target_version=target_version,
            num_records=num_records,
            mode=mode or "",
            new=new,
            extra_info=extra_info,
        )
        schema = StructType([
            StructField("log_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("process", StringType(), False),
            StructField("stage", IntegerType(), False),
            StructField("table_name", StringType(), True),
            StructField("df_name", StringType(), True),
            StructField("df_large_name", StringType(), True),
            StructField("source_path", StringType(), True),
            StructField("source_version", IntegerType(), True),
            StructField("target_path", StringType(), False),
            StructField("target_version", IntegerType(), False),
            StructField("num_records", LongType(), True),
            StructField("mode", StringType(), True),
            StructField("new", BooleanType(), True),
            StructField("extra_info", MapType(StringType(), StringType()), True)
        ])

        self._write_log([entry], "storage_log", schema, ("target_path", "target_version"))
        return log_id

    # ----------------------------------------------------------------------
    # 🔹 PROCESS LOG
    # ----------------------------------------------------------------------
    def log_process(
            self,
            description: str,
            status: str,
            start_time,
            end_time,
            duration,
            num_records: int = -1,
            data_layer: BaseDeltaDataLayer= None,
            extra_info: Optional[Dict] = None,
            process: str = None,
    ):
        """Records a process execution (ingest, cleanup, etc.)."""
        log_id = str(uuid.uuid4())
        entry = Row(
            log_id=log_id,
            timestamp=datetime.now(UTC).isoformat(),
            process=data_layer.transformer_name if data_layer else process,
            stage=data_layer.current_process_level if data_layer else -1,
            description=description,
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            num_records=num_records,
            extra_info=extra_info or {},
        )
        schema = StructType([
            StructField("log_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("process", StringType(), False),
            StructField("stage", IntegerType(), False),
            StructField("description", StringType(), True),
            StructField("status", StringType(), False),
            StructField("start_time", StringType(), False),
            StructField("end_time", StringType(), False),
            StructField("duration", FloatType(), False),
            StructField("num_records", LongType(), True),
            StructField("extra_info", MapType(StringType(), StringType()), True)
        ])
        self._write_log([entry], "process_log", schema=schema, partitionBy=["process"])
        return log_id

    # ----------------------------------------------------------------------
    # 🔹 DUPLICATES LOG
    # ----------------------------------------------------------------------
    def log_duplicates(
            self,
            data_layer: BaseDeltaDataLayer,
            action: str,
            publication: str,
            date: dict,
            edition: str,
            duplicates_df,  # <--- ara rep el DataFrame complet amb els duplicats
            source_path: Optional[str] = None,  # és el fitxer font
            source_version: Optional[int] = -1,
            target_path: Optional[str] = None,  # és el fitxer amb el qual es compara el sourc_epath.
            target_version: Optional[int] = -1,
            uploaded_by: Optional[str] = None,
    ):
        """
        Logs duplicate detection.
        Saves duplicate records as a separate Delta table,
        and creates a summary entry in the main log with their location.
        """
        # # Nom de subdirectori basat en data i publicació
        # safe_pub = publication.replace(" ", "_").lower()
        # dup_base = f"metadata/duplicates_records/{safe_pub}/{date}/{edition}"
        # dup_base = self._resolve_path(dup_base)
        dup_base = self._resolve_path("metadata/duplicates_records")
        dup_filter =f"lower(publication_name)='{publication.lower()}' AND publication_date_year={date['year']} AND publication_date_month={date['month']} AND publication_date_day={date['day']}"

        # Escriu els duplicats com a taula Delta
        duplicates_df.write.partitionBy("publication_name", "publication_date_year", "publication_date_month", "publication_date_day", "publication_edition").mode("append").format(self.format).save(dup_base)

        # Recompte
        num_dups = duplicates_df.count()
        ids = [r["entry_id"] for r in duplicates_df.select("entry_id").collect()]

        # Registra missing_date_list'entrada resum
        log_id = str(uuid.uuid4())
        entry = Row(
            log_id=log_id,
            timestamp=datetime.now(UTC).isoformat(),
            process=data_layer.transformer_name,
            stage=data_layer.current_process_level,
            source_path=self._resolve_relative_path(source_path),
            source_version=source_version,
            target_path=self._resolve_relative_path(target_path),
            target_version=target_version,
            action=action,
            publication=publication.lower(),
            date=f"{date['year']:04d}-{date['month']:02d}-{date['day']:02d}",
            edition=edition.lower(),
            uploaded_by=uploaded_by,
            duplicates=num_dups,
            duplicate_ids=ids,
            duplicates_filter=dup_filter,
        )

        self._write_log([entry], "duplicates_log", partitionBy=("publication",))

    # ----------------------------------------------------------------------
    # 🔹 FIELD LINEAGE
    # ----------------------------------------------------------------------
    def log_field_lineage(
            self,
            data_layer: BaseDeltaDataLayer,
            dataframe: TracedDataFrame,
            stored_log_id: str = None,
    ):
        """Records the origin and transformations of a specific field."""
        entries = []
        transformations = dataframe.transformations
        for i, t in enumerate(transformations):
            change_types = []
            if len(t["source_dataframes"]) > 1:
                change_types.append("new_dataframe")
            if len(t["added_columns"]) > 0:
                change_types.append("add_columns")
            if len(t["removed_columns"]) > 0:
                change_types.append("remove_columns")
            if t["operation"] in TRANSFORMING_METHODS:
                change_types.append("modify_columns")
            df_name = dataframe.get_partial_large_name(i)
            entry = Row(
                log_id= str(uuid.uuid4()),
                timestamp= datetime.now(UTC).isoformat(),
                stored_log_id= stored_log_id,
                transformer_name= dataframe._transformer_name if dataframe._transformer_name else data_layer.transformer_name,
                transformer_description= dataframe._transformer_description,
                stage= data_layer.current_process_level,
                table_name = dataframe.table_name,
                dataframe_name= df_name,
                source_path= self._resolve_relative_path(dataframe.df_name),
                source_version= dataframe.df_version,
                change_types= change_types,
                change_action= t["operation"],
                arguments= t["arguments"],
                involved_dataframes= [{"name": tr.name, "large_name": tr.large_name} for tr in
                                        t["source_dataframes"]],
                involved_columns= t["involved_columns"],
                added_columns= t["added_columns"],
                removed_columns= t["removed_columns"],
            )
            entries.append(entry)
        schema = StructType([
            StructField("log_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("stored_log_id", StringType(), True),
            StructField("transformer_name", StringType(), True),
            StructField("transformer_description", StringType(), True),
            StructField("stage", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("dataframe_name", StringType(), True),
            StructField("source_path", StringType(), True),
            StructField("source_version", StringType(), True),
            StructField("change_types", ArrayType(StringType())),
            StructField("change_action", StringType(), True),
            StructField("arguments", ArrayType(StringType())),
            StructField("involved_dataframes", ArrayType(StructType(
                [StructField("name", StringType(), False), StructField("large_name", StringType(), False)]))),
            StructField("involved_columns", ArrayType(StringType())),
            StructField("added_columns", ArrayType(StringType())),
            StructField("removed_columns", ArrayType(StringType())),
        ])
        self._write_log(entries, "field_lineage_log", schema=schema, partitionBy=("stored_log_id",))

    # ----------------------------------------------------------------------
    # 🔹 ERROR LOG
    # ----------------------------------------------------------------------
    def log_error(
            self,
            process: str,
            stage: int,
            message: str,
            record: Optional[Dict] = None,
            exception_type: Optional[str] = None,
    ):
        """Record an error or problematic record."""
        entry = Row(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC).isoformat(),
            process=process,
            stage=stage,
            message=message,
            record=record or {},
            exception_type=exception_type
        )

        self._write_log([entry], "error_log")

    # def _write_dataframe(self, df: TracedDataFrame):
    #     """Common logging to the Data Lake."""
    #     if df.df_version==-1:
    #         n = df.name.replace("/*", "").replace("/", "___DIR_SEP___").replace(".", "___EXT___")
    #         target_path = self._resolve_path("metadata", "values", n)
    #     else:
    #         target_path = self._resolve_path(df.df_name)
    #     df.write.mode("overwrite").format("delta").save(target_path)
    #     v = DeltaTable.forPath(self.spark, target_path).history(1).collect()[0]['version']
    #     return self._resolve_relative_path(target_path), v

    # ----------------------------------------------------------------------
    # 🔹 ESCRIPTURA GENERAL
    # ----------------------------------------------------------------------
    def _write_log(self, rows: List[Row], log_type: str, schema: StructType=None, partitionBy=None):
        """Common logging to the Data Lake."""
        if schema is None:
            df = self.spark.createDataFrame(rows)
        else:
            df = self.spark.createDataFrame(rows, schema=schema)
        target_path = self._resolve_path("metadata", log_type)
        if partitionBy:
            df.write.partitionBy(*partitionBy).mode("append").format(self.format).save(target_path)
        else:
            df.write.mode("append").format(self.format).save(target_path)


    # # ----------------------------------------------------------------------
    # # 🔹 CONSULTA DE LOGS
    # # ----------------------------------------------------------------------
    # def read_all_logs(self, include_types: list[str] | None = None):
    #     """
    #     Read and unify JSON logs from HDFS, optionally filtering by log_type.
    #     """
    #     # if not table_path:
    #     #     table_path = data_layer.
    #     path_pattern = self._resolve_path("/metadata/*")
    #     df = self.spark.read.json(path_pattern)
    #
    #     # Afegeix el tipus de log segons la ruta
    #     df = df.withColumn(
    #         "log_type",
    #         F.regexp_extract(F.input_file_name(), r"/([^/]+)/[^/]+\.json", 1)
    #     )
    #
    #     # Filtra si cal
    #     if include_types:
    #         df = df.filter(F.col("log_type").isin(include_types))
    #
    #     # Ordena cronològicament si hi ha timestamp
    #     if "timestamp" in df.columns:
    #         df = df.orderBy("timestamp")
    #
    #     return df

    # ----------------------------------------------------------------------
    # 🔹 CONSULTA DE LOGS
    # ----------------------------------------------------------------------
    def read_log(self, log_type: str | dict, include_duplicates: bool = False):
        """
        Reads a log (process_log, duplicates_log, etc.).
        If log_type == "duplicates_log" and include_duplicates=True,
        also loads the associated duplicate records from the referenced Delta tables.
        """

        path = self._resolve_path("metadata", log_type)
        df_log = self.spark.read.format(self.format).load(path)

        # Si no es demanen duplicats o no és el log adequat, retornem directament
        if log_type != "duplicates_log" or not include_duplicates:
            if "timestamp" in df_log.columns:
                df_log = df_log.orderBy("timestamp")
            elif "parsed_text_value" in df_log.columns:
                df_log = df_log.orderBy("parsed_text_value")
            return df_log

        # ------------------------------------------------------------------
        # Cas especial: duplicates_log + include_duplicates = True
        # ------------------------------------------------------------------
        path = self._resolve_path("metadata", "duplicates_records")
        duplicates_df = self.spark.read.format(self.format).load(path).orderBy("parsed_text_value")

        # duplicates_df = self.spark.createDataFrame([], StructType([StructField("log_id", StringType(), False)]))
        #
        # for row in df_log.collect():
        #     log_id = "unknown"
        #     try:
        #         log_id = row["log_id"]
        #         dup_path = row["duplicates_path"]
        #
        #         if dup_path and dup_path.strip():
        #             dup_path = self._resolve_path(dup_path, contains_project_name=True)
        #             dup_df = self.spark.read.json(dup_path)
        #             dup_df = dup_df.withColumn("log_id", F.lit(log_id))
        #             duplicates_df = duplicates_df.unionByName(dup_df, allowMissingColumns=True)
        #     except Exception as e:
        #         logger.error(f"Error loading duplicates for log_id={log_id}: {e}")

        return df_log, duplicates_df

    def get_duplicates_from_path(self, dup_path):
        dup_path = self._resolve_path(dup_path, contains_project_name=True)
        dup_df = self.spark.read.json(dup_path)
        return dup_df

    # ----------------------------------------------------------------------
    # 🔹 LLEGIR METADADES DE DELTA LAKE
    # ----------------------------------------------------------------------
    def read_delta_metadata(self, delta_path: str):
        """
        Reads the internal metadata of a Delta Lake table.
        Returns a dictionary with:
        - "detail": table schema and properties
        - "history": operation history (up to last 20 by default)
        """
        try:
            detail_df = self.spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`")
            history_df = self.spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")

            detail = detail_df.collect()[0].asDict()
            history = [r.asDict() for r in history_df.collect()]

            return {
                "detail": detail,
                "history": history
            }

        except Exception as e:
            logger.error(f"Error reading Delta metadata from {delta_path}: {e}")
            return None
