import inspect
from pyspark.sql import Row
from datetime import datetime, UTC
from typing import Optional, List, Dict
import uuid
import logging
from pyspark.sql import functions as F

from pyspark.sql.dataframe import DataFrame
from portada_data_layer.delta_data_layer import PathConfigDeltaDataLayer, BaseDeltaDataLayer, DeltaDataLayer
from functools import wraps

logger = logging.getLogger("delta_data_layer.data_lake_metadata_manager")


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

def __process_log_context(func, data_layer_key: str, *args, **kwargs):
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    if data_layer_key in bound.arguments:
        data_layer = bound.arguments[data_layer_key]
    else:
        raise Exception(f"Parameter {data_layer_key} is needed")
    func_name = func.__name__
    previous_context = data_layer.process_context_name
    new_context = (
        func_name if not previous_context else f"{previous_context}.{func_name}"
    )
    data_layer.process_context_name = new_context
    try:
        resultat = func(data_layer, *args, **kwargs)
        return resultat
    finally:
        # Restore the previous context after executing the method
        data_layer.process_context_name = previous_context


def __process_log(func, data_layer_key: str, *args, **kwargs):
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    if data_layer_key in bound.arguments:
        data_layer = bound.arguments[data_layer_key]
    else:
        raise Exception(f"Parameter {data_layer_key} is needed")
    inici = datetime.now(UTC)
    resultat = None
    num_records = -1
    estat = "OK"
    try:
        data_layer.clean_log_process_info()
        resultat = func(data_layer, *args, **kwargs)
        # If the method returns a DataFrame or similar, you may be able to infer num_records:
        if isinstance(resultat, DataFrame):
            num_records = resultat.count()
        elif isinstance(resultat, (list, tuple)):
            num_records = len(resultat)
    except Exception as e:
        estat = f"ERROR: {e}"
        raise
    finally:
        final = datetime.now(UTC)
        durada = (final - inici).total_seconds()
        process_name = data_layer.process_context_name

        # Get additional information from the class attribute
        extra_info = data_layer.log_process_info

        # Combine explicit values with those in the context
        if "num_records" in extra_info:
            num_records = extra_info.get("num_records")
            del extra_info["num_records"]

        # Invoke the metadata manager
        metadata_manager = None
        if hasattr(data_layer, "metadata_manager"):
            metadata_manager = data_layer.metadata_manager
        if not metadata_manager:
            metadata_manager = DataLakeMetadataManager(data_layer.get_configuration())

        extra_info_clean = {
            k: (str(v))
            for k, v in {
                "duration": durada,
                "start_time": inici.isoformat(),
                "end_time": final.isoformat(),
                **extra_info,
            }.items()
        }
        metadata_manager.log_process(
            data_layer=data_layer,
            status=estat,
            num_records=num_records,
            extra_info=extra_info_clean,
        )
    return resultat


def disable_storage_log(data_layer_key: str):
    def decorator(func):
        """
        decorator to disable storage logging
        """
        @wraps(func)
        def wrapper(data_layer:DeltaDataLayer, *args, **kwargs):
            return __set_enable_storage_log(func, False, data_layer_key, *args, **kwargs)
        return wrapper
    return decorator


def disable_storage_log_for_method(func):
    """
    decorator to disable storage logging
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return __set_enable_storage_log(func, False, "data_layer", *args, **kwargs)
    return wrapper


def enable_storage_log(data_layer_key: str):
    def decorator(func):
        """
        decorator to disable storage logging
        """
        @wraps(func)
        def wrapper(data_layer:DeltaDataLayer, *args, **kwargs):
            return __set_enable_storage_log(func, True, data_layer_key, *args, **kwargs)
        return wrapper
    return decorator


def enable_storage_log_for_method(func):
    """
    decorator to enable storage logging
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return __set_enable_storage_log(func, True, "data_layer", *args, **kwargs)
    return wrapper


def enable_storage_log_for_class(cls):
    original_init = cls.__init__
    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.log_storage = True

    cls.__init__ = new_init
    return cls

def process_log_context(data_layer_key: str):
    def decorator(func):
        """Updates the context attribute with the hierarchical name of the process."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            return __process_log_context(func, data_layer_key, *args, **kwargs)
        return wrapper
    return decorator


def process_log_context_for_method(func):
    """Updates the context attribute with the hierarchical name of the process."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        return __process_log_context(func, "data_layer", *args, **kwargs)
    return wrapper


def process_log_for(data_layer_key: str):
    def decorator(func):
       """
       Decorator that automatically logs storage process execution using DataLakeMetadataManager.log_process.
       """
       @wraps(func)
       def wrapper(*args, **kwargs):
            return __process_log(func, data_layer_key, *args, **kwargs)
       return wrapper
    return decorator


def process_log_for_method(func):
    """
    Decorator that automatically logs storage process execution using DataLakeMetadataManager.log_process.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return __process_log(func, "self", *args, **kwargs)
    return wrapper

class DataLakeMetadataManager(PathConfigDeltaDataLayer):
    DELETE_DUPLICATES_ACTION="delete_duplicates"
    SOURCE_PATH_PARAM= "source_path"
    TARGET_PATH_PARAM= "target_path"
    NUM_RECORDS_PARAM="num_records"

    """
    Centralized metadata manager for a Data Lake.
    Includes management of specific metadata (processes, duplicates, lineage, errors) and also automatic reading of internal Delta Lake metadata.
    """

    def __init__(self, config: dict, format: str = "json"):
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
        source_path: str = None,
        source_version: int = -1,
        target_path: str = None,
        target_version: int = -1,
    ):
        """Records a storage data process."""
        entry = Row(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC).isoformat(),
            process=data_layer.process_context_name,
            stage=data_layer.current_process_level,
            num_records=num_records,
            source_path=source_path,
            source_version=source_version,
            target_path=target_path,
            target_version=target_version,
        )

        self._write_log([entry], "storage_log")

    # ----------------------------------------------------------------------
    # 🔹 PROCESS LOG
    # ----------------------------------------------------------------------
    def log_process(
        self,
        data_layer: BaseDeltaDataLayer,
        status: str,
        num_records: int = -1,
        extra_info: Optional[Dict] = None,
    ):
        """Records a process execution (ingest, cleanup, etc.)."""
        entry = Row(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC).isoformat(),
            process=data_layer.process_context_name,
            stage=data_layer.current_process_level,
            status=status,
            num_records=num_records,
            extra_info=extra_info or {},
        )

        self._write_log([entry], "process_log")

    # ----------------------------------------------------------------------
    # 🔹 DUPLICATES LOG
    # ----------------------------------------------------------------------
    def log_duplicates(
            self,
            data_layer: BaseDeltaDataLayer,
            action: str,
            publication: str,
            date: str,
            edition: str,
            duplicates_df,  # <--- ara rep el DataFrame complet amb els duplicats
            from_table_full_path: Optional[str] = None,
    ):
        """
        Logs duplicate detection.
        Saves duplicate records as a separate Delta table,
        and creates a summary entry in the main log with their location.
        """
        # Nom de subdirectori basat en data i publicació
        safe_pub = publication.replace(" ", "_").lower()
        dup_base = f"metadata/duplicates_records/{safe_pub}/{date}/{edition}"
        dup_base =  self._resolve_path(dup_base)

        # Escriu els duplicats com a taula Delta
        duplicates_df.write.mode("overwrite").format("json").save(dup_base)

        # Recompte
        num_dups = duplicates_df.count()
        ids = [r["entry_id"] for r in duplicates_df.select("entry_id").collect()]

        # Registra l'entrada resum
        entry = Row(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC).isoformat(),
            process=data_layer.process_context_name,
            stage=data_layer.current_process_level,
            action=action,
            publication=publication,
            date=date,
            edition=edition,
            duplicates=num_dups,
            duplicate_ids=ids,
            duplicates_path=dup_base,
            source_path=from_table_full_path,
        )

        self._write_log([entry], "duplicates_log")

    # ----------------------------------------------------------------------
    # 🔹 FIELD LINEAGE
    # ----------------------------------------------------------------------
    def log_field_lineage(
        self,
        entry_id: str,
        table_path: str,
        field_name: str,
        final_value: str,
        previous_values: Optional[List[str]] = None,
        process: Optional[str] = None,
        extra_info: Optional[dict] = None,
    ):
        """Records the origin and transformations of a specific field."""
        entry = Row(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC).isoformat(),
            entry_id=entry_id,
            field_name=field_name,
            final_value=final_value,
            previous_values=previous_values or [],
            process=process,
            extra_info= extra_info or {}
        )

        self._write_log([entry], "field_lineage")

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

    # ----------------------------------------------------------------------
    # 🔹 ESCRIPTURA GENERAL
    # ----------------------------------------------------------------------
    def _write_log(self, rows: List[Row], log_type: str):
        """Common logging to the Data Lake."""
        df = self.spark.createDataFrame(rows)
        target_path = self._resolve_path("metadata", log_type)
        df.write.mode("append").format(self.format).save(target_path)

    # ----------------------------------------------------------------------
    # 🔹 CONSULTA DE LOGS
    # ----------------------------------------------------------------------
    def read_all_logs(self, include_types: list[str] | None = None):
        """
        Read and unify JSON logs from HDFS, optionally filtering by log_type.
        """
        # if not table_path:
        #     table_path = data_layer.
        path_pattern =self._resolve_path("/metadata/*")
        df = self.spark.read.json(path_pattern)

        # Afegeix el tipus de log segons la ruta
        df = df.withColumn(
            "log_type",
            F.regexp_extract(F.input_file_name(), r"/([^/]+)/[^/]+\.json", 1)
        )

        # Filtra si cal
        if include_types:
            df = df.filter(F.col("log_type").isin(include_types))

        # Ordena cronològicament si hi ha timestamp
        if "timestamp" in df.columns:
            df = df.orderBy("timestamp")

        return df

    # ----------------------------------------------------------------------
    # 🔹 CONSULTA DE LOGS
    # ----------------------------------------------------------------------
    def read_log(self, log_type: str | dict, include_duplicates: bool = False):
        """
        Reads a log (process_log, duplicates_log, etc.).
        If log_type == "duplicates_log" and include_duplicates=True,
        also loads the associated duplicate records from the referenced Delta tables.
        """

        path =  self._resolve_path("metadata", log_type)
        df_log = self.spark.read.format(self.format).load(path)

        # Si no es demanen duplicats o no és el log adequat, retornem directament
        if log_type != "duplicates_log" or not include_duplicates:
            return df_log

        # ------------------------------------------------------------------
        # Cas especial: duplicates_log + include_duplicates = True
        # ------------------------------------------------------------------
        duplicates_map = {}

        for row in df_log.collect():
            log_id="unknown"
            try:
                log_id = row["log_id"]
                dup_path = row.get("duplicates_path")

                if dup_path and dup_path.strip():
                    dup_df = self.spark.read.format("delta").load(dup_path)
                    duplicates_map[log_id] = dup_df
            except Exception as e:
                logger.error(f"Error loading duplicates for log_id={log_id}: {e}")

        return {
            "log": df_log,
            "duplicates": duplicates_map
        }

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