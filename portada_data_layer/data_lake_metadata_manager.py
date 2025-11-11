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
        resultat = func(data_layer, *args, **kwargs)
        return resultat
    finally:
        # Restore the previous context after executing the method
        data_layer.log_storage = previous_log_storage

def disable_storage_log(data_layer_key: str):
    def decorator(func):
        """
        decorator to disable storage logging
        """
        @wraps(func)
        def wrapper(data_layer:DeltaDataLayer, *args, **kwargs):
            return __set_enable_storage_log(func, False, data_layer_key=data_layer_key, *args, **kwargs)
        return wrapper
    return decorator

def disable_storage_log_for_data_layer_class(func):
    """
    decorator to disable storage logging
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return __set_enable_storage_log(func, False, data_layer_key="self", *args, **kwargs)
    return wrapper

def enable_storage_log_for_data_layer_class(func):
    """
    decorator to enable storage logging
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return __set_enable_storage_log(func, True, data_layer_key="self", *args, **kwargs)
    return wrapper

def process_log_context_for_data_layer_class(func):
    """Updates the context attribute with the hierarchical name of the process."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        func_name = func.__name__
        previous_context = self.process_context
        new_context = (
            func_name if not previous_context else f"{previous_context}.{func_name}"
        )
        self.process_context = new_context
        try:
            resultat = func(self, *args, **kwargs)
            return resultat
        finally:
            # Restore the previous context after executing the method
            self.process_context = previous_context
    return wrapper

def process_log_for_data_layer_class(func):
    """
    Decorator that automatically logs storage process execution using DataLakeMetadataManager.log_process.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        inici = datetime.now(UTC)
        resultat = None
        num_records = -1
        estat = "OK"
        try:
            self.clean_log_process_info()
            resultat = func(self, *args, **kwargs)
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
            process_name = self.process_context

            # Get additional information from the class attribute
            extra_info = self.log_process_info

            # Combine explicit values with those in the context
            if "num_records" in extra_info:
                num_records = extra_info.get("num_records")
                del extra_info["num_records"]

            # Invoke the metadata manager
            metadata_manager = None
            if hasattr(self, "metadata_manager"):
                metadata_manager = self.metadata_manager
            if not metadata_manager:
                metadata_manager = DataLakeMetadataManager(self.get_configuration())

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
                data_layer=self,
                status=estat,
                num_records=num_records,
                extra_info=extra_info_clean,
            )
        return resultat
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
        """Registra una execució de procés (ingesta, neteja, etc.)."""
        entry = Row(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC).isoformat(),
            process=data_layer.process_context,
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
        """Registra una execució de procés (ingesta, neteja, etc.)."""
        entry = Row(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC).isoformat(),
            process=data_layer.process_context,
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
        Registra la detecció de duplicats.
        Desa els registres duplicats com a taula Delta a part,
        i crea una entrada resum al log principal amb la seva ubicació.
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
            process=data_layer.process_context,
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
        """Registra l'origen i transformacions d'un camp concret."""
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
        """Registra un error o registre problemàtic."""
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
        """Escriptura comuna de logs al Data Lake."""
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
        Llegeix un log (process_log, duplicates_log, etc.).
        Si log_type == "duplicates_log" i include_duplicates=True,
        també carrega els registres duplicats associats de les taules Delta referenciades.
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
        Llegeix les metadades internes d'una taula Delta Lake.
        Retorna un diccionari amb:
          - "detail": esquema i propietats de la taula
          - "history": historial d'operacions (fins a 20 últimes per defecte)
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