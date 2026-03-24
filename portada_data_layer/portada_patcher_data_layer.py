"""
Mòdul Portada Patcher Data Layer: capa per aplicar patches o correccions sobre dades.
"""

import logging
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, month, dayofmonth

from portada_data_layer.delta_data_layer import DeltaDataLayer
from portada_data_layer.portada_delta_common import registry_to_portada_builder
from delta.tables import DeltaTable
import redis


logger = logging.getLogger("portada_data.delta_data_layer.portada_patcher")


class RadisDeltaDataLayerVersionManager:
    __version_key = "delta_data_layer_version"
    def __init__(self, host, port, db=3):
        self.client = redis.Redis(host=host, port=port, decode_responses=True, db=db)

    def get_version(self, table_name: str):
        return self.client.get(f"{self.__version_key}:{table_name}")

    def set_version(self, table_name: str, version: str):
        self.client.set(f"{self.__version_key}:{table_name}", version)

    def ping(self):
        try:
            return self.client.ping()
        except Exception:
            return False

class PatchError(Exception):
    def __init__(self, message: str="PatchE error setting version"):
        self.__message = message
        super().__init__(message)

    def get_message(self):
        return self.__message


class PatchErrorSettingVersion(PatchError):
    def __init__(self, error: Exception=None, version: str="PatchE error setting version"):
        self.version = version
        self.error = error
        super().__init__(f"Error setting version {version}: {error}")


class PortadaPatcherDataLayer(DeltaDataLayer):
    __delta_data_version__ = "v1"
    """Capa per aplicar patches o correccions sobre dataframes segons regles o schema."""

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self._current_process_level = 1
        self._client_db_state_manager = None
        self.delta_data_version_manager_params = None

    def set_delta_data_version_manager_params(self, host: str = None, port: str = None, db: int = 3):
        if host is None:
            host = self.sequencer_params["host"]
        if port is None:
            port = self.sequencer_params["port"]
        self.delta_data_version_manager_params = {"host": host, "port": port, "db": db}
        return self

    def delta_data_version_manager(self):
        if self._client_db_state_manager is None:
            if self.delta_data_version_manager_params is None:
                if self.sequencer_params is None:
                    return None
                else:
                    host = self.sequencer_params["host"]
                    port = self.sequencer_params["port"]
                    db = self.sequencer_params["db"]
                    self._client_db_state_manager = RadisDeltaDataLayerVersionManager(host, port)
            else:
                host = self.delta_data_version_manager_params["host"]
                port = self.delta_data_version_manager_params["port"]
                db = self.delta_data_version_manager_params["db"]
                self._client_db_state_manager = RadisDeltaDataLayerVersionManager(host, port, db)
        if self._client_db_state_manager.ping():
            return self._client_db_state_manager
        return None

    def get_last_data_version_value(self):
        ddvm = self.delta_data_version_manager()
        if ddvm is None:
            ret =  self._get_delta_data_lastversion("ship_entries")
        else:
            ret = ddvm.get_version("ship_entries")
        return ret or "v0"
    
    def set_data_version_value(self, version: str):
        ddvm = self.delta_data_version_manager()
        try:
            if ddvm is None:
                return self._set_delta_data_lastversion("ship_entries", version)
            else:
                return self._client_db_state_manager.set_version("ship_entries", version)
        except Exception as e:
            raise PatchErrorSettingVersion(e, version)

    def _get_delta_data_lastversion(self, table_name: str):
        path = self._resolve_path(table_name, process_level_dir="data_versions")
        
        if self.path_exists(table_name, process_level_dir="data_versions"):
            deltaTable = DeltaTable.forPath(self.spark, path)
            current_value = deltaTable.toDF().first()["value"]
        else:
            current_value = None
        return current_value

    def _set_delta_data_lastversion(self, table_name: str, version: str):
        path = self._resolve_path(table_name, process_level_dir="data_versions")
        if self.path_exists(table_name, process_level_dir="data_versions"):
            deltaTable = DeltaTable.forPath(self.spark, path)
            deltaTable.update(set={"value": version})
        else:
            df = self.spark.createDataFrame([(version,)], ["value"])
            df.write.format("delta").mode("overwrite").save(path)

    def patch_if_needed(self):
        self.start_session()
        last_data_version = self.get_last_data_version_value()
        if last_data_version != self.__delta_data_version__:
            last_num_version = int(last_data_version[1:])
            current_num_version = int(self.__delta_data_version__[1:])
            if last_num_version < current_num_version:
                for i in range(last_num_version, current_num_version):
                    try:
                        to_restore =getattr(self, f"_patch_v{i}_to_v{i+1}")()
                        try:
                            self.set_data_version_value(f"v{i+1}")
                        except PatchErrorSettingVersion as e:
                            logger.error(f"Error setting version {i}: {e}")
                            self.restore_data_version_value(to_restore)
                            raise e
                    except Exception as e:
                        logger.error(f"Error applying patch {i}: {e}")
                        raise e

    def restore_data_version_value(self, to_restore: dict):
        if to_restore is None:
            return
        elif to_restore["table_type"] == "delta" or to_restore["table_type"] == "parquet":
           delta_table = DeltaTable.forPath(self.spark, to_restore["table_path"])
           delta_table.restoreToVersion(to_restore["version_to_restore"])


@registry_to_portada_builder
class BoatFactPatcherDataLayer(PortadaPatcherDataLayer):
    __container_path = "ship_entries"

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self._patch_functions = [
            self._patch_v0_to_v1, #0
        ]

    def _patch_v0_to_v1(self):
        if not self.is_initialized():
            error_msg = "BoatFactPatcherDataLayer instance is not initialized. start_spark() method must be called first."
            logger.error(error_msg)
            raise ValueError(error_msg)

        base_path = f"{self._resolve_path(self.__container_path, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])}"
        base_dir = os.path.join(base_path, "*", "*", "*", "*", "*")
        path = os.path.join(base_dir, "*.json")
        df_original = self.read_json(path, has_extension=True)
        if df_original is None:
            return None

        df = df_original
        df = df.withColumn("publication_date_value", F.to_date("publication_date", "yyyy-MM-dd"))
        df = df.withColumn("publication_date_year", year(col("publication_date_value")))
        df = df.withColumn("publication_date_month", month(col("publication_date_value")))
        df = df.withColumn("publication_date_day", dayofmonth(col("publication_date_value")))
        df = df.drop("publication_date_value")

        # Materialitzem el DataFrame per desacoplar-lo dels fitxers originals; en sobreescriure
        # durant el bucle, les rutes deixarien d'existir i Spark llançaria SparkFileNotFoundException.
        df = df.localCheckpoint()

        to_update_groups = df.filter(F.length("entry_id") < 13) \
            .select("publication_name", "publication_date_year", "publication_date_month", "publication_date_day", "publication_edition") \
            .distinct().collect()

        df = df.withColumn(
            "entry_id",
            F.concat(
                F.split(F.col("entry_id"), "_")[0],
                F.lit("_"),
                F.lpad(F.split(F.col("entry_id"), "_")[1], 10, "0")
            )
        )

        for row in to_update_groups:
            # Construcció de ruta (més neta amb f-strings)
            full_path = os.path.join(
                base_path,
                row["publication_name"].lower(),
                f"{row['publication_date_year']:04d}",
                f"{row['publication_date_month']:02d}",
                f"{row['publication_date_day']:02d}",
                row["publication_edition"].lower()
            )

            subset = df.filter(
                (F.col("publication_name") == row["publication_name"]) &
                (F.col("publication_date_year") == row["publication_date_year"]) &
                (F.col("publication_date_month") == row["publication_date_month"]) &
                (F.col("publication_date_day") == row["publication_date_day"]) &
                (F.col("publication_edition") == row["publication_edition"])
            )
            subset.write.mode("overwrite").json(full_path)

        # grouped = df.select(
        #     "publication_name",
        #     "publication_date_year",
        #     "publication_date_month",
        #     "publication_date_day",
        #     "publication_edition"
        # ).distinct().orderBy("publication_name","publication_date_year", "publication_date_month", "publication_date_day", "publication_edition")
        #
        # base_path = f"{self._resolve_path(self.__container_path, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])}"
        # for row in grouped.collect():
        #     pub_name = row["publication_name"]
        #     year_ = row["publication_date_year"]
        #     month_ = row["publication_date_month"]
        #     day_ = row["publication_date_day"]
        #     edition = row["publication_edition"]
        #     full_path = os.path.join(base_path, pub_name.lower(), f"{year_:04d}", f"{month_:02d}", f"{day_:02d}", edition.lower())
        #
        #     subset = df.filter(
        #         (col("publication_name") == pub_name) &
        #         (col("publication_date_year") == year_) &
        #         (col("publication_date_month") == month_) &
        #         (col("publication_date_day") == day_) &
        #         (col("publication_edition") == edition)
        #     )
        #     subset_original = df_original.filter(
        #         (F.length(col("entry_id"))<13) &
        #         (col("publication_name") == pub_name) &
        #         (col("publication_date_year") == year_) &
        #         (col("publication_date_month") == month_) &
        #         (col("publication_date_day") == day_) &
        #         (col("publication_edition") == edition)
        #     )
        #     if subset_original.count() > 0:
        #         subset.write.mode("overwrite").json(full_path)
            
        return None

       
        
