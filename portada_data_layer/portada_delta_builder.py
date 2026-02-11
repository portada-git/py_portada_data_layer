import importlib
import json
import os

from pyspark.sql import SparkSession

from portada_data_layer.delta_data_layer import DeltaDataLayer, ConfigDeltaDataLayer
from portada_data_layer.portada_ingestion import PortadaIngestion
from portada_data_layer.portada_delta_common import PortadaDeltaConstants
from portada_data_layer.portada_ingestion import NewsExtractionIngestion, BoatFactIngestion, KnownEntitiesIngestion


# ==============================================================
# BUILDER: AbstractDeltaDataLayerBuilder
# ==============================================================
class AbstractDeltaDataLayerBuilder(PortadaDeltaConstants):
    """
    Builder to configure Spark + Delta Lake with a fluid and flexible API.
    Example:
        builder = (
            DeltaDataLayerBuilder()
            .protocol("hdfs://localhost:9000")
            .table_path("/datalake/bronze")
            .app_name("MyDeltaApp")
            .config("spark.sql.shuffle.partitions", "8")
        )
        layer = builder.build()
        layer.open_spark()
        ...
        layer.close()
    """
    def __init__(self, json_config=None):
        self._process_level = -1
        fc = "config/cfg.json"
        if json_config is None:
            if os.path.exists(fc):
                with open(fc) as json_cfg_file:
                    json_config = json.load(json_cfg_file)
            else:
                json_config = {}

        self._protocol = "file://"
        self._base_path = self.DEFAULT_BASE_PATH
        self._project_data_name = self.DEFAULT_PROJECT_DATA_NAME
        self._app_name = "DeltaLayerLib"
        self._master = "local[*]"
        self._configs = {}
        self._raw_subdir = self.DEFAULT_RAW_SUBDIR
        self._to_clean_subdir = self.DEFAULT_TO_CLEAN_SUBDIR
        self._to_curate_subdir = self.DEFAULT_TO_CURATE_SUBDIR
        self._curated_subdir = self.DEFAULT_CURATED_SUBDIR
        self._transformer_block_name = ""

        if "configs" in json_config:
            for c in json_config["configs"]:
                k = next(iter(c))
                self._configs[k]=c[k]

        for key, value in json_config.items():
            if key == "configs":
                continue
            self.config(key, value)

    def get_spark_builder(self) -> SparkSession.Builder:
        """Creates and returns a configured SparkSession.Builder."""
        builder = (
            SparkSession.builder.appName(self._app_name).master(self._master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        if self._protocol.startswith("hdfs://"):
            builder = (
                builder
                .config("spark.hadoop.fs.defaultFS", self._protocol)
                .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
            )

        for k, v in self._configs.items():
            builder = builder.config(k, v)

        return builder

    def build(self) -> "DeltaDataLayer":
        """Constructs and returns a DeltaDataLayer initialized with this constructor."""
        pass

    def config(self, key: str, value: str):
        """
        Add a generic configuration using key and value. This method supports all keys to configure spark, which must
        start with 'spark.'. Also supports attributes as "protocol", "app_name", "master" or "table_path".
        """
        if key.startswith("spark."):
            self._configs[key] = value
        else:
            setattr(self, f"_{key}", value)
        return self

    def protocol(self, protocol: str):
        """Define the protocol: file://, hdfs://, s3a://, etc."""
        protocol = protocol.strip()
        self._protocol = protocol if protocol.endswith("://") else protocol + "://"
        return self

    def raw_subdir(self, raw_subdir: str):
        """Define the name of raw_subdir."""
        self._raw_subdir = raw_subdir.strip().rstrip("/")
        return self

    def to_clean_subdir(self, subdir: str):
        """Define the name of clean subdirectory."""
        self._to_clean_subdir = subdir.strip().rstrip("/")
        return self

    def to_curate_subdir(self, subdir: str):
        """Define the name of clean subdirectory."""
        self._to_curate_subdir = subdir.strip().rstrip("/")
        return self

    def curated_subdir(self, subdir: str):
        """Define the name of clean subdirectory."""
        self._curated_subdir = subdir.strip().rstrip("/")
        return self

    def base_path(self, base_path: str):
        """Defines the base *container_path where the Delta data is located."""
        self._base_path = base_path.strip().rstrip("/")
        return self

    def project_name(self, project_data_name: str):
        """Defines the name of the project data, which will be the name of the folder where the project's Delta data is hosted."""
        self._project_data_name = project_data_name.strip()
        return self

    def app_name(self, app_name: str):
        self._app_name = app_name.strip()
        return self

    def master(self, master: str):
        self._master = master.strip()
        return self

    def from_transformer_block_name(self, transformer_block_name):
        self._transformer_block_name = transformer_block_name
        return self

    def process_level(self, process_level: int | str):
        if isinstance(process_level, str):
            process_level = self.process_levels().index(process_level)
            if process_level==4:
                process_level = -1
        if -1 <= process_level <= 2:
            self._process_level = process_level

    def process_levels(self):
        return self.DEFAULT_RAW_SUBDIR, self.DEFAULT_TO_CLEAN_SUBDIR, self.DEFAULT_TO_CURATE_SUBDIR, self.DEFAULT_CURATED_SUBDIR, ""

    def close_session(self):
        b = self.get_spark_builder()
        s = b.getOrCreate()
        s.stop()


# ==============================================================
# BUILDER: DeltaDataLayerBuilder
# ==============================================================
class DeltaDataLayerBuilder(AbstractDeltaDataLayerBuilder):

    def build(self, type:str = None) -> "DeltaDataLayer":
        """Constructs and returns a DeltaDataLayer initialized with this constructor."""
        delta_layer = DeltaDataLayer(builder=self)
        return delta_layer

    def built_configuration(self):
        return ConfigDeltaDataLayer(builder=self)


class PortadaBuilder(DeltaDataLayerBuilder):
    DELTA_DATA_LAYER = "DeltaDataLayer"
    NEWS_TYPE = "NewsExtractionIngestion"
    BOAT_NEWS_TYPE = "BoatFactIngestion"
    KNOWN_ENTITIES_TYPE = "KnownEntitiesIngestion"

    def __init__(self, json_config=None, json_extended_classes=None):
        super().__init__(json_config=json_config)
        self.__classes_to_build = json_extended_classes

    """
    Builder to configure Spark + Delta Lake with a fluid and flexible API.
    Example:
        data_layer_builder = (
            PortadaBuilder()
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

    def build(self, type:str = None, as_config_layer:dict = None) -> "PortadaIngestion":
        """Constructs and returns a DeltaDataLayer initialized with this constructor."""
        if not type or type==self.DELTA_DATA_LAYER:
            if as_config_layer:
                delta_layer = DeltaDataLayer(as_config_layer)
            else:
                delta_layer = DeltaDataLayer(builder=self)
        elif type==self.NEWS_TYPE:
            if as_config_layer:
                delta_layer = NewsExtractionIngestion(as_config_layer)
            else:
                delta_layer = NewsExtractionIngestion(builder=self)
        elif type == self.BOAT_NEWS_TYPE:
            if as_config_layer:
                delta_layer = BoatFactIngestion(as_config_layer)
            else:
                delta_layer = BoatFactIngestion(builder=self)
        elif type==self.KNOWN_ENTITIES_TYPE:
            if as_config_layer:
                delta_layer = KnownEntitiesIngestion(as_config_layer)
            else:
                delta_layer = KnownEntitiesIngestion(builder=self)
        elif type in self.CLASS_REGISTRY:
            cls = self.CLASS_REGISTRY[type]
            if as_config_layer:
                delta_layer = cls(as_config_layer)
            else:
                delta_layer = cls(builder=self)
        else:
            raise Exception(f"Unknown type {type}")
        return delta_layer
