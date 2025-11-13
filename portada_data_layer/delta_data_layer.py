from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from py4j.java_gateway import java_import
from datetime import datetime
import uuid
import os
import json
import logging
import re

from portada_data_layer.traced_data_frame import TracedDataFrame

logger = logging.getLogger("delta_data_layer")


# ==============================================================
# BUILDER: AbstractDeltaDataLayerBuilder
# ==============================================================

class AbstractDeltaDataLayerBuilder:
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
    DEFAULT_BASE_PATH = "~/.delta_lake/data"
    DEFAULT_RAW_SUBDIR = "bronze"
    DEFAULT_CLEAN_SUBDIR = "silver"
    DEFAULT_CURATED_SUBDIR = "gold"
    DEFAULT_PROJECT_DATA_NAME = "default"
    RAW_PROCESS_LEVEL = 0
    CLEAN_PROCESS_LEVEL = 1
    CURATED_PROCESS_LEVEL = 2

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
        self._base_path = DeltaDataLayerBuilder.DEFAULT_BASE_PATH
        self._project_data_name = DeltaDataLayerBuilder.DEFAULT_PROJECT_DATA_NAME
        self._app_name = "DeltaLayerLib"
        self._master = "local[*]"
        self._configs = {}
        self._raw_subdir = DeltaDataLayerBuilder.DEFAULT_RAW_SUBDIR
        self._clean_subdir = DeltaDataLayerBuilder.DEFAULT_CLEAN_SUBDIR
        self._curated_subdir = DeltaDataLayerBuilder.DEFAULT_CURATED_SUBDIR

        if "configs" in json_config:
            for c in json_config["configs"]:
                k = c.keys()[0]
                self._configs[k](c[k])

        for key, value in json_config:
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

    def clean_subdir(self, clean_subdir: str):
        """Define the name of clean subdirectory."""
        self._clean_subdir = clean_subdir.strip().rstrip("/")
        return self

    def curated_subdir(self, curated_subdir: str):
        """Define the name of clean subdirectory."""
        self._curated_subdir = curated_subdir.strip().rstrip("/")
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

    def process_level(self, process_level: int | str):
        if isinstance(process_level, str):
            process_level = self.process_levels().index(process_level)
            if process_level==4:
                process_level = -1
        if -1 <= process_level <= 2:
            self._process_level = process_level

    def process_levels(self):
        return self.DEFAULT_RAW_SUBDIR, self.DEFAULT_CLEAN_SUBDIR, self.DEFAULT_CURATED_SUBDIR, ""


# ==============================================================
# BUILDER: DeltaDataLayerBuilder
# ==============================================================
class DeltaDataLayerBuilder(AbstractDeltaDataLayerBuilder):

    def build(self) -> "DeltaDataLayer":
        """Constructs and returns a DeltaDataLayer initialized with this constructor."""
        delta_layer = DeltaDataLayer(builder=self)
        return delta_layer

    def built_configuration(self):
        return ConfigDeltaDataLayer(builder=self)



# ==============================================================
# CLASS: PathConfigDeltaDataLayer
# ==============================================================
class PathConfigDeltaDataLayer:
    """
    Configuration class which dynamically determine the *container_path where the data will be stored based on the configuration:
        - The protocol indicates the file system used, as well as the host and listening port when necessary. For
        example, hdfs://protadaproject.eu:9000 or simply file://
        - The base_path is the root *container_path where the rest of the directories and files of the Data Lake will be located.
        - The project name encapsulate all the project data as a directory. For example, "portadaproject", thus allowing
        the same data lake to be shared among different projects.
    """

    def __init__(self, cfg_json: dict = None):
        if cfg_json is not None:
            self._spark_builder = cfg_json["_spark_builder"] if "_spark_builder" in cfg_json else SparkSession.builder
            self._base_path = cfg_json["_base_path"] if "_base_path" in cfg_json else os.path.abspath(AbstractDeltaDataLayerBuilder.DEFAULT_BASE_PATH)
            self._protocol = cfg_json["_protocol"] if "_protocol" in cfg_json else "file://"
            self._project_data_name = cfg_json[
                "_project_data_name"] if "_project_data_name" in cfg_json else AbstractDeltaDataLayerBuilder.DEFAULT_PROJECT_DATA_NAME
            self.spark = cfg_json["spark"] if "spark" in cfg_json else None
        else:
            self._spark_builder = SparkSession.builder
            self.spark = None
            self._base_path = os.path.abspath(AbstractDeltaDataLayerBuilder.DEFAULT_BASE_PATH)
            self._protocol = "file://"
            self._project_data_name = AbstractDeltaDataLayerBuilder.DEFAULT_PROJECT_DATA_NAME

    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, value: str):
        if self._protocol != value:
            if value.startswith("hdfs://") and self._spark_builder is not None:
                self._spark_builder.config("spark.hadoop.fs.defaultFS", value)
            self._protocol = value

    @property
    def spark_builder(self):
        """ Builder to build a DeltaDataLayer object"""
        return self._spark_builder

    @spark_builder.setter
    def spark_builder(self, some_builder: SparkSession.Builder | DeltaDataLayerBuilder):
        if isinstance(some_builder, DeltaDataLayerBuilder):
            self._spark_builder = some_builder.get_spark_builder()
        elif isinstance(some_builder, SparkSession.Builder):
            self._spark_builder = some_builder
        else:
            erro_message = "Incompatible builder. Only builders of type 'DeltaDataLayerBuilder' or 'SparkSession.Builder' are accepted."
            logger.error(erro_message)
            raise Exception(erro_message)

    @property
    def base_path(self):
        return self._base_path

    @base_path.setter
    def base_path(self, value: str):
        self._base_path = value

    @property
    def project_name(self):
        return self._project_data_name

    @project_name.setter
    def project_name(self, value: str):
        self._project_data_name = value

    def _resolve_relative_path(self, *args, has_extension=False, contains_project_name=False) -> str:
        if len(args)==1 and (type(args[0]) == tuple or type(args[0]) == list):
            table_path = args[0]
        else:
            table_path = args
        if len(table_path) == 1 and re.match(r"\w+://.*", table_path[0]):
            p = re.compile(f"{self.protocol}{self.base_path}/(.*)")
            ret = re.sub(p,"\\g<1>", table_path[0], 0)
        else:
            table_path = '.'.join(map(str, table_path)).split('.')
            if has_extension:
                extension = table_path[-1]
                del table_path[-1]
            else:
                extension = ""
            table_path = '/'.join(map(str, table_path))
            if contains_project_name:
                project_name = ""
            else:
                project_name = f"{self._project_data_name}/"
            ret = f"{project_name}{table_path}"
            if extension:
                ret = f"{ret}.{extension}"
        return ret

    def _resolve_path(self, *args, has_extension=False, contains_project_name=False) -> str:
        if len(args)==1 and (type(args[0]) == tuple or type(args[0]) == list):
            table_path = args[0]
        else:
            table_path = args
        if len(table_path) == 1 and re.match(r"\w+://.*", table_path[0]):
            ret = table_path[0]
        else:
            table_path = '.'.join(map(str, table_path)).split('.')
            if has_extension:
                extension = table_path[-1]
                del table_path[-1]
            else:
                extension = ""
            table_path = '/'.join(map(str, table_path))
            if contains_project_name:
                project_name = ""
            else:
                project_name = f"{self._project_data_name}/"
            ret = f"{self.protocol}{self.base_path}/{project_name}{table_path}"
            if extension:
                ret = f"{ret}.{extension}"
        return ret

    def get_configuration(self):
        """
        Get a json object with the attributes of this object. You can use this json to build other object sharing the
        same values
        :return: configuration as json format
        """
        return {
            "_spark_builder": self._spark_builder,
            "_base_path": self._base_path,
            "_protocol": self._protocol,
            "_project_data_name": self._project_data_name,
            "spark": self.spark,
        }




# ==============================================================
# CLASS: ConfigDeltaDataLayer
# ==============================================================
class ConfigDeltaDataLayer(PathConfigDeltaDataLayer):
    """
        Configuration class which dynamically determine the *container_path where the data will be stored based on the configuration:
            - The protocol indicates the file system used, as well as the host and listening port when necessary. For
            example, hdfs://protadaproject.eu:9000 or simply file://
            - The base_path is the root *container_path where the rest of the directories and files of the Data Lake will be located.
            - The project name encapsulate all the project data as a directory. For example, "portadaproject", thus allowing
            the same data lake to be shared among different projects.
            - The stage or level of the processed data. This allows for the differentiation of all data at the same
            level according to the established procedures in ETL processes. Generally, the names used are 'bronze' for
            raw data, 'silver' for the transition from raw data to clean data or data prepared for processing, and 'gold'
            for the transition from clean data to cured data ready for use.
        """
    def __init__(self, builder: AbstractDeltaDataLayerBuilder = None, cfg_json: dict = None):
        self._process_context=""
        self._process_level_dirs_ = [AbstractDeltaDataLayerBuilder.DEFAULT_RAW_SUBDIR,
                                     AbstractDeltaDataLayerBuilder.DEFAULT_CURATED_SUBDIR,
                                     AbstractDeltaDataLayerBuilder.DEFAULT_PROJECT_DATA_NAME,
                                     ""]
        super().__init__(cfg_json)
        if builder is not None:
            self._spark_builder = builder.get_spark_builder()
            self._base_path = builder._base_path
            self._protocol = builder._protocol
            self._process_level_dirs_[AbstractDeltaDataLayerBuilder.RAW_PROCESS_LEVEL] = builder._raw_subdir
            self._process_level_dirs_[AbstractDeltaDataLayerBuilder.CLEAN_PROCESS_LEVEL] = builder._clean_subdir
            self._process_level_dirs_[AbstractDeltaDataLayerBuilder.CURATED_PROCESS_LEVEL] = builder._curated_subdir
            self._project_data_name = builder._project_data_name
            self._current_process_level = builder._process_level
        elif cfg_json is not None:
            self._process_level_dirs_ = cfg_json["_process_level_dirs_"] if "_process_level_dirs_" in cfg_json else [AbstractDeltaDataLayerBuilder.DEFAULT_RAW_SUBDIR,
                                                                                                                    AbstractDeltaDataLayerBuilder.DEFAULT_CURATED_SUBDIR,
                                                                                                                    AbstractDeltaDataLayerBuilder.DEFAULT_PROJECT_DATA_NAME,
                                                                                                                    ""]
            self._current_process_level= cfg_json["_current_process_level"] if "_current_process_level" in cfg_json else -1
            self._process_context = cfg_json["_process_context"] if "_process_context" in cfg_json else ""
            self.spark = cfg_json["spark"] if "spark" in cfg_json else None
        else:
            self._current_process_level=-1

    @property
    def current_process_level(self):
        """
        Level or stage to process the data in this layer. The property is a number which ranges between 0 and 2.
        :return: the current stage of data (0: bronze, 1: silver, 2: gold)
        """
        return self._current_process_level

    @property
    def level_names(self):
        """
        List of level names for the 3 stages of data.
        :return: the list
        """
        return self._process_level_dirs_

    @property
    def curated_subdir(self):
        """
        Name for the last level or stage of the data
        :return:
        """
        return self._process_level_dirs_[DeltaDataLayerBuilder.CURATED_PROCESS_LEVEL]

    @property
    def raw_subdir(self):
        """
        Name for the first level or stage of the data
        :return:
        """
        return self._process_level_dirs_[DeltaDataLayerBuilder.RAW_PROCESS_LEVEL]

    @property
    def clean_subdir(self):
        """
        Name for the middle level or stage of the data
        :return:
        """
        return self._process_level_dirs_[DeltaDataLayerBuilder.CLEAN_PROCESS_LEVEL]

    @property
    def process_context_name(self):
        """
        Name of the context understood as the concatenation of processes executed so far in the processing of data.
        :return:
        """
        return self._process_context

    @process_context_name.setter
    def process_context_name(self, value):
        if value is None:
            value = ""
        self._process_context = str(value)

    @staticmethod
    def _flatten_table_name(*table_name):
        if len(table_name)==1 and (type(table_name[0]) == tuple or type(table_name[0]) == list):
            table_path = table_name[0]
        else:
            table_path = table_name
        table_path = '.'.join(map(str, table_path))
        return table_path

    def _resolve_relative_path(self, *args, process_level_dir=None, has_extension=False, contains_project_name=False, contains_process_level=False) -> str:
        if len(args)==1 and (type(args[0]) == tuple or type(args[0]) == list):
            table_path = args[0]
        else:
            table_path = args
        if process_level_dir is None:
            process_level_dir = self._process_level_dirs_[self._current_process_level]
        else:
            process_level_dir = f"{process_level_dir.rstrip("/")}/"
        if len(table_path) == 1 and re.match(r"\w+://.*", table_path[0]):
            p = re.compile(f"{self.protocol}{self.base_path}/(.*)")
            ret = re.sub(p,"\\g<1>", table_path[0], 0)
        else:
            table_path = '.'.join(map(str, table_path)).split('.')
            if has_extension:
                extension = table_path[-1]
                del table_path[-1]
            else:
                extension = ""
            table_path = '/'.join(map(str, table_path))
            if contains_project_name:
                project_name = ""
            else:
                project_name = f"{self._project_data_name}/"
            if contains_process_level:
                process_level_dir = ""
            ret = f"{project_name}{process_level_dir}{table_path}"
            if extension:
                ret = f"{ret}.{extension}"
        return ret

    def _resolve_path(self, *args, process_level_dir=None, has_extension=False, contains_project_name=False, contains_process_level=False) -> str:
        if len(args)==1 and (type(args[0]) == tuple or type(args[0]) == list):
            table_path = args[0]
        else:
            table_path = args
        if process_level_dir is None:
            process_level_dir=self._process_level_dirs_[self._current_process_level]
        else:
            process_level_dir = f"{process_level_dir.rstrip("/")}/"
        if len(table_path)==1 and re.match(r"\w+://.*", table_path[0]):
            ret = table_path[0]
        else:
            table_path = '.'.join(map(str, table_path)).split('.')
            if has_extension:
                extension = table_path[-1]
                del table_path[-1]
            else:
                extension = ""
            table_path = '/'.join(map(str, table_path))
            if contains_project_name:
                project_name = ""
            else:
                project_name = f"{self._project_data_name}/"
            if contains_process_level:
                process_level_dir = ""
            ret = f"{self.protocol}{self.base_path}/{project_name}{process_level_dir}{table_path}"
            if extension:
                ret = f"{ret}.{extension}"
        return ret

    def register_udfs(self, name, func, return_type):
        """
        Register a function as "User defined Function" to process sequentially, the values of a column of a dataframe.
        The function must have the following signature: func(value) -> return value of return_type type. You will be
        able to use this kind of function when you need to change a column od a Dataframe. For example:
            df.withColumn("new_column", data_layer.some_udf_function(F.col("existing_column_name")))
        You will have been able to use that if previously you have register here the function "some_udf_function". To
        register a function you must code:
           def function_to_process_a_single_value(value):
               do something with value, for example
               return value * 2
           layer.register_udfs("some_udf_function", function_to_process_a_single_value, IntegralType())
        :param name: The name to get the udf function as attribute of data_layer
        :param func: function to register and convert to UDF format function.
        :param return_type: A return type compatible as column typer for dataframes of spark.sql.DataFrame
        :return:
        """
        @F.udf(returnType=return_type)
        def udf(value):
            return func(value)
        setattr(self, name, udf)

    def get_configuration(self):
        """
       Get a json object with the attributes of this object. You can use this json to build other object sharing the
       same values
       :return: configuration as json format
       """
        return {
            "_spark_builder": self._spark_builder,
            "_base_path": self._base_path,
            "_protocol": self._protocol,
            "_process_level_dirs_": self._process_level_dirs_,
            "_project_data_name": self._project_data_name,
            "_current_process_level": self._current_process_level,
            "_process_context":self._process_context,
            "spark": self.spark,
        }

    def is_initialized(self):
        """
        Return true if the current spark session is initialized or False otherwise.
        :return:
        """
        return not (self.spark is None or self.spark.sparkContext._jsc is None or self.spark.sparkContext._jvm is None)

    def start_session(self):
        """Initialize Spark with the builder configuration."""
        if not self.is_initialized():
            self.spark = configure_spark_with_delta_pip(self._spark_builder).getOrCreate()
        logger.info("Spark is initialized ")

    def stop_session(self):
        """
        Stop this SparkSession. Therefore, any action performed with this session will result in an error.
        """
        if self.is_initialized():
            self.spark.stop()
        logger.info("SparkSession was stopped.")


# ==============================================================
# CLASS: BaseDeltaDataLayer
# ==============================================================
class BaseDeltaDataLayer(ConfigDeltaDataLayer):
    """
   Base Delta Data Layer with Spark configurable via Builder.
   """

    def __init__(self, builder: AbstractDeltaDataLayerBuilder = None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self._a_log_process_info=[{}]

    @property
    def log_process_info(self):
        """
        Info value stored for log_process
        :return:
        """
        return self._a_log_process_info[-1]

    @log_process_info.setter
    def log_process_info(self, value):
        self._a_log_process_info.append(value)

    def _clean_log_process_info(self):
        del self._a_log_process_info[-1]
        if len(self._a_log_process_info)==0:
            self._a_log_process_info = [{}]

    def subdirs_list(self, *container_path, process_level_dir=None):
        """Returns subdirectories within an HDFS *container_path (without using os.listdir)."""
        path = self._resolve_path(*container_path, process_level_dir=process_level_dir)
        fs_ex = FileSystemTaskExecutor(self.get_configuration())
        return fs_ex.subdirs_list(base_path=path)

    def path_exists(self, *container_path, process_level_dir=None, has_extension=False):
        """
        Checks if a file or directory exists for any protocol supported by Hadoop.
        :param container_path: container_path to check as string or list of strings
        :param has_extension: This parameter, in case the container_path is not absolute, allows you to indicate that it contains
         a file with an extension so that the absolute container_path can be calculated correctly.
        :param process_level_dir: Allows you to specify a directory other than the one corresponding to the level at
        which the data process is located at the time of execution if container_path is not an absolute *container_path.
        :return: True o False if *container_path exists
        """
        path = self._resolve_path(*container_path, process_level_dir=process_level_dir, has_extension=has_extension)
        fs_ex = FileSystemTaskExecutor(self.get_configuration())
        return fs_ex.path_exists(path)

    def is_delta_table_type(self,  *container_path, process_level_dir=None):
        """
        Checks if a file or directory exists for any protocol supported by Hadoop.
        :param container_path: *container_path to check as string
        :param process_level_dir: Allows you to specify a directory other than the one corresponding to the level at
        which the data process is located at the time of execution if container_path is not an absolute *container_path.
        :return: True o False if *container_path exists
        """
        path = self._resolve_path(*container_path, process_level_dir=process_level_dir)
        fs_ex = FileSystemTaskExecutor(self.get_configuration())
        return fs_ex.is_delta_table_type(path)

    def is_json_type(self, *container_path, process_level_dir=None):
        """
        Checks if a file exists for any protocol supported by Hadoop and is a json type saved by spark.
        :param container_path: container_path to check
        :return: True o False if container_path exists and is a container for a json file
        """
        fs_ex = FileSystemTaskExecutor(self.get_configuration())
        return fs_ex.is_json_type(*container_path)

    def delta_file_exist(self, *container_path, process_level_dir=None):
        """
        Checks if the param container_path point to an existing delta storage.
        :param container_path: *container_path to check as string for any protocol supported by Hadoop
        :param process_level_dir: Allows you to specify a directory other than the one corresponding to the level at
        which the data process is located at the time of execution if container_path is not an absolute *container_path.
        :return: True o False if *container_path exists and pointed to a delta storage
        """
        path = self._resolve_path(*container_path, process_level_dir=process_level_dir)
        fs_ex = FileSystemTaskExecutor(self.get_configuration())
        return fs_ex.is_delta_table_type(path)

    def json_file_exist(self, *container_path, process_level_dir=None):
        """
        Checks if the param container_path point to an existing json storage.
        :param container_path: *container_path to check as string for any protocol supported by Hadoop
        :param process_level_dir: Allows you to specify a directory other than the one corresponding to the level at
        which the data process is located at the time of execution if container_path is not an absolute *container_path.
        :return: True o False if *container_path exists and pointed to a json storage
        """
        path = self._resolve_path(*container_path, process_level_dir=process_level_dir)
        fs_ex = FileSystemTaskExecutor(self.get_configuration())
        return fs_ex.path_exists(path) and fs_ex.is_json_type(path)


# ==============================================================
# CLASS: DeltaDataLayer
# ==============================================================

class DeltaDataLayer(BaseDeltaDataLayer):
    """
    Delta Data Layer with Spark configurable via DeltaDataLayerBuilder.
    """

    def __init__(self, builder: AbstractDeltaDataLayerBuilder = None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self.log_storage=False
        self.source_path=None

    def write_json(self, *table_path, df: DataFrame | TracedDataFrame, mode: str ="overwrite", process_level_dir: str = None, has_extension=False):
        """
        Write the dataframe df to json file addressed by table_path.
        table_path can be any of the following forms:
            1. tuple or list. Examples: write_delta(("portada", "ships"), df) or write_delta(["portada", "ships"], df). In these cases, the table "ships" will be saved in <delta_data_base_path>/portada. The table *container_path will be <delta_data_base_path>/portada/ships
            2. Only the table name or a sequence of strings. Examples:
                 - write_delta("ships", df). This case will be resolved as <delta_data_base_path>/ships
                 - write_delta("portada","ships", df) will be resolved as <delta_data_base_path>/portada/ships
            3. String, sequence of strings, dict or list with dots as separator. Examples:
                 - write_delta("portada.masters", df) will be resolved as <delta_data_base_path>/portada/masters
                 - write_delta(("bronze", "portada.masters"), df) will be resolved as <delta_data_base_path>/bronze/portada/masters
        :param table_path:
        :param df:
        :param mode:  mode to write the DataFrame in the delta table. Accepted options:
            * `append`: Append contents of this :class:`DataFrame` to existing data.
            * `overwrite`: Overwrite existing data.
            * `error` or `errorifexists`: Throw an exception if data already exists.
            * `ignore`: Silently ignore this operation if data already exists.
        :param process_level_dir:
        :param has_extension:
        """
        if isinstance(df, TracedDataFrame):
            source_path = df.source_name
            source_version = df.source_version
            original_df = df.toSparkDataFrame()
        else:
            source_path = "NEW"
            source_version = -1
            original_df = df

        path = self._resolve_path(*table_path, process_level_dir=process_level_dir, has_extension=has_extension)
        name = self._resolve_relative_path(path)
        logger.info(f"Writing Delta → {path}")
        original_df.coalesce(1).write.mode(mode).json(path)
        if self.log_storage:
            if hasattr(self, "metadata"):
                metadata = self.metadata
            else:
                from portada_data_layer.data_lake_metadata_manager import DataLakeMetadataManager
                metadata=DataLakeMetadataManager(self.get_configuration())
            metadata.log_storage(
                data_layer=self,
                num_records=original_df.count(),
                source_path = source_path,
                source_version= source_version,
                target_path=name,
                target_version=-1,
            )
        return TracedDataFrame(original_df, name, -1)

    def write_delta(self, *table_path, df: DataFrame | TracedDataFrame, mode: str = "overwrite"):
        """
        Write the dataframe df to delta table addressed by table_path.
        table_path can be any of the following forms:
            1. tuple or list. Examples: write_delta(("portada", "ships"), df) or write_delta(["portada", "ships"], df). In these cases, the table "ships" will be saved in <delta_data_base_path>/portada. The table *container_path will be <delta_data_base_path>/portada/ships
            2. Only the table name or a sequence of strings. Examples:
                 - write_delta("ships", df). This case will be resolved as <delta_data_base_path>/ships
                 - write_delta("portada","ships", df) will be resolved as <delta_data_base_path>/portada/ships
            3. String, sequence of strings, dict or list with dots as separator. Examples:
                 - write_delta("portada.masters", df) will be resolved as <delta_data_base_path>/portada/masters
                 - write_delta(("bronze", "portada.masters"), df) will be resolved as <delta_data_base_path>/bronze/portada/masters
        :param table_path:
        :param df:
        :param mode:  mode to write the DataFrame in the delta table. Accepted options:
            * `append`: Append contents of this :class:`DataFrame` to existing data.
            * `overwrite`: Overwrite existing data.
            * `error` or `errorifexists`: Throw an exception if data already exists.
            * `ignore`: Silently ignore this operation if data already exists.
        """
        if isinstance(df, TracedDataFrame):
            source_path = df.source_name
            source_version = df.source_version
            original_df = df.toSparkDataFrame()
        else:
            source_path = "NEW"
            source_version = -1
            original_df = df

        path = self._resolve_path(*table_path)
        name = self._resolve_relative_path(path)
        logger.info(f"Writing Delta → {path}")
        original_df.write.format("delta").mode(mode).save(path)
        version = self.get_delta_metatable(path).history(1).collect()[0]['version']
        if self.log_storage:
            if hasattr(self, "metadata"):
                metadata = self.metadata
            else:
                from portada_data_layer.data_lake_metadata_manager import DataLakeMetadataManager
                metadata=DataLakeMetadataManager(self.get_configuration())
            metadata.log_storage(
                data_layer=self,
                num_records=original_df.count(),
                source_path = source_path,
                source_version= source_version,
                target_path=name,
                target_version=version,
            )
        return TracedDataFrame(original_df, name, version)


    def read_json(self, *table_path, process_level_dir=None, has_extension=False) -> TracedDataFrame:
        """
       Read a delta table as a dataframe which is returned.
       table_path is the *container_path of the table to read and can be any of the following forms:
           1. dict or list. Examples: read_delta("portada", "ships") or read_delta(["portada", "ships"]). The table *container_path will be resolved as <delta_data_base_path>/portada/ships
           2. Only the table name or a sequence of strings. Examples:
               - read_delta("ships"). This case will be resolved as <delta_data_base_path>/ships
               - read_delta("portada", "ships") will be resolved as <delta_data_base_path>/portada/ships
           3. String, sequence of strings, dict or list with items including dots as separator. Examples:
               - read_delta("portada.masters") will be resolved as <delta_data_base_path>/portada/masters
               - read_delta(("bronze", "portada.masters")) will be resolved as <delta_data_base_path>/bronze/portada/masters
       :param table_path:
       :param process_level_dir:
       :param has_extension:
       :return: DataFrame type with the content of delta table
       """
        path = self._resolve_path(*table_path, process_level_dir=process_level_dir, has_extension=has_extension)
        name = self._resolve_relative_path(path)
        logger.info(f"Reading Json ← {path}")
        try:
            df = self.spark.read.json(path)
        except Exception as e:
            if "[PATH_NOT_FOUND]" in str(e):
                df = None
            else:
                raise e
        return None if df is None else TracedDataFrame(df, name, -1)

    def read_delta(self, *table_path, process_level_dir=None) -> TracedDataFrame:
        """
       Read a delta table as a dataframe which is returned.
       table_path is the *container_path of the table to read and can be any of the following forms:
           1. dict or list. Examples: read_delta("portada", "ships") or read_delta(["portada", "ships"]). The table *container_path will be resolved as <delta_data_base_path>/portada/ships
           2. Only the table name or a sequence of strings. Examples:
               - read_delta("ships"). This case will be resolved as <delta_data_base_path>/ships
               - read_delta("portada", "ships") will be resolved as <delta_data_base_path>/portada/ships
           3. String, sequence of strings, dict or list with items including dots as separator. Examples:
               - read_delta("portada.masters") will be resolved as <delta_data_base_path>/portada/masters
               - read_delta(("bronze", "portada.masters")) will be resolved as <delta_data_base_path>/bronze/portada/masters
        :param process_level_dir:
       :param table_path:
       :return: TracedDataFrame type with the content of delta table
       """
        path = self._resolve_path(*table_path, process_level_dir=process_level_dir)
        name = self._resolve_relative_path(path)
        logger.info(f"Reading Delta ← {path}")
        try:
            df = self.spark.read.format("delta").load(path)
            version = self.get_delta_metatable(path).history(1).collect()[0]['version']
        except Exception as e:
            if "[PATH_NOT_FOUND]" in str(e):
                df = None
                version = -1
            else:
                raise e
        return None if df is None else TracedDataFrame(df, name, version)

    def get_delta_metatable(self, *table_path) -> DeltaTable:
        """
       Load and return a delta table.
       table_path is the *container_path of the table to read and can be any of the following forms:
           1. dict or list. Examples: read_delta("portada", "ships") or read_delta(["portada", "ships"]). The table *container_path will be resolved as <delta_data_base_path>/portada/ships
           2. Only the table name or a sequence of strings. Examples:
               - read_delta("ships"). This case will be resolved as <delta_data_base_path>/ships
               - read_delta("portada", "ships") will be resolved as <delta_data_base_path>/portada/ships
           3. String, sequence of strings, dict or list with items including dots as separator. Examples:
               - read_delta("portada.masters") will be resolved as <delta_data_base_path>/portada/masters
               - read_delta(("bronze", "portada.masters")) will be resolved as <delta_data_base_path>/bronze/portada/masters
        :param table_path:
        :return: DeltaTable type
        """
        path = self._resolve_path(*table_path)
        logger.info(f"Loading DeltaTable ← {path}")
        return DeltaTable.forPath(self.spark, path)

    def sql(self, query: str) -> TracedDataFrame:
        """
        Returns a :class:`DataFrame` representing the result of the given query in SQL language.
        """
        logger.info(f"Executing SQL: {query}")
        df = self.spark.sql(query)
        return df if isinstance(df, TracedDataFrame) else TracedDataFrame(df, source_name=query)

    @staticmethod
    def register_temp_table(df: DataFrame | TracedDataFrame, name: str= None):
        """
        Creates or replaces a local temporary view with the df `DataFrame`.
                The lifetime of this temporary table is tied to the :class:`SparkSession`
                that was used to create this :class:`DataFrame`.
        :param df: DataFrame where create or replace the temporal view
        :param name: Name of the view
        """
        if not name:
            if isinstance(df, TracedDataFrame):
                if df.source_version==-1:
                    name = f"{df.source_name}"
                else:
                    name = f"{df.source_name}_{df.source_version}"
            else:
                raise Exception("For spark Dataframes, the name parameter is absolutely needed")
        df.createOrReplaceTempView(name)
        logger.info(f"Temporary view named '{name}' was registered.")

# def run_pipe(pipe_process_struct):



# ==============================================================
# CLASS: FileSystemTaskExecutor
# ==============================================================
class FileSystemTaskExecutor(BaseDeltaDataLayer):
    """Encapsulate copy operations to Hadoop/S3/FileSystem."""
#AFEGIR NOM DEL FITXER COPIA AMB DATA D'ENTRADA i VALOR RANDOM
    def __init__(self, cfg_json: dict):
        super().__init__(cfg_json=cfg_json)
        self._fs = None
        self._sc = None
        self._jvm = None
        self._jsc = None
        if self.is_initialized():
            self._fs = self._init_fs()

    def start_session(self):
        super().start_session()
        self._fs = self._init_fs()

    def _init_fs(self):
        self._sc = self.spark.sparkContext
        self._jvm = self._sc._jvm
        java_import(self._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(self._jvm, "org.apache.hadoop.fs.Path")
        self._jsc = self.spark._jsc
        conf = self._jsc.hadoopConfiguration()
        return self._jvm.FileSystem.get(conf)

    @staticmethod
    def date_random_file_name_generator(extension: str = ""):
        """
        Generate a random name for a file and add to the end the extension if the attribute has value
        :param extension: extension value to add to the end of random name generated
        :return:
        """
        if extension:
            extension = f".{extension}"
        return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}{extension}"

    def copy_from_local(self, *container_dest, file_name_dest: str, src_path: str, remove_local=False):
        """Copy a local file to the specified destination in the Hadoop FS. and return the complete *container_path were it was copied."""
        dest_path = self._resolve_path(*container_dest)
        dest_path = os.path.join(dest_path, file_name_dest)
        # sc = data_layer.spark.sparkContext
        src = self._jvm.Path(src_path)
        jvm_dest_path = self._jvm.Path(dest_path)
        self._fs.copyFromLocalFile(False, True, src, jvm_dest_path)
        if remove_local:
            os.remove(src_path)
        return dest_path

    def path_exists(self, path: str):
        """
        Checks if a file or directory exists for any protocol supported by Hadoop.
        :param *container_path: *container_path to check as string
        :return: True o False if *container_path exists
        """
        # sc = data_layer.spark.sparkContext
        hadoop_conf = self._sc._jsc.hadoopConfiguration()
        fs = self._jvm.org.apache.hadoop.fs.FileSystem.get(
            self._jvm.org.apache.hadoop.fs.Path(path).toUri(), hadoop_conf
        )
        return fs.exists(self._jvm.org.apache.hadoop.fs.Path(path))

    def is_delta_table_type(self, path:str):
        """
        Checks if a file or directory exists for any protocol supported by Hadoop.
        :param *container_path: *container_path to check as string
        :return: True o False if *container_path exists
        """
        return self.path_exists(f"{path}/_delta_log")

    def is_json_type(self, path:str):
        """
        Checks if a file exists for any protocol supported by Hadoop and is a json type saved by spark.
        :param *container_path: *container_path to check as string
        :return: True o False if *container_path exists
        """
        return self.path_exists(f"{path}/_SUCCESS")

    def subdirs_list(self, base_path: str):
        """
        Returns subdirectories within an HDFS *container_path (without using os.listdir).
        """
        fs = self._jvm.org.apache.hadoop.fs.FileSystem.get(
            self._jsc.hadoopConfiguration()
        )
        path = self._jvm.org.apache.hadoop.fs.Path(base_path)
        status = fs.listStatus(path)

        subdirs = [
            f.getPath().getName()
            for f in status
            if f.isDirectory()
        ]
        return subdirs

# ==============================================================
# CLASS: AbstractSparkPortadaProcess
# ==============================================================
class AbstractSparkPortadaProcess(ConfigDeltaDataLayer):
    """
    Abstract class to extend a new class with the specific functionality to process a TracedDataframe, for example
    """
    pass


