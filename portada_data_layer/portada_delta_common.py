import importlib
import inspect

class PortadaDeltaConstants:
    CURATED_PROCESS_LEVEL = 3
    TO_CURATE_PROCESS_LEVEL = 2
    TO_CLEAN_PROCESS_LEVEL = 1
    RAW_PROCESS_LEVEL = 0
    DEFAULT_PROJECT_DATA_NAME = "default"
    DEFAULT_CURATED_SUBDIR = "gold"
    DEFAULT_TO_CURATE_SUBDIR = "silver"
    DEFAULT_TO_CLEAN_SUBDIR = "bronze"
    DEFAULT_RAW_SUBDIR = "ingest"
    DEFAULT_BASE_PATH = "~/.delta_lake/data"
    CLASS_REGISTRY = {}

def registry_to_portada_builder(cls):
    PortadaDeltaConstants.CLASS_REGISTRY[cls.__name__] = cls
    return cls

def register_all_module_classes_to_portada_builder(module_name):
    module = importlib.import_module(module_name)
    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj):
            registry_to_portada_builder(obj)

