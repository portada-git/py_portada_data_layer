import importlib
import inspect

class BoatFactConstants:
    FLAG_ENTITY = 'flag'
    SHIP_TONS_ENTITY = 'ship_tons'
    TRAVEL_DURATION_ENTITY = 'travel_duration'
    COMMODITY_ENTITY = 'comodity'
    SHIP_TYPE_ENTITY = 'ship_type'
    UNIT_ENTITY = 'unit'
    PORT_ENTITY = 'port'
    MASTER_ROLE_ENTITY = 'master_role'
    KNOWN_ENTITIES_LIST = [
        FLAG_ENTITY,
        SHIP_TONS_ENTITY,
        TRAVEL_DURATION_ENTITY,
        COMMODITY_ENTITY,
        SHIP_TYPE_ENTITY,
        UNIT_ENTITY,
        PORT_ENTITY,
        MASTER_ROLE_ENTITY,
    ]
    SHIP_ENTRIES = "ship_entries"
    CARGO_SHIP_ENTRIES = "cargo_ship_entries"
    REVIEWED_ENTRY_TYPES = [SHIP_ENTRIES, CARGO_SHIP_ENTRIES]

class PortadaDeltaConstants:
    GOLD_DATA_PROCESS_LEVEL = 3
    CURATED_PROCESS_LEVEL = 3
    TO_CURATE_PROCESS_LEVEL = 2
    CLEANED_PROCESS_LEVEL = 2
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
