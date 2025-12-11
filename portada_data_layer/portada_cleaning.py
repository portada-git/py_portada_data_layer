import json
import re
from pyspark.sql import SparkSession, DataFrame
from portada_data_layer import DeltaDataLayer, TracedDataFrame
from portada_data_layer.data_lake_metadata_manager import data_transformer_method, LineageCheckingType, \
    enable_field_lineage_log_for_class
from portada_data_layer.portada_delta_common import registry_to_portada_builder


@registry_to_portada_builder
@enable_field_lineage_log_for_class
class PortadaCleaning(DeltaDataLayer):
    ARRAY_INDEX_REGEX = re.compile(r"\[\d+]")  # exemple: a[0].b → a[*].b

    def __init__(self, builder = None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self._schema={}
        self._allowed_paths=None
        self._current_process_level=1

    def use_schema(self, json_schema: dict):
        self._schema = json_schema
        self._allowed_paths = self._collect_allowed_paths(json_schema)
        return self

    @data_transformer_method(field_lineage=LineageCheckingType.FIELD_AND_VALUE)
    def prune_unaccepted_fields(self, df: DataFrame | TracedDataFrame):
        if self._schema is None:
            raise ValueError("Cal cridar use_schema() abans.")
        allowed = self._allowed_paths
        normalized_cols = {
            col: self._normalize_column(col) for col in df.columns
        }
        cols_to_keep = [
            original for original, norm in normalized_cols.items()
            if norm in allowed
        ]

        return df.select(cols_to_keep)

    def _normalize_column(self, col: str) -> str:
        """
        Normalitza columnes Spark, convertint:
            a[0].b → a[*].b
            a[12].x.y → a[*].x.y
        """
        return self.ARRAY_INDEX_REGEX.sub("[*]", col)

    def _collect_allowed_paths(self, node, prefix=""):
        """
        Returns all allowed flattened paths from a JSON Schema.
        Supports: object, array, oneOf, anyOf, allOf.
        """
        if "schema" in node:
            node = node["schema"]

        paths = []

        # Normalize prefix
        p = prefix + "." if prefix else ""

        # 1) If schema has oneOf / anyOf / allOf → merge results from sub-schemas
        combinators = ["oneOf", "anyOf", "allOf"]
        for key in combinators:
            if key in node:
                merged = []
                for option in node[key]:
                    merged.extend(self._collect_allowed_paths(option, prefix))
                return merged  # This is final for this node

        # 2) Object
        if node.get("type") == "object":
            props = node.get("properties", {})
            for field_name, field_schema in props.items():
                full = f"{p}{field_name}"
                paths.append(full)  # include the object field itself
                paths.extend(self._collect_allowed_paths(field_schema, full))
            return paths

        # 3) Array
        if node.get("type") == "array":
            items = node.get("items")

            if items is None:
                return paths  # no detail, cannot descend

            # Case: items contains oneOf / anyOf / allOf
            for key in combinators:
                if key in items:
                    merged = []
                    for option in items[key]:
                        merged.extend(self._collect_allowed_paths(option, prefix))
                    return merged

            # Case: items is a single schema
            return self._collect_allowed_paths(items, prefix)

        # 4) Primitive types (string, number, boolean…)
        #    → leaf node, prefix already represents the full path
        return paths

    # def _collect_allowed_paths(self, schema_fragment, prefix="", defs=None):
    #     """
    #     Returns all allowed flattened paths from a JSON Schema.
    #     Supports: object, array, oneOf, anyOf, allOf.
    #     """
    #     if defs:
    #         defs = defs.copy()
    #     else:
    #         defs = {}
    #
    #     if "schema" in schema_fragment:
    #         schema_fragment = schema_fragment["schema"]
    #
    #     if "$defs" in schema_fragment:
    #         for n, d in schema_fragment["Sdefs"]:
    #             defs[n] = d
    #
    #     allowed = []
    #
    #     # Normalize prefix
    #     p = prefix + "." if prefix else ""
    #
    #     # 1) If schema has oneOf / anyOf / allOf → merge results from sub-schemas
    #     combinators = ["oneOf", "anyOf", "allOf"]
    #     for key in combinators:
    #         if key in schema_fragment:
    #             merged = []
    #             for option in schema_fragment[key]:
    #                 merged.extend(self._collect_allowed_paths(option, prefix, defs))
    #             return merged  # This is final for this node
    #
    #     typ = schema_fragment.get("type")
    #
    #     # ---------------------------
    #     # OBJECT
    #     # ---------------------------
    #     if typ == "object":
    #         properties = schema_fragment.get("properties", {})
    #         additional = schema_fragment.get("additionalProperties", True)
    #
    #         if additional:
    #             allowed.append(prefix)
    #
    #         # Recorrem les propietats
    #         for prop, prop_schema in properties.items():
    #             path = f"{p}{prop}"
    #             allowed.append(path)
    #             allowed.extend(self._collect_allowed_paths(prop_schema, path, defs))
    #
    #         return allowed
    #
    #     # ---------------------------
    #     # ARRAY
    #     # ---------------------------
    #     if typ == "array":
    #         items_schema = schema_fragment.get("items")
    #
    #         if items_schema is None:
    #             return allowed  # no detail, cannot descend
    #
    #         # Representació normalitzada d'un array:
    #         #   a[0].b → a[*].b
    #         array_prefix = f"{prefix}[*]" if prefix else "[*]"
    #
    #         # Case: items contains oneOf / anyOf / allOf
    #         for key in combinators:
    #             if key in items_schema:
    #                 merged = []
    #                 for option in items_schema[key]:
    #                     merged.extend(self._collect_allowed_paths(option, prefix))
    #                 return merged
    #
    #         # L'array en si mateix sempre és vàlid
    #         allowed.add(array_prefix)
    #
    #         # Recorrem missing_date_list'esquema dels items
    #         allowed |= self._collect_allowed_paths(items_schema, array_prefix)
    #
    #         return allowed
    #
    #     # ---------------------------
    #     # TIPUS BÀSICS (string, number, boolean...)
    #     # ---------------------------
    #     if prefix:
    #         allowed.add(prefix)
    #
    #     return allowed


# # Funció recursiva de neteja
# def recursive_clean(value, type_, format_=None):
#     if value is None:
#         return None
#
#     try:
#         # Tipus simples
#         if type_ in ["string", "integer", "decimal", "date", "boolean"]:
#             # Aquí hi poses la lògica de neteja simple
#             # Exemple: trim, lower, validació de dates, etc.
#             return str(value).strip()
#
#         # Tipus complexos
#         elif type_ == "struct":
#             obj = json.loads(value)
#             cleaned_obj = {}
#             for k, v in obj.items():
#                 # Suposem que el format pot contenir subtipus
#                 sub_type = format_.get(k, {}).get("type", "string")
#                 sub_format = format_.get(k, {}).get("format", None)
#                 cleaned_obj[k] = recursive_clean(v, sub_type, sub_format)
#             return json.dumps(cleaned_obj)
#
#         elif type_ == "array":
#             arr = json.loads(value)
#             sub_type = format_.get("items", {}).get("type", "string")
#             sub_format = format_.get("items", {}).get("format", None)
#             cleaned_arr = [recursive_clean(v, sub_type, sub_format) for v in arr]
#             return json.dumps(cleaned_arr)
#
#         elif type_ == "map":
#             obj = json.loads(value)
#             sub_type = format_.get("values", {}).get("type", "string")
#             sub_format = format_.get("values", {}).get("format", None)
#             cleaned_obj = {k: recursive_clean(v, sub_type, sub_format) for k, v in obj.items()}
#             return json.dumps(cleaned_obj)
#
#         else:
#             # Tipus desconegut → retorna com string netejat
#             return str(value).strip()
#
#     except Exception:
#         # Si hi ha error de parseig, retorna el valor original netejat com string
#         return str(value).strip()
#
#
# # Wrapping en UDF
# def clean(value, type_, format_):
#     return recursive_clean(value, type_, format_)
#
# spark = SparkSession.builder.getOrCreate()
# spark.udf.register("clean", clean, StringType())
