import json
import re
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, StructField, DateType, TimestampType, LongType, DoubleType, BooleanType, \
    StructType, ArrayType
from portada_data_layer import DeltaDataLayer, TracedDataFrame, block_transformer_method
from portada_data_layer.boat_fact_model import BoatFactDataModel
from portada_data_layer.data_lake_metadata_manager import data_transformer_method, LineageCheckingType, \
    enable_field_lineage_log_for_class
from portada_data_layer.portada_delta_common import registry_to_portada_builder


@registry_to_portada_builder
@enable_field_lineage_log_for_class
class PortadaCleaning(DeltaDataLayer):
    # ARRAY_INDEX_REGEX = re.compile(r"\[\d+]")  # exemple: a[0].b → a[*].b

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json, )
        self._schema = {}
        # self._allowed_paths=None
        self._current_process_level = 1

    # @staticmethod
    # def prune_unaccepted_fields_in_row_udf(col_name, schema):
    #     sub_schema = schema[col_name]
    #     @F.udf(StringType())
    #     def prune_unaccepted_fields_in_row(value):
    #         if isinstance(value, Row):

    @staticmethod
    def _collect_list_of_fields(schema: dict) -> set:
        fields = set()
        if "schema" in schema:
            schema = schema["schema"]

        if "properties" in schema:
            props = schema.get("properties", {})
        else:
            props = schema
        for field_name in props:
            fields.add(field_name)
            fields.add(f"{field_name}_{BoatFactDataModel.EXTRACTION_SOURCE_METHOD_FIELD}")
            fields.add(f"{field_name}_{BoatFactDataModel.MORE_EXTRACTED_VALUES_FIELD}")
        return fields

    @staticmethod
    def json_schema_to_normalize_columns(schema: dict, col_name: str = None):
        ret = {}

        def _resolve_value(ref, _col):
            if ref["type"]=="expr":
                return F.expr(ref["value"])
            if ref["type"]=="column":
                _resolved_value = _col
                ref_txt = re.sub("^old_value\.?", "", ref["value"], 1)
                a_ref = ref_txt.split(".") if ref_txt else []
                for r in a_ref:
                    _resolved_value = _resolved_value[r]
                return _resolved_value
            if ref["type"]=="literal":
                return F.lit(ref["value"])

        def _normalize(_col, _norm_props: dict):
            if "options" in _norm_props:
                expr = None
                for option in _norm_props["options"]:
                    struct, _ = PortadaCleaning.json_schema_to_spark_type(option["try_cast_to"])
                    cast_condition = _col.isNotNull() & _col.cast(struct).isNotNull()
                    if option["equivalence"]["type"]=="struct":
                        par = [_resolve_value(str_ref, _col) for k, str_ref in option["equivalence"]["new_value"].items()]
                        new_value = F.struct(*par)
                    else:
                        new_value = _resolve_value(option["equivalence"]["new_value"], _col)
                    if expr:
                        expr.when(
                            cast_condition,
                            new_value
                        )
                    else:
                        expr = F.when(
                            cast_condition,
                            new_value
                        )
                if "otherwise" in _norm_props:
                    if _norm_props["otherwise"]["equivalence"]["type"]=="struct":
                        par = [_resolve_value(str_ref, _col) for k, str_ref in _norm_props["otherwise"]["equivalence"]["new_value"].items()]
                        new_value = F.struct(*par)
                    else:
                        new_value = _resolve_value(_norm_props["otherwise"]["equivalence"]["new_value"], _col)
                    expr.otherwise(new_value)
            else:
                expr = _resolve_value(_norm_props["equivalence"], _col)
            return expr

        def _json_schema_to_normalize_columns(_schema: dict, _col_name: str, _ret: dict):
            if "normalize" in _schema:
                normalize_properties = _schema.get("normalize")
                if normalize_properties["type"] == "foreach_item":
                    # normalize with transform
                    _ret[_col_name] = F.transform(F.col(_col_name), lambda x: _normalize(x, normalize_properties))
                else:  # normalize_properties["type"] == "value":
                    # normalize
                    _ret[_col_name] = _normalize(F.col(col_name), normalize_properties)
            elif _schema.get("type") == "object":
                properties = _schema.get("properties", {})
                for field_name, field_schema in properties.items():
                    if _col_name is not None:
                        fn = f"{_col_name}.{field_name}"
                    else:
                        fn = field_name
                    _ret = _json_schema_to_normalize_columns(field_schema, fn, _ret)
            return _ret

        ret = _json_schema_to_normalize_columns(schema, col_name, ret)
        return ret

    @staticmethod
    def json_schema_to_spark_type(schema: dict):
        schema_type = schema.get("type")
        nullable = schema.get("nullable", True)

        # --- Tipus primitius ---
        if schema_type == "string":
            fmt = schema.get("format")
            if fmt == "date":
                return DateType(), nullable
            elif fmt in ("date-time", "datetime"):
                return TimestampType(), nullable
            return StringType(), nullable

        if schema_type == "integer":
            # Recomanable LongType per dades històriques
            return LongType(), nullable

        if schema_type == "number":
            return DoubleType(), nullable

        if schema_type == "boolean":
            return BooleanType(), nullable

        # --- Objecte ---
        if schema_type == "object":
            fields = []
            properties = schema.get("properties", {})
            required = set(schema.get("required", []))

            for field_name, field_schema in properties.items():
                field_type, field_nullable = PortadaCleaning.json_schema_to_spark_type(field_schema)

                # JSON Schema: si és required → nullable = False
                if field_name in required:
                    field_nullable = False

                fields.append(
                    StructField(field_name, field_type, field_nullable)
                )

            return StructType(fields), nullable

        # --- Array ---
        if schema_type == "array":
            items_schema = schema.get("items")
            if not items_schema:
                raise ValueError("Array schema must define 'items'")

            element_type, _ = PortadaCleaning.json_schema_to_spark_type(items_schema)
            return ArrayType(element_type), nullable

        raise ValueError(f"Unsupported JSON schema type: {schema_type}")

    # normalize_foreach_item:{
    # options:[
    #   {
    #      try_cast:{
    #         type:{}
    #      }
    #      equivalence:{
    #         new_item: {
    #            "a":"item.a",
    #            "b":"item.c",
    #            "c": null
    # 	      }
    #      }
    #   }
    # ]
    # }

    # normalize_foreach_item:{
    # equivalence:{
    #   new_item: {
    #      "a":"item.a",
    #       "b":"item.c",
    #       "c": null
    # 	 }
    # }
    # }

    # normalize:{
    #   equivalence:{
    #     new_value: {
    #       "a":"value.a",
    #       "b":"value.c",
    #       "c": null
    # 	  }
    #   }
    # }

    # normalize:{
    # options:[
    #   {
    #      try_cast:{
    #         type:{}
    #      }
    #      equivalence:{
    #         new_item: {
    #            type:"struct"
    #            value:{
    #               "a":"item.a",
    #               "b":"item.c",
    #               "c": null
    #             }
    # 	      }
    #      }
    #   }
    # ]
    # }

    # {
    #     try_cast:{
    #         type:{}
    #     },
    #     equivalence:{
    #         new_value:{
    #           type:"struct"
    #         }
    #     }
    # }

    # Abans però, per tal de normalitzar els tipus d'una forma estandarditzada, voldria que aquells camps que ho requerissin tinguessin, dins del schema json un camp(al mateix nivell que "type" anomenat normalize:
    # cast_as({"type": "array", "items": {"type": "string"}}).isNotNull -> for_each(items, item = {"aaaa":item, "bbbb":null})
    # cast_as({"type": "array", "items": {"type":"object", "properties":{"ccc":{"type":"string"}, "dddd":{"type":"string"}}}}).isNotNull -> for_each(items, item = {"aaaa":item.cccc, "bbbb":item.dddd})
    # { if: {cast:, return:"isNotNull", actions: [cast]}, if: {
    #     cast_as: {"type": "array", "items": {"type": "string"}}
    # normalize: action
    # df = df.withColumn(
    #     "travel_port_of_call_name_list",
    #     F.transform(
    #         "travel_port_of_call_list",
    #         lambda x: F.when(
    #             x.isNotNull() & x.cast("string").isNotNull(),
    #             x.cast("string")
    #         ).otherwise(x["port_of_call_place"])
    #     )
    # ).withColumn(
    #     "travel_port_of_call_arrival_date_list",
    #     F.transform(
    #         "travel_port_of_call_list",
    #         lambda x: x["port_of_call_arrival_date"]
    #     )
    # )

    def use_schema(self, json_schema: dict):
        self._schema = json_schema
        return self

    @block_transformer_method
    def cleaning(self, df: DataFrame | TracedDataFrame) -> TracedDataFrame:
        # 1.- save original values
        df = self.save_original_values_of_ship_entries(df)
        # 2.- prune values
        df = self.prune_unaccepted_fields(df)
        # 3.- Normalice values
        df = self.normalize_field_structure(df)
        return df

    @data_transformer_method()
    def save_original_values_of_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        self.write_delta("original_data", "ship_entries", df=ship_entries_df, mode="overwrite",
                         partition_by=["publication_name", "publication_date_year", "publication_date_month"])
        return ship_entries_df

    def save_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        self.write_delta("ship_entries", mode="overwrite",
                         partition_by=["publication_name", "publication_date_year", "publication_date_month"])
        return ship_entries_df

    @data_transformer_method()
    def normalize_field_structure(self, df: TracedDataFrame) -> TracedDataFrame:
        if self._schema is None:
            raise ValueError("Cal cridar use_schema() abans.")
        f_columns = self.json_schema_to_normalize_columns(self._schema)
        for c_name, col_trans in f_columns.items():
            df = df.withColumn(c_name, col_trans)
        return df


    @data_transformer_method(description="prune unbelonging model fields ")
    def prune_unaccepted_fields(self, df: TracedDataFrame) -> TracedDataFrame:
        if self._schema is None:
            raise ValueError("Cal cridar use_schema() abans.")
        allowed = self._collect_list_of_fields(self._schema)
        cols_to_keep = [
            col for col in df.columns if col in allowed
        ]
        missing_cols = allowed - set(cols_to_keep)
        cols_to_add = {
            col: F.lit(None) for col in missing_cols
        }

        return df.select(cols_to_keep).withColumns(cols_to_add)

        #
        # def normalize_from_boat_fact_model(self, df: TracedDataFrame) -> TracedDataFrame:
        #     par = {
        #         col: F.transform(
        #             F.col(col),
        #             lambda x: F.when(
        #                 x.isNotNull() & x.cast("string").isNotNull(),
        #                 x.cast("string")
        #             ).when(x.isNotNull(),
        #     #                get_value_of(value):
        #     # if value is None:
        #     #     ret = ""
        #     # elif BoatFactDataModel.is_structured_value(value):
        #     #     if
        #     # BoatFactDataModel.STACKED_VALUE in value:
        #     # ret = value[BoatFactDataModel.STACKED_VALUE][-1] if value[BoatFactDataModel.STACKED_VALUE] else None
        #     # elif BoatFactDataModel.CALCULATED_VALUE_FIELD in value:
        #     # ret = value[BoatFactDataModel.CALCULATED_VALUE_FIELD]
        #     # elif BoatFactDataModel.ORIGINAL_VALUE_FIELD in value:
        #     # ret = value[BoatFactDataModel.ORIGINAL_VALUE_FIELD]
        #     # elif BoatFactDataModel.DEFAULT_VALUE_FIELD in value:
        #     # ret = value[BoatFactDataModel.DEFAULT_VALUE_FIELD]
        #     # else:
        #     # ret = ""
        #     # else:
        #     # ret = value
        #     # return str(ret)
        #             )
        #         )
        #         for col in df.columns:
        #     }

        df = df.withColumn(

        )

    # def _normalize_column(self, col: str) -> str:
    #     """
    #     Normalitza columnes Spark, convertint:
    #         a[0].b → a[*].b
    #         a[12].x.y → a[*].x.y
    #     """
    #     return self.ARRAY_INDEX_REGEX.sub("[*]", col)

    # def _collect_allowed_paths(self, node, prefix=""):
    #     """
    #     Returns all allowed flattened paths from a JSON Schema.
    #     Supports: object, array, oneOf, anyOf, allOf.
    #     """
    #     if "schema" in node:
    #         node = node["schema"]
    #
    #     paths = []
    #
    #     # Normalize prefix
    #     p = prefix + "." if prefix else ""
    #
    #     # 1) If schema has oneOf / anyOf / allOf → merge results from sub-schemas
    #     combinators = ["oneOf", "anyOf", "allOf"]
    #     for key in combinators:
    #         if key in node:
    #             merged = []
    #             for option in node[key]:
    #                 merged.extend(self._collect_allowed_paths(option, prefix))
    #             return merged  # This is final for this node
    #
    #     # 2) Object
    #     if node.get("type") == "object":
    #         props = node.get("properties", {})
    #         for field_name, field_schema in props.items():
    #             full = f"{p}{field_name}"
    #             paths.append(full)  # include the object field itself
    #             paths.extend(self._collect_allowed_paths(field_schema, full))
    #         return paths
    #
    #     # 3) Array
    #     if node.get("type") == "array":
    #         items = node.get("items")
    #
    #         if items is None:
    #             return paths  # no detail, cannot descend
    #
    #         # Case: items contains oneOf / anyOf / allOf
    #         for key in combinators:
    #             if key in items:
    #                 merged = []
    #                 for option in items[key]:
    #                     merged.extend(self._collect_allowed_paths(option, prefix))
    #                 return merged
    #
    #         # Case: items is a single schema
    #         return self._collect_allowed_paths(items, prefix)
    #
    #     # 4) Primitive types (string, number, boolean…)
    #     #    → leaf node, prefix already represents the full path
    #     return paths

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
