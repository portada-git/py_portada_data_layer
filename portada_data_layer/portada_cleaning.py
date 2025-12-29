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
    def _json_schema_to_normalize_columns(schema: dict, col_name: str = None):
        ret = {}

        def _parse_as(_col, struct):
            # Només parsegem si el tipus esperat és STRUCT
            if not isinstance(struct, StructType):
                return _col.cast(struct)
            # Convertim a string sempre
            s = _col.cast("string")

            # Detectem JSON
            looks_like_json = s.startswith("{") & s.endswith("}")

            # from_json mai llença error
            parsed = F.from_json(s, struct)
            return F.when(looks_like_json, parsed).otherwise(F.lit(None))

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
            if "options" not in _norm_props:
                return _resolve_value(_norm_props["equivalence"], _col)

            expr = None
            for option in _norm_props["options"]:
                struct, _ = PortadaCleaning.json_schema_to_spark_type(option["try_cast_to"])
                parsed = _parse_as(_col, struct)
                cast_condition = _col.isNotNull() & parsed.isNotNull()

                if option["equivalence"]["type"] == "struct":
                    fields = []
                    for k, str_ref in option["equivalence"]["new_value"].items():
                        fields.append(_resolve_value(str_ref, parsed).alias(k))
                    new_value = F.struct(*fields)
                else:
                    new_value = _resolve_value(option["equivalence"]["new_value"], parsed)
                expr = F.when(cast_condition, new_value) if expr is None else expr.when(cast_condition, new_value)

            # OTHERWISE
            if "otherwise" in _norm_props:
                if _norm_props["otherwise"]["equivalence"]["type"] == "struct":
                    fields = []
                    for k, str_ref in _norm_props["otherwise"]["equivalence"]["new_value"].items():
                        fields.append(_resolve_value(str_ref, _col).alias(k))
                    new_value = F.struct(*fields)
                else:
                    new_value = _resolve_value(_norm_props["otherwise"]["equivalence"]["new_value"], _col)
                expr = expr.otherwise(new_value)
            return expr

        def _json_schema_to_normalize_columns(_schema: dict, _col_name: str, _ret: dict):
            if "normalize" in _schema:
                normalize_properties = _schema.get("normalize")
                if normalize_properties["type"] == "foreach_item":
                    _ret[_col_name] = F.transform(
                        F.col(_col_name),
                        lambda x: _normalize(x, normalize_properties)
                    )
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
        f_columns = self._json_schema_to_normalize_columns(self._schema)
        for c_name, col_trans in f_columns.items():
            df = df.withColumn(c_name, col_trans)
        return df

    @data_transformer_method()
    def normalize_field_value(self, df: TracedDataFrame) -> TracedDataFrame:
        if self._schema is None:
            raise ValueError("Cal cridar use_schema() abans.")
        f_columns = self._json_schema_to_normalize_values(self._schema)
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

