import json
import re
import os
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
    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json, )
        schema_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', "config",  "schema.json"))
        mapping_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', "config", "mapping_to_clean_chars.json"))

        self._schema = {}
        self._mapping_to_clean_chars = {}
        # self._allowed_paths=None
        self._current_process_level = 1
        with open(schema_path) as f:
            self._schema = json.load(f)

        with open(mapping_path) as f:
            self._mapping_to_clean_chars = json.load(f)

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
            fields.add(f"{field_name}_{BoatFactDataModel.DETAILED_VALUE_FIELD}")
        return fields

    @staticmethod
    def _parse_as(_col, struct):
        # Només parsegem si el tipus esperat és STRUCT
        if not (isinstance(struct, StructType) or isinstance(struct, ArrayType)):
            return _col.cast(struct)
        # Convertim a string sempre
        s = _col.cast("string")

        # Detectem JSON
        looks_like_json = s.startswith("{") & s.endswith("}") | s.startswith("[") & s.endswith("]")

        # from_json mai llença error
        parsed = F.from_json(s, struct)
        return F.when(looks_like_json, parsed).otherwise(F.lit(None))

    @staticmethod
    def _resolve_column_path(root_col, ref):
        t = ref.get("type")
        v = ref.get("value")

        if t == "expr":
            return F.expr(v)

        if t == "literal":
            return F.lit(v)

        if t == "column":
            # Si el valor és exactament "old_value", retornem la columna arrel
            if v == "old_value":
                return root_col

            # Eliminem el prefix "old_value." si existeix
            clean_path = re.sub(r"^old_value\.", "", v)

            # Naveguem pel path
            parts = clean_path.split(".")
            res = root_col
            for p in parts:
                res = res[p]
            return res

        return F.lit(None)

    @staticmethod
    def _build_equivalence_expr(old_col, equivalence_rule):
        """
        Construeix l'expressió que transforma el valor 'castejat' (old_col)
        a la nova estructura definida a 'equivalence'.
        """
        eq_type = equivalence_rule.get("type")

        # Cas 1: Construir un nou Struct
        if eq_type == "struct":
            new_values = equivalence_rule.get("new_value", {})
            struct_fields = []
            for field_name, logic in new_values.items():
                # Cridem recursivament per a cada camp de l'struct
                col_expr = PortadaCleaning._resolve_column_path(old_col, logic).alias(field_name)
                struct_fields.append(col_expr)
            return F.struct(*struct_fields)
        else:
            return PortadaCleaning._resolve_column_path(old_col, equivalence_rule.get("new_value"))

    @staticmethod
    def _build_try_cast_logic(raw_col, normalize_spec):
        """
        Crea l'expressió complexa amb COALESCE per provar opcions.
        """
        if "options" not in normalize_spec:
            return PortadaCleaning._build_equivalence_expr(raw_col, normalize_spec["equivalence"])

        options_exprs = []
        # 1. Iterar per les opcions de 'try_cast_to'
        for option in normalize_spec.get("options", []):
            try_schema_json = option.get("try_cast_to")
            spark_schema, _ = PortadaCleaning.json_schema_to_spark_type(try_schema_json)
            parsed = PortadaCleaning._parse_as(raw_col, spark_schema)
            equivalence_expr = PortadaCleaning._build_equivalence_expr(parsed, option["equivalence"])
            # Si 'parsed' és null (el cast ha fallat), hem de retornar NULL explícitament
            # perquè el coalesce passi a la següent opció.
            options_exprs.append(F.when(parsed.isNotNull(), equivalence_expr))

        # 2. Gestionar el 'otherwise'
        if "otherwise" in normalize_spec:
            otherwise_spec = normalize_spec.get("otherwise")
            # Nota: L'otherwise sol treballar sobre l'string original cru
            otherwise_expr = PortadaCleaning._build_equivalence_expr(raw_col, otherwise_spec.get("equivalence"))
            options_exprs.append(otherwise_expr)

        # 3. Retornar el primer que no sigui null
        return F.coalesce(*options_exprs)

    @staticmethod
    def _build_normalize_expr_with_spec(raw_col, normalize_spec):
        norm_type = normalize_spec.get("type", "single")
        # options = normalize_spec.get("options", [])
        # otherwise = normalize_spec.get("otherwise")

        # CAS 1: FOREACH_ITEM (Arrays)
        if norm_type == "foreach_item":
            # Pas A: L'entrada és un string "[{},{}]". Primer el trenquem en Array<String>.
            # Això ens dóna un array on cada element és l'string JSON de l'objecte.
            array_of_strings = PortadaCleaning._parse_as(raw_col, ArrayType(StringType()))

            # Pas B: Utilitzem TRANSFORM per aplicar la lògica a cada element de l'array

            return F.transform(
                array_of_strings,
                lambda x: PortadaCleaning._build_try_cast_logic(x, normalize_spec)
            )

        # CAS 2: SINGLE (Objectes o valors simples)
        else:
            return PortadaCleaning._build_try_cast_logic(raw_col, normalize_spec)

    @staticmethod
    def _build_normalize_expr(raw_col, schema: dict):
        # Comprovar si hi ha lògica de normalització complexa
        if "normalize" in schema:
            print(f"Applying normalize logic for: {raw_col.alias()}")
            expr = PortadaCleaning._build_normalize_expr_with_spec(raw_col, schema["normalize"])
        else:
            # Lògica estàndard (Sense 'normalize')
            json_type = schema.get("type")

            target_schema, _ = PortadaCleaning.json_schema_to_spark_type(schema, primitive_types_as_strings=True)
            expr = PortadaCleaning._parse_as(raw_col, target_schema)
        return expr


    @staticmethod
    def _json_schema_to_normalize_columns_no(schema: dict, col_name: str = None):
        ret = {}

        def _parse_as(_col, struct):
            # Només parsegem si el tipus esperat és STRUCT
            if not (isinstance(struct, StructType) or isinstance(struct, ArrayType)):
                return _col.cast(struct)
            # Convertim a string sempre
            s = _col.cast("string")

            # Detectem JSON
            looks_like_json = s.startswith("{") & s.endswith("}") | s.startswith("[") & s.endswith("]")

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
            return None

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
                        _parse_as(F.col(_col_name), ArrayType(StringType())),
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

    def _json_schema_to_normalize_values(self, schema: dict, all_mapping: dict, current_expr):
        t = schema.get("type")
        # --- CASE: NUMBER OR DATE TYPE ---
        if t in ["integer", "number", "float"] or (t == "string" and schema.get("format") == "date"):
            mapping = all_mapping.get("numeric_map", {})
            return self.apply_ocr_corrections(current_expr, mapping)

        # --- CASE: STRINGS ---
        elif t == "string":
            if "for_cleaning" in schema:
                return self.apply_cleaning_process(current_expr, schema.get("for_cleaning"), all_mapping)
            else:
                return F.regexp_replace(F.trim(current_expr), r"[.,; \t]+$", "")

        # --- CASE: OBJECTS (STRUCTS) ---
        elif t == "object":
            properties = schema.get("properties", {})
            fields = []
            for f_name, f_schema in properties.items():
                transformed_field = self._json_schema_to_normalize_values(
                    f_schema, all_mapping, current_expr[f_name]
                )
                if transformed_field is not None:
                    fields.append(transformed_field.alias(f_name))

            # Retornem tot l'objecte reconstruït com un struct
            return F.struct(fields) if fields else current_expr

        # --- CASE: ARRAYS ---
        elif t == "array":
            items_schema = schema.get("items", {})
            return F.transform(
                current_expr,
                lambda x: self._json_schema_to_normalize_values(items_schema, all_mapping, x)
            )

        return current_expr

    @staticmethod
    def apply_ocr_corrections(col, mapping):
        wrong_chars = "".join(mapping.keys())
        correct_chars = "".join(mapping.values())

        return F.translate(col, wrong_chars, correct_chars)

    @staticmethod
    def apply_cleaning_process(col, for_cleaning_list: list, mapping: dict):
        def change_chars(c, m: dict = None):
            map_for_translate = {}
            map_for_replace = {}
            for k, v in m.items():
                if v and len(k)==1:
                    map_for_translate[k] = v
                else:
                    map_for_replace[k] = v
            wrong_chars = "".join(map_for_translate.keys())
            correct_chars = "".join(map_for_translate.values())
            expr_col = c
            for w, r in map_for_replace.items():
                if len(w)==1:
                    w = re.escape(w)
                expr_col = F.regexp_replace(expr_col, w, r)
            return F.translate(expr_col, wrong_chars, correct_chars)

        def one_word(c, m: dict = None):
            return change_chars(c, m.get("one_word_map", {}))

        def not_digit(c, m: dict = None):
            return change_chars(c, m.get("only_text_map", {}))

        def not_paragraph(c, m: dict = None):
            return change_chars(c, m.get("not_paragraph_map", {}))

        processes = {"one_word":one_word, "not_digits":not_digit}
        if "paragraph"  in for_cleaning_list or "not_cleanable" in for_cleaning_list:
            expr = col
        elif "accepted_abbreviations" in for_cleaning_list:
            expr = F.trim(col)
            expr = F.when(F.length(expr) > 5, F.regexp_replace(expr, r"[.,;]+$", "")).otherwise(expr)
        else:
            expr = F.regexp_replace(F.trim(col), r"[.,;]+$", "")
        if "paragraph" not in for_cleaning_list and "one_word" not in for_cleaning_list:
            not_paragraph(expr, mapping)
        for process in for_cleaning_list:
            if process in processes:
                expr = processes[process](expr, mapping)
        return expr

    @staticmethod
    def json_schema_to_spark_type(schema: dict, primitive_types_as_strings: bool = False):
        schema_type = schema.get("type")
        nullable = schema.get("nullable", True)

        # --- Tipus primitius ---
        if schema_type == "string":
            if not primitive_types_as_strings:
                fmt = schema.get("format")
                if fmt == "date":
                    return DateType(), nullable
                elif fmt in ("date-time", "datetime"):
                    return TimestampType(), nullable
            return StringType(), nullable

        if schema_type == "integer":
            # Recomanable LongType per dades històriques
            if primitive_types_as_strings:
                return StringType(), nullable
            return LongType(), nullable

        if schema_type == "number":
            if primitive_types_as_strings:
                return StringType(), nullable
            return DoubleType(), nullable

        if schema_type == "boolean":
            if primitive_types_as_strings:
                return StringType(), nullable
            return BooleanType(), nullable

        # --- Objecte ---
        if schema_type == "object":
            fields = []
            properties = schema.get("properties", {})
            required = set(schema.get("required", []))

            for field_name, field_schema in properties.items():
                field_type, field_nullable = PortadaCleaning.json_schema_to_spark_type(field_schema, primitive_types_as_strings)

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

            element_type, _ = PortadaCleaning.json_schema_to_spark_type(items_schema, primitive_types_as_strings)
            return ArrayType(element_type), nullable

        raise ValueError(f"Unsupported JSON schema type: {schema_type}")

    def use_schema(self, json_schema: dict):
        self._schema = json_schema
        return self

    def use_mapping_to_clean_chars(self, mapping: dict):
        self._mapping_to_clean_chars = mapping
        return self



    @block_transformer_method
    def cleaning(self, df: DataFrame | TracedDataFrame) -> TracedDataFrame:
        # 1.- save original values
        df = self.save_original_values_of_ship_entries(df)
        # 2.- prune values
        df = self.prune_unaccepted_fields(df)
        # 3.- Normalize structure
        df = self.normalize_field_structure(df)
        # 3.- Normalize values
        df = self.normalize_field_value(df)
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
        # if self._schema is None:
        #     raise ValueError("Cal cridar use_schema() abans.")
        # f_columns = self._json_schema_to_normalize_columns_no(self._schema)
        # for c_name, col_trans in f_columns.items():
        #     df = df.withColumn(c_name, col_trans)
        # return df
        if self._schema is None:
            raise ValueError("Cal cridar use_schema() abans.")
        final_expressions = {}
        properties = self._schema.get("properties", {})
        selects = []
        for field_name, field_schema in properties.items():
            # Obtenim l'expressió completa per a tota la columna (sigui simple, struct o array)
            expr = self._build_normalize_expr(F.col(field_name), field_schema)
            selects.append(expr.alias(field_name))
        return df.select(*selects)

    @data_transformer_method()
    def normalize_field_value(self, df: TracedDataFrame) -> TracedDataFrame:
        if self._schema is None:
            raise ValueError("Cal cridar use_schema() abans.")
        properties = self._schema.get("properties", {})

        for field_name, field_schema in properties.items():
            # Obtenim l'expressió completa per a tota la columna (sigui simple, struct o array)
            expr = self._json_schema_to_normalize_values(field_schema, self._mapping_to_clean_chars, F.col(field_name))
            df = df.withColumn(field_name, expr)
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

