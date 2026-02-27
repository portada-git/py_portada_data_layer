import re
import os
import logging

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, DateType, TimestampType, LongType, DoubleType, BooleanType, \
    StructType, ArrayType
from portada_data_layer import DeltaDataLayer, TracedDataFrame, block_transformer_method
from portada_data_layer.boat_fact_model import BoatFactDataModel
from portada_data_layer.data_lake_metadata_manager import data_transformer_method, LineageCheckingType, \
    enable_field_lineage_log_for_class
from portada_data_layer.portada_delta_common import registry_to_portada_builder

logger = logging.getLogger("portada_data.delta_data_layer.boat_fact_cleaning")


IDEM_TOKENS = ["idem", "id.", "id", "1dem"]


@registry_to_portada_builder
@enable_field_lineage_log_for_class
class PortadaCleaning(DeltaDataLayer):

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json, )
        # schema_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', "config",  "schema.json"))
        # mapping_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', "config", "mapping_to_clean_chars.json"))

        self._schema = {}
        self._mapping_to_clean_chars = {}
        # self._allowed_paths=None
        self._current_process_level = 1
        # with open(schema_path) as f:
        #     self._schema = json.load(f)
        #
        # with open(mapping_path) as f:
        #     self._mapping_to_clean_chars = json.load(f)

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
        # Only parse if the expected type is STRUCT
        if not (isinstance(struct, StructType) or isinstance(struct, ArrayType)):
            return _col.cast(struct)
        # Always convert to string
        s = _col.cast("string")

        # Detect JSON
        looks_like_json = s.startswith("{") & s.endswith("}") | s.startswith("[") & s.endswith("]")

        # from_json never throws
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
            # If the value is exactly "old_value", return the root column
            if v == "old_value":
                return root_col

            # Remove the "old_value." prefix if it exists
            clean_path = re.sub(r"^old_value\.", "", v)

            # Navigate the path
            parts = clean_path.split(".")
            res = root_col
            for p in parts:
                res = res[p]
            return res

        return F.lit(None)

    @staticmethod
    def _build_equivalence_expr(old_col, equivalence_rule):
        """
        Build the expression that transforms the 'cast' value (old_col)
        to the new structure defined in 'equivalence'.
        """
        eq_type = equivalence_rule.get("type")

        # Case 1: Build a new Struct
        if eq_type == "struct":
            new_values = equivalence_rule.get("new_value", {})
            struct_fields = []
            for field_name, logic in new_values.items():
                # Call recursively for each field of the struct
                col_expr = PortadaCleaning._resolve_column_path(old_col, logic).alias(field_name)
                struct_fields.append(col_expr)
            return F.struct(*struct_fields)
        else:
            return PortadaCleaning._resolve_column_path(old_col, equivalence_rule.get("new_value"))

    @staticmethod
    def _build_try_cast_logic(raw_col, normalize_spec):
        """
        Create the complex expression with COALESCE to try options.
        """
        if "options" not in normalize_spec:
            return PortadaCleaning._build_equivalence_expr(raw_col, normalize_spec["equivalence"])

        options_exprs = []
        # 1. Iterate over the 'try_cast_to' options
        for option in normalize_spec.get("options", []):
            try_schema_json = option.get("try_cast_to")
            spark_schema, _ = PortadaCleaning.json_schema_to_spark_type(try_schema_json)
            parsed = PortadaCleaning._parse_as(raw_col, spark_schema)
            equivalence_expr = PortadaCleaning._build_equivalence_expr(parsed, option["equivalence"])
            # If 'parsed' is null (cast failed), we must return NULL explicitly
            # so that coalesce moves to the next option.
            options_exprs.append(F.when(parsed.isNotNull(), equivalence_expr))

        # 2. Handle the 'otherwise'
        if "otherwise" in normalize_spec:
            otherwise_spec = normalize_spec.get("otherwise")
            # Note: The otherwise typically works on the raw original string
            otherwise_expr = PortadaCleaning._build_equivalence_expr(raw_col, otherwise_spec.get("equivalence"))
            options_exprs.append(otherwise_expr)

        # 3. Return the first one that is not null
        return F.coalesce(*options_exprs)

    @staticmethod
    def _build_normalize_expr_with_spec(raw_col, normalize_spec):
        norm_type = normalize_spec.get("type", "single")
        # options = normalize_spec.get("options", [])
        # otherwise = normalize_spec.get("otherwise")

        # CASE 1: FOREACH_ITEM (Arrays)
        if norm_type == "foreach_item":
            # Step A: The input is a string "[{},{}]". First we split it into Array<String>.
            # This gives us an array where each element is the JSON string of the object.
            array_of_strings = PortadaCleaning._parse_as(raw_col, ArrayType(StringType()))

            # Step B: Use TRANSFORM to apply the logic to each element of the array

            return F.transform(
                array_of_strings,
                lambda x: PortadaCleaning._build_try_cast_logic(x, normalize_spec)
            )

        # CASE 2: SINGLE (Objects or simple values)
        else:
            return PortadaCleaning._build_try_cast_logic(raw_col, normalize_spec)

    @staticmethod
    def _build_normalize_expr(raw_col, schema: dict):
        # Check if there is complex normalization logic
        if "normalize" in schema:
            print(f"Applying normalize logic for: {raw_col.alias()}")
            expr = PortadaCleaning._build_normalize_expr_with_spec(raw_col, schema["normalize"])
        else:
            # Standard logic (without 'normalize')
            json_type = schema.get("type")

            target_schema, _ = PortadaCleaning.json_schema_to_spark_type(schema, primitive_types_as_strings=True)
            expr = PortadaCleaning._parse_as(raw_col, target_schema)
        return expr


    @staticmethod
    def _json_schema_to_normalize_columns_no(schema: dict, col_name: str = None):
        ret = {}

        def _parse_as(_col, struct):
            # Only parse if the expected type is STRUCT
            if not (isinstance(struct, StructType) or isinstance(struct, ArrayType)):
                return _col.cast(struct)
            # Always convert to string
            s = _col.cast("string")

            # Detect JSON
            looks_like_json = s.startswith("{") & s.endswith("}") | s.startswith("[") & s.endswith("]")

            # from_json never throws
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

            # Return the whole object reconstructed as a struct
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

        def transform_mapping_with_params(mapping: dict, params: dict):
            return mapping

        def one_word(c, m: dict = None, params: dict = None):
            m = transform_mapping_with_params(m.get("one_word_map", {}), params)
            return change_chars(c, m)

        def not_digit(c, m: dict = None, params: dict = None):
            m = transform_mapping_with_params(m.get("one_word_map", {}), params)
            return change_chars(c, m)

        def not_paragraph(c, m: dict = None, params: dict = None):
            m = transform_mapping_with_params(m.get("one_word_map", {}), params)
            return change_chars(c, m)

        params_for_cleaning_list = []
        alg_for_cleaning_list = []
        for item in for_cleaning_list:
            if isinstance(item, dict):
                alg_for_cleaning_list.append(item["algorithm"])
                params_for_cleaning_list.append(item.get("params", None))
            else:
                alg_for_cleaning_list.append(item)
                params_for_cleaning_list.append(None)

        processes = {"one_word":one_word, "not_digits":not_digit}
        if "paragraph" in alg_for_cleaning_list or "not_cleanable" in alg_for_cleaning_list:
            expr = col
        elif "accepted_abbreviations" in alg_for_cleaning_list:
            expr = F.trim(col)
            expr = F.when(F.length(expr) > 5, F.regexp_replace(expr, r"[.,;]+$", "")).otherwise(expr)
        else:
            expr = F.regexp_replace(F.trim(col), r"[.,;]+$", "")
        if "paragraph" not in alg_for_cleaning_list and "one_word" not in alg_for_cleaning_list:
            not_paragraph(expr, mapping)
        for process in alg_for_cleaning_list:
            if process in processes:
                expr = processes[process](expr, mapping)
        return expr

    @staticmethod
    def json_schema_to_spark_type(schema: dict, primitive_types_as_strings: bool = False):
        schema_type = schema.get("type")
        nullable = schema.get("nullable", True)

        # --- Primitive types ---
        if schema_type == "string":
            if not primitive_types_as_strings:
                fmt = schema.get("format")
                if fmt == "date":
                    return DateType(), nullable
                elif fmt in ("date-time", "datetime"):
                    return TimestampType(), nullable
            return StringType(), nullable

        if schema_type == "integer":
            # LongType recommended for historical data
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

        # --- Object ---
        if schema_type == "object":
            fields = []
            properties = schema.get("properties", {})
            required = set(schema.get("required", []))

            for field_name, field_schema in properties.items():
                field_type, field_nullable = PortadaCleaning.json_schema_to_spark_type(field_schema, primitive_types_as_strings)

                # JSON Schema: if required → nullable = False
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


    @staticmethod
    def _has_accepted_idem(schema: dict) -> bool:
        """Check if the field has 'accepted_idem' in for_cleaning."""
        for_cleaning = schema.get("for_cleaning", [])
        for item in for_cleaning:
            alg = item["algorithm"] if isinstance(item, dict) else item
            if alg == "accepted_idem":
                return True
        return False

    @staticmethod
    def _is_simple_type(t: str) -> bool:
        """Check if type is a simple/primitive (string, number, integer, boolean)."""
        return t in ("string", "number", "integer", "float", "boolean")

    @staticmethod
    def _collect_idems_for_spark(
            schema: dict,
            path: str = "",
            is_inside_array: bool = False,
            current_array_path: str = None,
            result: dict = None
    ) -> dict:
        """
        Separa els camps amb 'accepted_idem' en dues categories per a Spark.
        """
        if result is None:
            result = {"root_fields": [], "arrays": {}}

        field_type = schema.get("type")

        # 1. CAS OBJECTE: Explorem les seves propietats
        if field_type == "object":
            props = schema.get("properties", {})
            for name, sub_schema in props.items():
                new_path = f"{path}.{name}" if path else name
                PortadaCleaning._collect_idems_for_spark(
                    sub_schema, new_path, is_inside_array, current_array_path, result
                )

        # 2. CAS ARRAY: Marquem que entrem en zona 'Nested'
        elif field_type == "array":
            items = schema.get("items", {})
            # Registrem l'array al diccionari de resultats si no hi és
            if path not in result["arrays"]:
                result["arrays"][path] = {
                    "items_type": items.get("type"),
                    "schema": items,
                    "fields": [],
                    "all_struct_fields": list(items.get("properties", {}).keys()) if items.get(
                        "type") == "object" else []
                }

            # Continuem la recursió DINS de l'array
            PortadaCleaning._collect_idems_for_spark(
                items, path, is_inside_array=True, current_array_path=path, result=result
            )

        # 3. CAS CAMP FINAL (String/Simple): Mirem si té l'etiqueta
        else:
            if PortadaCleaning._has_accepted_idem(schema):
                if is_inside_array:
                    # Si estem dins d'un array, el camp s'afegeix a la llista d'aquell array
                    # Només guardem el nom relatiu del camp dins de l'estructura
                    field_name = path.split(".")[-1] if "." in path else path
                    if field_name not in result["arrays"][current_array_path]["fields"]:
                        result["arrays"][current_array_path]["fields"].append(field_name)
                else:
                    # Si és primer nivell, va a la llista de Window
                    result["root_fields"].append(path)

        return result

    # @staticmethod
    # def _collect_fields_with_accepted_idem(
    #         schema: dict,
    #         parent_path: str = "",
    #         result: list = None
    # ) -> list:
    #     if result is None:
    #         result = []
    #
    #     field_type = schema.get("type")
    #
    #     # CAS 1: És un OBJECTE
    #     if field_type == "object":
    #         props = schema.get("properties", {})
    #         for name, sub_schema in props.items():
    #             path = f"{parent_path}.{name}" if parent_path else name
    #             PortadaCleaning._collect_fields_with_accepted_idem(sub_schema, path, result)
    #
    #     # CAS 2: És un ARRAY
    #     elif field_type == "array":
    #         items = schema.get("items", {})
    #         # Si els items de l'array tenen l'etiqueta o contenen objectes amb l'etiqueta
    #         target_fields = PortadaCleaning._get_fields_to_clean(items)
    #
    #         if target_fields:
    #             result.append({
    #                 "path": parent_path,
    #                 "type": "array",
    #                 "items_type": items.get("type"),
    #                 "fields_to_check": target_fields  # Llista de noms de camps (o None si és simple)
    #             })
    #
    #         # Continuem explorant recursivament dins dels items per si hi ha arrays aniuats
    #         PortadaCleaning._collect_fields_with_accepted_idem(items, parent_path, result)
    #
    #     return result
    #
    # @staticmethod
    # def _get_fields_to_clean(item_schema: dict) -> list:
    #     """Retorna quins camps dins d'un element d'array s'han de netejar."""
    #     fields = []
    #     if item_schema.get("type") == "object":
    #         props = item_schema.get("properties", {})
    #         for name, sch in props.items():
    #             if PortadaCleaning._has_accepted_idem(sch):
    #                 fields.append(name)
    #     elif PortadaCleaning._has_accepted_idem(item_schema):
    #         fields.append(None)  # Indica que l'array és de tipus simple (ex: list[str])
    #     return fields

    # @staticmethod
    # def _collect_fields_with_accepted_idem(
    #         schema: dict,
    #         parent_path: str = "",
    #         result: list = None,
    #         array_context: dict = None
    # ) -> list:
    #     """
    #     Traverse the schema and return the list of (path, field_schema, meta)
    #     for fields marked with accepted_idem in for_cleaning.
    #     Uses the 'type' attribute to deduce structure:
    #     - Array of simple types: first item gets last of previous row; others get previous in same row.
    #     - Array of structs: same logic, but only the idem field is replaced, not the whole item.
    #     """
    #     if result is None:
    #         result = []
    #
    #     # 1. Obtenir les propietats depenent de l'estructura del nivell actual
    #     # Si és un objecte, mirem 'properties'. Si no, potser som directament a l'esquema d'un camp.
    #     properties = schema.get("properties", {})
    #
    #     # 2. Si no té propietats però és un tipus simple amb l'etiqueta, l'anotem
    #     # (Això gestiona el cas de recursió cap a camps fulla)
    #     if not properties:
    #         if PortadaCleaning._has_accepted_idem(schema):
    #             result.append((parent_path, schema, array_context))
    #         return result
    #
    #     # 3. Iterar sobre cada camp de l'objecte actual
    #     for field_name, field_schema in properties.items():
    #         current_path = f"{parent_path}.{field_name}" if parent_path else field_name
    #         field_type = field_schema.get("type")
    #
    #         # CAS A: És un camp simple amb l'etiqueta
    #         if PortadaCleaning._is_simple_type(field_type) and PortadaCleaning._has_accepted_idem(field_schema):
    #             result.append((current_path, field_schema, array_context))
    #
    #         # CAS B: És un Objecte (Recursió estàndard)
    #         elif field_type == "object":
    #             PortadaCleaning._collect_fields_with_accepted_idem(
    #                 field_schema, current_path, result, array_context
    #             )
    #
    #         # CAS C: És un Array (Recursió amb context de col·lecció)
    #         elif field_type == "array":
    #             items = field_schema.get("items", {})
    #             items_type = items.get("type")
    #
    #             # Preparem el context de l'array per als fills
    #             new_array_context = {
    #                 "array_col": current_path,
    #                 "items_type": "simple" if PortadaCleaning._is_simple_type(items_type) else "object"
    #             }
    #
    #             if new_array_context["items_type"] == "object":
    #                 new_array_context["struct_fields"] = list(items.get("properties", {}).keys())
    #                 # Nota: No posem "idem_field" aquí perquè el trobarem en la següent iteració recursiva
    #
    #             # Cridem recursivament sobre el contingut de l'array ('items')
    #             # El path no canvia (o s'hi podria afegir '[]'), però passem el context
    #             PortadaCleaning._collect_fields_with_accepted_idem(
    #                 items, current_path, result, new_array_context
    #             )
    #
    #     return result

    def _build_resolve_idem_expr_for_column(
        self, col_expr, lag_col, idem_values: tuple = None
    ):
        """Build the expression that replaces idem/id/id. with the predecessor value."""
        idem_values = idem_values or IDEM_TOKENS
        trimmed_lower = F.trim(F.lower(F.col(col_expr) if isinstance(col_expr, str) else col_expr))
        is_idem = trimmed_lower.isin(idem_values)
        return F.when(is_idem, lag_col).otherwise(col_expr if isinstance(col_expr, str) else F.col(col_expr))

    def _build_resolve_idem_expr_for_array_simple(
        self, array_col: str, lag_col: str
    ) -> "F.Column":
        """
        Array of simple types: first item gets last of previous row; others get previous in same row.
        Uses Spark SQL aggregate(), element_at(..., -1), array_concat - compatible with Spark 3.5.x.
        """
        lag_col_escaped = f"`{lag_col}`" if "." in lag_col or "-" in lag_col else lag_col
        # element_at is 1-based; -1 = last. When acc is empty, use last of previous row.
        return F.expr(f"""
            aggregate(
                {array_col},
                array(),
                (acc, x) -> array_concat(
                    acc,
                    array(
                        CASE WHEN trim(lower(coalesce(cast(x as string), ''))) IN ('idem','id','id.')
                            THEN CASE WHEN size(acc) = 0
                                THEN element_at({lag_col_escaped}, -1)
                                ELSE element_at(acc, size(acc))
                                END
                            ELSE x
                        END
                    )
                )
            )
        """)

    def _build_resolve_idem_expr_for_array_struct(
        self, array_col: str, idem_field: str, struct_fields: list, lag_col: str
    ) -> "F.Column":
        """
        Array of structs: same logic as simple, but only the idem field is replaced.
        Build struct with all fields; for idem_field use CASE, for others use x.field.
        Uses Spark SQL aggregate(), element_at(..., -1) - compatible with Spark 3.5.x.
        """
        lag_col_escaped = f"`{lag_col}`" if "." in lag_col or "-" in lag_col else lag_col
        struct_parts = []
        for f in struct_fields:
            if f == idem_field:
                struct_parts.append(f"""
                    CASE WHEN trim(lower(coalesce(cast(x.{f} as string), ''))) IN ('idem','id','id.')
                        THEN (CASE WHEN size(acc) = 0
                            THEN element_at({lag_col_escaped}, -1)
                            ELSE element_at(acc, size(acc))
                            END).{f}
                        ELSE x.{f}
                    END as {f}
                """)
            else:
                struct_parts.append(f"x.{f} as {f}")
        struct_expr = ", ".join(struct_parts)
        return F.expr(f"""
            aggregate(
                {array_col},
                array(),
                (acc, x) -> array_concat(
                    acc,
                    array(struct({struct_expr}))
                )
            )
        """)

    def read_raw_entities(self, entity: str):
        df = self.read_delta("known_entities", entity, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])
        return df

    def read_raw_entries(self, *container_path, user: str = None, publication_name: str = None, y: int | str = None, m: int | str = None,
                           d: int | str = None, edition: str = None):
            base_path = f"{self._resolve_path(*container_path, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])}"
            if isinstance(y, int):
                y = f"{y:04d}"
            if isinstance(m, int):
                m = f"{m:02d}"
            if isinstance(d, int):
                d = f"{d:02d}"
            base_dir = os.path.join(base_path,
                                    publication_name.lower() if publication_name else "*",
                                    y or "*",
                                    m or "*",
                                    d or "*",
                                    edition.lower() if edition else "*")
            path = os.path.join(base_dir, "*.json")
        
            df = self.read_json(path, has_extension=True)
            if df is not None:
                if user is not None:
                    df = df.filter(F.col("uploaded_by") == user)
                logger.info(f"{0 if df is None else df.count()} entries was read")
            return df

    @data_transformer_method()
    def resolve_idems(self, df: DataFrame) -> DataFrame:
        """
        Aplica la lògica de reemplaçament recursiu IDEM fent servir Spark.
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        if "schema" in self._schema:
            schema_to_use = self._schema["schema"]
        else:
            schema_to_use = self._schema

        # 1. Obtenim el mapa de camps a processar des de l'esquema JSON
        idems_map = self._collect_idems_for_spark(schema_to_use)

        # Definim la finestra per al Forward Fill entre files
        # Fem que la finestra sigui des de l'inici dels temps fins a la fila actual
        order_and_partitions = schema_to_use.get("order_and_partitions") if "order_and_partitions" in schema_to_use else None

        if order_and_partitions is None:
            raise ValueError("No 'order' field in schema")

        order_cols = order_and_partitions.get("order")
        partitions = order_and_partitions.get("partitions") if "partitions" in order_and_partitions else None

        if order_cols is None:
            raise ValueError("No 'order' field in schema")

        if partitions is None:
            window_spec = Window.orderBy(*order_cols).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        else:
            window_spec = Window.partitionBy(*partitions).orderBy(*order_cols).rowsBetween(Window.unboundedPreceding, Window.currentRow)

        # --- FASE 1: NETEJA DE CAMPS ROOT ---
        for field in idems_map["root_fields"]:
            # Convertim tokens i Nones a Null real de Spark
            df = df.withColumn(field, F.when(F.col(field).isin(IDEM_TOKENS) | F.col(field).isNull(),
                                             F.lit(None)).otherwise(F.col(field)))
            # Apliquem forward fill entre files
            df = df.withColumn(field, F.last(field, ignorenulls=True).over(window_spec))

        # --- FASE 2: NETEJA D'ARRAYS (Amb memòria entre files) ---
        # Ordenem els arrays per profunditat (de més profund a més superficial)
        sorted_arrays = sorted(idems_map["arrays"].keys(), key=lambda x: x.count('.'), reverse=True)

        for array_path in sorted_arrays:
            meta = idems_map["arrays"][array_path]

            # # Aquesta part és la clau: per passar el valor de l'últim element de la fila anterior
            # # a la fila actual, creem una columna temporal "seed"
            # seed_col = f"{array_path}_seed"

            # CORRECCIÓ 1: Nom de la columna seed sense punts
            seed_col_name = f"{array_path.replace('.', '_')}_seed"

            # # Obtenim el darrer element de l'array de la fila anterior
            # last_element_expr = f"element_at({array_path}, -1)"
            # df = df.withColumn(seed_col, F.last(F.expr(last_element_expr), ignorenulls=True).over(window_spec))

            # CORRECCIÓ 2: Expressió de seed que navega per l'estructura
            # Si el path és 'cargo_list.cargo', volem l'últim 'cargo' de l'últim 'cargo_list'
            nested_seed_expr = self._build_nested_element_at_expr(array_path)
            # Obtenim el valor de la fila anterior
            df = df.withColumn(
                seed_col_name,
                F.last(F.expr(nested_seed_expr), ignorenulls=True).over(window_spec)
            )

            # # Construïm l'expressió aggregate per processar l'array
            # clean_expr = self._build_aggregate_expr(array_path, seed_col, meta)
            # df = df.withColumn(array_path, F.expr(clean_expr))

            # Apliquem l'aggregate (referenciant el seed_col_name sanejat)
            # Nota: L'aggregate s'ha d'aplicar sobre el nivell superior.
            # Si l'array_path és 'cargo_list.cargo', hem de transformar 'cargo_list'
            if "." in array_path:
                parent_path, current_array = array_path.rsplit(".", 1)
                # Obtenim la funció que genera l'aggregate
                aggregate_func = self._build_nested_aggregate_expr(current_array, seed_col_name, meta)

                # Apliquem el transform fent servir withField de forma nativa
                df = df.withColumn(
                    parent_path,
                    F.transform(
                        F.col(parent_path),
                        lambda x: x.withField(current_array, aggregate_func(x))
                    )
                )

                # df = df.withColumn(
                #     parent_path,
                #     F.transform(
                #         F.col(parent_path),
                #         lambda x: x.withField(current_array, F.expr(clean_expr))
                #     )
                # )
            else:
                # Si és un array d'arrel
                clean_expr = self._build_aggregate_expr(array_path, seed_col_name, meta)
                df = df.withColumn(array_path, clean_expr)

            # Netegem la columna seed temporal
            # df = df.drop(seed_col)
            df = df.drop(seed_col_name)

        return df

    def _build_nested_element_at_expr(self, path: str) -> str:
        """
        Converteix 'a.b.c' en 'element_at(element_at(a, -1).b, -1).c'
        per obtenir l'últim estat d'una estructura aniuada.
        """
        parts = path.split(".")
        expr = parts[0]
        for part in parts[1:]:
            expr = f"element_at({expr}, -1).{part}"
        # Finalment volem l'últim d'aquest últim
        return f"element_at({expr}, -1)"

    def _build_nested_aggregate_expr(self, child_array_name: str, seed_col: str, meta: dict) -> str:
        """
        Construeix l'aggregate per a un array que està DINS d'un altre array.
        Es diferencia en què l'origen de dades és 'x.{child_array_name}'
        en lloc d'un nom de columna global.
        """
        fields_to_fix = meta["fields"]
        all_fields = meta["all_struct_fields"]

        # 1. Obtenim el tipus real des del teu mètode
        element_struct_type, _ = PortadaCleaning.json_schema_to_spark_type(meta["schema"], primitive_types_as_strings=True)
        ddl_schema = f"array<{element_struct_type.simpleString()}>"
        # 2. Inicialitzador (Seed) - Ens assegurem que és una columna neta
        initial_acc = F.array(F.col(seed_col)).cast(ddl_schema)

        # 3. Lògica de combinació (Merge)
        def merge_logic(acc, el):
            struct_fields = []
            for f_name in all_fields:
                # Utilitzem getItem() que és més robust dins de lambdes que getField()
                current_field = el.getItem(f_name)

                if f_name in fields_to_fix:
                    prev_field = F.element_at(acc, -1).getItem(f_name)
                    # Comparem forçant a string el token
                    condition = current_field.cast("string").isin(IDEM_TOKENS)
                    field_val = F.when(condition, prev_field).otherwise(current_field)
                else:
                    field_val = current_field

                # .alias() retorna una columna, ens assegurem que no hi hagi res més
                struct_fields.append(field_val.alias(f_name))

            # Empaquetem la llista de columnes (*struct_fields desempaqueta la llista)
            return F.array_union(acc, F.array(F.struct(*struct_fields)))

        # 4. Funció de sortida
        def finish_logic(acc):
            return F.slice(acc, 2, F.size(acc))

        # Retornem la lambda per al transform
        return lambda obj: F.aggregate(
            obj.getItem(child_array_name),
            initial_acc,
            merge_logic,
            finish_logic
        )

    #     fields_to_fix = meta["fields"]
    #     all_fields = meta["all_struct_fields"]
    #
    #     # Construïm la definició de l'estructura (Schema) per al CAST
    #     # És vital perquè Spark sàpiga quin tipus de dades té la 'seed'
    #     struct_schema = ", ".join([f"{f}:string" for f in all_fields])
    #     array_struct_type = PortadaCleaning.json_schema_to_spark_type(meta["schema"])
    #
    #     # 2. Creem l'acumulador inicial (seed)
    #     # L'array només conté el darrer element de la fila anterior
    #     initial_acc = F.cast(F.array(F.col(seed_col)), array_struct_type)
    #
    #     # 3. Definim la lògica de combinació (merge)
    #     def merge_logic(acc, el):
    #         # Reconstruïm l'estructura element a element
    #         struct_fields = []
    #         for f_name in all_fields:
    #             if f_name in fields_to_fix:
    #                 # Lògica IDEM: si és token, agafa el de l'acumulador (element_at -1)
    #                 field_val = F.coalesce(
    #                     F.when(el[f_name].isin(IDEM_TOKENS), F.lit(None)).otherwise(el[f_name]),
    #                     F.element_at(acc, -1).getItem(f_name)
    #                 )
    #             else:
    #                 # Si no és IDEM, només omplim si és null (opcional, segons el teu criteri)
    #                 field_val = F.coalesce(el[f_name], F.element_at(acc, -1).getItem(f_name))
    #
    #             struct_fields.append(field_val.alias(f_name))
    #
    #         return F.array_union(acc, F.array(F.struct(*struct_fields)))
    #
    #     # 4. Definim la funció de sortida (finish)
    #     # Eliminem el primer element (la seed artificial)
    #     def finish_logic(acc):
    #         return F.slice(acc, 2, F.size(acc))
    #
    #     # Retornem l'objecte Column construït
    #     # Note: 'el' és l'argument que passarà el transform pare
    #     return lambda obj: F.aggregate(
    #         obj.getItem(child_array_name),
    #         initial_acc,
    #         merge_logic,
    #         finish_logic
    #     )
    #     # # Generem la lògica de reconstrucció de l'objecte (coalesce amb l'element anterior)
    #     # struct_gen = []
    #     # for f_name in all_fields:
    #     #     if f_name in fields_to_fix:
    #     #         # Si és IDEM, agafem el camp de l'últim element de l'acumulador (acc)
    #     #         join_idems = '", "'.join(IDEM_TOKENS)
    #     #         val = f"coalesce(if(element.{f_name} IN ('{join_idems}'), null, element.{f_name}), element_at(acc, -1).{f_name})"
    #     #     else:
    #     #         # Si no és IDEM, el mantenim o agafem el predecessor si és null
    #     #         val = f"coalesce(element.{f_name}, element_at(acc, -1).{f_name})"
    #     #     struct_gen.append(f"{val} AS {f_name}")
    #     #
    #     # struct_sql = ", ".join(struct_gen)
    #     #
    #     # # Nota: Aquí l'iterador de l'aggregate l'anomenem 'element' per no
    #     # # confondre'l amb la 'x' del transform pare.
    #     # return F.aggregate(
    #     #         x[child_array_name],
    #     #         F.cast()
    #     #         CAST(array({seed_col}) AS array<struct<{struct_schema}>>),
    #     #         (acc, element) -> array_union(acc, array(
    #     #             struct({struct_sql})
    #     #         )),
    #     #         acc -> slice(acc, 2, size(acc))
    #     #     )

    def _build_aggregate_expr(self, array_path: str, seed_col: str, meta: dict) -> str:
        """
        Construeix l'expressió SQL 'aggregate' per a Spark.
        """
        """
            Construeix l'aggregate per a un array de primer nivell usant PySpark pur.
            """
        fields_to_fix = meta["fields"]
        all_fields = meta["all_struct_fields"]

        # Obtenim l'estructura real per evitar el Datatype Mismatch
        element_struct_type, _ = PortadaCleaning.json_schema_to_spark_type(meta["schema"], primitive_types_as_strings=True)

        # Usem el format DDL per evitar l'AssertionError d'abans
        ddl_schema = f"array<{element_struct_type.simpleString()}>"

        # L'acumulador inicial amb el tipus perfectament definit
        initial_acc = F.array(F.col(seed_col)).cast(ddl_schema)

        def merge_logic(acc, el):
            struct_fields = []
            for f_name in all_fields:
                current_field = el[f_name]

                if f_name in fields_to_fix:
                    # Camps simples que poden ser "idem"
                    prev_field = F.element_at(acc, -1)[f_name]
                    is_idem = current_field.cast("string").isin(IDEM_TOKENS)

                    # Si és idem agafem l'anterior, si no l'actual
                    field_val = F.when(is_idem, prev_field).otherwise(current_field)
                else:
                    # Camps complexos (com 'cargo') o sense idem.
                    # Com que sabem que no són Null, ens els quedem directament
                    field_val = current_field

                struct_fields.append(field_val.alias(f_name))

            return F.array_union(acc, F.array(F.struct(*struct_fields)))

        def finish_logic(acc):
            return F.slice(acc, 2, F.size(acc))

        # ATENCIÓ: Aquí no retornem una lambda, sinó la columna directament,
        # ja que estem operant sobre una columna de primer nivell (F.col)
        return F.aggregate(
            F.col(array_path),
            initial_acc,
            merge_logic,
            finish_logic
        )

    # @data_transformer_method()
    # def resolve_idems(self, df: TracedDataFrame) -> TracedDataFrame:
    #     """
    #     For fields marked with accepted_idem in the schema, replace the values
    #     'idem', 'id', 'id.' with the value of the predecessor record (previous row).
    #     """
    #     if self._schema is None:
    #         raise ValueError("Must call use_schema() before.")
    #     if "schema" in self._schema:
    #         schema_to_use = self._schema["schema"]
    #     else:
    #         schema_to_use = self._schema
    #     props = schema_to_use.get("properties", {})
    #
    #     fields_info = self._collect_idems_for_spark(schema_to_use)
    #     if not fields_info:
    #         return df
    #
    #     # Window: partition by publication/date, order by entry_id (or numeric id if not present)
    #     part_cols = ["publication_name", "publication_date"]
    #     order_col = F.col("entry_id")
    #     w = Window.partitionBy(part_cols).orderBy(order_col)
    #
    #     lag_cols_to_drop = []
    #
    #     for path, field_schema, meta in fields_info:
    #         parts = path.split(".")
    #         if len(parts) == 1:
    #             # Top-level scalar field
    #             col_name = parts[0]
    #             if col_name not in df.columns:
    #                 continue
    #             lag_col = F.lag(F.col(col_name)).over(w)
    #             expr = F.when(
    #                 F.trim(F.lower(F.col(col_name))).isin(*self.IDEM_VALUES),
    #                 lag_col
    #             ).otherwise(F.col(col_name))
    #             df = df.withColumn(col_name, expr)
    #         else:
    #             # Array field (path = "array_col.idem_field" or "array_col" for simple)
    #             array_col = meta["array_col"] if meta else parts[0]
    #             if array_col not in df.columns:
    #                 continue
    #
    #             lag_col_name = f"_lag_resolve_{array_col}"
    #             if lag_col_name not in df.columns:
    #                 df = df.withColumn(lag_col_name, F.lag(F.col(array_col)).over(w))
    #                 lag_cols_to_drop.append(lag_col_name)
    #
    #             items_type = meta.get("items_type", "object") if meta else "object"
    #
    #             if items_type == "simple":
    #                 resolved = self._build_resolve_idem_expr_for_array_simple(
    #                     array_col, lag_col_name
    #                 )
    #                 df = df.withColumn(array_col, resolved)
    #             elif items_type == "object":
    #                 idem_field = meta.get("idem_field", parts[1])
    #                 struct_fields = meta.get("struct_fields", [])
    #                 if not struct_fields:
    #                     continue
    #                 resolved = self._build_resolve_idem_expr_for_array_struct(
    #                     array_col, idem_field, struct_fields, lag_col_name
    #                 )
    #                 df = df.withColumn(array_col, resolved)
    #             # Nested arrays (inner_array) skipped for now
    #
    #     for c in lag_cols_to_drop:
    #         if c in df.columns:
    #             df = df.drop(c)
    #
    #     return df

    @block_transformer_method
    def cleaning(self, df: DataFrame | TracedDataFrame) -> TracedDataFrame:
        # 1.- prune values
        df = self.prune_unaccepted_fields(df)
        # 2.- Normalize structure
        df = self.normalize_field_structure(df)
        # 3.- save original values
        self.save_original_values_of_ship_entries(df)
        # 4.- Normalize values
        df = self.normalize_field_value(df)
        # 6.- Resolve idem/id/id. with the value of the predecessor record
        df = self.resolve_idems(df)
        # # 7.- Convert values form string to corrected type
        # df = self.convert_string_to_schematype(df) - Numero en letras a número
        # # 8.- Save cleaned values
        # self.save_ship_entries(df)
        return df

    def save_original_values_of_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        self.write_delta("original_data", "ship_entries", df=ship_entries_df, mode="overwrite",
                         partition_by=["publication_name", "publication_date", "publication_edition"])
        return ship_entries_df

    def save_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        self.write_delta("ship_entries",df=ship_entries_df, mode="overwrite",
                         partition_by=["publication_name", "publication_date_year", "publication_date_month"])
        return ship_entries_df

    def read_ship_entries(self) -> TracedDataFrame:
        ship_entries_df = self.read_delta("ship_entries")
        return ship_entries_df

    @data_transformer_method()
    def normalize_field_structure(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Normalize the structure of a DataFrame according to the schema.

        This method iterates over the properties of the schema and builds an expression
        to normalize the structure of the corresponding column in the DataFrame.

        For example, if the schema defines a struct field "address" with two fields "street"
        and "city", this method ensures that all rows comply with the structure.

        :param df: The DataFrame to normalize
        :return: A new DataFrame with the normalized structure
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        final_expressions = {}
        properties = self._schema.get("properties", {})
        selects = []
        for col in df.columns:
            if col not in properties:
                selects.append(F.col(col).alias(col))
        for field_name, field_schema in properties.items():
            # Get the full expression for the column (simple, struct or array)
            expr = self._build_normalize_expr(F.col(field_name), field_schema)
            selects.append(expr.alias(field_name))
        return df.select(*selects)

    @data_transformer_method()
    def normalize_field_value(self, df: TracedDataFrame) -> TracedDataFrame:
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        properties = self._schema.get("properties", {})

        for field_name, field_schema in properties.items():
            # Get the full expression for the column (simple, struct or array)
            expr = self._json_schema_to_normalize_values(field_schema, self._mapping_to_clean_chars, F.col(field_name))
            df = df.withColumn(field_name, expr)
        return df

    @data_transformer_method()
    def duplicate_fields(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Duplicate each field in the DataFrame, adding a suffix "_original_value" to the new field name.

        This is useful when we want to keep a record of the original values of a field before
        applying some transformation to it.

        For example, if we want to normalize the values of a field, but also keep the original
        values, we can use this method to create a new field with the original values.

        :param df: The DataFrame to duplicate fields from
        :return: A new DataFrame with the duplicated fields
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        # Get the list of fields from the schema
        fields = self._collect_list_of_fields(self._schema)
        # Create a list of new field names
        new_fields = [
            f"{col}_original_value" for col in fields
        ]
        # Create a list of expressions to duplicate the fields
        exprs = [F.col(col).alias(new_field) for col, new_field in zip(fields, new_fields)]
        # Apply the expressions to the DataFrame
        df = df.select(*exprs)
        return df

    @data_transformer_method(description="prune unbelonging model fields ")
    def prune_unaccepted_fields(self, df: TracedDataFrame) -> TracedDataFrame:
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        allowed = self._collect_list_of_fields(self._schema)
        cols_to_keep = [
            col for col in df.columns if col in allowed
        ]
        missing_cols = allowed - set(cols_to_keep)
        cols_to_add = {
            col: F.lit(None).alias(col) for col in missing_cols
        }

        return df.select(cols_to_keep).withColumns(cols_to_add)

@registry_to_portada_builder
class BoatFactCleaning(PortadaCleaning):
    FLAG_ENTITY = 'flag'
    SHIP_TONS_ENTITY = 'ship_tons'
    TRAVEL_DURATION_ENTITY = 'travel_duration'
    COMODITY_ENTITY = 'comodity'
    SHIP_TYPE_ENTITY = 'ship_type'
    UNIT_ENTITY = 'unit'
    PORT_ENTITY = 'port'
    MASTER_ROLE_ENTITY = 'master_role'

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self.__container_path = "ship_entries"

    def read_ship_entries(self) -> TracedDataFrame:
        ship_entries_df = self.read_delta(self.__container_path)
        return ship_entries_df

    def read_raw_entries(self, user: str = None, publication_name: str = None, y: int | str = None,
                         m: int | str = None,
                         d: int | str = None, edition: str = None):

        ship_entries_df = super().read_raw_entries(self.__container_path, user=user, publication_name=publication_name,
                                                   y=y, m=m, d=d, edition=edition)
        return ship_entries_df

    @staticmethod
    def extract_ports(df_entries, from_departure_port = True, from_port_of_calls = True, from_arrival_port = True):
        if from_port_of_calls:
            df_exploded = df_entries.select(
                "entry_id",
                "temp_key",
                F.posexplode("travel_port_of_call_list").alias("port_idx", "port_of_call_item")
            )
            df_port_of_calls = df_exploded.select(
                "entry_id",
                "temp_key",
                F.lit("travel_port_of_call_list").alias("field_origin"),
                "port_idx",
                F.col("port_of_call_item.port_of_call_place").alias("citation")
            )
            df_port_of_calls = df_port_of_calls.withColumn(
                "id",
                F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "port_idx")
            )
        if from_arrival_port:
            df_port_arrival = df_entries.select(
                "entry_id",
                "temp_key",
                F.lit("travel_arrival_port").alias("field_origin"),
                F.lit(0).alias("port_idx"),
                F.col("travel_arrival_port").alias("citation")
            )
            df_port_arrival = df_port_arrival.withColumn(
                "id",
                F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "port_idx")
            )
        if from_departure_port:
            df_port_departure = df_entries.select(
                "entry_id",
                "temp_key",
                F.lit("travel_departure_port").alias("field_origin"),
                F.lit(0).alias("port_idx"),
                F.col("travel_departure_port").alias("citation")
            )
            df_port_departure = df_port_departure.withColumn(
                "id",
                F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "port_idx")
            )
        df = None
        if from_departure_port:
            df = df_port_departure
        if from_port_of_calls:
            if df is None:
                df = df_port_of_calls
            else:
                df = df.union(df_port_of_calls)
        if from_arrival_port:
            if df is None:
                df = df_port_arrival
            else:
                df = df.union(df_port_arrival)
        return df

    @staticmethod
    def extract_ship_types(df_entries):
        df_ship_entry = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("ship_type").alias("field_origin"),
            F.col("ship_type").alias("citation")
        )
        return df_ship_entry

    @staticmethod
    def extract_ship_tons_units(df_entries):
        df_ship_tons_unit = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("ship_tons_unit").alias("field_origin"),
            F.col("ship_tons_unit").alias("citation")
        )
        return df_ship_tons_unit

    @staticmethod
    def extract_ship_flags(df_entries):
        df_ship_flag = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("ship_flag").alias("field_origin"),
            F.col("ship_flag").alias("citation")
        )
        return df_ship_flag

    @staticmethod
    def extract_master_roles(df_entries):
        df_master_role = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("master_role").alias("field_origin"),
            F.col("master_role").alias("citation")
        )
        return df_master_role

    @staticmethod
    def extract_cargo_comodities_and_units(df_entries):
        # 1. Primer nivell: Explotem la llista de comerciants (cargo_list)
        # Fem servir posexplode per mantenir l'ordre dels comerciants
        df_merchants = df_entries.select(
            "entry_id",  # El teu camp identificador original
            "temp_key",
            F.posexplode("cargo_list").alias("merchant_idx", "merchant_struct")
        )

        # 2. Segon nivell: Explotem la llista de mercaderies (cargo) que hi ha dins de cada comerciant
        # Extraiem el nom del comerciant i explotem el seu array intern de càrrega
        df_cargo = df_merchants.select(
            "entry_id",
            "temp_key",
            "merchant_idx",
            F.posexplode("merchant_struct.cargo").alias("cargo_idx", "c")
        )

        df_comodity_and_unit = df_cargo.select(
            "entry_id",
            "temp_key",
            F.lit("cargo_list").alias("field_origin"),
            "merchant_idx",
            "cargo_idx",
            F.col("c.cargo_commodity").alias("cargo_commodity_citation"),
            F.col("c.cargo_unit").alias("cargo_unit_citation")
        )
        df_comodity_and_unit = df_comodity_and_unit.withColumn(
            "id",
            F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "merchant_idx", F.lit("-"), "cargo_idx")
        )

        return df_comodity_and_unit


    @staticmethod
    def extract_cargo_merchants(df_entries):
        df_merchants = df_entries.select(
            "entry_id",  # El teu camp identificador original
            "temp_key",
            F.posexplode("cargo_list").alias("merchant_idx", "merchant_struct")
        )

        df_merchants = df_merchants.select(
            "entry_id",
            "temp_key",
            F.lit("cargo_list.cargo.cargo_merchant_name").alias("field_origin"),
            "merchant_idx",
            F.col("merchant_struct.cargo_merchant_name").alias("citation")
        )
        df_merchants = df_merchants.withColumn(
            "id",
            F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "merchant_idx")
        )

        return df_merchants


    @staticmethod
    def extract_ship_agents(df_entries):
        df_ship_agent = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("ship_agent_name").alias("field_origin"),
            F.col("ship_agent_name").alias("citation")
        )
        return df_ship_agent


    @staticmethod
    def extract_brokers(df_entries):
        df_broker = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("broker_name").alias("field_origin"),
            F.col("broker_name").alias("citation")
        )
        return df_broker

    @staticmethod
    def extract_masters(df_entries):
       return BoatFactCleaning._extract_single_entry(df_entries)

    @staticmethod
    def extract_ships(df_entries):
        return BoatFactCleaning._extract_single_entry(df_entries)

    @staticmethod
    def _extract_single_entry(df_entries):
        df_to_return = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.col("travel_departure_port").alias("travel_departure_port_citation"),
            F.col("travel_departure_date").alias("travel_departure_date_citation"),
            F.col("travel_arrival_port").alias("travel_arrival_port_citation"),
            F.col("travel_arrival_date").alias("travel_arrival_date_citation"),
            F.col("ship_type").alias("ship_type_citation"),
            F.col("ship_flag").alias("ship_flag_citation"),
            F.col("ship_tons_capacity").alias("ship_tons_capacity_citation"),
            F.col("ship_name").alias("ship_name_citation"),
            F.col("master_role").alias("master_role_citation"),
            F.col("master_name").alias("master_name_citation"),
            F.lit("single_ship_entry_fields").alias("field_origin"),
        )
        return df_to_return

    def get_known_entity_voices(self, known_entity=None, df_entities = None):
        if known_entity is not None:
            df_entities = self.read_delta("known_entities", known_entity)
        if df_entities is None:
            raise ValueError("No known entities found")
        df_voices = df_entities.select(
            "name",  # El teu camp identificador original
            F.posexplode("voices").alias("voice_idx", "voice")
        )
        df_voices = df_voices.select(
            F.concat(F.col("name"), F.lit("-"), F.col("voice")).alias("id"),
            F.col("name"),
            F.col("voice_idx"),
            F.lit("voices").alias("field_origin"),
            F.col("voice")
        )
        return df_voices