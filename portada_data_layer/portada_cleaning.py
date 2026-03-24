import json
import re
import os
import logging

from delta import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, DateType, TimestampType, LongType, DoubleType, BooleanType, \
    StructType, ArrayType
from portada_data_layer import DeltaDataLayer, TracedDataFrame, block_transformer_method
from portada_data_layer.boat_fact_model import BoatFactDataModel
from portada_data_layer.data_lake_metadata_manager import data_transformer_method, enable_storage_log_for_class, \
    enable_field_lineage_log_for_class
from portada_data_layer.portada_delta_common import registry_to_portada_builder
from portada_data_layer.portada_patcher_data_layer import BoatFactPatcherDataLayer

logger = logging.getLogger("portada_data.delta_data_layer.boat_fact_cleaning")

# Regex per reconèixer expressions equivalents a "idem" (id., idem, id, 1dem, IdEm, etc.)
IDEM_REGEX = r"^\s*[Ii][dD](?:(?:\.)|(?:[Eeoa][nmMN]))?(?: +.{1,2} +[Ii][dD](?:(?:\.)|(?:[Eeoa][nmMN]))?)?\s*$"
# Mapa bàsic de normalització de caràcters per aplanar strings (diacrítics -> base ASCII)
FLATTEN_STRINGS_FROM = "àáâãäåèéêëìíîïòóôõöùúûüýÿçñ"
FLATTEN_STRINGS_TO = "aaaaaaeeeeiiiiooooouuuuyycn"


@registry_to_portada_builder
@enable_storage_log_for_class
@enable_field_lineage_log_for_class
class PortadaCleaning(DeltaDataLayer):

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self._schema = {}
        self._mapping_to_clean_chars = {}
        self._current_process_level = 1

    def use_schema(self, json_schema: dict):
        self._schema = json_schema
        return self

    def use_mapping_to_clean_chars(self, mapping: dict):
        self._mapping_to_clean_chars = mapping
        return self

    @data_transformer_method(description="fill row fields from other columns using schema fill_with")
    def fill_with_schema(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Fill field content from other columns according to the 'fill_with' specs
        in the JSON schema (replace, replace_if_not_exist, add_array_item, add_all_items).
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        props = self._schema_properties(self._schema)
        for field_name, field_schema in props.items():
            fill_with = field_schema.get("fill_with")
            if fill_with is None:
                continue
            if isinstance(fill_with, list):
                for spec in fill_with:
                    if isinstance(spec, dict):
                        df = self._fill_apply_one(df, field_name, field_schema, spec)
            elif isinstance(fill_with, dict):
                df = self._fill_apply_one(df, field_name, field_schema, fill_with)
        return df

    @data_transformer_method(description="calculate null fields from schema calculable_if_null")
    def calculate_from_schema(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        For each field in the schema that has 'calculable_if_null', if the field is null or missing,
        set it to the result of the defined operation (decrement_data or data_difference).
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        props = self._schema_properties(self._schema)
        for field_name, field_schema in props.items():
            calc_spec = field_schema.get("calculable_if_null")
            if not isinstance(calc_spec, dict):
                continue
            operation = (calc_spec.get("operation") or "").strip()
            parameters = calc_spec.get("parameters") or calc_spec.get("params") or {}
            if not parameters:
                continue
            if operation == "decrement_data":
                computed = PortadaCleaning._calc_decrement_data(parameters)
            elif operation == "data_difference":
                computed = PortadaCleaning._calc_data_difference(parameters)
            else:
                continue
            if field_name not in df.columns:
                df = df.withColumn(field_name, computed)
            else:
                df = df.withColumn(
                    field_name,
                    F.when(F.col(field_name).isNull(), computed).otherwise(F.col(field_name)),
                )
        return df

    def read_raw_entities(self, entity: str):
        df = self.read_delta("known_entities", entity, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])
        return df

    def read_raw_entries(self, *container_path, force_all=False):
        base_path = f"{self._resolve_path(*container_path, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])}"
        base_dir = os.path.join(base_path, "*", "*", "*", "*", "*")
        path = os.path.join(base_dir, "*.json")

        df = self.read_json(path, has_extension=True)
        if df is not None:
            logger.info(f"{0 if df is None else df.count()} entries was read")

        if not force_all:
            state_path = self._resolve_path(*container_path, process_level_dir="states")
            if self.path_exists(state_path):
                state_df = self.spark.read.parquet(state_path)
            else:
                state_df = self.spark.createDataFrame(data={}, schema=StructType([StructField("entry_id", StringType(), True),StructField("is_cleaned", BooleanType(), True),]))
            state_df = state_df.filter(F.col("is_cleaned") == True).select("entry_id")

            df = df.join(state_df, on="entry_id", how="left_anti")
        return df

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
            if field_name in  self._mapping_to_clean_chars:
                mapping = self._mapping_to_clean_chars[field_name]
                expr = self.apply_ocr_corrections(F.col(field_name), mapping)
            else:
                expr = F.col(field_name)
            expr = self._json_schema_to_normalize_values(field_schema, self._mapping_to_clean_chars, expr)
            df = df.withColumn(field_name, expr)
        return df

    @data_transformer_method(description="add schema fields that are missing in the dataframe")
    def complete_accepted_structure(self, df: TracedDataFrame) -> TracedDataFrame:
        """Add the 'official' fields defined in the schema that are missing from the dataframe (with correct structure)."""
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        allowed = self._collect_list_of_fields(self._schema)
        missing_cols = allowed - set(df.columns)
        if not missing_cols:
            return df
        cols_to_add = {
            col: self._expr_for_missing_column(col) for col in missing_cols
        }
        return df.withColumns(cols_to_add)

    @data_transformer_method(description="prune unbelonging model fields")
    def prune_unaccepted_fields(self, df: TracedDataFrame) -> TracedDataFrame:
        """Remove dataframe columns that are not in the schema (extra columns)."""
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        allowed = self._collect_list_of_fields(self._schema)
        cols_to_keep = [col for col in df.columns if col in allowed]
        return df.select(cols_to_keep)

    @data_transformer_method()
    def simplify_field_structure(self, df):
        """
        Simplify extraction-metadata columns using get_json_object
        to handle complex JSON structures dynamically.
        """

        # 1. Identifiquem els camps base a partir del sufix "_detailed_value"
        detailed_cols = [c for c in df.columns if c.endswith(BoatFactDataModel.DETAILED_VALUE_FIELD)]
        base_fields = [c.replace(f"_{BoatFactDataModel.DETAILED_VALUE_FIELD}", "") for c in detailed_cols]

        cleaned_df = df

        for field in base_fields:
            detailed_col = f"{field}_{BoatFactDataModel.DETAILED_VALUE_FIELD}"
            source_method_col = f"{field}_{BoatFactDataModel.EXTRACTION_SOURCE_METHOD_FIELD}"
            original_val_col = f"{field}_original_value"

             # Get the data type of the detailed column
            detailed_field_type = cleaned_df.schema[detailed_col].dataType

            # VALIDATION: If the column is not StructType (e.g. NullType),
            # we cannot extract subfields. Go straight to fallback.
            if not isinstance(detailed_field_type, StructType):
                cleaned_df = cleaned_df.withColumn(original_val_col, F.col(field))
                # Optional: drop empty columns to avoid clutter
                cols_to_drop = [c for c in [detailed_col, source_method_col] if c in cleaned_df.columns]
                cleaned_df = cleaned_df.drop(*cols_to_drop)
                continue

            # 3. We have a real StructType. Get the subfields.
            available_subfields = [f.name for f in detailed_field_type.fields]

            # 1. Get the schema (data type) of the original field for from_json
            target_schema = cleaned_df.schema[field].dataType

            # 2. Choose which STRUCT field to take (the flattened JSON text)
            # We access the struct with dot notation (col.field)
            if source_method_col in cleaned_df.columns:
                # Dynamic selection: use 'when' to pick struct subfield by method value
                conditions = None
                for method_type in [BoatFactDataModel.CALCULATED_VALUE_FIELD,
                                    BoatFactDataModel.ORIGINAL_VALUE_FIELD,
                                    BoatFactDataModel.DEFAULT_VALUE_FIELD]:
                    if method_type in available_subfields:
                        if conditions is None:
                            conditions = F.when(F.col(source_method_col) == method_type,
                                                F.col(f"{detailed_col}.{method_type}"))
                        else:
                            conditions = conditions.when(F.col(source_method_col) == method_type,
                                                         F.col(f"{detailed_col}.{method_type}"))
                json_string_col = conditions if conditions is not None else F.lit(None)
            else:
                # Priority selection (coalesce) when no method column is specified
                priority_list = [
                    F.col(f"{detailed_col}.{m}")
                    for m in [BoatFactDataModel.CALCULATED_VALUE_FIELD,
                              BoatFactDataModel.ORIGINAL_VALUE_FIELD,
                              BoatFactDataModel.DEFAULT_VALUE_FIELD]
                    if m in available_subfields
                ]
                json_string_col = F.coalesce(*priority_list) if priority_list else F.lit(None)

            # If the base field is complex (Struct or Array), unflatten the JSON string
            if isinstance(target_schema, (StructType, ArrayType)):
                # Try to parse JSON; on failure or null, fallback to current field
                extracted_col = F.from_json(json_string_col, target_schema)
                final_col = F.coalesce(extracted_col, F.col(field))
            else:
                # Simple field (String, Int...): no from_json needed, just cast and fallback
                final_col = F.coalesce(json_string_col.cast(target_schema), F.col(field))

            cleaned_df = cleaned_df.withColumn(original_val_col, final_col)

            # 4. Drop metadata columns
            cols_to_drop = [c for c in [detailed_col, source_method_col] if c in cleaned_df.columns]
            cleaned_df = cleaned_df.drop(*cols_to_drop)

        return cleaned_df

    @staticmethod
    def _cast_column_to_schema_type(col_expr: F.Column, spark_type):
        """
        Cast robust per tipus simples.
        Els tipus complexos (object/array) es tracten a _cast_column_by_json_schema recursivament.
        """
        return col_expr.cast(spark_type)

    @staticmethod
    def _cast_column_by_json_schema(col_expr: F.Column, schema: dict) -> F.Column:
        """
        Cast recursiu d'una columna segons JSON schema:
        - primitives: cast directe al tipus Spark corresponent
        - object: struct amb cast de cada subcamp
        - array: transform(item -> cast recursiu segons items)
        """
        t = schema.get("type")
        if t == "object":
            props = schema.get("properties", {})
            fields = []
            for name, sub_schema in props.items():
                fields.append(
                    PortadaCleaning._cast_column_by_json_schema(col_expr.getField(name), sub_schema).alias(name)
                )
            return F.struct(*fields)
        if t == "array":
            items_schema = schema.get("items", {})
            return F.transform(
                col_expr,
                lambda x: PortadaCleaning._cast_column_by_json_schema(x, items_schema),
            )
        spark_type, _ = PortadaCleaning.json_schema_to_spark_type(schema, primitive_types_as_strings=False)
        return PortadaCleaning._cast_column_to_schema_type(col_expr, spark_type)

    @data_transformer_method(description="cast current fields to schema types")
    def string_to_type_from_schema(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Converteix els camps actuals (sovint string) al tipus indicat al JSON schema.
        Només aplica a camps definits a schema.properties i presents al DataFrame.
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")

        schema_to_use, properties = self.get_schema_and_properties(self._schema)
        if not properties:
            return df

        for field_name, field_schema in properties.items():
            if field_name not in df.columns:
                continue
            casted = PortadaCleaning._cast_column_by_json_schema(F.col(field_name), field_schema)
            df = df.withColumn(field_name, casted)

        return df

    @staticmethod
    def _flatten_string_col(col_expr: F.Column) -> F.Column:
        """Lowercase + reemplaç de caràcters accentuats comuns."""
        lowered = F.lower(col_expr)
        return F.translate(lowered, FLATTEN_STRINGS_FROM, FLATTEN_STRINGS_TO)

    @staticmethod
    def _flatten_strings_by_schema(col_expr: F.Column, schema: dict) -> F.Column:
        """
        Aplica aplanament de caràcters només als camps string segons el JSON schema.
        - object: recursiu per subcamps
        - array: recursiu per item
        - string: lower + transliteració bàsica
        """
        for_cleaning = schema.get("for_cleaning", [])
        if "not_cleanable" in for_cleaning:
            return col_expr
        t = schema.get("type")
        if t == "object":
            props = schema.get("properties", {})
            fields = []
            for name, sub_schema in props.items():
                fields.append(
                    PortadaCleaning._flatten_strings_by_schema(col_expr.getField(name), sub_schema).alias(name)
                )
            return F.struct(*fields)
        if t == "array":
            items_schema = schema.get("items", {})
            return F.transform(
                col_expr,
                lambda x: PortadaCleaning._flatten_strings_by_schema(x, items_schema),
            )
        if t == "string":
            return PortadaCleaning._flatten_string_col(col_expr.cast("string"))
        return col_expr

    @data_transformer_method(description="flatten characters for string fields")
    def flatten_string_characters(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Aplana caràcters dels camps string: minúscules i eliminació de diacrítics bàsics
        (àáâãäå->a, èéêë->e, ìíîï->i, òóôõö->o, ùúûü->u, ç->c, ñ->n, ...).
        S'aplica recursivament en structs/arrays segons schema.properties.
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")

        _, properties = self.get_schema_and_properties(self._schema)
        if not properties:
            return df

        for field_name, field_schema in properties.items():
            if field_name not in df.columns:
                continue
            flattened = PortadaCleaning._flatten_strings_by_schema(F.col(field_name), field_schema)
            df = df.withColumn(field_name, flattened)
        return df

    @data_transformer_method()
    def resolve_idems(self, df: TracedDataFrame) -> TracedDataFrame:
        df = self._resolve_idems_for_root_fields(df)
        df = self._resolve_idems_for_arrays(df)
        return df

    def _resolve_idems_for_root_fields(self, df: TracedDataFrame) -> TracedDataFrame:
            """
            Resolves occurrences of 'idem' in top-level fields tagged with accepted_idem in the schema.
            Values matching IDEM_REGEX are replaced with the last non-idem value before (forward fill).
            If after forward fill the value would be null (e.g. first row of partition), it is restored
            from <field_name>_original_value if it exists, so that an idem is never replaced with null.
            Order and partitions are read from _schema["order_and_partitions"].
            """
            if self._schema is None:
                raise ValueError("Must call use_schema() before.")

            # Camps de primer nivell amb "accepted_idem" a for_cleaning
            schema_to_use, properties = self.get_schema_and_properties(self._schema)
            root_idem_fields = []
            for field_name, field_schema in properties.items():
                for_cleaning = field_schema.get("for_cleaning", [])
                if "accepted_idem" in for_cleaning:
                    root_idem_fields.append(field_name)

            if not root_idem_fields:
                return df

            order_and_partitions = schema_to_use.get("order_and_partitions") or {}
            order_cols = order_and_partitions.get("order")
            if not order_cols:
                raise ValueError("No 'order' in schema order_and_partitions")
            partitions = order_and_partitions.get("partitions")

            # Finestra amb frame per a last(..., ignorenulls=True): propagar darrer valor no null (forward fill)
            if partitions:
                window_ff = Window.partitionBy(*partitions).orderBy(*order_cols).rowsBetween(
                    Window.unboundedPreceding, Window.currentRow
                )
            else:
                window_ff = Window.orderBy(*order_cols).rowsBetween(
                    Window.unboundedPreceding, Window.currentRow
                )

            for field in root_idem_fields:
                if field not in df.columns:
                    continue
                # Només quan el valor coincideix amb la regex idem: posar null i omplir amb darrer valor no null
                is_idem = F.trim(F.col(field).cast("string")).rlike(IDEM_REGEX)
                original_null = F.col(field).isNull()
                # On hi ha idem posem null; la resta es manté
                value_to_fill = F.when(is_idem, F.lit(None)).otherwise(F.col(field))
                filled = F.last(value_to_fill, ignorenulls=True).over(window_ff)
                # On era idem: usar filled; si filled és null, restaurar des de <camp>_original_value si existeix
                idem_resolved = filled
                original_value_col = f"{field}_original_value"
                if original_value_col in df.columns:
                    idem_resolved = F.coalesce(filled, F.col(original_value_col))
                # Restaurar nulls originals; on era idem posar idem_resolved; altrament valor original
                df = df.withColumn(
                    field,
                    F.when(original_null, F.lit(None))
                    .when(is_idem, idem_resolved)
                    .otherwise(F.col(field)),
                )

            return df

    @staticmethod
    def _has_accepted_idem(field_schema: dict) -> bool:
        return "accepted_idem" in field_schema.get("for_cleaning", [])

    @staticmethod
    def _collect_idem_array_specs(
        schema: dict,
        path_prefix: str = "",
        current_array_path: str | None = None,
        result: dict | None = None,
    ) -> dict:
        """
        Recorre el schema i retorna un dict array_path -> { items_schema, fields: [str], nested_arrays: [str] }.
        'fields' són els noms de camps (dins l'item) amb accepted_idem; 'nested_arrays' són els noms
        d'arrays dins l'item que també tenen idem (path serà current_array_path.nested_name).
        """
        if result is None:
            result = {}
        props = schema.get("properties") if schema.get("type") == "object" else None
        if props is not None:
            for name, sub_schema in props.items():
                rel_path = f"{path_prefix}.{name}" if path_prefix else name
                if sub_schema.get("type") == "array":
                    array_path = rel_path
                    items_schema = sub_schema.get("items", {})
                    if array_path not in result:
                        result[array_path] = {
                            "items_schema": items_schema,
                            "items_type": items_schema.get("type", "object"),
                            "fields": [],
                            "nested_arrays": [],
                        }
                    if current_array_path:
                        if name not in result[current_array_path]["nested_arrays"]:
                            result[current_array_path]["nested_arrays"].append(name)
                    # Recursió dins l'array (path_prefix = array_path per camps dins l'item)
                    PortadaCleaning._collect_idem_array_specs(
                        items_schema, array_path, array_path, result
                    )
                else:
                    PortadaCleaning._collect_idem_array_specs(
                        sub_schema, rel_path, current_array_path, result
                    )
            return result
        if schema.get("type") == "array":
            items_schema = schema.get("items", {})
            array_path = f"{path_prefix}"  # path_prefix ja és el path de l'array pare
            if array_path not in result:
                result[array_path] = {
                    "items_schema": items_schema,
                    "items_type": items_schema.get("type", "object"),
                    "fields": [],
                    "nested_arrays": [],
                }
            PortadaCleaning._collect_idem_array_specs(
                items_schema, array_path, array_path, result
            )
            return result
        # Camp fulla (string, number, etc.)
        if current_array_path and PortadaCleaning._has_accepted_idem(schema):
            field_name = path_prefix.split(".")[-1] if "." in path_prefix else path_prefix
            if field_name not in result[current_array_path]["fields"]:
                result[current_array_path]["fields"].append(field_name)
        return result

    def _build_last_element_col(self, array_path: str) -> F.Column:
        """Columna que representa el darrer element de l'array al path (per seed de fila anterior)."""
        parts = array_path.split(".")
        col = F.col(parts[0])
        for i in range(1, len(parts)):
            col = F.element_at(col, -1).getField(parts[i])
        return F.element_at(col, -1)

    @staticmethod
    def _build_last_valid_nested_field_in_row(
        top_col: str, child_array_name: str, field_name: str, child_items_ddl: str | None = None
    ) -> F.Column:
        """
        Retorna l'últim valor vàlid (no null i no idem) d'un camp dins d'un array niat
        per a la fila actual, recorrent tots els items de top_col (p.ex. cargo_list[].cargo[].cargo_unit).
        El resultat es fa servir amb LAG per obtenir el seed del registre anterior.
        """
        regex_sql = IDEM_REGEX.replace("\\", "\\\\").replace("'", "\\'")
        empty_child_arr = "array()"
        if child_items_ddl:
            empty_child_arr = f"cast(array() as array<{child_items_ddl}>)"
        expr = f"""
            aggregate(
                aggregate(
                    {top_col},
                    {empty_child_arr},
                    (acc, m) -> flatten(array(acc, coalesce(m.{child_array_name}, {empty_child_arr})))
                ),
                cast(null as string),
                (acc, x) -> CASE
                    WHEN x.{field_name} is not null
                     AND NOT (trim(cast(x.{field_name} as string)) rlike '{regex_sql}')
                    THEN cast(x.{field_name} as string)
                    ELSE acc
                END
            )
        """
        return F.expr(expr)

    def _resolve_idems_for_arrays(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Resol idem dins dels camps tipus array.
        - Si idem no és a la primera posició: valor = item anterior del mateix array (mateix camp/subcamp).
        - Si idem és a la primera posició: valor = darrer element de l'array del registre anterior (forward fill).
        Suporta arrays d'items simples, structs i arrays niats recursivament.
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        schema_to_use, _ = self.get_schema_and_properties(self._schema)
        order_and_partitions = schema_to_use.get("order_and_partitions") or {}
        order_cols = order_and_partitions.get("order")
        if not order_cols:
            return df
        partitions = order_and_partitions.get("partitions")
        if partitions:
            window_ff = Window.partitionBy(*partitions).orderBy(*order_cols).rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )
            window_lag = Window.partitionBy(*partitions).orderBy(*order_cols)
        else:
            window_ff = Window.orderBy(*order_cols).rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )
            window_lag = Window.orderBy(*order_cols)

        # Recollir especificacions d'arrays amb accepted_idem (des del root properties)
        root_props = schema_to_use.get("properties", {})
        idem_arrays = {}
        for name, field_schema in root_props.items():
            if field_schema.get("type") == "array":
                PortadaCleaning._collect_idem_array_specs(
                    {"type": "object", "properties": {name: field_schema}},
                    path_prefix="",
                    current_array_path=None,
                    result=idem_arrays,
                )

        # Ordenar per profunditat (més profund primer) per processar arrays niats abans del pare
        array_paths = sorted(idem_arrays.keys(), key=lambda p: p.count("."), reverse=True)
        for array_path in array_paths:
            meta = idem_arrays[array_path]
            if not meta["fields"] and not meta.get("nested_arrays"):
                continue
            top_col = array_path.split(".")[0]
            if top_col not in df.columns:
                continue

            seed_col_name = array_path.replace(".", "_") + "_idem_seed"
            # Seed = darrer element de l'array al registre ANTERIOR (reg[current-1].cargo_list[-1].cargo[-1] etc.)
            last_elem = self._build_last_element_col(array_path)
            df = df.withColumn(
                seed_col_name,
                F.lag(last_elem, 1).over(window_lag),
            )

            if "." in array_path:
                # Array niat: transformar el pare i dins d'ell aquest array
                parent_path, child_array_name = array_path.rsplit(".", 1)
                parent_col = parent_path.split(".")[0]
                if parent_col not in df.columns:
                    continue
                child_items_ddl = None
                try:
                    parent_elem_type = df.schema[parent_col].dataType.elementType
                    if isinstance(parent_elem_type, StructType):
                        child_field = next((f for f in parent_elem_type.fields if f.name == child_array_name), None)
                        if child_field and isinstance(child_field.dataType, ArrayType) and isinstance(
                            child_field.dataType.elementType, StructType
                        ):
                            child_items_ddl = child_field.dataType.elementType.simpleString()
                except Exception:
                    child_items_ddl = None
                # Seed addicional per camp: darrer valor vàlid del registre anterior
                # (important quan l'últim item del registre anterior té null en algun subcamp).
                seed_field_cols = {}
                for f_name in meta.get("fields", []):
                    seed_field_col = f"{seed_col_name}_{f_name}_valid"
                    df = df.withColumn(
                        seed_field_col,
                        F.lag(
                            self._build_last_valid_nested_field_in_row(
                                parent_col, child_array_name, f_name, child_items_ddl
                            ),
                            1,
                        ).over(window_lag),
                    )
                    seed_field_cols[f_name] = seed_field_col
                # Transformar la columna pare (cargo_list) per aplicar idem al subarray 'cargo'
                df = self._apply_array_idem_transform_nested(
                    df, parent_path, child_array_name, meta, seed_col_name, seed_field_cols
                )
            else:
                df = self._apply_array_idem_transform(df, array_path, meta, seed_col_name)

            if seed_col_name in df.columns:
                df = df.drop(seed_col_name)
            if "." in array_path:
                for f_name in meta.get("fields", []):
                    seed_field_col = f"{seed_col_name}_{f_name}_valid"
                    if seed_field_col in df.columns:
                        df = df.drop(seed_field_col)

        return df

    def _apply_array_idem_transform(
        self, df: TracedDataFrame, array_path: str, meta: dict, seed_col_name: str
    ) -> TracedDataFrame:
        """Aplica la transformació idem a un array de primer nivell (struct o simple)."""
        idem_fields = set(meta["fields"])
        if not idem_fields:
            return df
        items_type = meta.get("items_type", "object")
        arr_col = F.col(array_path)
        seed = F.col(seed_col_name)

        if items_type == "object":
            all_fields = list(meta["items_schema"].get("properties", {}).keys())
            # No fer cast: el tipus del seed (del DataFrame) pot divergir del schema JSON i Spark falla
            initial_acc = F.array(seed)

            def merge_struct(acc, el):
                prev = F.element_at(acc, -1)
                struct_parts = []
                for f in all_fields:
                    c = el.getField(f)
                    if f in idem_fields:
                        is_idem = F.trim(F.coalesce(c.cast("string"), F.lit(""))).rlike(IDEM_REGEX)
                        prev_f = prev.getField(f)
                        # Només substituir quan el camp és idem: primera posició -> seed, resta -> item anterior
                        val = F.when(
                            is_idem,
                            F.when(F.size(acc) == 1, F.coalesce(seed.getField(f), c)).otherwise(prev_f),
                        ).otherwise(c)
                    else:
                        val = c
                    struct_parts.append(val.alias(f))
                # Afegim el nou element (flatten(array(acc, array(x))) = concat d'arrays, compatible amb PySpark sense array_concat)
                return F.flatten(F.array(acc, F.array(F.struct(*struct_parts))))

            def finish_struct(acc):
                # Traiem el seed (primer element) i mantenim la resta
                return F.slice(acc, 2, F.size(acc) - 1)

            new_array = F.when(
                arr_col.isNotNull() & (F.size(arr_col) > 0),
                F.aggregate(arr_col, initial_acc, merge_struct, finish_struct),
            ).otherwise(arr_col)

            df = df.withColumn(array_path, new_array)
        else:
            # Array de tipus simple
            initial_acc = F.array(seed)

            def merge_simple(acc, el):
                is_idem = F.trim(F.coalesce(el.cast("string"), F.lit(""))).rlike(IDEM_REGEX)
                prev_val = F.element_at(acc, -1)
                # Només substituir quan el valor és idem
                resolved = F.when(
                    is_idem,
                    F.when(F.size(acc) == 1, F.coalesce(seed, el)).otherwise(prev_val),
                ).otherwise(el)
                # Afegim el nou element (flatten(array(acc, array(x))) = concat d'arrays)
                return F.flatten(F.array(acc, F.array(resolved)))

            def finish_simple(acc):
                # Traiem el seed (primer element) i mantenim la resta
                return F.slice(acc, 2, F.size(acc) - 1)

            new_array = F.when(
                arr_col.isNotNull() & (F.size(arr_col) > 0),
                F.aggregate(arr_col, initial_acc, merge_simple, finish_simple),
            ).otherwise(arr_col)

            df = df.withColumn(array_path, new_array)
        return df

    def _apply_array_idem_transform_nested(
        self,
        df: TracedDataFrame,
        parent_path: str,
        child_array_name: str,
        meta: dict,
        seed_col_name: str,
        seed_field_cols: dict | None = None,
    ) -> TracedDataFrame:
        """
        Aplica idem a un array fill (cargo) dins d'un array pare (cargo_list).
        Seed per al primer element de cargo: si és el primer item de cargo_list -> registre anterior;
        si és el segon o més -> darrer cargo de l'item ANTERIOR de cargo_list (mateixa fila).
        """
        idem_fields = set(meta["fields"])
        if not idem_fields:
            return df
        parts = parent_path.split(".")
        top_col = parts[0]
        arr_col = F.col(top_col)
        row_seed = F.col(seed_col_name)  # darrer cargo del registre anterior (per primer merchant)
        items_schema = meta["items_schema"]
        all_fields = list(items_schema.get("properties", {}).keys())
        merchant_type = df.schema[top_col].dataType.elementType
        empty_merchant_list = F.expr(f"cast(array() as array<{merchant_type.simpleString()}>)")

        def finish_inner(acc):
            return F.slice(acc, 2, F.size(acc) - 1)

        # Acumulador exterior: (llista merchants transformats, darrer cargo struct, row_seed)
        initial_outer = F.struct(
            empty_merchant_list.alias("list"),
            row_seed.alias("last_cargo"),
            row_seed.alias("row_seed"),
        )

        def merge_outer(acc, merchant):
            list_so_far = acc.getField("list")
            last_cargo = acc.getField("last_cargo")
            r_seed = acc.getField("row_seed")
            # Seed per al primer element de cargo: primer item de cargo_list -> row; sinó -> item anterior
            seed_inner = F.when(F.size(list_so_far) == 0, r_seed).otherwise(last_cargo)
            initial_inner = F.array(seed_inner)

            def merge_inner(acc_inner, el):
                prev = F.element_at(acc_inner, -1)
                struct_parts = []
                for f in all_fields:
                    c = el.getField(f)
                    if f in idem_fields:
                        is_idem = F.trim(F.coalesce(c.cast("string"), F.lit(""))).rlike(IDEM_REGEX)
                        prev_f = prev.getField(f)
                        field_seed_fallback = (
                            F.col(seed_field_cols[f]) if seed_field_cols and f in seed_field_cols else F.lit(None)
                        )
                        val = F.when(
                            is_idem,
                            F.when(
                                F.size(acc_inner) == 1,
                                F.coalesce(seed_inner.getField(f), field_seed_fallback, c),
                            ).otherwise(F.coalesce(prev_f, field_seed_fallback, c)),
                        ).otherwise(c)
                    else:
                        val = c
                    struct_parts.append(val.alias(f))
                return F.flatten(F.array(acc_inner, F.array(F.struct(*struct_parts))))

            inner_arr = merchant.getField(child_array_name)
            transformed_cargo = F.when(
                inner_arr.isNotNull() & (F.size(inner_arr) > 0),
                F.aggregate(inner_arr, initial_inner, merge_inner, finish_inner),
            ).otherwise(inner_arr)
            new_merchant = merchant.withField(child_array_name, transformed_cargo)
            new_last_cargo = F.coalesce(F.element_at(transformed_cargo, -1), last_cargo)
            new_list = F.flatten(F.array(list_so_far, F.array(new_merchant)))
            return F.struct(
                new_list.alias("list"),
                new_last_cargo.alias("last_cargo"),
                r_seed.alias("row_seed"),
            )

        def finish_outer(acc):
            return acc.getField("list")

        new_cargo_list = F.when(
            arr_col.isNotNull() & (F.size(arr_col) > 0),
            F.aggregate(arr_col, initial_outer, merge_outer, finish_outer),
        ).otherwise(arr_col)

        df = df.withColumn(top_col, new_cargo_list)
        return df

    @block_transformer_method
    def cleaning(self, df: DataFrame | TracedDataFrame) -> TracedDataFrame:
        # 1.- prune values
        df = self.prune_unaccepted_fields(df)
        # 2.- Normalize structure
        df = self.normalize_field_structure(df)
        # 3.- save original values
        df = self.save_original_values_of_ship_entries(df)
        # 4.- Normalize values
        df = self.normalize_field_value(df)
        # 6.- Resolve idem/id/id. with the value of the predecessor record
        df = self.resolve_idems(df)
        # # 7.- Convert values form string to corrected type
        # df = self.convert_string_to_schematype(df) - Numero en letras a número
        # # 8.- Save cleaned values
        # self.save_ship_entries(df)
        return df

    #### -------------------- Auxiliar methods -------------------------- ###
    def _update_state(self, *container_path, df, value: bool = True):
        return super()._update_state(*container_path, df=df, value=True)

    @staticmethod
    def get_schema_and_properties(schema: dict) -> tuple:
        """Resolve schema root and return (schema_to_use, properties)."""
        if "schema" in schema:
            schema = schema["schema"]
        properties = schema.get("properties", {})
        return schema, properties
    
    @staticmethod
    def get_schema_root(schema: dict) -> dict:
        """Resolve schema root and return the 'properties' dict (field name -> field schema)."""
        if "schema" in schema:
            schema = schema["schema"]
        return schema

    @staticmethod
    def _schema_properties(schema: dict) -> dict:
        """Resolve schema root and return the 'properties' dict (field name -> field schema)."""
        schema = PortadaCleaning.get_schema_root(schema)
        if "properties" in schema:
            return schema.get("properties", {})
        return schema

    def _expr_for_missing_column(self, col: str) -> F.Column:
        """
        Build a column expression with the correct Spark type for a missing column.
        - Base fields (in schema): type from config/schema.json via json_schema_to_spark_type.
        - <nom_camp>_extraction_source_method: String nullable.
        - <nom_camp>_detailed_value: struct(default_value, original_value, calculated_value, boat_fact_model), all string nullable.
        """
        props = self._schema_properties(self._schema)
        suffix_esm = f"_{BoatFactDataModel.EXTRACTION_SOURCE_METHOD_FIELD}"
        suffix_dv = f"_{BoatFactDataModel.DETAILED_VALUE_FIELD}"

        if col in props:
            field_schema = props[col]
            spark_type, _ = PortadaCleaning.json_schema_to_spark_type(field_schema, primitive_types_as_strings=True)
            return F.lit(None).cast(spark_type).alias(col)
        if col.endswith(suffix_esm):
            return F.lit(None).cast(StringType()).alias(col)
        if col.endswith(suffix_dv):
            detailed_struct = StructType([
                StructField(BoatFactDataModel.DEFAULT_VALUE_FIELD, StringType(), True),
                StructField(BoatFactDataModel.ORIGINAL_VALUE_FIELD, StringType(), True),
                StructField(BoatFactDataModel.CALCULATED_VALUE_FIELD, StringType(), True),
                StructField(BoatFactDataModel.BOAT_FACT_MODEL_FIELD, StringType(), True),
            ])
            return F.lit(None).cast(detailed_struct).alias(col)
        return F.lit(None).alias(col)

    @staticmethod
    def _is_simple_type(t: str) -> bool:
        """Check if type is a simple/primitive (string, number, integer, boolean)."""
        return t in ("string", "number", "integer", "float", "boolean")

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
        """
        Cast the column _col as type struct. struct is an instance of DataType spark. If struct is a StructType or an
        ArrayType, this method converts the column, even if it is defined as a string in the dataframe, if it contains
        a JSON string.
        :param _col: expression or column to cast
        :param struct: type of DataType spark to cast the _col.
        :return: _col casted to struct
        """
        if not (isinstance(struct, StructType) or isinstance(struct, ArrayType)):
            return _col.cast(struct)
        # Always convert to string
        s = _col.cast("string")

        # Detect JSON
        looks_like_json = s.startswith("{") & s.endswith("}") | s.startswith("[") & s.endswith("]")

        # from_json never throws
        parsed = F.from_json(s, struct)
        return F.when(looks_like_json, parsed).otherwise(F.lit(None))


    ### -------------- used by normalize_field_structure -------------------###

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

    ### ---------------- used by normalize_field_value ---------------------- ###
    @staticmethod
    def apply_ocr_corrections(col, mapping):
        map_for_translate = {}
        map_for_replace = {}
        for k, v in mapping.items():
            if v and len(k) == 1:
                map_for_translate[k] = v
            else:
                map_for_replace[k] = v
        wrong_chars = "".join(map_for_translate.keys())
        correct_chars = "".join(map_for_translate.values())
        expr_col = col
        for w, r in map_for_replace.items():
            if len(w) == 1:
                w = re.escape(w)
            expr_col = F.regexp_replace(expr_col, w, r)
        return F.when(col.isNull(), col).when(col=="null", col).otherwise(F.translate(expr_col, wrong_chars, correct_chars))

    @staticmethod
    def apply_cleaning_process(col, for_cleaning_list: list, mapping: dict):
        def change_chars(c, m: dict = None):
            return PortadaCleaning.apply_ocr_corrections(c, m)

        def transform_mapping_with_params(mapping: dict, params: dict):
            if params is not None and "avoid_changes_for_keys" in params:
                for v in params["avoid_changes_for_keys"]:
                    if v in mapping:
                        mapping.pop(v)
            return mapping

        def one_word(c, m: dict = None, params: dict = None):
            m = transform_mapping_with_params(m.get("one_word_map", {}), params)
            return change_chars(c, m)

        def not_digit(c, m: dict = None, params: dict = None):
            m = transform_mapping_with_params(m.get("only_text_map", {}), params)
            return change_chars(c, m)

        def not_paragraph(c, m: dict = None, params: dict = None):
            m = transform_mapping_with_params(m.get("not_paragraph_map", {}), params)
            return change_chars(c, m)

        params_for_cleaning_list = []
        alg_for_cleaning_list = []
        for item in for_cleaning_list:
            if isinstance(item, dict):
                alg_for_cleaning_list.append(item["algorithm"])
                params = item.get("params", None)
                params_for_cleaning_list.append(params)
            else:
                alg_for_cleaning_list.append(item)
                params_for_cleaning_list.append(None)

        if "avoid_changes_for_keys" in alg_for_cleaning_list:
            p = params_for_cleaning_list[alg_for_cleaning_list.index("avoid_changes_for_keys")]
            params = {"avoid_changes_for_keys": p}
        else:
            params = None

        processes = {"one_word": one_word, "not_digits": not_digit}
        if "paragraph" in alg_for_cleaning_list or "not_cleanable" in alg_for_cleaning_list:
            expr = col
        elif "accepted_abbreviations" in alg_for_cleaning_list or "accepted_idem" in alg_for_cleaning_list:
            expr = F.trim(col)
            expr = F.when(F.length(expr) > 5, F.regexp_replace(expr, r"[.,;]+$", "")).otherwise(expr)
        else:
            expr = F.regexp_replace(F.trim(col), r"[.,;]+$", "")
        if "not_cleanable" not in alg_for_cleaning_list and "paragraph" not in alg_for_cleaning_list and "one_word" not in alg_for_cleaning_list:
            expr = not_paragraph(expr, mapping, params)
        for process in alg_for_cleaning_list:
            if process in processes:
                p = params_for_cleaning_list[alg_for_cleaning_list.index(process)]
                if params is None:
                    params = p
                elif p is not None:
                    for k, v in p.items():
                        params[k] = v
                expr = processes[process](expr, mapping, params)
        if "not_cleanable_if_value_is" in alg_for_cleaning_list:
            p = params_for_cleaning_list[alg_for_cleaning_list.index("not_cleanable_if_value_is")]
            if isinstance(p, list):
                expr_temp = col
                for i, p_item in enumerate(p):
                    if i==0:
                        expr_temp = F.when(col==F.lit(p[i]), col)
                    else:
                        expr_temp = expr_temp.when(col==F.lit(p[i]), col)
                expr = expr_temp.otherwise(expr)
            else:
                expr = F.when(col==F.lit(p), col).otherwise(expr)
        return expr

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
                if f_name in all_mapping:
                    mapping = all_mapping[f_name]
                    current_expr = current_expr.withField(f_name, self.apply_ocr_corrections(current_expr[f_name], mapping))
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

    # ---------- used by fill_with_schema: fill fields from other columns according to schema ----------

    @staticmethod
    def _fill_col_from_path(path: str):
        """Returns a Spark column from a dot-separated path (e.g. 'obs.automatic')."""
        if not path or not path.strip():
            return F.lit(None)
        parts = path.strip().split(".")
        col = F.col(parts[0])
        for p in parts[1:]:
            col = col[p]
        return col

    @staticmethod
    def _fill_resolve_from(from_ref, root_df=None):
        """
        Resolve the 'from' reference: string (field), null, or dict {type: 'f'|'field'|'l'|'literal', value: ...}.
        If type is 'l'/'literal', value is the literal; if string "LIT('x')" it is treated as literal 'x'.
        """
        if from_ref is None:
            return F.lit(None)
        if isinstance(from_ref, dict):
            t = from_ref.get("type", "f")
            v = from_ref.get("value")
            if t in ("l", "literal"):
                if v is None:
                    return F.lit(None)
                # Support for "LIT('text')" or "lit(true)" style strings in schema
                if isinstance(v, str) and v.strip().upper().startswith("LIT(") and v.rstrip().endswith(")"):
                    inner = v.strip()[4:-1].strip()
                    if (inner.startswith("'") and inner.endswith("'")) or (inner.startswith('"') and inner.endswith('"')):
                        inner = inner[1:-1].replace("\\'", "'").replace('\\"', '"')
                        return F.lit(inner)
                    # Unquoted: true/false/null as boolean/null
                    if inner.lower() == "true":
                        return F.lit(True)
                    if inner.lower() == "false":
                        return F.lit(False)
                    if inner.lower() == "null":
                        return F.lit(None)
                    return F.lit(inner)
                return F.lit(v)
            if t in ("f", "field", "column"):
                path = v if v is not None else ""
                return PortadaCleaning._fill_col_from_path(path)
            if t == "expr":
                return F.expr(v) if v else F.lit(None)
        if isinstance(from_ref, str):
            s = from_ref.strip()
            if not s:
                return F.lit(None)
            if s.upper().startswith("LIT(") and s.rstrip().endswith(")"):
                inner = s.strip()[4:-1].strip()
                if (inner.startswith("'") and inner.endswith("'")) or (inner.startswith('"') and inner.endswith('"')):
                    inner = inner[1:-1].replace("\\'", "'").replace('\\"', '"')
                    return F.lit(inner)
                if inner.lower() == "true":
                    return F.lit(True)
                if inner.lower() == "false":
                    return F.lit(False)
                if inner.lower() == "null":
                    return F.lit(None)
                return F.lit(inner)
            return PortadaCleaning._fill_col_from_path(s)
        return F.lit(None)

    @staticmethod
    def _fill_condition_expr(condition) -> F.Column:
        """
        Convert condition to boolean column. Supports:
        - None: True
        - String: Spark SQL expression (e.g. "in_ballast == true")
        - Dict with "expr" or "expression": F.expr(value)
        - Dict structured: {"column": "col_name", "op": "==", "value": <ref>}
          value uses same ref as elsewhere: string "lit(true)" or "columna_b", or {"type":"l","value":true} or {"type":"f","value":"columna_b"}.
          op: "==", "!=", ">", ">=", "<", "<=", "isNull", "isNotNull"
        - Dict with "and"/"or": list of conditions combined with & or |
        """
        if condition is None:
            return F.lit(True)
        if isinstance(condition, str) and condition.strip():
            return F.expr(condition.strip())
        if not isinstance(condition, dict):
            return F.lit(True)
        if "expression" in condition:
            v = condition["expression"]
            if isinstance(v, str) and v.strip():
                return F.expr(v.strip())
            if isinstance(v, type(F.lit(0))):  # Column (evitar F.expr(Column) -> error)
                return v
        if "expr" in condition:
            v = condition["expr"]
            if isinstance(v, str) and v.strip():
                return F.expr(v.strip())
            if isinstance(v, type(F.lit(0))):  # Column
                return v
        # Structured: column op value; value resolved like "from" (literal or column ref)
        if "column" in condition:
            col_name = condition["column"]
            op = (condition.get("op") or "==").strip()
            col = F.col(col_name)
            if op in ("isNull", "is null"):
                return col.isNull()
            if op in ("isNotNull", "is not null"):
                return col.isNotNull()
            # Right operand: same ref as elsewhere (string "lit(true)" / "columna_b", or {"type":"l"/"f", "value":...})
            right = PortadaCleaning._fill_resolve_from(condition.get("value"))
            # Usar operadors binaris per evitar "Column is not callable" (alguns entorns confonen col.eq amb Column)
            if op == "==":
                return (col == right)
            if op == "!=":
                return (col != right)
            if op == ">":
                return (col > right)
            if op == ">=":
                return (col >= right)
            if op == "<":
                return (col < right)
            if op == "<=":
                return (col <= right)
            return F.lit(True)
        # Compound: {"and": [cond1, cond2]} or {"or": [cond1, cond2]}
        if "and" in condition:
            sub = condition["and"]
            if not sub:
                return F.lit(True)
            exprs = [PortadaCleaning._fill_condition_expr(c) for c in sub]
            out = exprs[0]
            for e in exprs[1:]:
                out = out & e
            return out
        if "or" in condition:
            sub = condition["or"]
            if not sub:
                return F.lit(True)
            exprs = [PortadaCleaning._fill_condition_expr(c) for c in sub]
            out = exprs[0]
            for e in exprs[1:]:
                out = out | e
            return out
        return F.lit(True)

    @staticmethod
    def _fill_required_columns_from_condition(condition) -> set:
        """
        Return the set of column names referenced by the condition (so they must exist in the dataframe).
        Supports: None; string (simple "col op value" -> left identifier); dict "column"; dict "columns"; dict "and"/"or".
        """
        out = set()
        if condition is None:
            return out
        if isinstance(condition, str):
            s = condition.strip()
            if not s:
                return out
            # Simple heuristic: left-hand side of first comparison is the column
            for sep in ("==", "!=", ">=", "<=", ">", "<"):
                if sep in s:
                    left = s.split(sep)[0].strip()
                    if left and left.replace("_", "").isalnum() and left.lower() not in ("true", "false", "null"):
                        out.add(left)
                    break
            return out
        if not isinstance(condition, dict):
            return out
        if "column" in condition:
            out.add(condition["column"])
            # value can be a column ref (string or {"type":"f", "value": "col_b"})
            out |= PortadaCleaning._fill_required_columns(condition.get("value"))
            return out
        if "columns" in condition:
            out |= set(condition["columns"])
            return out
        if "and" in condition or "or" in condition:
            for sub in condition.get("and", []) + condition.get("or", []):
                out |= PortadaCleaning._fill_required_columns_from_condition(sub)
        return out

    @staticmethod
    def _fill_default_from_condition(from_ref) -> F.Column:
        """
        Condició per defecte: 'from' existeix i no és null.
        Per add_array_item (from és llista): només s'aplica si totes les columnes
        referenciades a from existeixen i no són null (condició implícita).
        """
        if isinstance(from_ref, list):
            required = PortadaCleaning._fill_required_columns(from_ref)
            if not required:
                return F.lit(True)
            cond = F.lit(True)
            for col_name in required:
                cond = cond & F.col(col_name).isNotNull()
            return cond
        from_col = PortadaCleaning._fill_resolve_from(from_ref)
        return from_col.isNotNull()

    @staticmethod
    def _fill_required_columns(from_ref) -> set:
        """
        Return the set of root column names (first path segment) referenced by from_ref.
        If any of these are missing from the dataframe, the fill spec should be skipped.
        """
        out = set()
        if from_ref is None:
            return out
        if isinstance(from_ref, dict):
            t = from_ref.get("type", "f")
            v = from_ref.get("value")
            if t in ("l", "literal", "expr"):
                return out
            if t in ("f", "field", "column") and v:
                path = (v if isinstance(v, str) else "").strip()
                if path and not path.upper().startswith("LIT("):
                    out.add(path.split(".")[0])
            return out
        if isinstance(from_ref, str):
            s = from_ref.strip()
            if s and not s.upper().startswith("LIT("):
                out.add(s.split(".")[0])
            return out
        if isinstance(from_ref, list):
            for item in from_ref:
                if isinstance(item, dict) and item.get("type") == "add_array_item":
                    sub_from = item.get("from")
                    if isinstance(sub_from, list):
                        for sub in sub_from:
                            out |= PortadaCleaning._fill_required_columns(sub)
                    else:
                        out |= PortadaCleaning._fill_required_columns(sub_from)
                else:
                    out |= PortadaCleaning._fill_required_columns(item)
            return out
        return out

    def _fill_apply_one(self, df: TracedDataFrame, field_name: str, field_schema: dict, spec: dict) -> TracedDataFrame:
        """Apply a single fill_with spec (replace, replace_if_not_exist, add_array_item, add_all_items)."""
        fill_type = spec.get("type", "replace")
        from_ref = spec.get("from")
        to_ref = spec.get("to", field_name)
        condition = spec.get("condition")
        drop_origin = spec.get("drop_origin", False)
        current = F.col(field_name)

        # Skip if any column referenced by "from" or "condition" does not exist in the dataframe
        existing_columns = set(df.columns)
        required_columns = self._fill_required_columns(from_ref) | self._fill_required_columns_from_condition(condition)
        if required_columns and not required_columns.issubset(existing_columns):
            return df

        # Condition: default "from exists and is not null" when not specified
        cond_expr = self._fill_condition_expr(condition) if condition is not None else self._fill_default_from_condition(from_ref)

        if fill_type == "replace":
            if isinstance(from_ref, list) and isinstance(to_ref, list):
                struct_fields = [
                    self._fill_resolve_from(from_ref[i]).alias(to_ref[i])
                    for i in range(min(len(from_ref), len(to_ref)))
                ]
                new_val = F.struct(*struct_fields) if struct_fields else current
                df = df.withColumn(field_name, F.when(cond_expr, new_val).otherwise(current))
            else:
                from_expr = self._fill_resolve_from(from_ref)
                if isinstance(to_ref, str) and "." in to_ref and to_ref.split(".")[0] == field_name:
                    path_inside = ".".join(to_ref.split(".")[1:])
                    new_val = current.withField(path_inside, from_expr)
                elif isinstance(to_ref, str) and to_ref.strip():
                    new_val = self._fill_resolve_from(from_ref)
                else:
                    new_val = from_expr
                df = df.withColumn(field_name, F.when(cond_expr, new_val).otherwise(current))

        elif fill_type == "replace_if_not_exist":
            if isinstance(from_ref, list) and isinstance(to_ref, list):
                # Object: multiple from -> to (e.g. from ["observations.ai","observations.human"], to ["obs.automatic","obs.manual"])
                struct_fields = []
                for i, to_name in enumerate(to_ref):
                    if i < len(from_ref):
                        struct_fields.append(self._fill_resolve_from(from_ref[i]).alias(to_name))
                if struct_fields:
                    new_struct = F.struct(*struct_fields)
                    new_val = F.when(current.isNull(), new_struct).otherwise(current)
                    df = df.withColumn(field_name, F.when(cond_expr, new_val).otherwise(current))
            else:
                from_expr = self._fill_resolve_from(from_ref)
                if isinstance(to_ref, str) and "." in to_ref and to_ref.split(".")[0] == field_name:
                    path_inside = ".".join(to_ref.split(".")[1:])
                    branch = current[path_inside] if path_inside else current
                    new_branch = F.when(branch.isNull(), from_expr).otherwise(branch)
                    new_val = current.withField(path_inside, new_branch)
                else:
                    new_val = F.when(current.isNull(), from_expr).otherwise(current)
                df = df.withColumn(field_name, F.when(cond_expr, new_val).otherwise(current))
                if drop_origin and isinstance(from_ref, str) and from_ref.strip() and from_ref in df.columns:
                    df = df.drop(from_ref)

        elif fill_type == "add_array_item":
            new_item = self._fill_build_array_item_expr(field_name, field_schema, spec)
            if new_item is None:
                return df
            try:
                current_dtype = df.schema[field_name].dataType
                if isinstance(current_dtype, ArrayType):
                    new_item = new_item.cast(current_dtype.elementType)
            except Exception:
                pass
            # Afegir un sol item al final de l'array (append); flatten preserva l'ordre (array_union podria desordenar/deduplicar)
            new_array = F.when(current.isNull(), F.array(new_item)).otherwise(
                F.flatten(F.array(current, F.array(new_item)))
            )
            df = df.withColumn(field_name, F.when(cond_expr, new_array).otherwise(current))

        elif fill_type == "add_all_items":
            from_expr = self._fill_resolve_from(from_ref)
            new_array = F.when(current.isNull(), from_expr).otherwise(
                F.coalesce(current, from_expr)
            )
            df = df.withColumn(field_name, F.when(cond_expr, new_array).otherwise(current))

        return df

    def _fill_build_array_item_expr(self, parent_field: str, parent_schema: dict, spec: dict) -> F.Column:
        """
        Build the expression for the item to add in add_array_item.
        from/to can be simple or nested (from = [null, { type: "add_array_item", to: [...], from: [...] }]).
        """
        from_ref = spec.get("from")
        to_ref = spec.get("to")
        if to_ref is None:
            to_ref = []
        if not isinstance(to_ref, list):
            to_ref = [to_ref] if to_ref is not None else []

        from_list = from_ref if isinstance(from_ref, list) else [from_ref] if from_ref is not None else []
        if len(from_list) != len(to_ref):
            if not to_ref and from_list:
                to_ref = [f"field_{i}" for i in range(len(from_list))]
            elif to_ref and len(from_list) < len(to_ref):
                from_list = from_list + [None] * (len(to_ref) - len(from_list))

        fields = []
        items_schema = parent_schema.get("items", {}) if parent_schema else {}
        item_props = items_schema.get("properties", {}) if isinstance(items_schema, dict) else {}

        for i, to_name in enumerate(to_ref):
            if i >= len(from_list):
                fields.append(F.lit(None).alias(to_name))
                continue
            from_item = from_list[i]
            if isinstance(from_item, dict) and from_item.get("type") == "add_array_item":
                # Schema del subarray: l'item que afegim ha de tenir l'estructura de l'array fill (p.ex. cargo),
                # no la de l'item pare. Si to_name és un array al schema (p.ex. "cargo"), agafar items d'aquest.
                nested_parent_schema = item_props.get(to_name) if isinstance(item_props, dict) else {}
                if isinstance(nested_parent_schema, dict) and nested_parent_schema.get("type") == "array":
                    nested_items_schema = nested_parent_schema.get("items", from_item.get("items", {}))
                else:
                    nested_items_schema = from_item.get("items", item_props)
                nested_item = self._fill_build_array_item_expr(
                    f"{parent_field}.{to_name}", nested_items_schema, from_item
                )
                # Un sol item per al subarray; fill_with penja del camp pare (cargo_list), així que
                # l'item sencer (cargo_merchant_name + cargo) s'afegeix a cargo_list, no dins el darrer element.
                fields.append(F.array(nested_item).alias(to_name))
            elif isinstance(from_item, dict) and from_item.get("type") == "add_all_items":
                from_expr = self._fill_resolve_from(from_item.get("from"))
                fields.append(from_expr.alias(to_name))
            else:
                fields.append(self._fill_resolve_from(from_item).alias(to_name))

        if not fields:
            return None
        return F.struct(*fields)

    # ---------- used by calculate_from_schema: fill null fields from calculable_if_null ----------

    @staticmethod
    def _calc_resolve_param(param_value):
        """Resolve a parameter value (column or literal) using same convention as fill_with: string = column, LIT(...) or {type, value}."""
        return PortadaCleaning._fill_resolve_from(param_value)

    @staticmethod
    def _calc_decrement_data(params: dict) -> F.Column:
        """
        Compute date = origin_date - decrement in decrement_unit.
        parameters: origin_date, decrement, decrement_unit (each string column name, LIT(...), or {type, value}).
        Unit: 'd'/'days' -> days; 'h'/'hours' -> hours.
        """
        origin_col = PortadaCleaning._calc_resolve_param(params.get("origin_date"))
        decrement_col = PortadaCleaning._calc_resolve_param(params.get("decrement"))
        unit_col = PortadaCleaning._calc_resolve_param(params.get("decrement_unit"))
        decrement_int = F.floor(decrement_col).cast("int")
        # When unit is days (d, days, d.): date_sub
        days_expr = F.date_sub(origin_col.cast("date"), decrement_int)
        # When unit is hours: timestamp - hours, then cast to date
        hours_expr = F.from_unixtime(
            F.to_timestamp(origin_col.cast("date")).cast("long") - decrement_int * 3600
        ).cast(DateType())
        unit_lower = F.lower(F.trim(unit_col.cast("string")))
        return F.when(unit_lower.isin("d", "days", "d."), days_expr).when(
            unit_lower.isin("h", "hours", "h."), hours_expr
        ).otherwise(days_expr)

    @staticmethod
    def _calc_data_difference(params: dict) -> F.Column:
        """
        Compute difference in days between final_date and origin_date (final - origin).
        parameters: origin_date, final_date (each string column name, LIT(...), or {type, value}).
        """
        origin_col = PortadaCleaning._calc_resolve_param(params.get("origin_date"))
        final_col = PortadaCleaning._calc_resolve_param(params.get("final_date"))
        return F.datediff(final_col.cast("date"), origin_col.cast("date"))

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

    def start_session(self):
        super().start_session()
        patcher = BoatFactPatcherDataLayer(cfg_json=self.get_configuration())
        if self.sequencer_params is not None:
            patcher.set_delta_data_version_manager_params(host=self.sequencer_params["host"], port=self.sequencer_params["port"])
        patcher.patch_if_needed()

    def save_original_values_of_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        self.write_delta("original_data", self.__container_path, df=ship_entries_df, mode="merge",
                         partition_by=["publication_name", "publication_date", "publication_edition"])
        return ship_entries_df

    def save_ship_entries(self, ship_entries_df: TracedDataFrame, is_cleaned=False) -> TracedDataFrame:
        self.write_delta(self.__container_path,df=ship_entries_df, mode="merge",
                         partition_by=["publication_name", "publication_date", "publication_edition"])
        if is_cleaned:
            self._update_state(self.__container_path, df=ship_entries_df)
        return ship_entries_df

    def read_ship_entries(self) -> TracedDataFrame:
        ship_entries_df = self.read_delta(self.__container_path)
        return ship_entries_df

    def read_raw_entries(self, *container_path, force_all=False):
        if len(container_path) > 0:
            cp = container_path
        else:
            cp = (self.__container_path,)
        ship_entries_df = super().read_raw_entries(cp, force_all=force_all)
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
        # 1. First level: explode the list of merchants (cargo_list)
        # Use posexplode to preserve order
        df_merchants = df_entries.select(
            "entry_id",  # Your original id field
            "temp_key",
            F.posexplode("cargo_list").alias("merchant_idx", "merchant_struct")
        )

        # 2. Second level: explode the cargo list inside each merchant
        # Extract merchant name and explode their inner cargo array
        df_cargo = df_merchants.select(
            "entry_id",
            "temp_key",
            "merchant_idx",
            F.posexplode("merchant_struct.cargo").alias("cargo_idx", "col")
        )

        df_comodity_and_unit = df_cargo.select(
            "entry_id",
            "temp_key",
            F.lit("cargo_list").alias("field_origin"),
            "merchant_idx",
            "cargo_idx",
            F.col("col.cargo_commodity").alias("cargo_commodity_citation"),
            F.col("col.cargo_unit").alias("cargo_unit_citation"),
            F.concat("col.cargo_commodity", F.lit("-"), "col.cargo_unit").alias("citation")
        )
        df_comodity_and_unit = df_comodity_and_unit.withColumn(
            "id",
            F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "merchant_idx", F.lit("-"), "cargo_idx")
        )

        return df_comodity_and_unit


    @staticmethod
    def extract_cargo_merchants(df_entries):
        df_merchants = df_entries.select(
            "entry_id",  # Your original id field
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
        """
        Extract brokers from a DataFrame of entries.

        Parameters
        ----------
        df_entries : pyspark.sql.DataFrame
            A DataFrame containing entries.

        Returns
        -------
        pyspark.sql.DataFrame
            A DataFrame containing brokers.
        """
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
        """
        Extract the masters citations in newspapers from the given DataFrame.

        Parameters
        ----------
        df_entries : DataFrame
            The DataFrame containing the masters to be extracted.

        Returns
        -------
        DataFrame
            A DataFrame containing the extracted masters citations.
        """
        return BoatFactCleaning._extract_single_entry(df_entries)

    @staticmethod
    def extract_ships(df_entries):
        """
        Extract the ships citations in newspapers from the given DataFrame.

        Parameters
        ----------
        df_entries : DataFrame
            The DataFrame containing the ships to be extracted.

        Returns
        -------
        DataFrame
            The DataFrame containing the extracted ships.
        """
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

    def get_known_entity_voices(self, known_entity: str = None, df_entities: DataFrame = None) -> DataFrame:
        """
        Given a known entity, return a DataFrame with the voices associated with each name.
        If no known entity is provided, use the df_entities provided.

        Parameters
        ----------
        known_entity : str
            The known entity to extract voices from.
        df_entities : DataFrame, optional
            The DataFrame containing the known entities to extract voices from.

        Returns
        -------
        DataFrame
            A DataFrame with the voices associated with each name.
        """
        if known_entity is not None:
            df_entities = self.read_delta("known_entities", known_entity)
        if df_entities is None:
            raise ValueError("No known entities found")
        # Select the columns of interest
        df_voices = df_entities.select(
            "name",  # Your original id field
            F.posexplode("voices").alias("voice_idx", "voice")
        )
        # Create a new column with the concatenation of the name and the voice idx
        df_voices = df_voices.select(
            F.concat(F.col("name"), F.lit("-"), F.col("voice")).alias("id"),
            F.col("name"),
            F.col("voice_idx"),
            F.lit("voices").alias("field_origin"),
            F.col("voice")
        )
        return df_voices
