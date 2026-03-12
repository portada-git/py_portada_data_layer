import re
import os
import logging

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



    @data_transformer_method()
    def resolve_idems(self, df: TracedDataFrame) -> TracedDataFrame:
        df = self._resolve_idems_for_root_fields(df)
        df = self._resolve_idems_for_arrays(df)
        return df

    def _resolve_idems_for_root_fields(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Resol les aparicions de 'idem' als camps de primer nivell etiquetats amb accepted_idem al schema.
        Els valors que coincideixen amb IDEM_REGEX es reemplacen pel darrer valor no idem anterior (forward fill).
        Si després del forward fill el valor quedaria null (p. ex. primera fila de la partició), es restaura
        des de <nom_camp>_original_value si existeix, de manera que mai no es substitueix un idem per null.
        L'ordre i particions es llegeixen de _schema["order_and_partitions"].
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
        else:
            window_ff = Window.orderBy(*order_cols).rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )

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
            last_elem = self._build_last_element_col(array_path)
            df = df.withColumn(
                seed_col_name,
                F.last(last_elem, ignorenulls=True).over(window_ff),
            )

            if "." in array_path:
                # Array niat: transformar el pare i dins d'ell aquest array
                parent_path, child_array_name = array_path.rsplit(".", 1)
                parent_col = parent_path.split(".")[0]
                if parent_col not in df.columns:
                    continue
                # Transformar la columna pare (cargo_list) per aplicar idem al subarray 'cargo'
                df = self._apply_array_idem_transform_nested(
                    df, parent_path, child_array_name, meta, seed_col_name
                )
            else:
                df = self._apply_array_idem_transform(df, array_path, meta, seed_col_name)

            if seed_col_name in df.columns:
                df = df.drop(seed_col_name)

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
                        val = F.when(F.size(acc) == 1, F.coalesce(seed.getField(f), c)).when(
                            is_idem, prev_f
                        ).otherwise(c)
                    else:
                        val = c
                    struct_parts.append(val.alias(f))
                return F.array_union(acc, F.array(F.struct(*struct_parts)))

            def finish_struct(acc):
                return F.slice(acc, 2, F.size(acc) - 1)

            df = df.withColumn(
                array_path,
                F.aggregate(arr_col, initial_acc, merge_struct, finish_struct),
            )
        else:
            # Array de tipus simple
            initial_acc = F.array(seed)
            def merge_simple(acc, el):
                is_idem = F.trim(F.coalesce(el.cast("string"), F.lit(""))).rlike(IDEM_REGEX)
                prev_val = F.element_at(acc, -1)
                resolved = F.when(F.size(acc) == 1, F.coalesce(seed, el)).when(
                    is_idem, prev_val
                ).otherwise(el)
                return F.array_union(acc, F.array(resolved))

            def finish_simple(acc):
                return F.slice(acc, 2, F.size(acc) - 1)

            df = df.withColumn(
                array_path,
                F.aggregate(arr_col, initial_acc, merge_simple, finish_simple),
            )
        return df

    def _apply_array_idem_transform_nested(
        self,
        df: TracedDataFrame,
        parent_path: str,
        child_array_name: str,
        meta: dict,
        seed_col_name: str,
    ) -> TracedDataFrame:
        """Aplica idem a un array fill (dins d'un struct que és dins d'un array pare)."""
        idem_fields = set(meta["fields"])
        if not idem_fields:
            return df
        parts = parent_path.split(".")
        top_col = parts[0]
        arr_col = F.col(top_col)
        seed = F.col(seed_col_name)
        items_schema = meta["items_schema"]
        all_fields = list(items_schema.get("properties", {}).keys())
        # No fer cast: evita DATATYPE_MISMATCH entre struct del DataFrame i del schema JSON
        initial_inner = F.array(seed)

        def merge_inner(acc, el):
            prev = F.element_at(acc, -1)
            struct_parts = []
            for f in all_fields:
                c = el.getField(f)
                if f in idem_fields:
                    is_idem = F.trim(F.coalesce(c.cast("string"), F.lit(""))).rlike(IDEM_REGEX)
                    prev_f = prev.getField(f)
                    val = F.when(F.size(acc) == 1, F.coalesce(seed.getField(f), c)).when(
                        is_idem, prev_f
                    ).otherwise(c)
                else:
                    val = c
                struct_parts.append(val.alias(f))
            return F.array_union(acc, F.array(F.struct(*struct_parts)))

        def finish_inner(acc):
            return F.slice(acc, 2, F.size(acc) - 1)

        def transform_outer_item(outer_item):
            inner_arr = outer_item.getField(child_array_name)
            new_inner = F.aggregate(
                inner_arr, initial_inner, merge_inner, finish_inner
            )
            return outer_item.withField(child_array_name, new_inner)

        df = df.withColumn(
            top_col,
            F.transform(arr_col, transform_outer_item),
        )
        return df

    # @data_transformer_method()
    # def resolve_idems_wrong(self, df: TracedDataFrame) -> TracedDataFrame:
    #     """
    #     Aplica la lògica de reemplaçament recursiu IDEM fent servir Spark.
    #     """
    #     if self._schema is None:
    #         raise ValueError("Must call use_schema() before.")
    #     if "schema" in self._schema:
    #         schema_to_use = self._schema["schema"]
    #     else:
    #         schema_to_use = self._schema
    #
    #     # 1. Get the map of fields to process from the JSON schema
    #     idems_map = self._collect_idems_for_spark(schema_to_use)
    #
    #     # Define the window for forward fill across rows
    #     # Window from the start up to the current row
    #     order_and_partitions = schema_to_use.get("order_and_partitions") if "order_and_partitions" in schema_to_use else None
    #
    #     if order_and_partitions is None:
    #         raise ValueError("No 'order' field in schema")
    #
    #     order_cols = order_and_partitions.get("order")
    #     partitions = order_and_partitions.get("partitions") if "partitions" in order_and_partitions else None
    #
    #     if order_cols is None:
    #         raise ValueError("No 'order' field in schema")
    #
    #     if partitions is None:
    #         window_spec = Window.orderBy(*order_cols).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    #     else:
    #         window_spec = Window.partitionBy(*partitions).orderBy(*order_cols).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    #
    #     # --- PHASE 1: ROOT FIELDS CLEANUP ---
    #     for field in idems_map["root_fields"]:
    #         # Marcar les cel·les que eren NULL per poder restaurar-les després del forward fill
    #         was_null_col = f"__{field}_was_null"
    #         df = df.withColumn(was_null_col, F.col(field).isNull())
    #         # Només substituir per None els que coincideixen amb la regex idem (no els NULL)
    #         is_idem = F.trim(F.col(field).cast("string")).rlike(IDEM_REGEX)
    #         df = df.withColumn(field, F.when(is_idem, F.lit(None)).otherwise(F.col(field)))
    #         # Forward fill
    #         df = df.withColumn(field, F.last(field, ignorenulls=True).over(window_spec))
    #         # Restaurar els NULL originals (no substituir-los pel valor anterior)
    #         df = df.withColumn(field, F.when(F.col(was_null_col), F.lit(None)).otherwise(F.col(field)))
    #         df = df.drop(was_null_col)
    #
    #     # --- PHASE 2: ARRAY CLEANUP (with memory across rows) ---
    #     # Sort arrays by depth (deepest first)
    #     sorted_arrays = sorted(idems_map["arrays"].keys(), key=lambda x: x.count('.'), reverse=True)
    #
    #     for array_path in sorted_arrays:
    #         meta = idems_map["arrays"][array_path]
    #
    #         # # Aquesta part és la clau: per passar el valor de l'últim element de la fila anterior
    #         # # a la fila actual, creem una columna temporal "seed"
    #         # seed_col = f"{array_path}_seed"
    #
    #         # CORRECCIÓ 1: Nom de la columna seed sense punts
    #         seed_col_name = f"{array_path.replace('.', '_')}_seed"
    #
    #         # # Obtenim el darrer element de l'array de la fila anterior
    #         # last_element_expr = f"element_at({array_path}, -1)"
    #         # df = df.withColumn(seed_col, F.last(F.expr(last_element_expr), ignorenulls=True).over(window_spec))
    #
    #         # FIX 2: Seed expression that navigates the structure
    #         # If path is 'cargo_list.cargo', we want the last 'cargo' of the last 'cargo_list'
    #         nested_seed_expr = self._build_nested_element_at_expr(array_path)
    #         # Get the value from the previous row
    #         df = df.withColumn(
    #             seed_col_name,
    #             F.last(F.expr(nested_seed_expr), ignorenulls=True).over(window_spec)
    #         )
    #
    #         # # Construïm l'expressió aggregate per processar l'array
    #         # clean_expr = self._build_aggregate_expr(array_path, seed_col, meta)
    #         # df = df.withColumn(array_path, F.expr(clean_expr))
    #
    #         # Apply the aggregate (using the sanitized seed_col_name)
    #         # Note: The aggregate must be applied at the top level.
    #         # If array_path is 'cargo_list.cargo', we transform 'cargo_list'
    #         if "." in array_path:
    #             parent_path, current_array = array_path.rsplit(".", 1)
    #             # Get the function that builds the aggregate
    #             aggregate_func = self._build_nested_aggregate_expr(current_array, seed_col_name, meta)
    #
    #             # Apply transform using withField natively
    #             df = df.withColumn(
    #                 parent_path,
    #                 F.transform(
    #                     F.col(parent_path),
    #                     lambda x: x.withField(current_array, aggregate_func(x))
    #                 )
    #             )
    #
    #             # df = df.withColumn(
    #             #     parent_path,
    #             #     F.transform(
    #             #         F.col(parent_path),
    #             #         lambda x: x.withField(current_array, F.expr(clean_expr))
    #             #     )
    #             # )
    #         else:
    #             # Root-level array
    #             clean_expr = self._build_aggregate_expr(array_path, seed_col_name, meta)
    #             df = df.withColumn(array_path, clean_expr)
    #
    #         # Drop the temporary seed column
    #         # df = df.drop(seed_col)
    #         df = df.drop(seed_col_name)
    #
    #     return df

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
    def __update_state(self, *container_path, df):
        ## ACTUALITZANT L'ESTAT
        state_path = self._resolve_path(*container_path, process_level_dir="states")
        new_status_df = df.select(
            F.col("entry_id"),
            F.lit(True).alias("is_cleaned")
        ).distinct()
        if self.path_exists(state_path):  # O parquet_file_exist
            # 2. Llegim l'estat actual
            existing_state_df = self.spark.read.parquet(state_path)

            existing_state_df.localCheckpoint()

            # 3. ELIMINEM de l'estat vell els IDs que acaben d'arribar
            # Així, si un ID ja hi era amb is_new=False, l'esborrem de la versió vella
            state_without_updates = existing_state_df.join(
                new_status_df,
                on="entry_id",
                how="left_anti"
            )

            # 4. UNIM: Registres vells no modificats + Registres nous/actualitzats
            final_state_df = state_without_updates.unionByName(new_status_df)
        else:
            # Si és la primera vegada, l'estat és simplement la ingesta actual
            final_state_df = new_status_df

        # 5. Guardem l'estat actualitzat
        final_state_df.write.mode("overwrite").parquet(state_path)

        return final_state_df

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
        return F.translate(expr_col, wrong_chars, correct_chars)

    @staticmethod
    def apply_cleaning_process(col, for_cleaning_list: list, mapping: dict):
        def change_chars(c, m: dict = None):
            return PortadaCleaning.apply_ocr_corrections(c, m)

        def transform_mapping_with_params(mapping: dict, params: dict):
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
                params_for_cleaning_list.append(item.get("params", None))
            else:
                alg_for_cleaning_list.append(item)
                params_for_cleaning_list.append(None)

        processes = {"one_word": one_word, "not_digits": not_digit}
        if "paragraph" in alg_for_cleaning_list or "not_cleanable" in alg_for_cleaning_list:
            expr = col
        elif "accepted_abbreviations" in alg_for_cleaning_list or "accepted_idem" in alg_for_cleaning_list:
            expr = F.trim(col)
            expr = F.when(F.length(expr) > 5, F.regexp_replace(expr, r"[.,;]+$", "")).otherwise(expr)
        else:
            expr = F.regexp_replace(F.trim(col), r"[.,;]+$", "")
        if "not_cleanable" not in alg_for_cleaning_list and "paragraph" not in alg_for_cleaning_list and "one_word" not in alg_for_cleaning_list:
            expr = not_paragraph(expr, mapping)
        for process in alg_for_cleaning_list:
            if process in processes:
                expr = processes[process](expr, mapping)
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
            return F.expr(condition["expression"])
        if "expr" in condition:
            return F.expr(condition["expr"])
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
            if op == "==":
                return col.eq(right)
            if op == "!=":
                return col.neq(right)
            if op == ">":
                return col > right
            if op == ">=":
                return col >= right
            if op == "<":
                return col < right
            if op == "<=":
                return col <= right
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
        """Default condition: from exists and is not null. If from is a list (e.g. add_array_item), always apply."""
        if isinstance(from_ref, list):
            return F.lit(True)
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
            new_array = F.when(current.isNull(), F.array(new_item)).otherwise(
                F.array_union(current, F.array(new_item))
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
                nested_item = self._fill_build_array_item_expr(
                    f"{parent_field}.item", from_item.get("items", item_props), from_item
                )
                # Nested fill yields a struct (one item); target field is array -> F.array(struct)
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

    # ### ---------------------- USED BY RESOLVE_IDEMS --------------------------- ###
    # @staticmethod
    # def _has_accepted_idem(schema: dict) -> bool:
    #     """Check if the field has 'accepted_idem' in for_cleaning."""
    #     for_cleaning = schema.get("for_cleaning", [])
    #     for item in for_cleaning:
    #         alg = item["algorithm"] if isinstance(item, dict) else item
    #         if alg == "accepted_idem":
    #             return True
    #     return False
    #
    # @staticmethod
    # def _collect_idems_for_spark(
    #         schema: dict,
    #         path: str = "",
    #         is_inside_array: bool = False,
    #         current_array_path: str = None,
    #         result: dict = None
    # ) -> dict:
    #     """
    #     Split fields with 'accepted_idem' into two categories for Spark.
    #     """
    #     if result is None:
    #         result = {"root_fields": [], "arrays": {}}
    #
    #     field_type = schema.get("type")
    #
    #     # 1. OBJECT CASE: explore its properties
    #     if field_type == "object":
    #         props = schema.get("properties", {})
    #         for name, sub_schema in props.items():
    #             new_path = f"{path}.{name}" if path else name
    #             PortadaCleaning._collect_idems_for_spark(
    #                 sub_schema, new_path, is_inside_array, current_array_path, result
    #             )
    #
    #     # 2. ARRAY CASE: mark that we enter 'nested' zone
    #     elif field_type == "array":
    #         items = schema.get("items", {})
    #         # Register the array in the result dict if not already present
    #         if path not in result["arrays"]:
    #             result["arrays"][path] = {
    #                 "items_type": items.get("type"),
    #                 "schema": items,
    #                 "fields": [],
    #                 "all_struct_fields": list(items.get("properties", {}).keys()) if items.get(
    #                     "type") == "object" else []
    #             }
    #
    #         # Continue recursion inside the array
    #         PortadaCleaning._collect_idems_for_spark(
    #             items, path, is_inside_array=True, current_array_path=path, result=result
    #         )
    #
    #     # 3. LEAF FIELD (String/Simple): check if it has the label
    #     else:
    #         if PortadaCleaning._has_accepted_idem(schema):
    #             if is_inside_array:
    #                 # Inside an array: add field to that array's list
    #                 # Store only the relative field name within the structure
    #                 field_name = path.split(".")[-1] if "." in path else path
    #                 if field_name not in result["arrays"][current_array_path]["fields"]:
    #                     result["arrays"][current_array_path]["fields"].append(field_name)
    #             else:
    #                 # Top-level: add to Window list
    #                 result["root_fields"].append(path)
    #
    #     return result
    #
    # def _build_resolve_idem_expr_for_column(
    #         self, col_expr, lag_col, idem_values: tuple = None
    # ):
    #     """Build the expression that replaces idem/id/id. with the predecessor value."""
    #     idem_values = idem_values or IDEM_TOKENS
    #     trimmed_lower = F.trim(F.lower(F.col(col_expr) if isinstance(col_expr, str) else col_expr))
    #     is_idem = trimmed_lower.isin(idem_values)
    #     return F.when(is_idem, lag_col).otherwise(col_expr if isinstance(col_expr, str) else F.col(col_expr))
    #
    # def _build_resolve_idem_expr_for_array_simple(
    #         self, array_col: str, lag_col: str
    # ) -> "F.Column":
    #     """
    #     Array of simple types: first item gets last of previous row; others get previous in same row.
    #     Uses Spark SQL aggregate(), element_at(..., -1), array_concat - compatible with Spark 3.5.x.
    #     """
    #     lag_col_escaped = f"`{lag_col}`" if "." in lag_col or "-" in lag_col else lag_col
    #     # element_at is 1-based; -1 = last. When acc is empty, use last of previous row.
    #     return F.expr(f"""
    #             aggregate(
    #                 {array_col},
    #                 array(),
    #                 (acc, x) -> array_concat(
    #                     acc,
    #                     array(
    #                         CASE WHEN trim(lower(coalesce(cast(x as string), ''))) IN ('idem','id','id.')
    #                             THEN CASE WHEN size(acc) = 0
    #                                 THEN element_at({lag_col_escaped}, -1)
    #                                 ELSE element_at(acc, size(acc))
    #                                 END
    #                             ELSE x
    #                         END
    #                     )
    #                 )
    #             )
    #         """)
    #
    # def _build_resolve_idem_expr_for_array_struct(
    #         self, array_col: str, idem_field: str, struct_fields: list, lag_col: str
    # ) -> "F.Column":
    #     """
    #     Array of structs: same logic as simple, but only the idem field is replaced.
    #     Build struct with all fields; for idem_field use CASE, for others use x.field.
    #     Uses Spark SQL aggregate(), element_at(..., -1) - compatible with Spark 3.5.x.
    #     """
    #     lag_col_escaped = f"`{lag_col}`" if "." in lag_col or "-" in lag_col else lag_col
    #     struct_parts = []
    #     for f in struct_fields:
    #         if f == idem_field:
    #             struct_parts.append(f"""
    #                     CASE WHEN trim(lower(coalesce(cast(x.{f} as string), ''))) IN ('idem','id','id.')
    #                         THEN (CASE WHEN size(acc) = 0
    #                             THEN element_at({lag_col_escaped}, -1)
    #                             ELSE element_at(acc, size(acc))
    #                             END).{f}
    #                         ELSE x.{f}
    #                     END as {f}
    #                 """)
    #         else:
    #             struct_parts.append(f"x.{f} as {f}")
    #     struct_expr = ", ".join(struct_parts)
    #     return F.expr(f"""
    #             aggregate(
    #                 {array_col},
    #                 array(),
    #                 (acc, x) -> array_concat(
    #                     acc,
    #                     array(struct({struct_expr}))
    #                 )
    #             )
    #         """)
    #
    # def _build_nested_element_at_expr(self, path: str) -> str:
    #     """
    #     Convert 'a.b.col' to 'element_at(element_at(a, -1).b, -1).col'
    #     to get the last element of a nested structure.
    #     """
    #     parts = path.split(".")
    #     expr = parts[0]
    #     for part in parts[1:]:
    #         expr = f"element_at({expr}, -1).{part}"
    #     # We want the last of this last
    #     return f"element_at({expr}, -1)"
    #
    # def _build_nested_aggregate_expr(self, child_array_name: str, seed_col: str, meta: dict) -> str:
    #     """
    #     Build the aggregate for an array that is INSIDE another array.
    #     Differs in that the data source is 'x.{child_array_name}'
    #     instead of a global column name.
    #     """
    #     fields_to_fix = meta["fields"]
    #     all_fields = meta["all_struct_fields"]
    #
    #     # 1. Get the real type from the schema method
    #     element_struct_type, _ = PortadaCleaning.json_schema_to_spark_type(meta["schema"], primitive_types_as_strings=True)
    #     ddl_schema = f"array<{element_struct_type.simpleString()}>"
    #     # 2. Initializer (Seed) - ensure it is a clean column
    #     initial_acc = F.array(F.col(seed_col)).cast(ddl_schema)
    #
    #     # 3. Merge logic
    #     def merge_logic(acc, el):
    #         struct_fields = []
    #         for f_name in all_fields:
    #             # Use getItem() which is more robust inside lambdas than getField()
    #             current_field = el.getItem(f_name)
    #
    #             if f_name in fields_to_fix:
    #                 prev_field = F.element_at(acc, -1).getItem(f_name)
    #                 # Compare by casting the token to string
    #                 condition = current_field.cast("string").isin(IDEM_TOKENS)
    #                 field_val = F.when(condition, prev_field).otherwise(current_field)
    #             else:
    #                 field_val = current_field
    #
    #             # .alias() returns a column, ensure nothing else is chained
    #             struct_fields.append(field_val.alias(f_name))
    #
    #         # Pack the column list (*struct_fields unpacks the list)
    #         return F.array_union(acc, F.array(F.struct(*struct_fields)))
    #
    #     # 4. Finish function
    #     def finish_logic(acc):
    #         return F.slice(acc, 2, F.size(acc))
    #
    #     # Return the lambda for the transform
    #     return lambda obj: F.aggregate(
    #         obj.getItem(child_array_name),
    #         initial_acc,
    #         merge_logic,
    #         finish_logic
    #     )
    #
    # def _build_aggregate_expr(self, array_path: str, seed_col: str, meta: dict) -> str:
    #     """
    #     Construeix l'expressió SQL 'aggregate' per a Spark.
    #     """
    #     """
    #         Build the aggregate for a top-level array using pure PySpark.
    #         """
    #     fields_to_fix = meta["fields"]
    #     all_fields = meta["all_struct_fields"]
    #
    #     # Get the real structure to avoid Datatype Mismatch
    #     element_struct_type, _ = PortadaCleaning.json_schema_to_spark_type(meta["schema"], primitive_types_as_strings=True)
    #
    #     # Use DDL format to avoid previous AssertionError
    #     ddl_schema = f"array<{element_struct_type.simpleString()}>"
    #
    #     # Initial accumulator with type properly defined
    #     initial_acc = F.array(F.col(seed_col)).cast(ddl_schema)
    #
    #     def merge_logic(acc, el):
    #         struct_fields = []
    #         for f_name in all_fields:
    #             current_field = el[f_name]
    #
    #             if f_name in fields_to_fix:
    #                 # Simple fields that can be "idem"
    #                 prev_field = F.element_at(acc, -1)[f_name]
    #                 is_idem = current_field.cast("string").isin(IDEM_TOKENS)
    #
    #                 # If idem use previous, otherwise current
    #                 field_val = F.when(is_idem, prev_field).otherwise(current_field)
    #             else:
    #                 # Complex fields (e.g. 'cargo') or without idem.
    #                 # We know they are not Null, keep as-is
    #                 field_val = current_field
    #
    #             struct_fields.append(field_val.alias(f_name))
    #
    #         return F.array_union(acc, F.array(F.struct(*struct_fields)))
    #
    #     def finish_logic(acc):
    #         return F.slice(acc, 2, F.size(acc))
    #
    #     # NOTE: Here we return the column directly, not a lambda,
    #     # since we are operating on a top-level column (F.col)
    #     return F.aggregate(
    #         F.col(array_path),
    #         initial_acc,
    #         merge_logic,
    #         finish_logic
    #     )

    # @staticmethod
    # def _json_schema_to_normalize_columns_no(schema: dict, col_name: str = None):
    #     ret = {}
    #     def _resolve_value(ref, _col):
    #         if ref["type"] == "expr":
    #             return F.expr(ref["value"])
    #         if ref["type"] == "column":
    #             _resolved_value = _col
    #             ref_txt = re.sub("^old_value\.?", "", ref["value"], 1)
    #             a_ref = ref_txt.split(".") if ref_txt else []
    #             for r in a_ref:
    #                 _resolved_value = _resolved_value[r]
    #             return _resolved_value
    #         if ref["type"] == "literal":
    #             return F.lit(ref["value"])
    #         return None
    #
    #     def _normalize(_col, _norm_props: dict):
    #         if "options" not in _norm_props:
    #             return _resolve_value(_norm_props["equivalence"], _col)
    #
    #         expr = None
    #         for option in _norm_props["options"]:
    #             struct, _ = PortadaCleaning.json_schema_to_spark_type(option["try_cast_to"])
    #             parsed = PortadaCleaning._parse_as(_col, struct)
    #             cast_condition = _col.isNotNull() & parsed.isNotNull()
    #
    #             if option["equivalence"]["type"] == "struct":
    #                 fields = []
    #                 for k, str_ref in option["equivalence"]["new_value"].items():
    #                     fields.append(_resolve_value(str_ref, parsed).alias(k))
    #                 new_value = F.struct(*fields)
    #             else:
    #                 new_value = _resolve_value(option["equivalence"]["new_value"], parsed)
    #             expr = F.when(cast_condition, new_value) if expr is None else expr.when(cast_condition, new_value)
    #
    #         # OTHERWISE
    #         if "otherwise" in _norm_props:
    #             if _norm_props["otherwise"]["equivalence"]["type"] == "struct":
    #                 fields = []
    #                 for k, str_ref in _norm_props["otherwise"]["equivalence"]["new_value"].items():
    #                     fields.append(_resolve_value(str_ref, _col).alias(k))
    #                 new_value = F.struct(*fields)
    #             else:
    #                 new_value = _resolve_value(_norm_props["otherwise"]["equivalence"]["new_value"], _col)
    #             expr = expr.otherwise(new_value)
    #         return expr
    #
    #     def _json_schema_to_normalize_columns(_schema: dict, _col_name: str, _ret: dict):
    #         if "normalize" in _schema:
    #             normalize_properties = _schema.get("normalize")
    #             if normalize_properties["type"] == "foreach_item":
    #                 _ret[_col_name] = F.transform(
    #                     _parse_as(F.col(_col_name), ArrayType(StringType())),
    #                     lambda x: _normalize(x, normalize_properties)
    #                 )
    #             else:  # normalize_properties["type"] == "value":
    #                 # normalize
    #                 _ret[_col_name] = _normalize(F.col(col_name), normalize_properties)
    #         elif _schema.get("type") == "object":
    #             properties = _schema.get("properties", {})
    #             for field_name, field_schema in properties.items():
    #                 if _col_name is not None:
    #                     fn = f"{_col_name}.{field_name}"
    #                 else:
    #                     fn = field_name
    #                 _ret = _json_schema_to_normalize_columns(field_schema, fn, _ret)
    #         return _ret
    #
    #     ret = _json_schema_to_normalize_columns(schema, col_name, ret)
    #     return ret


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
        patcher.patch_if_needed()

    def save_original_values_of_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        self.write_delta("original_data", self.__container_path, df=ship_entries_df, mode="merge",
                         partition_by=["publication_name", "publication_date", "publication_edition"])
        return ship_entries_df

    def save_ship_entries(self, ship_entries_df: TracedDataFrame, is_cleaned=False) -> TracedDataFrame:
        self.write_delta(self.__container_path,df=ship_entries_df, mode="merge",
                         partition_by=["publication_name", "publication_date", "publication_edition"])
        if is_cleaned:
            self.__update_state(self.__container_path, df=ship_entries_df)
        return ship_entries_df

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
