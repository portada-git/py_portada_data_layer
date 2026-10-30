import json
import re
import os
import logging

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, DateType, TimestampType, LongType, DoubleType, BooleanType, \
    StructType, ArrayType, MapType
from portada_data_layer import DeltaDataLayer, TracedDataFrame
from portada_data_layer.boat_fact_model import BoatFactDataModel
from portada_data_layer.data_lake_metadata_manager import (
    DataframeOperation,
    data_transformer_method,
    enable_storage_log_for_class,
    enable_field_lineage_log_for_class,
)
from portada_data_layer.portada_delta_common import registry_to_portada_builder, BoatFactConstants
from portada_data_layer.portada_extraction_for_disambiguation import BoatFactCitationExtractor, BoatFactVoicesExtractor
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
    HISTORY_FIELD = "history"
    HISTORY_SEQUENCE_FIELD = "history_sequence"
    HISTORY_ENTRY_STRUCT = StructType([
        StructField("obtained_value", StringType(), True),
        StructField("operations", ArrayType(StringType()), True),
    ])
    HISTORY_MAP_TYPE = MapType(StringType(), HISTORY_ENTRY_STRUCT, True)

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self._schema = {}
        self._mapping_to_clean_chars = {}
        self._current_process_level = 1

    @staticmethod
    def _history_spec_id(spec, index: int) -> str:
        if isinstance(spec, dict) and spec.get("id") is not None:
            return str(spec["id"])
        return f"{index:04d}"

    @staticmethod
    def _history_spec_description(spec) -> str:
        if isinstance(spec, dict):
            return str(spec.get("description") or "")
        return ""

    @staticmethod
    def _history_spec_detailed(spec) -> str:
        if isinstance(spec, str):
            return spec
        if isinstance(spec, dict):
            return json.dumps(spec, ensure_ascii=False, sort_keys=True)
        return str(spec) if spec is not None else ""

    def _register_dataframe_operation(self, op: DataframeOperation) -> None:
        key = (op.type, op.stage, op.field, op.process, op.operation)
        existing = {
            (o.type, o.stage, o.field, o.process, o.operation) for o in self.dataframe_operations
        }
        if key not in existing:
            self.dataframe_operations.append(op)

    def _with_columns(self, df: TracedDataFrame, mapping: dict) -> TracedDataFrame:
        """Apply many column updates in one Project (shallower plans than chained withColumn)."""
        if not mapping:
            return df
        return df.withColumns(mapping)

    def _ensure_history_columns(self, df: TracedDataFrame, fields) -> TracedDataFrame:
        """Add missing ``*_history`` / ``*_history_sequence`` columns in one Project."""
        mapping = {}
        existing = set(df.columns)
        for field in fields:
            hist_col = f"{field}_{self.HISTORY_FIELD}"
            seq_col = f"{field}_{self.HISTORY_SEQUENCE_FIELD}"
            if hist_col not in existing and hist_col not in mapping:
                mapping[hist_col] = F.lit(None).cast(self.HISTORY_MAP_TYPE)
            if seq_col not in existing and seq_col not in mapping:
                mapping[seq_col] = F.array().cast(ArrayType(StringType()))
        return self._with_columns(df, mapping)

    def _history_process_column_updates(
            self,
            field: str,
            process: str,
            obtained_value_col: F.Column,
            operations_col: F.Column,
            row_condition: F.Column,
            merge_operations: bool = False,
    ) -> dict:
        """
        Build hist/seq Column updates for one process key.

        Removes any existing ``process`` key before ``map_concat`` so Spark does not
        raise DUPLICATED_MAP_KEY under the default mapKeyDedupPolicy=EXCEPTION.
        """
        hist_col = f"{field}_{self.HISTORY_FIELD}"
        seq_col = f"{field}_{self.HISTORY_SEQUENCE_FIELD}"
        hist = F.coalesce(F.col(hist_col), F.create_map().cast(self.HISTORY_MAP_TYPE))
        seq = F.coalesce(F.col(seq_col), F.array().cast(ArrayType(StringType())))
        existing_entry = F.element_at(hist, F.lit(process))
        new_ops = F.coalesce(operations_col, F.array().cast(ArrayType(StringType())))
        if merge_operations:
            merged_ops = F.when(
                existing_entry.isNotNull(),
                F.concat(
                    F.coalesce(existing_entry.getField("operations"), F.array().cast(ArrayType(StringType()))),
                    new_ops,
                ),
            ).otherwise(new_ops)
        else:
            merged_ops = new_ops
        new_entry = F.struct(
            obtained_value_col.cast("string").alias("obtained_value"),
            merged_ops.alias("operations"),
        )
        hist_without = F.map_filter(hist, lambda k, _v: k != F.lit(process))
        updated_hist = F.map_concat(hist_without, F.create_map(F.lit(process), new_entry))
        already_in_seq = F.array_contains(seq, F.lit(process))
        updated_seq = F.when(already_in_seq, seq).otherwise(F.concat(seq, F.array(F.lit(process))))
        return {
            hist_col: F.when(row_condition, updated_hist).otherwise(F.col(hist_col)),
            seq_col: F.when(row_condition, updated_seq).otherwise(F.col(seq_col)),
        }

    def _append_history_process(
            self,
            df: TracedDataFrame,
            field: str,
            process: str,
            obtained_value_col: F.Column,
            operations_col: F.Column,
            row_condition: F.Column,
            merge_operations: bool = False,
    ) -> TracedDataFrame:
        """
        Update ``{field}_history`` / ``{field}_history_sequence`` for rows matching
        ``row_condition``. When ``merge_operations`` and process already exists,
        concatenate operations and refresh obtained_value without re-appending sequence.
        """
        df = self._ensure_history_columns(df, [field])
        return self._with_columns(
            df,
            self._history_process_column_updates(
                field,
                process,
                obtained_value_col,
                operations_col,
                row_condition,
                merge_operations=merge_operations,
            ),
        )

    def _append_history_batch(self, df: TracedDataFrame, updates: list) -> TracedDataFrame:
        """
        Apply many history updates in one Project.

        Each item is a dict with keys: field, process, obtained_value_col,
        operations_col, row_condition, and optional merge_operations.
        At most one update per field is expected in a single batch.
        """
        if not updates:
            return df
        df = self._ensure_history_columns(df, [u["field"] for u in updates])
        mapping = {}
        for u in updates:
            mapping.update(
                self._history_process_column_updates(
                    u["field"],
                    u["process"],
                    u["obtained_value_col"],
                    u["operations_col"],
                    u["row_condition"],
                    merge_operations=u.get("merge_operations", False),
                )
            )
        return self._with_columns(df, mapping)

    def use_schema(self, json_schema: dict):
        self._schema = json_schema
        return self

    def use_mapping_to_clean_chars(self, mapping: dict):
        self._mapping_to_clean_chars = mapping
        return self

    @data_transformer_method(description="fill row fields from other columns using schema_path fill_with")
    def fill_with_schema(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Fill field content from other columns according to the 'fill_with' specs
        in the JSON schema_path. Also records ``fill_gaps`` history and DataframeOperation.
        """
        return self._apply_schema_fill_attribute(
            df, attr_name="fill_with", process="fill_gaps"
        )

    @data_transformer_method(
        description="fix extraction inconsistencies using schema_path fix_extraction_inconsistencies"
    )
    def fix_extraction_inconsistencies_schema(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Same mechanics as ``fill_with_schema`` but driven by
        ``fix_extraction_inconsistencies`` and history process of that name.
        """
        return self._apply_schema_fill_attribute(
            df,
            attr_name="fix_extraction_inconsistencies",
            process="fix_extraction_inconsistencies",
        )

    def _apply_schema_fill_attribute(
            self, df: TracedDataFrame, attr_name: str, process: str
    ) -> TracedDataFrame:
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        props = self._schema_properties(self._schema)
        eager_jobs = []
        lazy_jobs = []
        fields_with_specs = set()
        for field_name, field_schema in props.items():
            fill_with = field_schema.get(attr_name)
            if fill_with is None:
                continue
            specs = fill_with if isinstance(fill_with, list) else [fill_with]
            for index, spec in enumerate(specs):
                if not isinstance(spec, dict):
                    continue
                op_id = self._history_spec_id(spec, index)
                self._register_dataframe_operation(DataframeOperation(
                    type="modify",
                    stage="cleaning",
                    field=field_name,
                    process=process,
                    operation=op_id,
                    detailed_operation=self._history_spec_detailed(spec),
                    description=self._history_spec_description(spec),
                ))
                job = (field_name, field_schema, index, spec)
                fields_with_specs.add(field_name)
                if spec.get("lazy"):
                    lazy_jobs.append(job)
                else:
                    eager_jobs.append(job)

        ops_tmps = [f"__hist_ops_{field_name}" for field_name in fields_with_specs]
        df = self._with_columns(
            df,
            {
                ops_tmp: F.array().cast(ArrayType(StringType()))
                for ops_tmp in ops_tmps
            },
        )

        for field_name, field_schema, index, spec in eager_jobs + lazy_jobs:
            op_id = self._history_spec_id(spec, index)
            # Evaluate fire flag and new value against the same parent row in one
            # Project (avoids a separate fired_tmp column that would deepen the plan).
            fired = self._fill_spec_fires(df, field_name, field_schema, spec)
            new_val, drop_origin_col = self._fill_new_value_expr(
                df, field_name, field_schema, spec
            )
            if new_val is None:
                continue
            ops_tmp = f"__hist_ops_{field_name}"
            df = self._with_columns(
                df,
                {
                    field_name: new_val,
                    ops_tmp: F.when(
                        fired,
                        F.concat(F.col(ops_tmp), F.array(F.lit(op_id))),
                    ).otherwise(F.col(ops_tmp)),
                },
            )
            if drop_origin_col and drop_origin_col in df.columns:
                df = df.drop(drop_origin_col)

        history_updates = []
        for field_name in fields_with_specs:
            ops_tmp = f"__hist_ops_{field_name}"
            history_updates.append({
                "field": field_name,
                "process": process,
                "obtained_value_col": (
                    F.col(field_name).cast("string") if field_name in df.columns else F.lit(None)
                ),
                "operations_col": F.col(ops_tmp),
                "row_condition": F.size(F.col(ops_tmp)) > 0,
                "merge_operations": False,
            })
        df = self._append_history_batch(df, history_updates)
        if ops_tmps:
            df = df.drop(*ops_tmps)
        return df

    def _fill_spec_fires(
            self, df: TracedDataFrame, field_name: str, field_schema: dict, spec: dict
    ) -> F.Column:
        """Boolean column: whether this fill spec would apply on the current row state."""
        fill_type = spec.get("type", "replace")
        from_ref = spec.get("from")
        condition = spec.get("condition")
        existing_columns = set(df.columns)
        required_columns = self._fill_required_columns(from_ref) | self._fill_required_columns_from_condition(
            condition
        )
        if required_columns and not required_columns.issubset(existing_columns):
            return F.lit(False)
        cond_expr = (
            self._fill_condition_expr(condition)
            if condition is not None
            else self._fill_default_from_condition(from_ref)
        )
        current = F.col(field_name) if field_name in df.columns else F.lit(None)
        if fill_type == "replace_if_not_exist":
            return F.coalesce(cond_expr, F.lit(False)) & current.isNull()
        return F.coalesce(cond_expr, F.lit(False))

    def _empty_entry_ids_df(self) -> DataFrame:
        """Return an empty DataFrame with a single ``entry_id`` string column."""
        return self.spark.createDataFrame(
            [],
            schema=StructType([StructField("entry_id", StringType(), True)]),
        )

    @data_transformer_method(description="drop rows matching schema_path delete_if conditions")
    def delete_if_from_schema(self, df: TracedDataFrame) -> tuple[TracedDataFrame, DataFrame]:
        """
        Drop rows that match the root-level ``delete_if`` condition(s) in the JSON schema.

        ``delete_if`` uses the same condition format as ``fill_with`` (string SQL,
        structured column/op/value, expr, and/or). It may be a single condition or
        a list; if a list, a row is dropped when any condition matches (OR).

        Null condition results are treated as False (rows with null predicate are kept).

        :return: ``(df_kept, deleted_ids_df)`` where ``deleted_ids_df`` has distinct ``entry_id``.
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        schema_to_use, _ = self.get_schema_and_properties(self._schema)
        delete_if = schema_to_use.get("delete_if")
        if delete_if is None:
            return df, self._empty_entry_ids_df()

        specs = delete_if if isinstance(delete_if, list) else [delete_if]
        existing_columns = set(df.columns)
        to_delete = F.lit(False)
        any_spec_applied = False
        for index, spec in enumerate(specs):
            if spec is None:
                continue
            if not isinstance(spec, (dict, str)):
                continue
            condition = spec
            if isinstance(spec, dict) and "condition" in spec:
                condition = spec["condition"]
            op_id = self._history_spec_id(spec if isinstance(spec, dict) else {}, index)
            if isinstance(spec, dict) and spec.get("id") is not None:
                op_id = str(spec["id"])
            elif not isinstance(spec, dict):
                op_id = f"{index:04d}"
            self._register_dataframe_operation(DataframeOperation(
                type="delete",
                stage="cleaning",
                field="",
                process="delete_if",
                operation=op_id,
                detailed_operation=self._history_spec_detailed(
                    condition if not isinstance(spec, dict) or "condition" in spec else spec
                ),
                description=self._history_spec_description(spec) if isinstance(spec, dict) else "",
            ))
            required_columns = self._fill_required_columns_from_condition(condition)
            if required_columns and not required_columns.issubset(existing_columns):
                continue
            cond_expr = self._fill_condition_expr(condition)
            to_delete = to_delete | F.coalesce(cond_expr, F.lit(False))
            any_spec_applied = True

        if not any_spec_applied:
            return df, self._empty_entry_ids_df()

        df_deleted = df.filter(to_delete)
        df_kept = df.filter(~to_delete)
        if "entry_id" in existing_columns:
            deleted_ids_df = df_deleted.select("entry_id").distinct()
        else:
            deleted_ids_df = self._empty_entry_ids_df()
        return df_kept, deleted_ids_df

    @data_transformer_method(description="calculate null fields from schema_path calculable_if_null")
    def calculate_from_schema(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        For each field in the schema_path that has 'calculable_if_null', if the field is null,
        missing, or blank (empty/whitespace string), set it to the result of the defined
        operation (decrement_data or data_difference).
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        props = self._schema_properties(self._schema)
        value_updates = {}
        missing_tmps = []
        history_updates = []
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
            self._register_dataframe_operation(DataframeOperation(
                type="modify",
                stage="cleaning",
                field=field_name,
                process="calculation",
                operation=operation,
                detailed_operation=self._history_spec_detailed(calc_spec),
                description=self._history_spec_description(calc_spec),
            ))
            missing_tmp = f"__calc_missing_{field_name}"
            missing_tmps.append(missing_tmp)
            if field_name not in df.columns:
                value_updates[field_name] = computed
                value_updates[missing_tmp] = F.lit(True)
            else:
                col = F.col(field_name)
                # Empty string is not Spark null; treat blank as missing so calculable_if_null runs
                # after fill_with that clears invalid values with LIT('').
                is_missing = col.isNull() | (F.trim(col.cast("string")) == "")
                value_updates[missing_tmp] = is_missing
                value_updates[field_name] = F.when(is_missing, computed).otherwise(col)
            history_updates.append({
                "field": field_name,
                "process": "calculation",
                "obtained_value_col": F.col(field_name).cast("string"),
                "operations_col": F.array(F.lit(operation)),
                "row_condition": F.col(missing_tmp),
                "merge_operations": False,
            })
        df = self._with_columns(df, value_updates)
        df = self._append_history_batch(df, history_updates)
        if missing_tmps:
            df = df.drop(*missing_tmps)
        return df

    def read_entries(self, *container_path) -> TracedDataFrame:
        ship_entries_df = self.read_delta(*container_path)
        return ship_entries_df

    def read_entities(self, entity:str) -> TracedDataFrame:
        df = self.read_delta("known_entities", entity)
        return df

    def read_raw_entities(self, entity: str, force_all=False):
        df = self.read_delta("known_entities", entity, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])
        if df is not None:
            logger.info(f"{0 if df is None else df.count()} entities were read")
        if not force_all:
            state_path = self._resolve_path("known_entities", entity, process_level_dir="states")
            if not self.path_exists(state_path):
                # state_df = self.spark.createDataFrame(data={}, schema=StructType(
                #     [StructField("name", StringType(), True), StructField("is_cleaned", BooleanType(), True), ]))
                return df
            state_df = self.spark.read.parquet(state_path)
            state_df = state_df.filter(F.col("is_cleaned") == True).select("name")

            df = df.join(state_df, on="name", how="left_anti")
        return df

    def read_raw_entries(self, *container_path, force_all=False):
        base_path = f"{self._resolve_path(*container_path, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])}"
        base_dir = os.path.join(base_path, "*", "*", "*", "*", "*")
        path = os.path.join(base_dir, "*.json")

        df = self.read_json(path, has_extension=True)
        if df is not None:
            logger.info(f"{0 if df is None else df.count()} entries were read")
        if not force_all:
            state_path = self._resolve_path(*container_path, process_level_dir="states")
            if not self.path_exists(state_path):
                # state_df = self.spark.createDataFrame(data={}, schema=StructType([StructField("entry_id", StringType(), True),StructField("is_cleaned", BooleanType(), True),]))
                return df
            state_df = self.spark.read.parquet(state_path)
            state_df = state_df.filter(F.col("is_cleaned") == True).select("entry_id")

            df = df.join(state_df, on="entry_id", how="left_anti")
        return df

    def save_entities(self, entity: str, df: TracedDataFrame, key_id_name="name", is_cleaned=False) -> TracedDataFrame:
        self.write_delta("known_entities", entity, df=df, mode="merge", key_id_name=key_id_name)
        if is_cleaned:
            self._update_state("known_entities", entity, df=df, key_name="name")
        return df

    def read_original_values_of_entries(self, *container_path, force_all=False) -> TracedDataFrame:
        ship_entries_df = self.read_delta("original_data", *container_path)
        if not force_all:
            state_path = self._resolve_path(*container_path, process_level_dir="states")
            if not self.path_exists(state_path):
                # state_df = self.spark.createDataFrame(data={}, schema=StructType(
                #     [StructField("entry_id", StringType(), True), StructField("is_cleaned", BooleanType(), True), ]))
                return ship_entries_df
            state_df = self.spark.read.parquet(state_path)
            state_df = state_df.filter(F.col("is_cleaned") == True).select("entry_id")

            ship_entries_df = ship_entries_df.join(state_df, on="entry_id", how="left_anti")
        return ship_entries_df


    def save_original_values_of_entries(self, *container_path, entries_df: TracedDataFrame, key_id_name: str, partition_by:list = None) -> TracedDataFrame:
        entries_df = entries_df.sortWithinPartitions(*partition_by)
        self.write_delta("original_data", *container_path, df=entries_df, mode="merge", key_id_name=key_id_name, partition_by=partition_by)
        return entries_df

    def save_entries(self, *container_path, entries_df: TracedDataFrame, key_id_name: str, partition_by:list = None, is_cleaned=False) -> TracedDataFrame:
        entries_df = entries_df.sortWithinPartitions(*partition_by)
        self.write_delta(*container_path, df=entries_df, mode="merge", key_id_name=key_id_name, partition_by=partition_by)
        if is_cleaned:
            self._update_state(*container_path, df=entries_df, key_name=key_id_name)
        return entries_df

    def delete_entries_by_ids(self, *container_path, ids, key_id_name: str):
        """Delete rows from the Delta table at ``container_path`` whose key is in ``ids``."""
        self.delete_delta_from_ids(*container_path, ids=ids, key_id_name=key_id_name)

    def _normalize_field_structure(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Normalize DataFrame column structures according to the schema_path,
        then bootstrap ``*_history`` / ``*_history_sequence`` from ingest
        ``*_detailed_value`` (+ ``*_extraction_source_method``) and drop them.
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        properties = self._schema_properties(self._schema)
        selects = []
        for col in df.columns:
            if col not in properties:
                selects.append(F.col(col).alias(col))
        for field_name, field_schema in properties.items():
            if field_name in df.columns:
                expr = self._build_normalize_expr(F.col(field_name), field_schema)
                selects.append(expr.alias(field_name))
        df = df.select(*selects)
        return df

    def _init_history_from_extraction(self, df: TracedDataFrame) -> TracedDataFrame:
        """Build history maps from ``*_detailed_value`` and register extraction ops."""
        props = self._schema_properties(self._schema)
        dv_suffix = f"_{BoatFactDataModel.DETAILED_VALUE_FIELD}"
        method_suffix = f"_{BoatFactDataModel.EXTRACTION_SOURCE_METHOD_FIELD}"
        extraction_methods = [
            BoatFactDataModel.DEFAULT_VALUE_FIELD,
            BoatFactDataModel.ORIGINAL_VALUE_FIELD,
            BoatFactDataModel.CALCULATED_VALUE_FIELD,
        ]

        detailed_fields = []
        for col in df.columns:
            if col.endswith(dv_suffix):
                field = col[: -len(dv_suffix)]
                detailed_fields.append(field)

        # Catalog: which extraction ops appear at least once per field
        agg_exprs = []
        alias_to_field_method = []
        for field in detailed_fields:
            detailed_col = f"{field}_{BoatFactDataModel.DETAILED_VALUE_FIELD}"
            dtype = df.schema[detailed_col].dataType
            if not isinstance(dtype, StructType):
                continue
            available = {f.name for f in dtype.fields}
            for method in extraction_methods:
                if method not in available:
                    continue
                alias = f"{field}__{method}"
                agg_exprs.append(
                    F.max(F.col(detailed_col).getField(method).isNotNull().cast("int")).alias(alias)
                )
                alias_to_field_method.append((alias, field, method))

        if agg_exprs:
            stats = df.agg(*agg_exprs).collect()[0].asDict()
            for alias, field, method in alias_to_field_method:
                if stats.get(alias):
                    self._register_dataframe_operation(DataframeOperation(
                        type="modify",
                        stage="cleaning",
                        field=field,
                        process="extraction",
                        operation=method,
                    ))

        if BoatFactDataModel.PUBLICATION_DATE_FIELD in props:
            self._register_dataframe_operation(DataframeOperation(
                type="modify",
                stage="cleaning",
                field=BoatFactDataModel.PUBLICATION_DATE_FIELD,
                process="boat_fact_model",
                operation="datetime to string date",
            ))
        if "entry_id" in props:
            self._register_dataframe_operation(DataframeOperation(
                type="modify",
                stage="cleaning",
                field="entry_id",
                process="boat_fact_model",
                operation="create unique ID",
            ))

        cols_to_drop = []
        history_updates = {}
        for field in detailed_fields:
            detailed_col = f"{field}_{BoatFactDataModel.DETAILED_VALUE_FIELD}"
            method_col = f"{field}_{BoatFactDataModel.EXTRACTION_SOURCE_METHOD_FIELD}"
            hist_col = f"{field}_{self.HISTORY_FIELD}"
            seq_col = f"{field}_{self.HISTORY_SEQUENCE_FIELD}"
            dtype = df.schema[detailed_col].dataType
            if not isinstance(dtype, StructType):
                empty_entry = F.struct(
                    F.col(field).cast("string").alias("obtained_value"),
                    F.array().cast(ArrayType(StringType())).alias("operations"),
                )
                history_updates[hist_col] = F.create_map(F.lit("extraction"), empty_entry)
                history_updates[seq_col] = F.array(F.lit("extraction"))
                cols_to_drop.extend([c for c in [detailed_col, method_col] if c in df.columns])
                continue

            available = {f.name for f in dtype.fields}
            dv = F.col(detailed_col)
            default_v = dv.getField(BoatFactDataModel.DEFAULT_VALUE_FIELD) if BoatFactDataModel.DEFAULT_VALUE_FIELD in available else F.lit(None)
            original_v = dv.getField(BoatFactDataModel.ORIGINAL_VALUE_FIELD) if BoatFactDataModel.ORIGINAL_VALUE_FIELD in available else F.lit(None)
            calculated_v = dv.getField(BoatFactDataModel.CALCULATED_VALUE_FIELD) if BoatFactDataModel.CALCULATED_VALUE_FIELD in available else F.lit(None)
            boat_v = dv.getField(BoatFactDataModel.BOAT_FACT_MODEL_FIELD) if BoatFactDataModel.BOAT_FACT_MODEL_FIELD in available else F.lit(None)

            op_parts = []
            if BoatFactDataModel.DEFAULT_VALUE_FIELD in available:
                op_parts.append(
                    F.when(default_v.isNotNull(), F.array(F.lit(BoatFactDataModel.DEFAULT_VALUE_FIELD)))
                    .otherwise(F.array().cast(ArrayType(StringType())))
                )
            if BoatFactDataModel.ORIGINAL_VALUE_FIELD in available:
                op_parts.append(
                    F.when(original_v.isNotNull(), F.array(F.lit(BoatFactDataModel.ORIGINAL_VALUE_FIELD)))
                    .otherwise(F.array().cast(ArrayType(StringType())))
                )
            if BoatFactDataModel.CALCULATED_VALUE_FIELD in available:
                keep_calculated = calculated_v.isNotNull() & (
                    original_v.isNull()
                    | (calculated_v.cast("string") != original_v.cast("string"))
                )
                op_parts.append(
                    F.when(keep_calculated, F.array(F.lit(BoatFactDataModel.CALCULATED_VALUE_FIELD)))
                    .otherwise(F.array().cast(ArrayType(StringType())))
                )
            ops = F.concat(*op_parts) if op_parts else F.array().cast(ArrayType(StringType()))
            last_op = F.element_at(ops, F.lit(-1))
            obtained = (
                F.when(last_op == F.lit(BoatFactDataModel.CALCULATED_VALUE_FIELD), calculated_v.cast("string"))
                .when(last_op == F.lit(BoatFactDataModel.ORIGINAL_VALUE_FIELD), original_v.cast("string"))
                .when(last_op == F.lit(BoatFactDataModel.DEFAULT_VALUE_FIELD), default_v.cast("string"))
                .otherwise(F.lit(None).cast(StringType()))
            )
            extraction_entry = F.struct(
                obtained.alias("obtained_value"),
                ops.alias("operations"),
            )
            base_map = F.create_map(F.lit("extraction"), extraction_entry)

            if field == "entry_id":
                has_boat = F.lit(True)
                boat_ops = F.array(F.lit("create unique ID"))
            elif field == BoatFactDataModel.PUBLICATION_DATE_FIELD:
                has_boat = boat_v.isNotNull()
                boat_ops = F.array(F.lit("datetime to string date"))
            else:
                has_boat = boat_v.isNotNull()
                boat_ops = F.array(F.lit("boat_fact_model"))

            boat_entry = F.struct(
                F.col(field).cast("string").alias("obtained_value"),
                boat_ops.alias("operations"),
            )
            full_map = F.when(
                has_boat,
                F.map_concat(base_map, F.create_map(F.lit("boat_fact_model"), boat_entry)),
            ).otherwise(base_map)
            sequence = F.when(
                has_boat,
                F.array(F.lit("extraction"), F.lit("boat_fact_model")),
            ).otherwise(F.array(F.lit("extraction")))

            history_updates[hist_col] = full_map.cast(self.HISTORY_MAP_TYPE)
            history_updates[seq_col] = sequence
            cols_to_drop.extend([c for c in [detailed_col, method_col] if c in df.columns])

        # Ensure history columns for schema fields without detailed_value
        for field_name in props:
            hist_col = f"{field_name}_{self.HISTORY_FIELD}"
            seq_col = f"{field_name}_{self.HISTORY_SEQUENCE_FIELD}"
            if hist_col not in history_updates and hist_col not in df.columns:
                history_updates[hist_col] = F.lit(None).cast(self.HISTORY_MAP_TYPE)
            if seq_col not in history_updates and seq_col not in df.columns:
                history_updates[seq_col] = F.array().cast(ArrayType(StringType()))

        df = self._with_columns(df, history_updates)
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
        return df

    @data_transformer_method(description="materialize history snapshots and drop history columns")
    def simplify_field_structure(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Create ``*_from_extraction``, ``*_from_source_value``, ``*_fixed_value``,
        ``*_normalized_value`` from history and drop history / history_sequence.
        """
        props = self._schema_properties(self._schema)
        snapshot_keys = [
            ("from_extraction", "extraction"),
            ("from_source_value", "fix_extraction_inconsistencies"),
            ("fixed_value", "cleaned"),
            ("normalized_value", "normalized"),
        ]
        drop_cols = []
        for field_name in props:
            hist_col = f"{field_name}_{self.HISTORY_FIELD}"
            seq_col = f"{field_name}_{self.HISTORY_SEQUENCE_FIELD}"
            if hist_col in df.columns:
                hist = F.col(hist_col)
                for suffix, process in snapshot_keys:
                    entry = F.element_at(hist, F.lit(process))
                    df = df.withColumn(
                        f"{field_name}_{suffix}",
                        entry.getField("obtained_value"),
                    )
                drop_cols.append(hist_col)
            if seq_col in df.columns:
                drop_cols.append(seq_col)
        if drop_cols:
            df = df.drop(*drop_cols)
        return df

    def _cleaning_ops_for_field(
            self, field_name: str, field_schema: dict
    ) -> list[tuple[str, str]]:
        """
        Operations that ``clean_field_values_from_schema`` will apply for this field,
        as (operation_name, detailed_operation_json) matching
        ``_json_schema_to_normalize_values`` / ``apply_cleaning_process``.
        """
        mapping = self._mapping_to_clean_chars or {}
        ops: list[tuple[str, str]] = []

        def _dump_map(key: str) -> tuple[str, str] | None:
            char_map = mapping.get(key)
            if isinstance(char_map, dict) and char_map:
                return key, json.dumps(char_map, ensure_ascii=False)
            return None

        # Per-field OCR map (legacy keys in mapping_to_clean_chars)
        if field_name in mapping and isinstance(mapping.get(field_name), dict):
            ops.append((field_name, json.dumps(mapping[field_name], ensure_ascii=False)))

        t = field_schema.get("type")
        if t in ("integer", "number", "float") or (
            t == "string" and field_schema.get("format") == "date"
        ):
            dumped = _dump_map("numeric_map")
            if dumped:
                ops.append(dumped)
            elif not ops:
                ops.append(("numeric_map", ""))
            return ops

        if t != "string":
            return ops

        raw_fc = field_schema.get("for_cleaning") or []
        if not isinstance(raw_fc, list):
            raw_fc = [raw_fc]
        algs = []
        for item in raw_fc:
            if isinstance(item, str):
                algs.append(item)
            elif isinstance(item, dict) and item.get("algorithm"):
                algs.append(item["algorithm"])

        if "not_cleanable" in algs:
            return ops
        if "paragraph" in algs:
            return ops

        if algs:
            # apply_cleaning_process: not_paragraph_map unless one_word
            if "one_word" not in algs:
                dumped = _dump_map("not_paragraph_map")
                if dumped:
                    ops.append(dumped)
            if "one_word" in algs:
                dumped = _dump_map("one_word_map")
                if dumped:
                    ops.append(dumped)
            if "not_digits" in algs:
                dumped = _dump_map("only_text_map")
                if dumped:
                    ops.append(dumped)
            if "not_parenthesis" in algs:
                dumped = _dump_map("not_parenthesis_map") or _dump_map("not_parenthesis")
                if dumped:
                    ops.append(dumped)
            if "accepted_idem" in algs or "accepted_abbreviations" in algs:
                ops.append(("trim_punctuation", ""))
            if not ops:
                ops.append(("trim_punctuation", ""))
        else:
            ops.append(("trim_punctuation", ""))
        return ops

    @data_transformer_method()
    def clean_field_values_from_schema(self, df: TracedDataFrame) -> TracedDataFrame:
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        properties = self._schema_properties(self._schema)

        column_updates = {}
        before_tmps = []
        history_meta = []
        for field_name, field_schema in properties.items():
            if field_name not in df.columns:
                continue

            ops = self._cleaning_ops_for_field(field_name, field_schema)
            for op_name, detailed in ops:
                self._register_dataframe_operation(DataframeOperation(
                    type="modify",
                    stage="cleaning",
                    field=field_name,
                    process="fixing",
                    operation=op_name,
                    detailed_operation=detailed,
                ))

            if ops:
                before_tmp = f"__before_fix_{field_name}"
                before_tmps.append(before_tmp)
                column_updates[before_tmp] = F.col(field_name).cast("string")
                history_meta.append((field_name, before_tmp, ops))

            if field_name in self._mapping_to_clean_chars and isinstance(
                self._mapping_to_clean_chars.get(field_name), dict
            ):
                mapping = self._mapping_to_clean_chars[field_name]
                expr = self.apply_ocr_corrections(F.col(field_name), mapping)
            else:
                expr = F.col(field_name)
            expr = self._json_schema_to_normalize_values(
                field_schema, self._mapping_to_clean_chars, expr
            )
            column_updates[field_name] = expr

        df = self._with_columns(df, column_updates)
        history_updates = []
        for field_name, before_tmp, ops in history_meta:
            after = F.col(field_name).cast("string")
            history_updates.append({
                "field": field_name,
                "process": "fixing",
                "obtained_value_col": after,
                "operations_col": F.array(*[F.lit(op_name) for op_name, _ in ops]),
                "row_condition": ~F.col(before_tmp).eqNullSafe(after),
                "merge_operations": True,
            })
        df = self._append_history_batch(df, history_updates)
        if before_tmps:
            df = df.drop(*before_tmps)
        return df

    @data_transformer_method(
        description="add missing schema fields and normalize column structures"
    )
    def complete_accepted_structure(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Add missing schema_path fields, then normalize each schema column's
        structure and bootstrap ``*_history`` from ingest extraction metadata.
        Keeps ``*_extraction_source_method`` names (no rename to original_data_method).
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        method_suffix = f"_{BoatFactDataModel.EXTRACTION_SOURCE_METHOD_FIELD}"
        bad_suffix = f":_{BoatFactDataModel.DETAILED_VALUE_FIELD}"
        good_suffix = f"_{BoatFactDataModel.DETAILED_VALUE_FIELD}"
        bad_char = "_BAD_CHAR"
        new_names = []
        to_rename = []
        for col in list(df.columns):
            if col.endswith(":"):
                new_names.append(col[:-1] + bad_char)
                to_rename.append(new_names[-1])
            elif col.endswith(f":{method_suffix}"):
                new_names.append(col[:-(len(method_suffix) + 1)] + method_suffix + bad_char)
                to_rename.append(new_names[-1])
            elif col.endswith(bad_suffix):
                new_names.append(col[:-len(bad_suffix)] + good_suffix + bad_char)
                to_rename.append(new_names[-1])
            else:
                new_names.append(col)
        df = df.toDF(*new_names)
        for col_to_rename in to_rename:
            nc = col_to_rename[:-len(bad_char)]
            df = df.withColumn(nc, F.col(col_to_rename))
            df = df.drop(col_to_rename)
        allowed = self._collect_list_of_fields(self._schema)
        missing_cols = allowed - set(df.columns)
        if missing_cols:
            cols_to_add = {
                col: self._expr_for_missing_column(col) for col in missing_cols
            }
            df = df.withColumns(cols_to_add)
        df =  self._normalize_field_structure(df)
        return self._init_history_from_extraction(df)


    @data_transformer_method(
        description="prune unbelonging fields"
    )
    def prune_unaccepted_fields(self, df: TracedDataFrame) -> TracedDataFrame:
        """Keep only schema_path columns (including history / history_sequence)."""
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")
        allowed = self._collect_list_of_fields(self._schema)
        cols_to_keep = [col for col in df.columns if col in allowed]
        return df.select(cols_to_keep)

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
        Cast recursiu d'una columna segons JSON schema_path:
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

    @staticmethod
    def _reorder_by_json_schema(col_expr: F.Column, schema: dict) -> F.Column:
        """
        Rebuild object/array columns to enforce deterministic field order from JSON schema_path.
        Primitive values are returned as-is to avoid altering idem semantics.
        """
        t = schema.get("type")
        if t == "object":
            props = schema.get("properties", {})
            fields = []
            for name, sub_schema in props.items():
                fields.append(
                    PortadaCleaning._reorder_by_json_schema(col_expr.getField(name), sub_schema).alias(name)
                )
            return F.struct(*fields)
        if t == "array":
            items_schema = schema.get("items", {})
            return F.transform(
                col_expr,
                lambda x: PortadaCleaning._reorder_by_json_schema(x, items_schema),
            )
        return col_expr

    @staticmethod
    def _reorder_by_spark_type(col_expr: F.Column, spark_type):
        """
        Rebuild object/array columns following an existing Spark type field order.
        Used to keep compatibility with parent struct types after canonical idem processing.
        """
        if isinstance(spark_type, StructType):
            fields = []
            for field in spark_type.fields:
                fields.append(
                    PortadaCleaning._reorder_by_spark_type(col_expr.getField(field.name), field.dataType).alias(
                        field.name
                    )
                )
            return F.struct(*fields)
        if isinstance(spark_type, ArrayType):
            return F.transform(
                col_expr,
                lambda x: PortadaCleaning._reorder_by_spark_type(x, spark_type.elementType),
            )
        return col_expr

    @data_transformer_method(description="cast current fields to schema_path types")
    def string_to_type_from_schema(self, df: TracedDataFrame) -> TracedDataFrame:
        """
        Cast fields to JSON schema types. Records ``cleaned`` history only when the
        cast nullifies a previously non-null value (impossible conversions).
        """
        if self._schema is None:
            raise ValueError("Must call use_schema() before.")

        schema_to_use, properties = self.get_schema_and_properties(self._schema)
        if not properties:
            return df

        column_updates = {}
        nullified_tmps = []
        history_updates = []
        for field_name, field_schema in properties.items():
            if field_name not in df.columns:
                continue
            schema_type = field_schema.get("type")
            if schema_type == "string":
                continue
            data_type_name = schema_type or "unknown"
            self._register_dataframe_operation(DataframeOperation(
                type="modify",
                stage="cleaning",
                field=field_name,
                process="cleaned",
                operation=f"cast_to_{data_type_name}",
                description=f"cast from string to {data_type_name}",
            ))
            before = F.col(field_name)
            before_str = before.cast("string")
            was_non_null = before.isNotNull() & (F.trim(before_str) != "")
            casted = PortadaCleaning._cast_column_by_json_schema(before, field_schema)
            nullified_tmp = f"__cast_nullified_{field_name}"
            nullified_tmps.append(nullified_tmp)
            # Evaluate nullified against pre-update values in the same Project as the cast.
            column_updates[nullified_tmp] = was_non_null & casted.isNull()
            column_updates[field_name] = casted
            history_updates.append({
                "field": field_name,
                "process": "cleaned",
                "obtained_value_col": F.lit(None).cast(StringType()),
                "operations_col": F.array(F.lit(f"cast_to_{data_type_name}")),
                "row_condition": F.col(nullified_tmp),
                "merge_operations": False,
            })

        df = self._with_columns(df, column_updates)
        df = self._append_history_batch(df, history_updates)
        if nullified_tmps:
            df = df.drop(*nullified_tmps)
        return df

    @staticmethod
    def _flatten_string_col(col_expr: F.Column) -> F.Column:
        """Lowercase + reemplaç de caràcters accentuats comuns."""
        lowered = F.lower(col_expr)
        return F.translate(lowered, FLATTEN_STRINGS_FROM, FLATTEN_STRINGS_TO)

    @staticmethod
    def _flatten_strings_by_schema(col_expr: F.Column, schema: dict) -> F.Column:
        """
        Aplica aplanament de caràcters només als camps string segons el JSON schema_path.
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
    def flatten_string_characters(self, df: TracedDataFrame, schema: dict = None) -> TracedDataFrame:
        """
        Aplana caràcters dels camps string: minúscules i eliminació de diacrítics bàsics
        (àáâãäå->a, èéêë->e, ìíîï->i, òóôõö->o, ùúûü->u, ç->c, ñ->n, ...).
        S'aplica recursivament en structs/arrays segons schema_path.properties.
        """
        if schema is None:
            schema = self._schema

        if schema is None:
            raise ValueError("Must call use_schema() before.")

        _, properties = self.get_schema_and_properties(schema)
        if not properties:
            return df

        column_updates = {}
        for field_name, field_schema in properties.items():
            if field_name not in df.columns:
                continue
            column_updates[field_name] = PortadaCleaning._flatten_strings_by_schema(
                F.col(field_name), field_schema
            )
        return self._with_columns(df, column_updates)

    @data_transformer_method()
    def resolve_idems(self, df: TracedDataFrame) -> TracedDataFrame:
        df = self._resolve_idems_for_root_fields(df)
        df = self._resolve_idems_for_arrays(df)
        return df

    def _resolve_idems_for_root_fields(self, df: TracedDataFrame) -> TracedDataFrame:
            """
            Resolves occurrences of 'idem' in top-level fields tagged with accepted_idem in the schema_path.
            Values matching IDEM_REGEX are replaced with the last non-idem value before (forward fill).
            If after forward fill the value would be null (e.g. first row of partition), it is restored
            from history[\"extraction\"].obtained_value if present, so that an idem is never replaced with null.
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
                raise ValueError("No 'order' in schema_path order_and_partitions")
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
                self._register_dataframe_operation(DataframeOperation(
                    type="modify",
                    stage="cleaning",
                    field=field,
                    process="fixing",
                    operation="resolve_idems",
                    description="resolve idems with referred previous values",
                ))
                # Materialize flags before overwrite (Column exprs rebind to new values otherwise).
                is_idem_tmp = f"__is_idem_{field}"
                was_null_tmp = f"__was_null_{field}"
                df = df.withColumn(
                    is_idem_tmp,
                    F.trim(F.col(field).cast("string")).rlike(IDEM_REGEX),
                )
                df = df.withColumn(was_null_tmp, F.col(field).isNull())
                # Només quan el valor coincideix amb la regex idem: posar null i omplir amb darrer valor no null
                value_to_fill = F.when(F.col(is_idem_tmp), F.lit(None)).otherwise(F.col(field))
                filled = F.last(value_to_fill, ignorenulls=True).over(window_ff)
                # Restore from extraction history when forward-fill is null
                hist_col = f"{field}_{self.HISTORY_FIELD}"
                idem_resolved = filled
                if hist_col in df.columns:
                    from_extraction = F.element_at(F.col(hist_col), F.lit("extraction")).getField(
                        "obtained_value"
                    )
                    idem_resolved = F.coalesce(filled, from_extraction)
                # Restaurar nulls originals; on era idem posar idem_resolved; altrament valor original
                df = df.withColumn(
                    field,
                    F.when(F.col(was_null_tmp), F.lit(None))
                    .when(F.col(is_idem_tmp), idem_resolved)
                    .otherwise(F.col(field)),
                )
                df = self._append_history_process(
                    df,
                    field,
                    "fixing",
                    F.col(field).cast("string"),
                    F.array(F.lit("resolve_idems")),
                    F.col(is_idem_tmp) & ~F.col(was_null_tmp),
                    merge_operations=True,
                )
                df = df.drop(is_idem_tmp, was_null_tmp)

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
        Recorre el schema_path i retorna un dict array_path -> { items_schema, fields: [str], nested_arrays: [str] }.
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

    @staticmethod
    def _child_items_ddl_from_df_schema(
        df: TracedDataFrame, parent_col: str, child_array_name: str
    ) -> str | None:
        """DDL of nested array element struct from the DataFrame schema (runtime types at resolve_idems)."""
        try:
            parent_field = df.schema[parent_col]
            parent_elem_type = parent_field.dataType.elementType
            if not isinstance(parent_elem_type, StructType):
                return None
            child_field = next(
                (f for f in parent_elem_type.fields if f.name == child_array_name), None
            )
            if (
                child_field
                and isinstance(child_field.dataType, ArrayType)
                and isinstance(child_field.dataType.elementType, StructType)
            ):
                return child_field.dataType.elementType.simpleString()
        except Exception:
            return None
        return None

    @staticmethod
    def _child_items_ddl_from_items_schema(items_schema: dict) -> str | None:
        """Fallback DDL from JSON items schema (pre string_to_type_from_schema: primitives as strings)."""
        if not items_schema:
            return None
        try:
            spark_type, _ = PortadaCleaning.json_schema_to_spark_type(
                items_schema, primitive_types_as_strings=True
            )
            return spark_type.simpleString()
        except Exception:
            return None

    @staticmethod
    def _resolve_nested_child_items_ddl(
        df: TracedDataFrame, parent_col: str, child_array_name: str, items_schema: dict
    ) -> str | None:
        ddl = PortadaCleaning._child_items_ddl_from_df_schema(df, parent_col, child_array_name)
        if ddl is None:
            ddl = PortadaCleaning._child_items_ddl_from_items_schema(items_schema)
        return ddl

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
        if not child_items_ddl:
            raise ValueError(
                f"child_items_ddl is required for nested idem seed on "
                f"{top_col}.{child_array_name}.{field_name}"
            )
        regex_sql = IDEM_REGEX.replace("\\", "\\\\").replace("'", "\\'")
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
                child_items_ddl = self._resolve_nested_child_items_ddl(
                    df, parent_col, child_array_name, meta.get("items_schema", {})
                )
                if child_items_ddl is None:
                    logger.warning(
                        "Skipping per-field nested idem seeds for %s.%s: could not resolve child_items_ddl",
                        parent_col,
                        child_array_name,
                    )
                # Seed addicional per camp: darrer valor vàlid del registre anterior
                # (important quan l'últim item del registre anterior té null en algun subcamp).
                seed_field_cols = {}
                for f_name in meta.get("fields", []):
                    if child_items_ddl is None:
                        continue
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
            items_schema = meta["items_schema"]
            all_fields = list(items_schema.get("properties", {}).keys())
            seed_canonical = PortadaCleaning._reorder_by_json_schema(seed, items_schema)
            arr_canonical = F.transform(
                arr_col,
                lambda x: PortadaCleaning._reorder_by_json_schema(x, items_schema),
            )
            initial_acc = F.array(seed_canonical)

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
                # Afegim el nou element (flatten(array(acc, array(x))) = concat d'arrays, compatible amb PySpark sense array_concat).
                return F.flatten(F.array(acc, F.array(F.struct(*struct_parts))))

            def finish_struct(acc):
                # Traiem el seed (primer element) i mantenim la resta.
                return F.slice(acc, 2, F.size(acc) - 1)

            new_array_canonical = F.when(
                arr_canonical.isNotNull() & (F.size(arr_canonical) > 0),
                F.aggregate(arr_canonical, initial_acc, merge_struct, finish_struct),
            ).otherwise(arr_canonical)

            original_arr_type = None
            try:
                original_arr_type = df.schema[array_path].dataType
            except Exception:
                original_arr_type = None
            if isinstance(original_arr_type, ArrayType):
                new_array = PortadaCleaning._reorder_by_spark_type(new_array_canonical, original_arr_type)
            else:
                new_array = new_array_canonical

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
        child_array_type = None
        if isinstance(merchant_type, StructType):
            child_field = next((f for f in merchant_type.fields if f.name == child_array_name), None)
            if child_field and isinstance(child_field.dataType, ArrayType):
                child_array_type = child_field.dataType

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
            seed_inner_canonical = PortadaCleaning._reorder_by_json_schema(seed_inner, items_schema)
            initial_inner = F.array(seed_inner_canonical)

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
            inner_arr_canonical = F.transform(
                inner_arr,
                lambda x: PortadaCleaning._reorder_by_json_schema(x, items_schema),
            )
            transformed_cargo_canonical = F.when(
                inner_arr_canonical.isNotNull() & (F.size(inner_arr_canonical) > 0),
                F.aggregate(inner_arr_canonical, initial_inner, merge_inner, finish_inner),
            ).otherwise(inner_arr_canonical)
            transformed_cargo = transformed_cargo_canonical
            if isinstance(child_array_type, ArrayType):
                transformed_cargo = PortadaCleaning._reorder_by_spark_type(transformed_cargo_canonical, child_array_type)
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

    def cleaning_entities(self, entity: str, df: DataFrame | TracedDataFrame) -> TracedDataFrame:
        # 1.- Normalize string types
        # df = self.normalize_strings_for_entities(df)
        # 2.- Save cleaned values
        self.save_entities(entity=entity, df=df, is_cleaned=True)
        return df

    def cleaning_entries(self, df: DataFrame | TracedDataFrame, saving_original_data: bool = True) -> TracedDataFrame:
        # 1.- Complete accepted structure, normalize fields, bootstrap history
        df = self.complete_accepted_structure(df)
        # 2.- Fix extraction inconsistencies (schema-driven)
        df = self.fix_extraction_inconsistencies_schema(df)
        # 3.- Fill gaps from schema_path fill_with
        df = self.fill_with_schema(df)
        # 3b.- Drop rows matching schema_path delete_if conditions
        df, _deleted_ids = self.delete_if_from_schema(df)
        # 4.- Prune unaccepted fields (keep history; no simplify)
        df = self.prune_unaccepted_fields(df)
        # 5.- Resolve idem/id/id. with the value of the predecessor record
        df = self.resolve_idems(df)
        # 6.- Character-map cleaning
        df = self.clean_field_values_from_schema(df)
        # 7.- If null or error, calculate values from schema_path instructions
        df = self.calculate_from_schema(df)
        # 8.- Convert values from string to corrected type
        df = self.string_to_type_from_schema(df)
        # 9.- Normalize string types
        df = self.normalize_strings_for_ship_entries(df)
        # 10.- Optional: simplify_field_structure(df) materializes snapshots
        return df

    #### -------------------- Auxiliar methods -------------------------- ###
    def _update_state(self, *container_path, df, key_name:str, value: bool = True):
        return super()._update_state(*container_path, df=df, key_name=key_name, value=True)

    @staticmethod
    def get_schema_and_properties(schema: dict) -> tuple:
        """Resolve schema_path root and return (schema_to_use, properties)."""
        if "schema_path" in schema:
            schema = schema["schema_path"]
        properties = schema.get("properties", {})
        return schema, properties

    @staticmethod
    def get_schema_root(schema: dict) -> dict:
        """Resolve schema_path root and return the 'properties' dict (field name -> field schema_path)."""
        if "schema_path" in schema:
            schema = schema["schema_path"]
        return schema

    @staticmethod
    def _schema_properties(schema: dict) -> dict:
        """Resolve schema_path root and return the 'properties' dict (field name -> field schema_path)."""
        schema = PortadaCleaning.get_schema_root(schema)
        if "properties" in schema:
            return schema.get("properties", {})
        return schema

    def _expr_for_missing_column(self, col: str) -> F.Column:
        """
        Build a column expression with the correct Spark type for a missing column.
        - Base fields (in schema_path): type from config/schema_path.json via json_schema_to_spark_type.
        - <nom_camp>_history: map process -> {obtained_value, operations}.
        - <nom_camp>_history_sequence: array of process keys in order.
        """
        props = self._schema_properties(self._schema)
        suffix_hist = f"_{PortadaCleaning.HISTORY_FIELD}"
        suffix_seq = f"_{PortadaCleaning.HISTORY_SEQUENCE_FIELD}"

        if col in props:
            field_schema = props[col]
            spark_type, _ = PortadaCleaning.json_schema_to_spark_type(field_schema, primitive_types_as_strings=True)
            return F.lit(None).cast(spark_type).alias(col)
        if col.endswith(suffix_hist):
            return F.lit(None).cast(PortadaCleaning.HISTORY_MAP_TYPE).alias(col)
        if col.endswith(suffix_seq):
            return F.array().cast(ArrayType(StringType())).alias(col)
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
                raise ValueError("Array schema_path must define 'items'")

            element_type, _ = PortadaCleaning.json_schema_to_spark_type(items_schema, primitive_types_as_strings)
            return ArrayType(element_type), nullable

        raise ValueError(f"Unsupported JSON schema_path type: {schema_type}")

    @staticmethod
    def _collect_list_of_fields(schema: dict) -> set:
        fields = set()
        if "schema_path" in schema:
            schema = schema["schema_path"]

        if "properties" in schema:
            props = schema.get("properties", {})
        else:
            props = schema
        for field_name in props:
            fields.add(field_name)
            fields.add(f"{field_name}_{PortadaCleaning.HISTORY_FIELD}")
            fields.add(f"{field_name}_{PortadaCleaning.HISTORY_SEQUENCE_FIELD}")
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


    ### -------------- used by _normalize_field_structure -------------------###

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

        def not_parenthesis(c, m: dict = None, params: dict = None):
            m = transform_mapping_with_params(m.get("not_parenthesis", {}), params)
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

        processes = {"one_word": one_word, "not_digits": not_digit, "not_parenthesis": not_parenthesis}
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

    # ---------- used by fill_with_schema: fill fields from other columns according to schema_path ----------

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
                # Support for "LIT('text')" or "lit(true)" style strings in schema_path
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

    @staticmethod
    def _fill_apply_array_mode(current: F.Column, new_items: F.Column, mode: str, empty_arr: F.Column) -> F.Column:
        """
        Combine current array with new_items according to mode:
        replace | replace_null | append.
        """
        if mode == "replace":
            return new_items
        if mode == "replace_null":
            return F.when(current.isNull(), new_items).otherwise(current)
        if mode != "append":
            logger.warning("Unknown array fill mode %r; using append", mode)
        return F.concat(F.coalesce(current, empty_arr), new_items)

    def _fill_project_struct_array_items(
        self, from_expr: F.Column, parent_schema: dict, to_ref
    ) -> F.Column:
        """
        Project/remap each struct item in from_expr according to to_ref.

        - None / absent: full item copy (return from_expr unchanged).
        - list: copy same-name fields; other dest item properties → null.
        - dict: dest_field → source_field; unmapped dest properties → null.
        """
        if to_ref is None:
            return from_expr

        items_schema = parent_schema.get("items", {}) if parent_schema else {}
        item_props = items_schema.get("properties", {}) if isinstance(items_schema, dict) else {}
        if not item_props:
            return from_expr

        dest_to_src = {}
        if isinstance(to_ref, dict):
            for dest_name, src_name in to_ref.items():
                if dest_name not in item_props:
                    logger.warning(
                        "add_array_from_array 'to' dest field %r not in item schema",
                        dest_name,
                    )
                    continue
                if not src_name:
                    continue
                dest_to_src[dest_name] = src_name
        elif isinstance(to_ref, list):
            for name in to_ref:
                if not name:
                    continue
                if name not in item_props:
                    logger.warning(
                        "add_array_from_array 'to' field %r not in item schema",
                        name,
                    )
                    continue
                dest_to_src[name] = name
        elif isinstance(to_ref, str) and to_ref.strip():
            name = to_ref.strip()
            if name not in item_props:
                logger.warning(
                    "add_array_from_array 'to' field %r not in item schema",
                    name,
                )
            else:
                dest_to_src[name] = name
        else:
            return from_expr

        def _project_item(x: F.Column) -> F.Column:
            fields = []
            for prop_name in item_props.keys():
                if prop_name in dest_to_src:
                    fields.append(x.getField(dest_to_src[prop_name]).alias(prop_name))
                else:
                    fields.append(F.lit(None).alias(prop_name))
            return F.struct(*fields)

        return F.transform(from_expr, _project_item)

    def _fill_new_value_expr(
            self, df: TracedDataFrame, field_name: str, field_schema: dict, spec: dict
    ):
        """
        Build the Column expression for applying one fill spec.

        Returns ``(new_value_col, drop_origin_col)`` where ``new_value_col`` is None
        when the spec cannot be applied, and ``drop_origin_col`` is an optional
        column name to drop after the update (``drop_origin``).
        """
        fill_type = spec.get("type", "replace")
        from_ref = spec.get("from")
        to_ref = spec.get("to", field_name)
        condition = spec.get("condition")
        drop_origin = spec.get("drop_origin", False)
        current = F.col(field_name)

        existing_columns = set(df.columns)
        required_columns = self._fill_required_columns(from_ref) | self._fill_required_columns_from_condition(condition)
        if required_columns and not required_columns.issubset(existing_columns):
            return None, None

        cond_expr = self._fill_condition_expr(condition) if condition is not None else self._fill_default_from_condition(from_ref)
        drop_origin_col = None

        if fill_type == "replace":
            if isinstance(from_ref, list) and isinstance(to_ref, list):
                struct_fields = [
                    self._fill_resolve_from(from_ref[i]).alias(to_ref[i])
                    for i in range(min(len(from_ref), len(to_ref)))
                ]
                new_val = F.struct(*struct_fields) if struct_fields else current
            else:
                from_expr = self._fill_resolve_from(from_ref)
                if isinstance(to_ref, str) and "." in to_ref and to_ref.split(".")[0] == field_name:
                    path_inside = ".".join(to_ref.split(".")[1:])
                    new_val = current.withField(path_inside, from_expr)
                elif isinstance(to_ref, str) and to_ref.strip():
                    new_val = self._fill_resolve_from(from_ref)
                else:
                    new_val = from_expr
            return F.when(cond_expr, new_val).otherwise(current), None

        if fill_type == "replace_if_not_exist":
            if isinstance(from_ref, list) and isinstance(to_ref, list):
                struct_fields = []
                for i, to_name in enumerate(to_ref):
                    if i < len(from_ref):
                        struct_fields.append(self._fill_resolve_from(from_ref[i]).alias(to_name))
                if not struct_fields:
                    return None, None
                new_struct = F.struct(*struct_fields)
                new_val = F.when(current.isNull(), new_struct).otherwise(current)
            else:
                from_expr = self._fill_resolve_from(from_ref)
                if isinstance(to_ref, str) and "." in to_ref and to_ref.split(".")[0] == field_name:
                    path_inside = ".".join(to_ref.split(".")[1:])
                    branch = current[path_inside] if path_inside else current
                    new_branch = F.when(branch.isNull(), from_expr).otherwise(branch)
                    new_val = current.withField(path_inside, new_branch)
                else:
                    new_val = F.when(current.isNull(), from_expr).otherwise(current)
                if drop_origin and isinstance(from_ref, str) and from_ref.strip() and from_ref in df.columns:
                    drop_origin_col = from_ref
            return F.when(cond_expr, new_val).otherwise(current), drop_origin_col

        if fill_type == "add_array_item":
            new_item = self._fill_build_array_item_expr(field_name, field_schema, spec)
            if new_item is None:
                return None, None
            empty_arr = F.array()
            try:
                current_dtype = df.schema[field_name].dataType
                if isinstance(current_dtype, ArrayType):
                    new_item = new_item.cast(current_dtype.elementType)
                    empty_arr = F.array().cast(current_dtype)
            except Exception:
                pass
            mode = spec.get("mode", "append")
            new_array = self._fill_apply_array_mode(current, F.array(new_item), mode, empty_arr)
            return F.when(cond_expr, new_array).otherwise(current), None

        if fill_type in ("add_array_from_array", "add_all_items"):
            from_expr = self._fill_resolve_from(from_ref)
            new_items = self._fill_project_struct_array_items(
                from_expr, field_schema, spec.get("to")
            )
            empty_arr = F.array()
            try:
                current_dtype = df.schema[field_name].dataType
                if isinstance(current_dtype, ArrayType):
                    new_items = new_items.cast(current_dtype)
                    empty_arr = F.array().cast(current_dtype)
            except Exception:
                pass
            mode = spec.get("mode", "replace_null")
            new_array = self._fill_apply_array_mode(current, new_items, mode, empty_arr)
            return F.when(cond_expr, new_array).otherwise(current), None

        if fill_type == "add_array_from_list":
            list_col = self._fill_resolve_from(from_ref)
            new_items = self._fill_build_items_from_scalar_list(
                field_schema, list_col, spec.get("to")
            )
            if new_items is None:
                return None, None
            try:
                current_dtype = df.schema[field_name].dataType
                if isinstance(current_dtype, ArrayType):
                    new_items = new_items.cast(current_dtype)
                    empty_arr = F.array().cast(current_dtype)
                else:
                    empty_arr = F.array()
            except Exception:
                empty_arr = F.array()
            mode = spec.get("mode", "append")
            new_array = self._fill_apply_array_mode(current, new_items, mode, empty_arr)
            return F.when(cond_expr, new_array).otherwise(current), None

        return None, None

    def _fill_apply_one(self, df: TracedDataFrame, field_name: str, field_schema: dict, spec: dict) -> TracedDataFrame:
        """Apply a single fill_with spec (replace, replace_if_not_exist, add_array_item, add_array_from_array, add_array_from_list)."""
        new_val, drop_origin_col = self._fill_new_value_expr(df, field_name, field_schema, spec)
        if new_val is None:
            return df
        df = self._with_columns(df, {field_name: new_val})
        if drop_origin_col and drop_origin_col in df.columns:
            df = df.drop(drop_origin_col)
        return df

    def _fill_build_items_from_scalar_list(
        self, parent_schema: dict, list_col: F.Column, to_ref
    ) -> F.Column | None:
        """
        Map each scalar in list_col to an array item struct.

        ``to`` names which item property receives the scalar; other item properties
        from the JSON schema are set to null.
        """
        items_schema = parent_schema.get("items", {}) if parent_schema else {}
        item_props = items_schema.get("properties", {}) if isinstance(items_schema, dict) else {}
        if not item_props:
            return None

        if to_ref is None:
            to_fields = [next(iter(item_props.keys()))]
        elif isinstance(to_ref, list):
            to_fields = [t for t in to_ref if t]
        elif isinstance(to_ref, str) and to_ref.strip():
            to_fields = [to_ref.strip()]
        else:
            to_fields = [next(iter(item_props.keys()))]

        if len(to_fields) != 1:
            # v1: only a single target property per scalar element is supported
            logger.warning(
                "add_array_from_list supports a single 'to' field; got %s", to_fields
            )
            to_fields = to_fields[:1]

        target_prop = to_fields[0]

        def _item_struct(x: F.Column) -> F.Column:
            fields = []
            for prop_name in item_props.keys():
                if prop_name == target_prop:
                    fields.append(x.cast("string").alias(prop_name))
                else:
                    fields.append(F.lit(None).cast("string").alias(prop_name))
            return F.struct(*fields)

        return F.transform(list_col, _item_struct)

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
                # no la de l'item pare. Si to_name és un array al schema_path (p.ex. "cargo"), agafar items d'aquest.
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
            elif isinstance(from_item, dict) and from_item.get("type") in (
                "add_array_from_array",
                "add_all_items",
            ):
                from_expr = self._fill_resolve_from(from_item.get("from"))
                nested_parent_schema = item_props.get(to_name) if isinstance(item_props, dict) else {}
                if not isinstance(nested_parent_schema, dict):
                    nested_parent_schema = {}
                projected = self._fill_project_struct_array_items(
                    from_expr, nested_parent_schema, from_item.get("to")
                )
                fields.append(projected.alias(to_name))
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
        Unit is chosen by the first letter of the trimmed/lowercased unit string:
        starts with 'h' -> hours; starts with 'd' -> days; otherwise days.
        """
        origin_col = PortadaCleaning._calc_resolve_param(params.get("origin_date"))
        decrement_col = PortadaCleaning._calc_resolve_param(params.get("decrement"))
        unit_col = PortadaCleaning._calc_resolve_param(params.get("decrement_unit"))
        decrement_int = F.floor(decrement_col).cast("int")
        # When unit is moths: date_sub
        month_expr = F.date_sub(origin_col.cast("date"), decrement_int * 30)
        # When unit is days: date_sub
        days_expr = F.date_sub(origin_col.cast("date"), decrement_int)
        # When unit is hours: timestamp - hours, then cast to date
        hours_expr = F.from_unixtime(
            F.to_timestamp(origin_col.cast("date")).cast("long") - decrement_int * 3600
        ).cast(DateType())
        # When unit is minutes: timestamp - minutes, then cast to date
        mins_expr = F.from_unixtime(
            F.to_timestamp(origin_col.cast("date")).cast("long") - decrement_int * 60
        ).cast(DateType())
        unit_norm = F.lower(F.trim(unit_col.cast("string")))
        return (
            F.when(unit_norm.startswith("h"), hours_expr)
            .when(unit_norm.startswith("d"), days_expr)
            .when(unit_norm.startswith("mes"), month_expr)
            .when(unit_norm.startswith("mon"), month_expr)
            .when(unit_norm.startswith("moi"), month_expr)
            .when(unit_norm.startswith("m"), mins_expr)
            .otherwise(F.lit(None).cast(DateType()))
        )

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
class BoatFactCleaning(PortadaCleaning, BoatFactCitationExtractor, BoatFactVoicesExtractor):
    FLAG_ENTITY = 'flag'
    SHIP_TONS_ENTITY = 'ship_tons'
    TRAVEL_DURATION_ENTITY = 'travel_duration'
    COMMODITY_ENTITY = 'comodity'
    SHIP_TYPE_ENTITY = 'ship_type'
    UNIT_ENTITY = 'unit'
    PORT_ENTITY = 'port'
    MASTER_ROLE_ENTITY = 'master_role'
    ENTITY_LIST = [FLAG_ENTITY, SHIP_TONS_ENTITY, TRAVEL_DURATION_ENTITY, COMMODITY_ENTITY, SHIP_TYPE_ENTITY, UNIT_ENTITY, PORT_ENTITY, MASTER_ROLE_ENTITY]

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self.__container_path = "ship_entries"
        self.redis_params = None

    def set_redis_params(self, host, port, db=3):
        self.redis_params = {"host": host, "port": port, "db": db}

    def start_session(self):
        super().start_session()
        patcher = BoatFactPatcherDataLayer(cfg_json=self.get_configuration())
        if self.redis_params is not None:
            patcher.set_delta_data_version_manager_params(host=self.redis_params["host"], port=self.redis_params["port"], db=self.redis_params["db"])
            patcher.patch_if_needed()
        elif self.sequencer_params is not None:
            patcher.set_delta_data_version_manager_params(host=self.sequencer_params["host"], port=self.sequencer_params["port"])
            patcher.patch_if_needed()
        elif not self._use_redis_metadata:
            patcher.patch_if_needed()
        else:
            raise ValueError("No redis params or sequencer params set")

    def read_original_values_of_ship_entries(self, force_all=False) -> TracedDataFrame:
        return self.read_original_values_of_entries(self.__container_path, force_all=force_all)


    def save_original_values_of_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        self.save_original_values_of_entries(self.__container_path, entries_df=ship_entries_df, key_id_name="entry_id",
                                             partition_by=["publication_name"])
        return ship_entries_df
        # self.write_delta("original_data", self.__container_path, df=cargo_ship_entries_df, mode="merge",
        #                  partition_by=["publication_name", "publication_date", "publication_edition"])
        # return cargo_ship_entries_df

    def save_ship_entries(self, ship_entries_df: TracedDataFrame, is_cleaned=False) -> TracedDataFrame:
        self.save_entries(self.__container_path, entries_df=ship_entries_df, key_id_name="entry_id",
                         partition_by=["publication_name"])
        if is_cleaned:
            self._update_state(self.__container_path, df=ship_entries_df, key_name="entry_id")
        return ship_entries_df

    def delete_ship_entries_by_ids(self, ids):
        """Delete ship_entries rows whose ``entry_id`` is in ``ids``."""
        self.delete_entries_by_ids(self.__container_path, ids=ids, key_id_name="entry_id")

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

    def read_reviewed_ship_entries(self):
        ship_entries_df = self.read_delta("reviewed_entries", BoatFactConstants.SHIP_ENTRIES)
        return ship_entries_df

    def read_reviewed_cargo_ship_entries(self):
        ship_entries_df = self.read_delta("reviewed_entries", BoatFactConstants.CARGO_SHIP_ENTRIES)
        return ship_entries_df

    def save_reviewed_ship_entries(self, df: TracedDataFrame, is_cleaned=False) -> TracedDataFrame:
        self.write_delta("reviewed_entries", BoatFactConstants.SHIP_ENTRIES, df=df, mode="merge", key_id_name="temp_key")
        if is_cleaned:
            self._update_state("reviewed_entries", BoatFactConstants.SHIP_ENTRIES, df=df, key_name="temp_key")
        return df

    def save_reviewed_cargo_ship_entries(self, df: TracedDataFrame, is_cleaned=False) -> TracedDataFrame:
        df_id = df.withColumn("id",F.concat(df.temp_key, F.lit("_"), df.cargo_merchant_id, F.lit("_"), df.cargo_commodity_id))
        self.write_delta("reviewed_entries", BoatFactConstants.CARGO_SHIP_ENTRIES, df=df, mode="merge",
                         key_id_name="temp_key")
        if is_cleaned:
            self._update_state("reviewed_entries", BoatFactConstants.CARGO_SHIP_ENTRIES, df=df_id, key_name="id")
        return df

    def read_raw_reviewed_ship_entries(self, *container_path, force_all=False):
        if len(container_path) > 1:
            cp = container_path
        elif len(container_path) == 1:
            cp = ("reviewed_entries",*container_path,)
        else:
            cp = ("reviewed_entries", self.__container_path,)
        df = self.read_delta(*cp, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])
        if df is None:
            return df
        logger.info(f"{0 if df is None else df.count()} reviewed_entries were read")
        if not force_all:
            state_path = self._resolve_path(*cp, process_level_dir="states")
            if not self.path_exists(state_path):
                # state_df = self.spark.createDataFrame(data={}, schema=StructType(
                #     [StructField("temp_key", StringType(), True), StructField("is_cleaned", BooleanType(), True), ]))
                return df
            state_df = self.spark.read.parquet(state_path)
            state_df = state_df.filter(F.col("is_cleaned") == True).select("temp_key")

            df = df.join(state_df, on="temp_key", how="left_anti")
        return df


    def read_raw_reviewed_cargo_ship_entries(self, *container_path, force_all=False):
        if len(container_path) > 1:
            cp = container_path
        elif len(container_path) == 1:
            cp = ("reviewed_entries",*container_path,)
        else:
            cp = ("reviewed_entries", BoatFactConstants.CARGO_SHIP_ENTRIES,)
        df = self.read_delta(*cp, process_level_dir=self._process_level_dirs_[self.RAW_PROCESS_LEVEL])
        if df is None:
            return df
        logger.info(f"{0 if df is None else df.count()} reviewed_entries were read")
        if not force_all:
            df = df.withColumn("id", F.concat(df.temp_key, F.lit("_"), df.cargo_merchant_id, F.lit("_"),
                                              df.cargo_commodity_id))
            state_path = self._resolve_path(*cp, process_level_dir="states")
            if not self.path_exists(state_path):
                # state_df = self.spark.createDataFrame(data={}, schema=StructType(
                #     [StructField("id", StringType(), True),
                #      StructField("is_cleaned", BooleanType(), True), ]))
                return df
            state_df = self.spark.read.parquet(state_path)
            state_df = state_df.filter(F.col("is_cleaned") == True).select("id")

            df = df.join(state_df, on="id", how="left_anti")
            df.drop("id")
        return df


    def normalize_strings_for_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        _, properties = self.get_schema_and_properties(self._schema)
        # Only fields that flatten_string_characters can actually change (skips not_cleanable).
        string_fields = []
        df = ship_entries_df
        before_updates = {}
        before_tmps = []
        for field_name, field_schema in properties.items():
            if field_name not in df.columns:
                continue
            if field_schema.get("type") != "string":
                continue
            if "not_cleanable" in (field_schema.get("for_cleaning") or []):
                continue
            string_fields.append(field_name)
            self._register_dataframe_operation(DataframeOperation(
                type="modify",
                stage="cleaning",
                field=field_name,
                process="normalized",
                operation="normalize_string",
                description="normalize strings to lowercase, remove accents and special characters",
            ))
            before_tmp = f"__before_norm_{field_name}"
            before_tmps.append(before_tmp)
            before_updates[before_tmp] = F.col(field_name).cast("string")
        df = self._with_columns(df, before_updates)
        df = self.flatten_string_characters(df)
        history_updates = []
        for field_name in string_fields:
            before = F.col(f"__before_norm_{field_name}")
            after = F.col(field_name).cast("string")
            # Record history only when normalization changes the value.
            history_updates.append({
                "field": field_name,
                "process": "normalized",
                "obtained_value_col": after,
                "operations_col": F.array(F.lit("normalize_string")),
                "row_condition": before.isNotNull() & (before != after),
                "merge_operations": False,
            })
        df = self._append_history_batch(df, history_updates)
        if before_tmps:
            df = df.drop(*before_tmps)
        return df

    def normalize_strings_for_reviewed_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        schema = """{
          "title": "reviewed_ship_entries",
          "type": "object",
          "properties": {
            "temp_key": {
              "type": "string",
              "nullable": false,
              "for_cleaning":["not_cleanable"]
            },
            "rev_travel_departure_date": {
              "type": "date",
              "nullable": false
            },
            "rev_travel_duration_unit": {
              "type": "string",
              "nullable": false
            },
            "rev_travel_duration_value": {
              "type": "integer",
              "nullable": false
            },
            "rev_travel_arrival_date": {
              "type": "date",
              "nullable": false
            },
            "rev_travel_departure_port": {
              "type": "string",
              "nullable": false
            },
            "rev_ship_type": {
              "type": "string",
              "nullable": false
            },
            "rev_ship_name": {
              "type": "string",
              "nullable": false
            },
            "rev_ship_tons_capacity": {
              "type": "integer",
              "nullable": false
            },
            "rev_ship_tons_unit": {
              "type": "string",
              "nullable": false
            },
            "rev_ship_flag": {
              "type": "string",
              "nullable": false
            },
            "rev_master_role": {
              "type": "string",
              "nullable": false
            },
            "rev_master_name": {
              "type": "string",
              "nullable": false
            }
          },
          "additionalProperties": false
        }
        """
        ship_entries_df = self.flatten_string_characters(df=ship_entries_df, schema=json.loads(schema))
        ship_entries_df = self.string_to_type_from_schema(ship_entries_df)
        return ship_entries_df

    def normalize_strings_for_reviewed_cargo_ship_entries(self, cargo_ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        schema = """{
          "title": "reviewed_cargo_ship_entries",
          "type": "object",
          "properties": {
            "temp_key": {
              "type": "string",
              "nullable": false,
              "for_cleaning":["not_cleanable"]
            },
            "cargo_merchant_id": {
              "type": "integer",
              "nullable": false,
              "for_cleaning":["not_cleanable"]
            },
            "cargo_commodity_id": {
              "type": "integer",
              "nullable": false,
              "for_cleaning":["not_cleanable"]
            },
            "rev_cargo_merchant_name": {
              "type": "string",
              "nullable": false
            },
            "rev_cargo_commodity": {
              "type": "string",
              "nullable": false
            },
            "rev_cargo_unit": {
              "type": "string",
              "nullable": false
            }
          },
          "additionalProperties": false
        }
        """
        cargo_ship_entries_df = self.flatten_string_characters(df=cargo_ship_entries_df, schema=json.loads(schema))
        cargo_ship_entries_df = self.string_to_type_from_schema(cargo_ship_entries_df)
        return cargo_ship_entries_df

    def normalize_strings_for_entities(self, df: TracedDataFrame) -> TracedDataFrame:
        schema = """{
  "title": "known_entity",
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "Identificador único de la entidad",
      "nullable": false,
      "for_cleaning":["not_cleanable"]
    },
    "normalized_name": {
      "type": "string",
      "description": "Identificador único de la entidad normalizado",
      "nullable": false
    },
    "voices": {
      "type": "array",
      "description": "Lista de voces de la entidad.",
      "items": {
        "type": "string"
      }
    }
  },
  "required": [
    "name",
    "normalized_name",
    "voices"
  ],
  "additionalProperties": false
}
"""
        df = df.withColumn("voices_original_value", F.col("voices").cast("string")).withColumn("normalized_name", F.col("name").cast("string"))
        return self.flatten_string_characters(df=df, schema=json.loads(schema))


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
        df_voices = BoatFactVoicesExtractor.get_known_entity_voices(df_entities=df_entities)
        return df_voices
