import logging
import os
import shutil
from typing import Any, Dict, Literal, Optional, Sequence, Union

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import ArrayType, StructType
from pyspark.sql.window import Window
from splink import Linker, block_on
from splink.backends.spark import SparkAPI
from splink.blocking_analysis import count_comparisons_from_blocking_rule

from portada_data_layer.portada_patcher_data_layer import BoatFactPatcherDataLayer
from portada_data_layer.portada_linker_with_splink import PortadaLinkerWithSplink
from portada_data_layer import DeltaDataLayer, TracedDataFrame
from portada_data_layer.portada_delta_common import registry_to_portada_builder, BoatFactConstants
from portada_data_layer.portada_extraction_for_disambiguation import BoatFactCitationExtractor, BoatFactVoicesExtractor

logger = logging.getLogger(__name__)

SparkCheckpointCleanup = Optional[Literal["start", "stop"]]
_CITATION_ENRICHMENT_FIELDS = ("voice_id", "voice_normalized", "match_probability")


@registry_to_portada_builder
class BoatFactDisambiguation(DeltaDataLayer, BoatFactCitationExtractor, BoatFactVoicesExtractor):
    _ENRICHED_CITATIONS_CONTAINER = "enriched_citations"

    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self.disambiguation_cfg = {}
        self.algorithms = {}
        self._current_process_level = 2
        self._cleaned_entries_container_path = "ship_entries"
        self._sample_for_training_container_path = "reviewed_entries"
        self.started = False
        self.redis_params = None
        self.__container_path = "ship_entries"
        self._spark_checkpoint_dir: Optional[str] = None
        self.spark_checkpoint_cleanup: SparkCheckpointCleanup = None

    def set_spark_checkpoint_dir(self, path: str):
        """Set the Spark/Splink checkpoint directory used for intermediate materialization."""
        self._spark_checkpoint_dir = path
        return self

    def set_spark_checkpoint_cleanup(self, when: SparkCheckpointCleanup):
        """When to delete checkpoint files: 'start', 'stop', or None to keep them."""
        if when not in (None, "start", "stop"):
            raise ValueError("spark_checkpoint_cleanup must be None, 'start', or 'stop'")
        self.spark_checkpoint_cleanup = when
        return self

    def get_spark_checkpoint_dir(self) -> str:
        """Return the resolved Spark checkpoint directory path."""
        return self._resolve_spark_checkpoint_dir()

    def _resolve_spark_checkpoint_dir(self) -> str:
        if self._spark_checkpoint_dir:
            return os.path.abspath(self._spark_checkpoint_dir)
        return os.path.abspath(os.path.join(os.getcwd(), "data", "spark_checkpoints"))

    def _cleanup_spark_checkpoint_dir(self) -> None:
        checkpoint_dir = self._resolve_spark_checkpoint_dir()
        if os.path.isdir(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
            logger.info("Removed Spark checkpoint directory: %s", checkpoint_dir)

    def _configure_spark_checkpoint_dir(self) -> None:
        if not self.is_initialized():
            raise RuntimeError("Spark session must be initialized before configuring checkpoint dir")
        checkpoint_dir = self._resolve_spark_checkpoint_dir()
        os.makedirs(checkpoint_dir, exist_ok=True)
        self.spark.sparkContext.setCheckpointDir(checkpoint_dir)
        logger.info("Spark checkpoint directory set to: %s", checkpoint_dir)

    def read_ship_entries(self) -> TracedDataFrame:
        ship_entries_df = self.read_delta(self.__container_path)
        return ship_entries_df

    def save_ship_entries(self, ship_entries_df: TracedDataFrame) -> TracedDataFrame:
        self.save_entries(self.__container_path, entries_df=ship_entries_df, key_id_name="entry_id",
                         partition_by=["publication_name"])
        return ship_entries_df

    def save_entries(self, *container_path, entries_df: TracedDataFrame, key_id_name: str, partition_by:list = None) -> TracedDataFrame:
        entries_df = entries_df.sortWithinPartitions(*partition_by)
        if len(container_path) > 0:
            container_path = (self.__container_path,)
        self.write_delta(*container_path, df=entries_df, mode="merge", key_id_name=key_id_name, partition_by=partition_by)
        return entries_df

    def get_path_for_delta_lake(self, *container_path):
        return self._resolve_path(*container_path)

    def set_redis_params(self, host, port, db=3):
        self.redis_params = {"host": host, "port": port, "db": db}

    def start_session(self):
        if self.spark_checkpoint_cleanup == "start":
            self._cleanup_spark_checkpoint_dir()
        os.makedirs(self._resolve_spark_checkpoint_dir(), exist_ok=True)
        super().start_session()
        self._configure_spark_checkpoint_dir()
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

    def stop_session(self):
        super().stop_session()
        if self.spark_checkpoint_cleanup == "stop":
            self._cleanup_spark_checkpoint_dir()

    def use_disambiguation_cfg(self, disambiguation_cfg: dict):
        self.disambiguation_cfg = disambiguation_cfg
        return self

    def read_cleaned_entries(self, *container_path):
        df = self.read_delta(*container_path, process_level_dir=self._process_level_dirs_[self._current_process_level-1])
        return df

    def read_cleaned_ship_entries(self) -> TracedDataFrame:
        ship_entries_df = self.read_cleaned_entries(self._cleaned_entries_container_path)
        return ship_entries_df

    def read_sample_for_training(self):
        df = self.read_delta(self._sample_for_training_container_path, self._cleaned_entries_container_path, process_level_dir=self._process_level_dirs_[self._current_process_level-1])
        return df

    def get_cleaned_known_entity_voices(self, known_entity: str = None, df_entities: Union[DataFrame,TracedDataFrame] = None) -> Union[DataFrame,TracedDataFrame]:
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
            df_entities = self.read_delta("known_entities", known_entity, process_level_dir=self._process_level_dirs_[self._current_process_level-1])
        if df_entities is None:
            raise ValueError("No known entities found")
        df_voices = BoatFactVoicesExtractor.get_known_entity_voices(df_entities=df_entities)
        return df_voices

    def enrich_citations_with_predictions(
        self,
        df_predictions: Union[DataFrame, TracedDataFrame],
        df_citations: Union[DataFrame, TracedDataFrame],
    ) -> Union[DataFrame, TracedDataFrame]:
        """Add best-match voice fields from expanded Splink predictions to citations.

        ``df_predictions`` must be the expanded form returned by
        ``PortadaLinkerWithSplink.ensure_expanded_predictions`` (one row per
        citation × normalized_name). For each citation ``id``, the voice with
        the highest ``match_probability`` is kept and joined back onto
        ``df_citations`` as ``voice_id``, ``voice_normalized``, and
        ``match_probability``.
        """
        if df_predictions is None:
            raise ValueError("df_predictions is required")
        if df_citations is None:
            raise ValueError("df_citations is required")
        for col_name in ("id", "normalized_name", "name", "match_probability"):
            if col_name not in df_predictions.columns:
                raise ValueError(f"df_predictions must include column '{col_name}'")
        if "id" not in df_citations.columns:
            raise ValueError("df_citations must include column 'id'")

        rank_window = Window.partitionBy("id").orderBy(
            F.col("match_probability").desc_nulls_last(),
            F.col("normalized_name"),
        )
        best = (
            df_predictions.withColumn("_rank", F.row_number().over(rank_window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
            .select(
                F.col("id"),
                F.col("name").alias("voice_id"),
                F.col("normalized_name").alias("voice_normalized"),
                F.col("match_probability"),
            )
        )

        extra_cols = ["voice_id", "voice_normalized", "match_probability"]
        existing = [c for c in extra_cols if c in df_citations.columns]
        if existing:
            df_citations = df_citations.drop(*existing)

        return df_citations.join(best, on="id", how="left")

    @staticmethod
    def _combine_citation_frames(
        df_citations: Union[
            DataFrame,
            TracedDataFrame,
            Sequence[Union[DataFrame, TracedDataFrame, None]],
        ],
    ) -> Union[DataFrame, TracedDataFrame]:
        if isinstance(df_citations, (list, tuple)):
            combined = None
            for frame in df_citations:
                if frame is None:
                    continue
                spark_df = (
                    frame.toSparkDataFrame()
                    if isinstance(frame, TracedDataFrame)
                    else frame
                )
                if combined is None:
                    combined = spark_df
                else:
                    combined = combined.unionByName(spark_df, allowMissingColumns=True)
            if combined is None:
                raise ValueError("df_citations sequence is empty")
            return combined
        return df_citations

    def enrich_entries_with_citations(
        self,
        df_citations: Union[
            DataFrame,
            TracedDataFrame,
            Sequence[Union[DataFrame, TracedDataFrame, None]],
        ],
        df_entries: Union[DataFrame, TracedDataFrame],
    ) -> Union[DataFrame, TracedDataFrame]:
        """Write citation disambiguation results back onto entries.

        ``df_citations`` may be a single DataFrame or a sequence of citation
        DataFrames (e.g. ports, flags, commodities). They are unioned before
        enrichment so all fields are applied to ``df_entries``.

        Uses ``field_origin`` to locate the target field on each entry:

        * Simple scalar field (e.g. ``ship_flag``): adds
          ``{field}_voice_id``, ``{field}_voice_normalized``,
          ``{field}_match_probability``.
        * Array / nested path (e.g. ``travel_port_of_call_list`` or
          ``cargo_list.cargo_merchant_name``): uses citation ``*_idx``
          columns (in schema order) to update the element at that
          position, attaching the same enrichment fields on the
          containing struct (prefixed with the leaf field name when the
          path is nested).
        """
        if df_citations is None:
            raise ValueError("df_citations is required")
        if df_entries is None:
            raise ValueError("df_entries is required")
        df_citations = self._combine_citation_frames(df_citations)
        for col_name in ("entry_id", "field_origin", *_CITATION_ENRICHMENT_FIELDS):
            if col_name not in df_citations.columns:
                raise ValueError(f"df_citations must include column '{col_name}'")
        if "entry_id" not in df_entries.columns:
            raise ValueError("df_entries must include column 'entry_id'")

        field_origins = [
            row["field_origin"]
            for row in df_citations.select("field_origin").distinct().collect()
            if row["field_origin"] is not None
        ]
        result = df_entries
        for field_origin in field_origins:
            cites = df_citations.filter(F.col("field_origin") == field_origin)
            result = self._enrich_entries_for_field_origin(result, cites, field_origin)
        return result

    def _enrich_entries_for_field_origin(
        self,
        df_entries: Union[DataFrame, TracedDataFrame],
        df_citations: Union[DataFrame, TracedDataFrame],
        field_origin: str,
    ) -> Union[DataFrame, TracedDataFrame]:
        path = [p for p in field_origin.split(".") if p]
        if not path:
            raise ValueError(f"Invalid field_origin: {field_origin!r}")

        root = path[0]
        if root not in df_entries.columns:
            raise ValueError(
                f"field_origin '{field_origin}' refers to missing entry column '{root}'"
            )

        root_type = df_entries.schema[root].dataType
        idx_cols = [c for c in df_citations.columns if c.endswith("_idx")]

        if isinstance(root_type, ArrayType):
            if not idx_cols:
                raise ValueError(
                    f"field_origin '{field_origin}' is an array field but "
                    "df_citations has no '*_idx' index column"
                )
            return self._enrich_array_field(df_entries, df_citations, path, idx_cols)

        if len(path) > 1:
            raise ValueError(
                f"field_origin '{field_origin}' is nested but '{root}' is not an array"
            )
        return self._enrich_simple_field(df_entries, df_citations, field_origin)

    @staticmethod
    def _enrichment_sidecar_names(field_name: str) -> Dict[str, str]:
        return {
            src: f"{field_name}_{src}"
            for src in _CITATION_ENRICHMENT_FIELDS
        }

    def _enrich_simple_field(
        self,
        df_entries: Union[DataFrame, TracedDataFrame],
        df_citations: Union[DataFrame, TracedDataFrame],
        field_name: str,
    ) -> Union[DataFrame, TracedDataFrame]:
        rename = self._enrichment_sidecar_names(field_name)
        best = df_citations.select(
            "entry_id",
            *[F.col(src).alias(dst) for src, dst in rename.items()],
        )
        # One enriched citation expected per entry for a scalar field.
        rank_window = Window.partitionBy("entry_id").orderBy(F.col("entry_id"))
        best = (
            best.withColumn("_rank", F.row_number().over(rank_window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )

        existing = [c for c in rename.values() if c in df_entries.columns]
        if existing:
            df_entries = df_entries.drop(*existing)
        return df_entries.join(best, on="entry_id", how="left")

    def _enrich_array_field(
        self,
        df_entries: Union[DataFrame, TracedDataFrame],
        df_citations: Union[DataFrame, TracedDataFrame],
        path: Sequence[str],
        idx_cols: Sequence[str],
    ) -> Union[DataFrame, TracedDataFrame]:
        array_depth = self._array_depth_along_path(df_entries.schema[path[0]].dataType, path)
        if len(idx_cols) < array_depth:
            raise ValueError(
                f"field_origin '{'.'.join(path)}' needs {array_depth} index column(s), "
                f"found: {list(idx_cols)}"
            )
        idx_cols = list(idx_cols[:array_depth])

        if array_depth == 1:
            return self._enrich_single_level_array(df_entries, df_citations, path, idx_cols[0])
        if array_depth == 2:
            return self._enrich_two_level_array(
                df_entries, df_citations, path, idx_cols[0], idx_cols[1]
            )
        raise ValueError(
            f"Unsupported nested array depth {array_depth} for field_origin '{'.'.join(path)}'"
        )

    @staticmethod
    def _array_depth_along_path(root_type, path: Sequence[str]) -> int:
        """Count how many array levels are crossed to reach the leaf named by path."""
        depth = 0
        dtype = root_type
        # path[0] is the root column already resolved to root_type
        for i, segment in enumerate(path):
            if i == 0:
                if isinstance(dtype, ArrayType):
                    depth += 1
                    dtype = dtype.elementType
                continue
            if not isinstance(dtype, StructType):
                break
            field = next((f for f in dtype.fields if f.name == segment), None)
            if field is None:
                break
            dtype = field.dataType
            if isinstance(dtype, ArrayType):
                depth += 1
                dtype = dtype.elementType
        return depth

    @staticmethod
    def _attach_enrichment_to_struct(elem: Column, leaf_name: Optional[str]) -> Column:
        """Attach voice_* fields onto a struct element Column."""
        if leaf_name:
            rename = {
                src: f"{leaf_name}_{src}" for src in _CITATION_ENRICHMENT_FIELDS
            }
        else:
            rename = {src: src for src in _CITATION_ENRICHMENT_FIELDS}

        out = elem
        for src, dst in rename.items():
            out = out.withField(dst, F.col(f"_enrich.{src}"))
        return out

    def _enrich_single_level_array(
        self,
        df_entries: Union[DataFrame, TracedDataFrame],
        df_citations: Union[DataFrame, TracedDataFrame],
        path: Sequence[str],
        idx_col: str,
    ) -> Union[DataFrame, TracedDataFrame]:
        array_col = path[0]
        # path == [array] -> enrich the element itself;
        # path == [array, leaf] -> enrich with leaf-prefixed sidecar fields on the element.
        leaf_name = path[1] if len(path) > 1 else None
        array_type = df_entries.schema[array_col].dataType

        exploded = df_entries.select(
            "entry_id",
            F.posexplode_outer(F.col(array_col)).alias("_arr_idx", "_elem"),
        )
        cites = df_citations.select(
            "entry_id",
            F.col(idx_col).cast("int").alias("_arr_idx"),
            F.struct(*[F.col(c) for c in _CITATION_ENRICHMENT_FIELDS]).alias("_enrich"),
        )
        joined = exploded.join(cites, on=["entry_id", "_arr_idx"], how="left")
        enriched_elem = F.when(
            F.col("_enrich").isNull() | F.col("_elem").isNull(),
            F.col("_elem"),
        ).otherwise(self._attach_enrichment_to_struct(F.col("_elem"), leaf_name))
        joined = joined.withColumn("_elem", enriched_elem)

        rebuilt_arrays = (
            joined.groupBy("entry_id")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.when(
                            F.col("_arr_idx").isNotNull(),
                            F.struct(F.col("_arr_idx").alias("i"), F.col("_elem").alias("e")),
                        )
                    )
                ).alias("_sorted")
            )
            .withColumn(
                array_col,
                F.when(
                    F.size(F.col("_sorted")) == 0,
                    F.lit(None).cast(array_type),
                ).otherwise(F.expr("transform(_sorted, x -> x.e)")),
            )
            .select("entry_id", array_col)
        )
        return df_entries.drop(array_col).join(rebuilt_arrays, on="entry_id", how="left")

    def _enrich_two_level_array(
        self,
        df_entries: Union[DataFrame, TracedDataFrame],
        df_citations: Union[DataFrame, TracedDataFrame],
        path: Sequence[str],
        outer_idx_col: str,
        inner_idx_col: str,
    ) -> Union[DataFrame, TracedDataFrame]:
        """Enrich paths like cargo_list.cargo[.cargo_commodity] (array → struct → array)."""
        if len(path) < 2:
            raise ValueError(
                f"Two-level array enrichment expects path like "
                f"'array.inner_array[.leaf]', got '{'.'.join(path)}'"
            )
        array_col, inner_array_name = path[0], path[1]
        leaf_name = path[2] if len(path) > 2 else None
        array_type = df_entries.schema[array_col].dataType

        outer = df_entries.select(
            "entry_id",
            F.posexplode_outer(F.col(array_col)).alias("_outer_idx", "_outer_elem"),
        )
        inner = outer.select(
            "entry_id",
            "_outer_idx",
            "_outer_elem",
            F.posexplode_outer(F.col(f"_outer_elem.{inner_array_name}")).alias(
                "_inner_idx", "_inner_elem"
            ),
        )
        cites = df_citations.select(
            "entry_id",
            F.col(outer_idx_col).cast("int").alias("_outer_idx"),
            F.col(inner_idx_col).cast("int").alias("_inner_idx"),
            F.struct(*[F.col(c) for c in _CITATION_ENRICHMENT_FIELDS]).alias("_enrich"),
        )
        joined = inner.join(cites, on=["entry_id", "_outer_idx", "_inner_idx"], how="left")
        enriched_inner = F.when(
            F.col("_enrich").isNull() | F.col("_inner_elem").isNull(),
            F.col("_inner_elem"),
        ).otherwise(self._attach_enrichment_to_struct(F.col("_inner_elem"), leaf_name))
        joined = joined.withColumn("_inner_elem", enriched_inner)

        outer_rebuilt = (
            joined.groupBy("entry_id", "_outer_idx", "_outer_elem")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.when(
                            F.col("_inner_idx").isNotNull(),
                            F.struct(
                                F.col("_inner_idx").alias("i"),
                                F.col("_inner_elem").alias("e"),
                            ),
                        )
                    )
                ).alias("_inner_sorted")
            )
            .withColumn(
                "_outer_elem",
                F.when(
                    F.col("_outer_elem").isNull(),
                    F.col("_outer_elem"),
                ).otherwise(
                    F.col("_outer_elem").withField(
                        inner_array_name,
                        F.when(
                            F.size(F.col("_inner_sorted")) == 0,
                            F.col(f"_outer_elem.{inner_array_name}"),
                        ).otherwise(F.expr("transform(_inner_sorted, x -> x.e)")),
                    )
                ),
            )
            .drop("_inner_sorted")
        )

        rebuilt_arrays = (
            outer_rebuilt.groupBy("entry_id")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.when(
                            F.col("_outer_idx").isNotNull(),
                            F.struct(
                                F.col("_outer_idx").alias("i"),
                                F.col("_outer_elem").alias("e"),
                            ),
                        )
                    )
                ).alias("_outer_sorted")
            )
            .withColumn(
                array_col,
                F.when(
                    F.size(F.col("_outer_sorted")) == 0,
                    F.lit(None).cast(array_type),
                ).otherwise(F.expr("transform(_outer_sorted, x -> x.e)")),
            )
            .select("entry_id", array_col)
        )
        return df_entries.drop(array_col).join(rebuilt_arrays, on="entry_id", how="left")

    def save_enriched_citations(
            self,
            df: Union[DataFrame, TracedDataFrame],
            entity_name: str,
            mode: str = "overwrite",
            key_id_name: str = "id",
    ) -> Union[DataFrame, TracedDataFrame]:
        """Write enriched citations to ``enriched_citations/<entity_name>`` (silver)."""
        self.write_delta(
            self._ENRICHED_CITATIONS_CONTAINER,
            entity_name,
            df=df,
            mode=mode,
            key_id_name=key_id_name,
        )
        return df

    def read_enriched_citations(self, entity_name: str) -> TracedDataFrame:
        return self.read_delta(self._ENRICHED_CITATIONS_CONTAINER, entity_name)

    def save_enriched_port_citations(self, df: Union[DataFrame, TracedDataFrame]):
        return self.save_enriched_citations(df, "port")

    def save_enriched_ship_type_citations(self, df: Union[DataFrame, TracedDataFrame]):
        return self.save_enriched_citations(df, "ship_type")

    def save_enriched_ship_tons_unit_citations(self, df: Union[DataFrame, TracedDataFrame]):
        return self.save_enriched_citations(df, "ship_tons")

    def save_enriched_ship_flag_citations(self, df: Union[DataFrame, TracedDataFrame]):
        return self.save_enriched_citations(df, "flag")

    def save_enriched_master_role_citations(self, df: Union[DataFrame, TracedDataFrame]):
        return self.save_enriched_citations(df, "master_role")

    def save_enriched_cargo_commodity_citations(self, df: Union[DataFrame, TracedDataFrame]):
        return self.save_enriched_citations(df, "commodity")

    def save_enriched_cargo_unit_citations(self, df: Union[DataFrame, TracedDataFrame]):
        return self.save_enriched_citations(df, "unit")

    def save_enriched_travel_duration_citations(self, df: Union[DataFrame, TracedDataFrame]):
        return self.save_enriched_citations(df, "travel_duration")
