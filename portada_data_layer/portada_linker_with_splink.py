from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Literal, Optional, Self, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType
from pyspark.sql.window import Window
from splink import SettingsCreator

from portada_data_layer.similarity_algorithms import (
    AbstractEmbeddingModelAlgorithm,
    LinkerPreprocessCache,
    PhoneticDmAlgorithm,
    RootLevenshteinAlgorithm,
    SimilarityAlgorithm,
    SoundexAlgorithm,
    instantiate_similarity_algorithms,
)

from splink import Linker
from splink.backends.spark import SparkAPI

BatchOutputStorage = Literal["auto", "parquet_dirs", "delta"]
BATCH_INDEX_COLUMN = "batch_index"


class PortadaLinkerWithSplink:
    def __init__(
        self,
        citations: Optional[DataFrame] = None,
        voices: Optional[DataFrame] = None,
        reviewed_data: Optional[DataFrame] = None,
        entity_cfg: Optional[dict[str, Any]] = None,
        disambiguation_cfg: Optional[dict[str, Any]] = None,
        entity_name: Optional[str] = None,
        entity_field_name: Optional[str] = None,
        spark_session: Optional[SparkSession] = None,
    ) -> None:
        self.citations = citations
        self.voices = voices
        self.reviewed_data = reviewed_data
        self.entity_cfg = entity_cfg
        self.disambiguation_cfg = disambiguation_cfg
        self.entity_name = entity_name
        self.entity_field_name = entity_field_name
        self.spark_session = spark_session
        self.algorithms: Optional[List[SimilarityAlgorithm]] = None
        self.citations_unique_values: Optional[DataFrame] = None
        self.voices_unique_values: Optional[DataFrame] = None
        self.linker: Optional[Linker] = None
        self.model_path: Optional[str] = None
        self.predictions = None
        self.expanded_predictions: Optional[DataFrame] = None
        self._spark_checkpoint_dir: Optional[str] = None
        self._preprocess_cache: Optional[LinkerPreprocessCache] = None
        self._voices_linker_prepared: Optional[DataFrame] = None

    def with_citations(self, citations: DataFrame) -> Self:
        self.citations = citations
        return self

    def with_voices(self, voices: DataFrame) -> Self:
        self.voices = voices
        return self

    def with_reviewed_data(self, reviewed_data: DataFrame) -> Self:
        self.reviewed_data = reviewed_data
        return self

    def with_entity_cfg(self, entity_cfg: dict[str, Any]) -> Self:
        self.entity_cfg = entity_cfg
        return self

    def with_disambiguation_cfg(self, disambiguation_cfg: dict[str, Any]) -> Self:
        self.disambiguation_cfg = disambiguation_cfg
        return self

    def with_entity_name(self, entity_name: str) -> Self:
        self.entity_name = entity_name
        return self

    def with_entity_field_name(self, entity_field_name: str) -> Self:
        self.entity_field_name = entity_field_name
        return self

    def with_spark_session(self, spark_session: SparkSession) -> Self:
        self.spark_session = spark_session
        return self

    def with_spark_checkpoint_dir(self, path: str) -> Self:
        self._spark_checkpoint_dir = os.path.abspath(path)
        return self

    def get_spark_checkpoint_dir(self) -> Optional[str]:
        return self._spark_checkpoint_dir

    def calculate_algorithms(self, output_name: str) -> None:
        if self.entity_cfg is None:
            raise ValueError("entity_cfg is required to build algorithms")
        algorithm_cfg = (self.disambiguation_cfg or {}).get("general_config_algorithms", {})
        algorithm_keys = self.entity_cfg.get("algorithms", [])
        algorithms = []
        for algorithm_key in algorithm_keys:
            alg_entry = algorithm_cfg[algorithm_key]
            class_name = alg_entry["class"]
            thresholds = alg_entry["thresholds"]
            params = alg_entry.get("params", {})
            alg = instantiate_similarity_algorithms(
                class_name,
                self.entity_cfg["field_l"],
                self.entity_cfg["field_r"],
                output_name=output_name,
                thresholds=thresholds,
                params=params,
            )
            algorithms.append(alg)
        self.algorithms = algorithms

    def _ensure_spark_session(self) -> SparkSession:
        if self.spark_session is None:
            raise ValueError("A SparkSession is required")
        session = self.spark_session
        if session.sparkContext.getCheckpointDir() is None:
            if self._spark_checkpoint_dir:
                spark_checkpoint_dir = self._spark_checkpoint_dir
            else:
                spark_checkpoint_dir = os.path.abspath(
                    os.path.join(os.getcwd(), "data", "spark_checkpoints")
                )
            os.makedirs(spark_checkpoint_dir, exist_ok=True)
            session.sparkContext.setCheckpointDir(spark_checkpoint_dir)
        return session

    def _ensure_preprocess_cache(self) -> LinkerPreprocessCache:
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required for preprocess cache")
        if self._preprocess_cache is None:
            self._preprocess_cache = LinkerPreprocessCache(self.entity_field_name)
        return self._preprocess_cache

    def _linker_seed_frames(self) -> Tuple[DataFrame, DataFrame]:
        if self.citations is None or self.voices is None:
            raise ValueError("citations and voices are required")
        citation_seed = SimilarityAlgorithm.force_unique(self.citations, self._field_l())
        voice_seed = SimilarityAlgorithm.force_unique(self.voices, self._field_r())
        return citation_seed, voice_seed

    def _warm_preprocess_cache(
        self,
        citations_df: DataFrame,
        voices_df: DataFrame,
    ) -> DataFrame:
        self._ensure_algorithms_ready()
        cache = self._ensure_preprocess_cache()
        citation_seed, voice_seed = self._linker_seed_frames()
        cache.register_udfs(self.algorithms, citation_seed, voice_seed)

        cache.ensure_side_cached(
            voices_df,
            self._field_r(),
            self._field_r(),
            "voice",
            self.algorithms,
            citation_seed,
        )
        cache.ensure_side_cached(
            citations_df,
            self._field_l(),
            self._field_l(),
            "citation",
            self.algorithms,
            voice_seed,
        )
        linker_cols = self.linker_columns_for_algorithms()
        voices_linker = cache.lookup_linker_rows(
            voices_df,
            self._field_r(),
            self._field_r(),
            "voice",
            linker_cols,
        )
        self._voices_linker_prepared = voices_linker
        return voices_linker

    def _linker_voice_count(self, voices_df: DataFrame) -> int:
        return SimilarityAlgorithm.force_unique(voices_df, self._field_r()).count()

    def _input_table_aliases(self) -> list[str]:
        if self.entity_cfg is None:
            raise ValueError("entity_cfg is required")
        return [
            self.entity_cfg.get("field_l", "citation"),
            self.entity_cfg.get("field_r", "voice"),
        ]

    def _field_l(self) -> str:
        if self.entity_cfg is None:
            raise ValueError("entity_cfg is required")
        return self.entity_cfg.get("field_l", "citation")

    def _field_r(self) -> str:
        if self.entity_cfg is None:
            raise ValueError("entity_cfg is required")
        return self.entity_cfg.get("field_r", "voice")

    def _ensure_algorithms_ready(self) -> None:
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required to prepare linker input data")
        if not self.algorithms:
            self.calculate_algorithms(self.entity_field_name)

    def _prepare_citations_and_voices_for_linking(
        self, citations_df: Optional[DataFrame] = None
    ) -> Tuple[DataFrame, DataFrame]:
        if self.citations is None and citations_df is None:
            raise ValueError("citations is required")
        if self.voices is None:
            raise ValueError("voices is required")
        source_citations = citations_df if citations_df is not None else self.citations
        voices_linker = self._warm_preprocess_cache(source_citations, self.voices)
        cache = self._ensure_preprocess_cache()
        linker_cols = self.linker_columns_for_algorithms()
        citations_linker = cache.lookup_linker_rows(
            source_citations,
            self._field_l(),
            self._field_l(),
            "citation",
            linker_cols,
        )
        return citations_linker, voices_linker

    def _prepare_voices_for_linking(self, voices: Optional[DataFrame] = None) -> DataFrame:
        voices_df = voices if voices is not None else self.voices
        if voices_df is None:
            raise ValueError("voices is required")
        if self.citations is None:
            raise ValueError("citations is required to seed voice preprocessing")
        if voices is None and self._voices_linker_prepared is not None:
            return self._voices_linker_prepared

        self._ensure_algorithms_ready()
        cache = self._ensure_preprocess_cache()
        citation_seed, voice_seed = self._linker_seed_frames()
        cache.register_udfs(self.algorithms, citation_seed, voice_seed)
        cache.ensure_side_cached(
            voices_df,
            self._field_r(),
            self._field_r(),
            "voice",
            self.algorithms,
            citation_seed,
        )
        linker_cols = self.linker_columns_for_algorithms()
        voices_linker = cache.lookup_linker_rows(
            voices_df,
            self._field_r(),
            self._field_r(),
            "voice",
            linker_cols,
        )
        if voices is None:
            self._voices_linker_prepared = voices_linker
        return voices_linker

    def _prepare_citations_for_linking(
        self,
        citations_df: DataFrame,
        voices_linker: DataFrame,
        voices: Optional[DataFrame] = None,
    ) -> DataFrame:
        voices_df = voices if voices is not None else self.voices
        if voices_df is None:
            raise ValueError("voices is required")
        self._ensure_algorithms_ready()
        cache = self._ensure_preprocess_cache()
        citation_seed, voice_seed = self._linker_seed_frames()
        if not cache.udfs_registered:
            cache.register_udfs(self.algorithms, citation_seed, voice_seed)
        voice_seed = SimilarityAlgorithm.force_unique(voices_df, self._field_r())
        cache.ensure_side_cached(
            citations_df,
            self._field_l(),
            self._field_l(),
            "citation",
            self.algorithms,
            voice_seed,
        )
        linker_cols = self.linker_columns_for_algorithms()
        return cache.lookup_linker_rows(
            citations_df,
            self._field_l(),
            self._field_l(),
            "citation",
            linker_cols,
        )

    @staticmethod
    def _batch_storage_kind(output_dir: str, output_storage: BatchOutputStorage) -> str:
        if output_storage not in ("auto", "parquet_dirs", "delta"):
            raise ValueError("output_storage must be 'auto', 'parquet_dirs', or 'delta'")
        if output_storage == "delta":
            return "delta"
        if output_storage == "parquet_dirs":
            return "parquet_dirs"
        if re.match(r"\w+://", output_dir):
            return "delta"
        return "parquet_dirs"

    def _delta_batch_table_exists(self, output_dir: str) -> bool:
        from delta.tables import DeltaTable

        session = self._ensure_spark_session()
        try:
            return DeltaTable.isDeltaTable(session, output_dir)
        except Exception:
            return False

    def _list_batch_paths(self, output_dir: str) -> List[str]:
        if not os.path.isdir(output_dir):
            return []
        paths: List[str] = []
        for name in sorted(os.listdir(output_dir)):
            batch_path = os.path.join(output_dir, name)
            if name.startswith("batch_") and os.path.isdir(batch_path):
                paths.append(batch_path)
        return paths

    def _load_processed_pairs(
        self,
        output_dir: str,
        output_storage: BatchOutputStorage = "auto",
    ) -> Optional[DataFrame]:
        storage_kind = self._batch_storage_kind(output_dir, output_storage)
        session = self._ensure_spark_session()
        if storage_kind == "delta":
            if not self._delta_batch_table_exists(output_dir):
                return None
            return (
                session.read.format("delta")
                .load(output_dir)
                .select("id", "normalized_name")
                .distinct()
            )

        batch_paths = self._list_batch_paths(output_dir)
        if not batch_paths:
            return None
        session = self._ensure_spark_session()
        return (
            session.read.option("mergeSchema", "true")
            .parquet(*batch_paths)
            .select("id", "normalized_name")
            .distinct()
        )

    def _pending_citations_and_voices(
        self, processed_pairs: Optional[DataFrame]
    ) -> Tuple[DataFrame, DataFrame]:
        if self.citations is None:
            raise ValueError("citations is required")
        if self.voices is None:
            raise ValueError("voices is required")

        citations = self.citations
        voices = self.voices
        if processed_pairs is None:
            return citations, voices

        expected_pairs = citations.select("id").distinct().crossJoin(
            voices.select("normalized_name").distinct()
        )
        missing_pairs = expected_pairs.join(
            processed_pairs, on=["id", "normalized_name"], how="left_anti"
        )
        if missing_pairs.limit(1).count() == 0:
            return citations.limit(0), voices.limit(0)

        pending_ids = missing_pairs.select("id").distinct()
        pending_names = missing_pairs.select("normalized_name").distinct()
        citations_pending = citations.join(pending_ids, on="id", how="inner")
        voices_pending = voices.join(pending_names, on="normalized_name", how="inner")
        return citations_pending, voices_pending

    def _next_batch_index(
        self,
        output_dir: str,
        output_storage: BatchOutputStorage = "auto",
    ) -> int:
        storage_kind = self._batch_storage_kind(output_dir, output_storage)
        if storage_kind == "delta":
            if not self._delta_batch_table_exists(output_dir):
                return 0
            session = self._ensure_spark_session()
            batches = session.read.format("delta").load(output_dir)
            if BATCH_INDEX_COLUMN not in batches.columns:
                return 0
            max_index = batches.agg(F.max(BATCH_INDEX_COLUMN).alias("max_index")).collect()[0][
                "max_index"
            ]
            if max_index is None:
                return 0
            return int(max_index) + 1

        batch_paths = self._list_batch_paths(output_dir)
        if not batch_paths:
            return 0
        max_index = -1
        for path in batch_paths:
            name = os.path.basename(path)
            try:
                max_index = max(max_index, int(name.split("_", 1)[1]))
            except (IndexError, ValueError):
                continue
        return max_index + 1

    def _read_all_batches(
        self,
        output_dir: str,
        output_storage: BatchOutputStorage = "auto",
    ) -> DataFrame:
        storage_kind = self._batch_storage_kind(output_dir, output_storage)
        session = self._ensure_spark_session()
        if storage_kind == "delta":
            if not self._delta_batch_table_exists(output_dir):
                raise ValueError(f"No Delta batch table found at {output_dir}")
            batches = session.read.format("delta").load(output_dir)
            if BATCH_INDEX_COLUMN in batches.columns:
                batches = batches.drop(BATCH_INDEX_COLUMN)
            return batches

        batch_paths = self._list_batch_paths(output_dir)
        if not batch_paths:
            raise ValueError(f"No batch parquet directories found in {output_dir}")
        session = self._ensure_spark_session()
        return session.read.option("mergeSchema", "true").parquet(*batch_paths)

    def ensure_expanded_predictions(
        self,
        output_dir: Optional[str] = None,
        output_storage: BatchOutputStorage = "auto",
        reload: bool = False,
    ) -> DataFrame:
        """Return expanded predictions, loading from batch storage when needed.

        If ``expanded_predictions`` is already cached and ``reload`` is False, returns
        the cached DataFrame. Otherwise reads all batches from ``output_dir`` when
        provided, or raises if neither cache nor ``output_dir`` is available.
        """
        if self.expanded_predictions is not None and not reload:
            return self.expanded_predictions
        if output_dir is None:
            raise ValueError(
                "expanded_predictions is not loaded; provide output_dir to read stored batches"
            )
        self.expanded_predictions = self._read_all_batches(output_dir, output_storage)
        return self.expanded_predictions

    def _write_batch(
        self,
        batch_result: DataFrame,
        output_dir: str,
        storage_kind: str,
        batch_index: int,
    ) -> str:
        if storage_kind == "delta":
            (
                batch_result.withColumn(BATCH_INDEX_COLUMN, F.lit(batch_index))
                .write.format("delta")
                .mode("append")
                .save(output_dir)
            )
            return output_dir

        os.makedirs(output_dir, exist_ok=True)
        batch_path = os.path.join(output_dir, f"batch_{batch_index:04d}")
        batch_result.write.mode("overwrite").parquet(batch_path)
        return batch_path

    @staticmethod
    def _citation_batch_size_from_max_pairs(max_pairs_per_batch: int, voice_count: int) -> int:
        if max_pairs_per_batch <= 0:
            raise ValueError("max_pairs_per_batch must be positive")
        if voice_count <= 0:
            raise ValueError("voice_count must be positive")
        return max(1, max_pairs_per_batch // voice_count)

    def _iter_citation_batches(
        self, batch_size: int, citations: Optional[DataFrame] = None
    ) -> Iterator[DataFrame]:
        citations_df = citations if citations is not None else self.citations
        if citations_df is None:
            raise ValueError("citations is required")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        id_col = "id" if "id" in citations_df.columns else citations_df.columns[0]
        n_rows = citations_df.count()
        if n_rows == 0:
            return

        n_batches = max(1, (n_rows + batch_size - 1) // batch_size)
        indexed = citations_df.withColumn(
            "_batch_id",
            F.pmod(F.xxhash64(F.col(id_col)), F.lit(n_batches)).cast("int"),
        )
        batch_ids = [
            row["_batch_id"]
            for row in indexed.select("_batch_id").distinct().orderBy("_batch_id").collect()
        ]
        for batch_id in batch_ids:
            batch_df = indexed.filter(F.col("_batch_id") == batch_id).drop("_batch_id")
            if not batch_df.isEmpty():
                yield batch_df

    def _build_citation_representation_map(self, citations_linker: DataFrame) -> DataFrame:
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required to build citation representation map")
        return citations_linker.select(
            F.col(self.entity_field_name).alias("_linker_citation_text"),
            F.col(SimilarityAlgorithm.UNIQUE_ID_COLUMN_NAME).alias("unique_id_l"),
        )

    def _build_voice_lookup(self, voices: Optional[DataFrame] = None) -> DataFrame:
        voices_df = voices if voices is not None else self.voices
        if voices_df is None:
            raise ValueError("voices is required")
        id_col = (
            SimilarityAlgorithm.UNIQUE_ID_COLUMN_NAME
            if SimilarityAlgorithm.UNIQUE_ID_COLUMN_NAME in voices_df.columns
            else "id"
        )
        return voices_df.select(
            F.col(id_col).alias("unique_id_r"),
            "name",
            "normalized_name",
            "voice",
        )

    def _predictions_to_spark_dataframe(self, predictions) -> DataFrame:
        if hasattr(predictions, "as_spark_dataframe"):
            return predictions.as_spark_dataframe()
        if isinstance(predictions, DataFrame):
            return predictions
        raise TypeError("predictions must be a SplinkDataFrame or Spark DataFrame")

    def _best_predictions_per_name(
        self, predictions, voices: Optional[DataFrame] = None
    ) -> DataFrame:
        predictions_df = self._predictions_to_spark_dataframe(predictions)
        voice_lookup = self._build_voice_lookup(voices)
        enriched = predictions_df.join(voice_lookup, on="unique_id_r", how="left")
        window = Window.partitionBy("unique_id_l", "normalized_name").orderBy(
            F.desc("match_probability")
        )
        return (
            enriched.withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )

    def _metric_columns_for_expanded_join(
        self, best_by_name: DataFrame, pair_cols: set[str]
    ) -> List[str]:
        join_keys = {"unique_id_l", "normalized_name"}
        metric_cols: List[str] = []
        for field in best_by_name.schema.fields:
            name = field.name
            if name in join_keys or name in pair_cols:
                continue
            if isinstance(field.dataType, ArrayType):
                continue
            if name.endswith("_l") or (name.endswith("_r") and name != "unique_id_r"):
                continue
            metric_cols.append(name)
        return metric_cols

    def expand_predictions(
        self,
        predictions=None,
        citations_linker: Optional[DataFrame] = None,
        citations: Optional[DataFrame] = None,
        voices: Optional[DataFrame] = None,
    ) -> DataFrame:
        if predictions is None:
            predictions = self.predictions
        if predictions is None:
            raise ValueError("predictions is required; call predict() first or pass predictions")
        if self.citations is None or self.voices is None:
            raise ValueError("citations and voices are required to expand predictions")

        citations_df = citations if citations is not None else self.citations
        if citations_linker is None:
            citations_linker, _ = self._prepare_citations_and_voices_for_linking(citations_df)

        citation_map = self._build_citation_representation_map(citations_linker)
        field_l = self._field_l()
        citations_expanded = citations_df.join(
            citation_map,
            F.col(field_l) == citation_map["_linker_citation_text"],
            "left",
        ).drop("_linker_citation_text")

        voices_df = voices if voices is not None else self.voices
        if voices_df is None:
            raise ValueError("voices is required to expand predictions")
        names = voices_df.select("normalized_name", "name").dropDuplicates(["normalized_name"])
        pairs = citations_expanded.crossJoin(F.broadcast(names))

        best_by_name = self._best_predictions_per_name(predictions, voices=voices_df)
        join_keys = ["unique_id_l", "normalized_name"]
        pair_cols = set(pairs.columns)
        metric_cols = self._metric_columns_for_expanded_join(best_by_name, pair_cols)
        result = pairs.join(best_by_name.select(*join_keys, *metric_cols), on=join_keys, how="left")

        self.expanded_predictions = result
        return result

    def _scored_reviewed_predictions(
        self,
        expanded_predictions: Optional[DataFrame] = None,
        threshold_match_probability: float = 0.65,
    ) -> DataFrame:
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required to score reviewed predictions")
        if self.reviewed_data is None:
            raise ValueError("reviewed_data is required to score reviewed predictions")

        expanded = (
            expanded_predictions
            if expanded_predictions is not None
            else self.expanded_predictions
        )
        if expanded is None:
            raise ValueError(
                "expanded_predictions is required; call predict() first or pass expanded_predictions"
            )
        if "temp_key" not in expanded.columns:
            raise ValueError("expanded_predictions must include temp_key")
        if "normalized_name" not in expanded.columns:
            raise ValueError("expanded_predictions must include normalized_name")
        if "match_probability" not in expanded.columns:
            raise ValueError("expanded_predictions must include match_probability")

        rev_field_name = f"rev_{self.entity_field_name}"
        reviewed = self.reviewed_data.filter(
            F.col(rev_field_name).isNotNull() & ~F.col(rev_field_name).startswith("#")
        ).select("temp_key", F.col(rev_field_name).alias("y_true"))

        labeled = expanded.join(reviewed, on="temp_key", how="inner")
        rank_window = Window.partitionBy("temp_key").orderBy(
            F.col("match_probability").desc_nulls_last(),
            F.col("normalized_name"),
        )
        return (
            labeled.withColumn("_rank", F.row_number().over(rank_window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
            .withColumn(
                "y_pred",
                F.when(
                    F.col("match_probability") >= F.lit(threshold_match_probability),
                    F.col("normalized_name"),
                ).otherwise(F.lit("NONE")),
            )
        )

    def confusion_matrix_from_reviewed(
        self,
        expanded_predictions: Optional[DataFrame] = None,
        threshold_match_probability: float = 0.65,
    ) -> DataFrame:
        """Build a confusion matrix from expanded predictions and reviewed labels.

        For each reviewed citation (joined on temp_key), picks the normalized_name
        with the highest match_probability. If that maximum is below
        threshold_match_probability, the prediction is recorded as "NONE".
        """
        scored = self._scored_reviewed_predictions(
            expanded_predictions=expanded_predictions,
            threshold_match_probability=threshold_match_probability,
        )
        return (
            scored.groupBy("y_true", "y_pred")
            .agg(F.count("*").alias("count"))
            .orderBy("y_true", "y_pred")
        )

    def confusion_outcome_summary_from_reviewed(
        self,
        expanded_predictions: Optional[DataFrame] = None,
        threshold_match_probability: float = 0.65,
    ) -> DataFrame:
        """Summarize reviewed predictions as ++, +-, -+ and -- outcome rates."""
        scored = self._scored_reviewed_predictions(
            expanded_predictions=expanded_predictions,
            threshold_match_probability=threshold_match_probability,
        )
        outcomes = scored.withColumn(
            "outcome",
            F.when(F.col("y_true") == F.col("y_pred"), F.lit("++"))
            .when(F.col("y_pred") == F.lit("NONE"), F.lit("+-"))
            .when(
                (F.col("y_pred") != F.lit("NONE")) & (F.col("y_true") != F.col("y_pred")),
                F.lit("-+"),
            )
            .otherwise(F.lit("--")),
        )
        counts = outcomes.groupBy("outcome").agg(F.count("*").alias("count"))
        total = outcomes.count()
        return (
            counts.withColumn("pct", F.round(F.col("count") / F.lit(total) * F.lit(100.0), 2))
            .orderBy("outcome")
        )

    def abstained_reviewed_predictions_detail(
        self,
        expanded_predictions: Optional[DataFrame] = None,
        threshold_match_probability: float = 0.65,
    ) -> DataFrame:
        """Return citation x voice rows for reviewed predictions abstained as NONE (+-).

        Each abstained citation (y_pred == NONE) is expanded with every candidate
        normalized_name and its match_probability, plus the reviewed y_true label.
        """
        scored = self._scored_reviewed_predictions(
            expanded_predictions=expanded_predictions,
            threshold_match_probability=threshold_match_probability,
        )
        abstained = scored.filter(F.col("y_pred") == F.lit("NONE")).select(
            "temp_key",
            "y_true",
            F.col("match_probability").alias("max_match_probability"),
        )

        expanded = (
            expanded_predictions
            if expanded_predictions is not None
            else self.expanded_predictions
        )
        if expanded is None:
            raise ValueError(
                "expanded_predictions is required; call predict() first or pass expanded_predictions"
            )

        citation_col = self._field_l()
        if citation_col not in expanded.columns and "citation" in expanded.columns:
            citation_col = "citation"

        rank_window = Window.partitionBy("temp_key").orderBy(
            F.col("match_probability").desc_nulls_last(),
            F.col("normalized_name"),
        )
        joined = (
            expanded.join(abstained, on="temp_key", how="inner")
            .withColumn("is_true_voice", F.col("normalized_name") == F.col("y_true"))
            .withColumn("voice_rank", F.row_number().over(rank_window))
        )
        select_exprs = []
        if "id" in joined.columns:
            select_exprs.append(F.col("id"))
        select_exprs.append(F.col("temp_key"))
        if citation_col in joined.columns:
            select_exprs.append(F.col(citation_col).alias("citation"))
        select_exprs.extend(
            [
                F.col("y_true"),
                F.col("normalized_name"),
                F.col("name"),
                F.col("voice"),
                F.col("match_probability"),
                F.col("max_match_probability"),
                F.col("is_true_voice"),
                F.col("voice_rank"),
            ]
        )
        return joined.select(*select_exprs).orderBy("temp_key", "voice_rank")

    def _unregister_input_tables(self) -> None:
        session = self._ensure_spark_session()
        for alias in self._input_table_aliases():
            try:
                session.catalog.dropTempView(alias)
            except Exception:
                pass

    def _resolve_linker_settings(self, linker_or_model_path: Union[str, Linker]) -> Union[str, dict[str, Any]]:
        if isinstance(linker_or_model_path, str):
            return linker_or_model_path
        if isinstance(linker_or_model_path, Linker):
            return linker_or_model_path._settings_obj.as_dict()
        raise TypeError("linker_or_model_path must be a model path string or a Splink Linker instance")

    def predict(
        self,
        linker_or_model_path: Union[str, Linker],
        threshold_match_probability: Optional[float] = None,
        threshold_match_weight: Optional[float] = None,
        expand: bool = True,
        citations_batch: Optional[DataFrame] = None,
        citations_for_expand: Optional[DataFrame] = None,
        voices_linker: Optional[DataFrame] = None,
        voices_for_expand: Optional[DataFrame] = None,
        voices_for_linking: Optional[DataFrame] = None,
    ):
        session = self._ensure_spark_session()
        settings = self._resolve_linker_settings(linker_or_model_path)

        if citations_batch is not None and voices_linker is not None:
            citations_linker = self._prepare_citations_for_linking(
                citations_batch,
                voices_linker,
                voices=voices_for_linking,
            )
        elif citations_batch is not None:
            citations_linker, voices_linker = self._prepare_citations_and_voices_for_linking(
                citations_batch
            )
        else:
            citations_linker, voices_linker = self._prepare_citations_and_voices_for_linking()

        self._unregister_input_tables()
        self.linker = Linker(
            [citations_linker, voices_linker],
            settings,
            db_api=SparkAPI(spark_session=session),
            input_table_aliases=self._input_table_aliases(),
        )
        self.predictions = self.linker.inference.predict(
            threshold_match_probability=threshold_match_probability,
            threshold_match_weight=threshold_match_weight,
        )
        if expand:
            expand_citations = (
                citations_for_expand if citations_for_expand is not None else citations_batch
            )
            self.expanded_predictions = self.expand_predictions(
                self.predictions,
                citations_linker=citations_linker,
                citations=expand_citations,
                voices=voices_for_expand,
            )
            return self.expanded_predictions
        return self.predictions

    def predict_in_batches(
        self,
        linker_or_model_path: Union[str, Linker],
        max_pairs_per_batch: int = 1_750_000,
        expand: bool = True,
        output_dir: Optional[str] = None,
        output_storage: BatchOutputStorage = "auto",
        max_batches: Optional[int] = None,
        threshold_match_probability: Optional[float] = None,
        threshold_match_weight: Optional[float] = None,
    ) -> DataFrame:
        if self.citations is None:
            raise ValueError("citations is required")
        if self.voices is None:
            raise ValueError("voices is required")

        citations_to_process = self.citations
        voices_to_process = self.voices
        start_batch_index = 0
        storage_kind = (
            self._batch_storage_kind(output_dir, output_storage) if output_dir else None
        )

        if output_dir:
            processed_pairs = self._load_processed_pairs(output_dir, output_storage)
            citations_to_process, voices_to_process = self._pending_citations_and_voices(
                processed_pairs
            )
            if citations_to_process.limit(1).count() == 0:
                print("All citation×voice pairs already processed; skipping prediction.")
                if processed_pairs is not None:
                    combined = self._read_all_batches(output_dir, output_storage)
                    if expand:
                        self.expanded_predictions = combined
                    else:
                        self.predictions = combined
                    return combined
                raise ValueError("No citation batches to process")
            start_batch_index = self._next_batch_index(output_dir, output_storage)

        voice_count = self._linker_voice_count(voices_to_process)
        citation_batch_size = self._citation_batch_size_from_max_pairs(
            max_pairs_per_batch, voice_count
        )
        pairs_per_batch = citation_batch_size * voice_count
        print(
            f"Citation batch size: {citation_batch_size} "
            f"({voice_count} unique linker voices, up to {pairs_per_batch} pairs per batch)"
        )

        voices_linker = self._warm_preprocess_cache(citations_to_process, voices_to_process)
        batch_results: List[DataFrame] = []
        wrote_batches = False

        for batch_index, batch_df in enumerate(
            self._iter_citation_batches(citation_batch_size, citations_to_process)
        ):
            if max_batches is not None and batch_index >= max_batches:
                break

            print(f"Processing citation batch {batch_index + 1} (size up to {citation_batch_size})...")
            batch_result = self.predict(
                linker_or_model_path,
                threshold_match_probability=threshold_match_probability,
                threshold_match_weight=threshold_match_weight,
                expand=expand,
                citations_batch=batch_df,
                citations_for_expand=batch_df,
                voices_linker=voices_linker,
                voices_for_expand=voices_to_process,
                voices_for_linking=voices_to_process,
            )

            if output_dir:
                batch_location = self._write_batch(
                    batch_result,
                    output_dir,
                    storage_kind,
                    start_batch_index + batch_index,
                )
                wrote_batches = True
                print(f"  Wrote batch {start_batch_index + batch_index} to {batch_location}")
            else:
                batch_results.append(batch_result)

        if output_dir and wrote_batches:
            combined = self._read_all_batches(output_dir, output_storage)
        elif batch_results:
            combined = batch_results[0]
            for batch_result_df in batch_results[1:]:
                combined = combined.unionByName(batch_result_df, allowMissingColumns=True)
        else:
            raise ValueError("No citation batches to process")

        if expand:
            self.expanded_predictions = combined
        else:
            self.predictions = combined
        return combined

    def get_citations_for_training(self) -> DataFrame:
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required to filter citations for training")
        if self.citations is None:
            raise ValueError("citations is required to filter citations for training")
        if self.reviewed_data is None:
            raise ValueError("reviewed_data is required to filter citations for training")

        rev_field_name = f"rev_{self.entity_field_name}"
        citations_for_training = (
            self.citations.alias("a")
            .join(
                F.broadcast(
                    self.reviewed_data.filter(
                        F.col(rev_field_name).isNotNull() & ~F.col(rev_field_name).startswith("#")
                    )
                ).alias("b"),
                on="temp_key",
                how="inner",
            )
            .select("a.*")
        )
        return citations_for_training

    def generate_training(self) -> Linker:
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required to generate training algorithms")
        if self.citations is None:
            raise ValueError("citations is required to generate training")
        if self.voices is None:
            raise ValueError("voices is required to generate training")

        session = self._ensure_spark_session()
        self.calculate_algorithms(self.entity_field_name)

        citations_linker = self.get_citations_for_training()
        citations_linker = SimilarityAlgorithm.force_unique(citations_linker, self._field_l())
        voices_linker = self._warm_preprocess_cache(citations_linker, self.voices)
        cache = self._ensure_preprocess_cache()
        linker_cols = self.linker_columns_for_algorithms()
        citations_linker = cache.lookup_linker_rows(
            citations_linker,
            self._field_l(),
            self._field_l(),
            "citation",
            linker_cols,
        )

        df_pairs = self.get_pairs_for_training()
        df_labels = df_pairs.select(
            F.lit(self._field_l()).alias("source_dataset_l"),
            F.col("unique_id_l"),
            F.lit(self._field_r()).alias("source_dataset_r"),
            F.col("unique_id_r"),
            F.col("clerical_match_score"),
        )

        settings_creator = self.build_splink_settings_for_entity()

        linker = Linker(
            [citations_linker, voices_linker],
            settings_creator,
            db_api=SparkAPI(spark_session=session),
            input_table_aliases=self._input_table_aliases(),
        )

        max_pairs_u_sampling = 1e6
        linker.table_management.register_table(df_labels, "training_labels", overwrite=True)
        linker.training.estimate_m_from_pairwise_labels("training_labels")
        linker.training.estimate_u_using_random_sampling(max_pairs=max_pairs_u_sampling)
        linker.training.estimate_parameters_using_expectation_maximisation(
            f"l.source_dataset = '{self.entity_cfg.get('field_l', 'citation')}' "
            f"and r.source_dataset = '{self.entity_cfg.get('field_r', 'voice')}'"
        )

        self.linker = linker
        return linker

    def _model_comparison_columns(self, model_path: str) -> set[str]:
        with open(model_path, encoding="utf-8") as f:
            settings_dict = json.load(f)
        cols: set[str] = set()
        if self.entity_field_name:
            cols.add(self.entity_field_name)
            cols.add(f"{self.entity_field_name}_dm_s")
        for comp in settings_dict.get("comparisons", []):
            cols.add(comp["output_column_name"])
        return cols

    def _is_stub_array_column(self, column_name: str) -> bool:
        lowered = column_name.lower()
        return "vector" in lowered or "embedding" in lowered

    def _stub_linker_input_tables(self, model_path: str) -> Tuple[DataFrame, DataFrame]:
        session = self._ensure_spark_session()
        comparison_columns = sorted(self._model_comparison_columns(model_path))
        unique_id_col = SimilarityAlgorithm.UNIQUE_ID_COLUMN_NAME
        source_dataset_col = SimilarityAlgorithm.SOURCE_DATASET_COLUMN_NAME

        def build_stub_row(unique_id: str, source_dataset: str) -> Tuple[dict[str, Any], StructType]:
            row: dict[str, Any] = {
                unique_id_col: unique_id,
                source_dataset_col: source_dataset,
            }
            fields = [
                StructField(unique_id_col, StringType(), True),
                StructField(source_dataset_col, StringType(), True),
            ]
            for column_name in comparison_columns:
                if self._is_stub_array_column(column_name):
                    row[column_name] = []
                    fields.append(StructField(column_name, ArrayType(FloatType()), True))
                else:
                    row[column_name] = "stub"
                    fields.append(StructField(column_name, StringType(), True))
            return row, StructType(fields)

        citations_row, citations_schema = build_stub_row("stub_l", self._field_l())
        voices_row, voices_schema = build_stub_row("stub_r", self._field_r())
        citations_linker = session.createDataFrame([citations_row], citations_schema)
        voices_linker = session.createDataFrame([voices_row], voices_schema)
        return citations_linker, voices_linker

    def load_model(self, model_path: str) -> Linker:
        """Load a pre-trained Splink model from JSON into self.linker.

        Does not instantiate similarity algorithms or preprocess production data.
        Algorithms are prepared lazily when prediction runs via _ensure_algorithms_ready().
        """
        if not model_path:
            raise ValueError("model_path is required")
        if not os.path.isfile(model_path):
            raise ValueError(f"Model file not found: {model_path}")
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required to load a model")
        if self.entity_cfg is None:
            raise ValueError("entity_cfg is required to load a model")

        self.model_path = model_path
        session = self._ensure_spark_session()
        citations_linker, voices_linker = self._stub_linker_input_tables(model_path)
        self._unregister_input_tables()
        linker = Linker(
            [citations_linker, voices_linker],
            model_path,
            db_api=SparkAPI(spark_session=session),
            input_table_aliases=self._input_table_aliases(),
        )
        self.linker = linker
        return linker

    def get_pairs_for_training(self) -> DataFrame:
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required to build training pairs")
        # if self.citations is None:
        #     raise ValueError("citations is required to build training pairs")
        # if self.voices is None:
        #     raise ValueError("voices is required to build training pairs")
        if self.reviewed_data is None:
            raise ValueError("reviewed_data is required to build training pairs")

        rev_field_name = f"rev_{self.entity_field_name}"
        df_sample = self.reviewed_data.filter(
            F.col(rev_field_name).isNotNull() & ~F.col(rev_field_name).startswith("#")
        )

        df_citations_ids = self.citations.select(
            F.col("id").alias("unique_id_l"),
            "temp_key",
        )
        df_voices_ids = self.voices.select(
            F.col("id").alias("unique_id_r"),
            F.col("normalized_name").alias(rev_field_name),
        )

        return (
            df_sample.join(df_citations_ids, on="temp_key", how="inner")
            .join(df_voices_ids, on=rev_field_name, how="inner")
            .select(F.col("unique_id_l"), F.col("unique_id_r"))
            .withColumn("clerical_match_score", F.lit(1.0))
        )

    @staticmethod
    def _comparison_levels_for_column(output_column_name: str, algo_levels: List[dict]) -> List[dict]:
        col = output_column_name
        levels = [
            {
                "sql_condition": f"{col}_l IS NULL OR {col}_r IS NULL",
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": f"{col}_l = {col}_r",
                "label_for_charts": "Exact match",
            },
        ]
        levels.extend(algo_levels)
        levels.append({"sql_condition": "ELSE", "label_for_charts": "All other comparisons"})
        return levels

    @staticmethod
    def _get_comparison_specs_for_algorithm(algo: SimilarityAlgorithm) -> List[Tuple[str, List[dict]]]:
        if isinstance(algo, (SoundexAlgorithm, RootLevenshteinAlgorithm)):
            return [(algo.output_derived_col, algo.get_splink_configuration())]
        if isinstance(algo, PhoneticDmAlgorithm):
            levels = algo.get_splink_configuration()
            return [
                (algo.output_derived_col_p, [levels[0]]),
                (algo.output_name, levels[1:]),
            ]
        if isinstance(algo, AbstractEmbeddingModelAlgorithm):
            return [(algo.output_vector_col, algo.get_splink_configuration())]
        return [(algo.output_name, algo.get_splink_configuration())]

    def build_splink_settings_for_entity(self) -> SettingsCreator:
        if not self.algorithms:
            raise ValueError("algorithms is required; call calculate_algorithms() first")

        column_to_levels: Dict[str, List[dict]] = defaultdict(list)
        for algo in self.algorithms:
            for col, levels in self._get_comparison_specs_for_algorithm(algo):
                column_to_levels[col].extend(levels)

        comparisons = []
        for col, levels in column_to_levels.items():
            comparisons.append(
                {
                    "output_column_name": col,
                    "comparison_levels": self._comparison_levels_for_column(col, levels),
                }
            )

        return SettingsCreator(
            link_type="link_only",
            unique_id_column_name=SimilarityAlgorithm.UNIQUE_ID_COLUMN_NAME,
            source_dataset_column_name=SimilarityAlgorithm.SOURCE_DATASET_COLUMN_NAME,
            comparisons=comparisons,
            blocking_rules_to_generate_predictions=["1=1"],
            retain_matching_columns=True,
        )

    def linker_columns_for_algorithms(self) -> list[str]:
        if not self.entity_field_name:
            raise ValueError("entity_field_name is required to resolve linker columns")
        if not self.algorithms:
            raise ValueError("algorithms is required; call calculate_algorithms() first")

        cols = [SimilarityAlgorithm.UNIQUE_ID_COLUMN_NAME, SimilarityAlgorithm.SOURCE_DATASET_COLUMN_NAME, self.entity_field_name]
        seen = set(cols)
        for algo in self.algorithms:
            for col, _ in self._get_comparison_specs_for_algorithm(algo):
                if col not in seen:
                    cols.append(col)
                    seen.add(col)
            # dm_s is used in phonetic_dm cross-match SQL but is not a comparison output column
            dm_s_col = getattr(algo, "output_derived_col_s", None)
            if dm_s_col and dm_s_col not in seen:
                cols.append(dm_s_col)
                seen.add(dm_s_col)
        return cols
