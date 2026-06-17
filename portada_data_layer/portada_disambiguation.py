from collections import defaultdict
import os
from typing import Any, Dict, List, Tuple, Union

from pyspark.sql import DataFrame, functions as F
from splink import Linker, SettingsCreator, block_on
from splink.backends.spark import SparkAPI
from splink.blocking_analysis import count_comparisons_from_blocking_rule

from portada_data_layer.portada_patcher_data_layer import BoatFactPatcherDataLayer
from portada_data_layer import DeltaDataLayer, TracedDataFrame
from portada_data_layer.portada_delta_common import registry_to_portada_builder, BoatFactConstants
from portada_data_layer.portada_extraction_for_disambiguation import BoatFactCitationExtractor, BoatFactVoicesExtractor
from portada_data_layer.similarity_algorithms import (
    AbstractEmbeddingModelAlgorithm,
    PhoneticDmAlgorithm,
    SimilarityAlgorithm,
    SoundexAlgorithm,
    instantiate_similarity_algorithms,
)


@registry_to_portada_builder
class BoatFactDisambiguation(DeltaDataLayer, BoatFactCitationExtractor, BoatFactVoicesExtractor):
    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self.disambiguation_cfg = {}
        self.algorithms = {}
        self._current_process_level = 2
        self._cleaned_entries_container_path = "ship_entries"
        self._sample_for_training_container_path = "reviewed_entries"
        self.started = False
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

    @staticmethod
    def get_citations_for_training(df_citations: Union[DataFrame, TracedDataFrame],
                                   df_sample: Union[DataFrame, TracedDataFrame], field_name: str) -> Union[DataFrame, TracedDataFrame]:
        extend_name = "rev_"
        rev_field_name = f"{extend_name}{field_name}"
        df_citations = df_citations.alias("a").join(
            F.broadcast(df_sample.filter(F.col(rev_field_name).isNotNull() & ~F.col(rev_field_name).startswith("#"))).alias("b"),
            on="temp_key",
            how="inner"
        ).select("a.*")
        return df_citations

    def get_pairs_for_training(self, df_citations, df_voices, df_sample, field_name: str):
        extend_name = "rev_"
        rev_field_name = f"{extend_name}{field_name}"
        df_sample = df_sample.filter(F.col(rev_field_name).isNotNull() & ~F.col(rev_field_name).startswith("#"))

        df_citations_ids = df_citations.select(F.col("unique_id").alias("unique_id_l"), "temp_key")
        df_voices_ids = df_voices.select(F.col("unique_id").alias("unique_id_r"), F.col("normalized_name").alias(rev_field_name))

        df_pairs = df_sample.join(df_citations_ids, on="temp_key", how="inner")\
            .join(df_voices_ids, on=rev_field_name, how="inner")\
            .select(F.col("unique_id_l"), F.col("unique_id_r")).withColumn("clerical_match_score", F.lit(1.0))
        return df_pairs

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

    def get_algorithms(self, output_name: str, entity_cfg: dict) -> List[SimilarityAlgorithm]:
        algorithm_cfg = self.disambiguation_cfg.get("general_config_algorithms", {})
        algorithm_keys = entity_cfg.get("algorithms", [])
        algorithms = []
        for algorithm_key in algorithm_keys:
            alg_entry = algorithm_cfg[algorithm_key]
            class_name = alg_entry["class"]
            thresholds = alg_entry["thresholds"]
            params = alg_entry.get("params", {})
            alg = instantiate_similarity_algorithms(
                class_name,
                entity_cfg["field_l"],
                entity_cfg["field_r"],
                output_name=output_name,
                thresholds=thresholds,
                params=params,
            )
            algorithms.append(alg)
        return algorithms

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
        if isinstance(algo, SoundexAlgorithm):
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

    @staticmethod
    def build_splink_settings_for_entity(
        entity_key: str,
        algorithms: List[SimilarityAlgorithm],
        citations_alias: str = "citations",
        voices_alias: str = "voices",
    ) -> SettingsCreator:
        column_to_levels: Dict[str, List[dict]] = defaultdict(list)
        for algo in algorithms:
            for col, levels in BoatFactDisambiguation._get_comparison_specs_for_algorithm(algo):
                column_to_levels[col].extend(levels)

        comparisons = []
        for col, levels in column_to_levels.items():
            comparisons.append(
                {
                    "output_column_name": col,
                    "comparison_levels": BoatFactDisambiguation._comparison_levels_for_column(col, levels),
                }
            )

        blocking_rules = [
            "1=1"
        ]

        return SettingsCreator(
            link_type="link_only",
            unique_id_column_name="unique_id",
            source_dataset_column_name="source_dataset",
            comparisons=comparisons,
            blocking_rules_to_generate_predictions=blocking_rules,
            retain_matching_columns=True,
        )

    def generate_training_for_ports(
        self,
        #rev_field_name: str = "travel_departure_port",
        # input_table_aliases: Tuple[str, str] = ("citations", "voices"),
        max_pairs_u_sampling: float = 1e6,
    ) -> Dict[str, Any]:
        citations_alias = "citations"
        voices_alias = "voices"
        field_name = "travel_departure_port"
        spark = self.spark
        checkpoint_dir = os.path.join(os.getcwd(), "data", "spark_checkpoints")
        os.makedirs(checkpoint_dir, exist_ok=True)
        spark.sparkContext.setCheckpointDir(checkpoint_dir)

        df_entries = self.read_cleaned_ship_entries()
        df_citations = self.extract_ports(
            df_entries,
            from_departure_port=True,
            from_arrival_port=False,
            from_port_of_calls=False,
        )
        df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.PORT_ENTITY)
        df_sample = self.read_sample_for_training()
        df_citations = self.get_citations_for_training(df_citations, df_sample, field_name)

        entity_cfg = self.disambiguation_cfg["entity_categories"]["port"]
        algorithms = self.get_algorithms("port", entity_cfg)

        for alg in algorithms:
            df_citations, df_voices = alg.pre_process_data(df_citations, df_voices)


        df_citations = df_citations.withColumn("source_dataset", F.lit(citations_alias))
        df_voices = df_voices.withColumn("source_dataset", F.lit(voices_alias))

        df_pairs = self.get_pairs_for_training(df_citations, df_voices, df_sample, field_name)
        df_labels = df_pairs.select(
            F.lit(citations_alias).alias("source_dataset_l"),
            F.col("unique_id_l"),
            F.lit(voices_alias).alias("source_dataset_r"),
            F.col("unique_id_r"),
        )

        counts = count_comparisons_from_blocking_rule(
            table_or_tables=[df_citations, df_voices],
            blocking_rule="1=1",
            link_type="link_only",
            db_api=SparkAPI(spark),
            unique_id_column_name="unique_id",
            source_dataset_column_name="source_dataset",
        )

        settings_creator = self.build_splink_settings_for_entity(
            "port",
            algorithms,
            citations_alias=citations_alias,
            voices_alias=voices_alias,
        )

        linker = Linker(
            [df_citations, df_voices],
            settings_creator,
            db_api=SparkAPI(spark),
            input_table_aliases=list(citations_alias, voices_alias),
        )

        linker.table_management.register_table(df_labels, "training_labels", overwrite=True)
        linker.training.estimate_m_from_pairwise_labels("training_labels")
        linker.training.estimate_u_using_random_sampling(max_pairs=max_pairs_u_sampling)
        linker.training.estimate_parameters_using_expectation_maximisation(
            f"l.source_dataset = '{citations_alias}' and r.source_dataset = '{voices_alias}'"
        )

        return linker.misc.save_model_to_json(out_path=None)
