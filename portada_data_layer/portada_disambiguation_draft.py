from typing import Union, List, Tuple

from pyspark.sql import DataFrame, functions as F
from portada_data_layer import DeltaDataLayer, TracedDataFrame
from portada_data_layer.portada_delta_common import registry_to_portada_builder, BoatFactConstants
from portada_data_layer.portada_extraction_for_disambiguation import BoatFactCitationExtractor, BoatFactVoicesExtractor
from portada_data_layer.similarity_algorithms import SimilarityAlgorithm, instantiate_similarity_algorithms


@registry_to_portada_builder
class BoatFactDisambiguation(DeltaDataLayer, BoatFactCitationExtractor, BoatFactVoicesExtractor):
    def __init__(self, builder=None, cfg_json: dict = None):
        super().__init__(builder=builder, cfg_json=cfg_json)
        self.disambiguation_cfg = {}
        self.algorithms = {}
        self._current_process_level = 2
        self.started = False

    def use_disambiguation_cfg(self, disambiguation_cfg: dict):
        self.disambiguation_cfg = disambiguation_cfg
        return self

    def read_cleaned_entries(self, *container_path):
        df = self.read_delta(*container_path, process_level_dir=self._process_level_dirs_[self._current_process_level-1])
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

    # @staticmethod
    # def __get_citations_for_training(df: Union[DataFrame, TracedDataFrame],
    #                                        df_labeled_data: Union[DataFrame, TracedDataFrame], labeled_data_field: str ) -> Union[DataFrame, TracedDataFrame]:
    #     extend_name = "_rev"
    #     field_name = f"{labeled_data_field}{extend_name}"
    #     df = df.alias("a").join(F.broadcast(df_labeled_data.filter(
    #         F.col(field_name).isNotNull & ~F.col(field_name).startswith("#"))).alias("b"), on="temp_key", how="inner").select(
    #         "a.*",
    #         f"b.{field_name}"
    #     )
    #     return df

    def get_algorithms(self, output_name:str, entity_cfg: dict):
        algorithm_cfg = self.disambiguation_cfg.get("general_config_algorithms", {})
        algorithm_keys = entity_cfg.get("algorithms", {})
        algorithms = []
        for algorithm_key in algorithm_keys:
            class_name = algorithm_cfg[algorithm_key]["class"]
            thresholds = algorithm_cfg[algorithm_key]["thresholds"]
            params = algorithm_cfg[algorithm_key]["params"]
            alg = instantiate_similarity_algorithms(class_name, entity_cfg["field_l"], entity_cfg["field_r"],
                                                    output_name=output_name, thresholds=thresholds, params=params)
            algorithms.append(alg)
        return algorithms


    #
    #
    #
    # def train_for_ports(self, algorithms: list):
    #     for alg in algorithms:
    #         df_voices, df = alg.preprocess(df_voices, df)
    #         _, df_unique_citations = alg.force_unique_data(df_voices, df)
    #         algorithms.append(alg)
    #
    #
    #
    # def _get_preprocess_ports(self, df_entries: Union[DataFrame, TracedDataFrame], for_training: bool = False):
    #     df_labeled_data = None
    #     if for_training:
    #         labeled_data_path = self._resolve_path(self.disambiguation_cfg["labeled_data_path"])
    #         df_labeled_data = self.read_delta(labeled_data_path)
    #     df_voices, df = self.get_voices_and_citations_for_ports(df_entries, df_labeled_data)
    #
    #     for alg in algorithms:
    #         df_voices, df = alg.preprocess(df_voices, df)
    #         _, df_unique_citations = alg.force_unique_data(df_voices, df)
    #
    #     voice_path = ("tmp", "voices", "ports")
    #     if for_training:
    #         unique_citation_path = ("tmp", "for_training", "citations", "uports")
    #         citation_path = ("tmp", "for_training", "citations", "ports")
    #     else:
    #         unique_citation_path = ("tmp", "citations", "uports")
    #         citation_path = ("tmp", "citations", "ports")
    #
    #     self.write_delta(*voice_path, df=df_voices)
    #     self.write_delta(*citation_path, df=df)
    #     self.write_delta(*unique_citation_path, df=df)
    #     return voice_path, citation_path, unique_citation_path
    #
    #
    #
    # def get_voices_and_citations_for_ports(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                            df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Tuple[Union[DataFrame, TracedDataFrame], Union[DataFrame, TracedDataFrame]]:
    #         df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.PORT_ENTITY)
    #         if df_labeled_data is None:
    #             df = BoatFactCitationExtractor.extract_ports(df_entries, from_departure_port=True, from_arrival_port=True, from_port_of_calls=True)
    #         else:
    #             df = BoatFactCitationExtractor.extract_ports(df_entries, from_departure_port=True, from_arrival_port=False, from_port_of_calls=False)
    #             df = self.__get_citations_for_training(df, df_labeled_data,  labeled_data_field="travel_departure_port")
    #         return df_voices, df
    #
    #
    # def get_voices_and_citations_for_ship_types(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                        df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Tuple[Union[DataFrame, TracedDataFrame], Union[DataFrame, TracedDataFrame]]:
    #     df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.SHIP_TYPE_ENTITY)
    #     df = BoatFactCitationExtractor.extract_ship_types(df_entries)
    #     if df_labeled_data is not None:
    #         df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                          labeled_data_field="ship_type")
    #     return df_voices, df
    #
    # def get_voices_and_citations_for_ship_tons_units(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                             df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Tuple[Union[DataFrame, TracedDataFrame], Union[DataFrame, TracedDataFrame]]:
    #     df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.SHIP_TONS_ENTITY)
    #     df = BoatFactCitationExtractor.extract_ship_tons_units(df_entries)
    #     if df_labeled_data is not None:
    #         df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                          labeled_data_field="ship_tons_unit")
    #     return df_voices, df
    #
    # def get_voices_and_citations_for_ship_flags(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                                  df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Tuple[Union[DataFrame, TracedDataFrame], Union[DataFrame, TracedDataFrame]]:
    #     df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.FLAG_ENTITY)
    #     df = BoatFactCitationExtractor.extract_ship_flags(df_entries)
    #     if df_labeled_data is not None:
    #         df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                          labeled_data_field="ship_flag")
    #     return df_voices, df
    #
    # def get_voices_and_citations_for_master_roles(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                                  df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Tuple[Union[DataFrame, TracedDataFrame], Union[DataFrame, TracedDataFrame]]:
    #     df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.MASTER_ROLE_ENTITY)
    #     df = BoatFactCitationExtractor.extract_master_roles(df_entries)
    #     if df_labeled_data is not None:
    #         df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                          labeled_data_field="master_role")
    #     return df_voices, df
    #
    #



    # def get_voices_and_citations_for_cargo_comodities(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                               df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Tuple[Union[DataFrame, TracedDataFrame], Union[DataFrame, TracedDataFrame]]:
    #     df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.COMMODITY_ENTITY)
    #     df = BoatFactCitationExtractor.extract_cargo_comodities(df_entries)
    #     if df_labeled_data is not None:
    #         df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                          labeled_data_field="cargo_commodity")
    #     return df_voices, df

    # def get_voices_and_citations_for_cargo_units(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                               df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Tuple[Union[DataFrame, TracedDataFrame], Union[DataFrame, TracedDataFrame]]:
    #     df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.UNIT_ENTITY)
    #     df = BoatFactCitationExtractor.extract_cargo_units(df_entries)
    #     if df_labeled_data is not None:
    #         df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                          labeled_data_field="cargo_unit")
    #     return df_voices, df

    # def get_voices_and_citations_for_travel_duration(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                               df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Tuple[Union[DataFrame, TracedDataFrame], Union[DataFrame, TracedDataFrame]]:
    #     df_voices = self.get_cleaned_known_entity_voices(BoatFactConstants.TRAVEL_DURATION_ENTITY)
    #     df = BoatFactCitationExtractor.extract_travel_duration(df_entries)
    #     if df_labeled_data is not None:
    #         df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                          labeled_data_field="travel_duration")
    #     return df_voices, df

    # def get_citations_for_cargo_merchants(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                               df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Union[DataFrame, TracedDataFrame]:
    #     df = BoatFactCitationExtractor.extract_cargo_merchants(df_entries)
    #     if df_labeled_data is not None:
    #         df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                          labeled_data_field="cargo_merchant_name")
    #     return df
    #
    # def get_voices_and_citations_for_ship_agents(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                                df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Union[DataFrame, TracedDataFrame]:
    #      df = BoatFactCitationExtractor.extract_ship_agents(df_entries)
    #      if df_labeled_data is not None:
    #          df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                           labeled_data_field="ship_agent_name")
    #      return df

    # def get_voices_and_citations_for_ship_brokers(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                                df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Union[DataFrame, TracedDataFrame]:
    #      df = BoatFactCitationExtractor.extract_brokers(df_entries)
    #      if df_labeled_data is not None:
    #          df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                           labeled_data_field="broker_name")
    #      return df

    # def get_voices_and_citations_for_masters(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                                df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Union[DataFrame, TracedDataFrame]:
    #      df = BoatFactCitationExtractor.extract_masters(df_entries)
    #      if df_labeled_data is not None:
    #          df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                           labeled_data_field="master_name")
    #      return df

    # def get_voices_and_citations_for_ships(self, df_entries: Union[DataFrame, TracedDataFrame],
    #                                                 df_labeled_data: Union[DataFrame, TracedDataFrame] = None) -> Union[DataFrame, TracedDataFrame]:
    #       df = BoatFactCitationExtractor.extract_ships(df_entries)
    #       if df_labeled_data is not None:
    #           df = self.__get_citations_for_training(df, df_labeled_data,
    #                                                            labeled_data_field="ship_name")
    #       return df



    # @staticmethod
    # def build_master_splink_settings_for_known_entity_voice_citation_type(field:str, cfg: dict) -> dict:
    #     comparisons_list = []
    #
    #     if "entity_categories" not in cfg and field not in cfg["entity_categories"]:
    #         raise ValueError("Entity categories not provided")
    #
    #     algorithms: List[SimilarityAlgorithm] = []
    #     entity_cfg = cfg["entity_categories"][field]
    #     for algorithm_name in entity_cfg["algorithms"]:
    #         col_a = entity_cfg["field_l"]
    #         col_b = entity_cfg["field_r"]
    #         algorithms.append(BoatFactDisambiguation.get_similarity_algorithm(algorithm_name, col_a, col_b, output_name=field))
    #
    #     for algo in algorithms:
    #         col = algo.output_name
    #
    #         # 1. Definim la capçalera fixa per a qualsevol columna
    #         levels = [
    #             {
    #                 "sql_condition": f"{col}_l IS NULL OR {col}_r IS NULL",
    #                 "label_for_charts": "Null",
    #                 "is_null_level": True
    #             },
    #             {
    #                 "sql_condition": f"{col}_l = {col}_r",
    #                 "label_for_charts": "Match exacte"
    #             }
    #         ]
    #
    #         # 2. Injectem els nivells que fan especial aquest algoritme
    #         levels.extend(algo.get_specific_levels())
    #
    #         # 3. Tanquem amb el nivell "ELSE" fix
    #         levels.append({
    #             "sql_condition": "ELSE",
    #             "label_for_charts": "Diferent"
    #         })
    #
    #         # 4. Construïm el bloc de la columna
    #         comparison_block = {
    #             "output_column_name": col,
    #             "comparison_levels": levels
    #         }
    #         comparisons_list.append(comparison_block)
    #
    #     # Construeix l'objecte de configuració global final de Splink
    #     return {
    #         "link_type": "dedupe_only",
    #         "blocking_rules_to_generate_predictions": blocking_rules,
    #         "comparisons": comparisons_list,
    #         "retain_matching_columns": True
    #     }
