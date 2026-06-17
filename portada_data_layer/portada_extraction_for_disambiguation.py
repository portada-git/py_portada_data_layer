from typing import Union, Tuple

from pyspark.sql import DataFrame, functions as F

from portada_data_layer import TracedDataFrame


class BoatFactCitationExtractor:
    @staticmethod
    def extract_ports(df_entries, from_departure_port = True, from_port_of_calls = True, from_arrival_port = True) -> Union[DataFrame, TracedDataFrame]:
        """

        :rtype: Any | None
        """
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
    def extract_ship_types(df_entries) -> Union[DataFrame, TracedDataFrame]:
        df_ship_entry = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("ship_type").alias("field_origin"),
            F.col("ship_type").alias("citation")
        )
        return df_ship_entry

    @staticmethod
    def extract_ship_tons_units(df_entries) -> Union[DataFrame, TracedDataFrame]:
        df_ship_tons_unit = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("ship_tons_unit").alias("field_origin"),
            F.col("ship_tons_unit").alias("citation")
        )
        return df_ship_tons_unit

    @staticmethod
    def extract_ship_flags(df_entries) -> Union[DataFrame, TracedDataFrame]:
        df_ship_flag = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("ship_flag").alias("field_origin"),
            F.col("ship_flag").alias("citation")
        )
        return df_ship_flag

    @staticmethod
    def extract_master_roles(df_entries) -> Union[DataFrame, TracedDataFrame]:
        df_master_role = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("master_role").alias("field_origin"),
            F.col("master_role").alias("citation")
        )
        return df_master_role

    @staticmethod
    def extract_cargo_comodities_and_units(df_entries) -> Union[DataFrame, TracedDataFrame]:
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
            F.lit("cargo_list.cargo").alias("field_origin"),
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
    def extract_cargo_comodities(df_entries) -> Union[DataFrame, TracedDataFrame]:
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

        df_comodity = df_cargo.select(
            "entry_id",
            "temp_key",
            F.lit("cargo_list.cargo.cargo_commodity").alias("field_origin"),
            "merchant_idx",
            "cargo_idx",
            F.col("col.cargo_commodity").alias("citation"),
        )
        df_comodity = df_comodity.withColumn(
            "id",
            F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "merchant_idx", F.lit("-"), "cargo_idx")
        )

        return df_comodity

    @staticmethod
    def extract_cargo_units(df_entries) -> Union[DataFrame, TracedDataFrame]:
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

        df_units = df_cargo.select(
            "entry_id",
            "temp_key",
            F.lit("cargo_list.cargo.cargo_unit").alias("field_origin"),
            "merchant_idx",
            "cargo_idx",
            F.col("col.cargo_unit").alias("citation"),
        )
        df_units = df_units.withColumn(
            "id",
            F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "merchant_idx", F.lit("-"), "cargo_idx")
        )

        return df_units


    @staticmethod
    def extract_cargo_merchants(df_entries) -> Union[DataFrame, TracedDataFrame]:
        df_merchants = df_entries.select(
            "entry_id",  # Your original id field
            "temp_key",
            F.posexplode("cargo_list").alias("merchant_idx", "merchant_struct")
        )

        df_merchants = df_merchants.select(
            "entry_id",
            "temp_key",
            F.lit("cargo_list.cargo_merchant_name").alias("field_origin"),
            "merchant_idx",
            F.col("merchant_struct.cargo_merchant_name").alias("citation")
        )
        df_merchants = df_merchants.withColumn(
            "id",
            F.concat("entry_id", F.lit("-"), "field_origin", F.lit("-"), "merchant_idx")
        )

        return df_merchants


    @staticmethod
    def extract_ship_agents(df_entries) -> Union[DataFrame, TracedDataFrame]:
        df_ship_agent = df_entries.select(
            F.col("entry_id").alias("id"),
            "entry_id",
            "temp_key",
            F.lit("ship_agent_name").alias("field_origin"),
            F.col("ship_agent_name").alias("citation")
        )
        return df_ship_agent


    @staticmethod
    def extract_brokers(df_entries) -> Union[DataFrame, TracedDataFrame]:
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
    def extract_masters(df_entries) -> Union[DataFrame, TracedDataFrame]:
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
        return BoatFactCitationExtractor._extract_single_entry(df_entries)

    @staticmethod
    def extract_ships(df_entries) -> Union[DataFrame, TracedDataFrame]:
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
        return BoatFactCitationExtractor._extract_single_entry(df_entries)

    @staticmethod
    def _extract_single_entry(df_entries) -> Union[DataFrame, TracedDataFrame]:

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

class BoatFactVoicesExtractor:
    @staticmethod
    def get_known_entity_voices(df_entities):
        # Select the columns of interest
        df_voices = df_entities.select(
            "name",  # Your original id field
            "normalized_name",
            F.posexplode("voices").alias("voice_idx", "voice")
        )
        # Create a new column with the concatenation of the name and the voice idx
        df_voices = df_voices.select(
            F.concat(F.col("name"), F.lit("-"), F.col("voice")).alias("id"),
            F.col("name"),
            F.col("normalized_name"),
            F.col("voice_idx"),
            F.lit("voices").alias("field_origin"),
            F.col("voice")
        )
        return df_voices
