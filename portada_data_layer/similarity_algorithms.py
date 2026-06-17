import os
import re
import unicodedata
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional, Iterator, Union, List
import pandas as pd
import numpy as np
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf
from portada_data_layer import TracedDataFrame
import torch
import importlib
import inspect

__SIMILARITY_ALGORITHMS = {}

def registry_to_portada_similarity_algorithms(cls):
    __SIMILARITY_ALGORITHMS[cls.__name__] = cls
    return cls

def register_all_module_classes_to_portada_similarity_algorithms(module_name):
    module = importlib.import_module(module_name)
    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj):
            registry_to_portada_similarity_algorithms(obj)


def instantiate_similarity_algorithms(name: str, col_a: str, col_b: str, output_name: str, thresholds: list, params: dict = None):
    if name in __SIMILARITY_ALGORITHMS:
        sim = __SIMILARITY_ALGORITHMS[name]
        return sim(col_a=col_a, col_b=col_b, output_name=output_name, thresholds=thresholds, **(params or {}))
    raise ValueError(f"Unknown similarity algorithm {name}")


def _levenshtein_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr.append(min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]


def _levenshtein_ratio(a: Optional[str], b: Optional[str]) -> float:
    if a is None or b is None:
        return 0.0
    max_len = max(len(a), len(b), 1)
    return 1.0 - (_levenshtein_distance(a, b) / max_len)


def _char_ngrams(text: Optional[str], n: int) -> set[str]:
    if text is None or n <= 0:
        return set()
    padded = f"  {text}  "
    if len(padded) < n:
        return set()
    return {padded[i : i + n] for i in range(len(padded) - n + 1)}


def _ngram_jaccard(a: Optional[str], b: Optional[str], n: int) -> float:
    grams_a = _char_ngrams(a, n)
    grams_b = _char_ngrams(b, n)
    if not grams_a or not grams_b:
        return 0.0
    intersection = len(grams_a & grams_b)
    union = len(grams_a | grams_b)
    return intersection / union if union else 0.0


def _dmetaphone_codes(text: Optional[str]) -> tuple[str, str]:
    if text is None:
        return ("", "")
    import phonetics

    primary, secondary = phonetics.dmetaphone(text)
    return (primary or "", secondary or "")


def _dmetaphone_similarity(a: Optional[str], b: Optional[str]) -> float:
    if a is None or b is None:
        return 0.0
    codes_a = _dmetaphone_codes(a)
    codes_b = _dmetaphone_codes(b)
    all_a = {c for c in codes_a if c}
    all_b = {c for c in codes_b if c}
    if not all_a or not all_b:
        return 0.0
    if all_a & all_b:
        return 1.0
    best = 0.0
    for ca in all_a:
        for cb in all_b:
            best = max(best, _levenshtein_ratio(ca, cb))
    return best


def _enrich_with_distinct_expression(
    df: Union[DataFrame, TracedDataFrame], text_col: str, derived_col: str, expr: F.Column
) -> Union[DataFrame, TracedDataFrame]:
    unique_texts_df = df.select(F.col(text_col)).distinct()
    unique_with_derived = unique_texts_df.withColumn(derived_col, expr)
    return df.alias("a").join(unique_with_derived.alias("b"), on=text_col, how="left").select("a.*", f"b.{derived_col}")


def _normalize_spark_identifier(name: str) -> str:
    if name is None:
        return "_"
    name = name.strip()
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    if not name:
        name = "_"
    if re.match(r"^[0-9]", name):
        name = f"_{name}"
    return name


class SimilarityAlgorithm(ABC):
    def __init__(self, col_a: str, col_b: str, output_name: str, thresholds: list, **nargs):
        self.col_a = col_a
        self.col_b = col_b
        self.output_name = output_name
        self.thresholds = thresholds

    @abstractmethod
    def get_splink_configuration(self) -> List[Any]:
        """Splink configuration already using the unified name (output_name)."""
        pass

    @staticmethod
    def force_unique_citations(df_citations: DataFrame, col_name) -> Union[DataFrame, Optional[DataFrame]]:
        df_a_unique = df_citations.dropDuplicates([col_name])
        return df_a_unique


    def pre_process_data(self, df_a: DataFrame, df_b: Optional[DataFrame] = None) -> Tuple[DataFrame, Optional[DataFrame]]:
        if "comparing_data_type" in df_a.columns:
            return df_a, df_b
        if not "unique_id" in df_a.columns:
            df_a = df_a.withColumn("unique_id", F.col("id"))
        if self.col_a in df_a.columns:
            df_a = df_a.withColumn(f"{self.output_name}", F.col(self.col_a))
            df_a = df_a.drop(self.col_a)
        if df_b is not None:
            if not "unique_id" in df_b.columns:
                df_b = df_b.withColumn("unique_id", F.col("id"))
            if self.col_b in df_b.columns:
                df_b = df_b.withColumn(f"{self.output_name}", F.col(self.col_b))
                df_b = df_b.drop(self.col_b)
        return df_a, df_b


@registry_to_portada_similarity_algorithms
class LevenshteinAlgorithm(SimilarityAlgorithm):
    def get_splink_configuration(self) -> List[Any]:
        r =[{
                "sql_condition": (
                    f"1.0 - (levenshtein({self.output_name}_l, {self.output_name}_r) / "
                    f"cast(greatest(length({self.output_name}_l), length({self.output_name}_r), 1) as double)) >= {thr}"
                ),
                "label_for_charts": f"Levenshtein Similarity >= {thr}"
            } for thr in self.thresholds]
        return  r

@registry_to_portada_similarity_algorithms
class JaroWinklerAlgorithm(SimilarityAlgorithm):
    def get_splink_configuration(self) -> List[Any]:
        # Splink Spark jar registers "jaro_winkler", not "jaro_winkler_similarity".
        r = [{"sql_condition": f"jaro_winkler({self.output_name}_l, {self.output_name}_r) >= {thr}",
             "label_for_charts": f"Jaro_Winkler Similarity >= {thr}"} for thr in self.thresholds]
        return r


@registry_to_portada_similarity_algorithms
class SoundexAlgorithm(SimilarityAlgorithm):
    def __init__(self, col_a: str, col_b: str, output_name: str, thresholds: list, **nargs):
        super().__init__(col_a, col_b, output_name, thresholds, **nargs)
        self.output_derived_col = f"{output_name}_soundex"

    def pre_process_data(self, df_a: DataFrame, df_b: Optional[DataFrame] = None) -> Tuple[DataFrame, Optional[DataFrame]]:
        df_a, df_b = super().pre_process_data(df_a, df_b)
        df_a = _enrich_with_distinct_expression(
            df_a, self.output_name, self.output_derived_col, F.soundex(F.col(self.output_name))
        )
        if df_b is not None:
            df_b = _enrich_with_distinct_expression(
                df_b, self.output_name, self.output_derived_col, F.soundex(F.col(self.output_name))
            )
        return df_a, df_b

    def get_splink_configuration(self) -> List[Any]:
        col = self.output_derived_col
        return [
            {
                "sql_condition": (
                    f"1.0 - (levenshtein({col}_l, {col}_r) / "
                    f"cast(greatest(length({col}_l), length({col}_r), 1) as double)) >= {thr}"
                ),
                "label_for_charts": f"Soundex Similarity >= {thr}",
            }
            for thr in self.thresholds
        ]


@registry_to_portada_similarity_algorithms
class PhoneticDmAlgorithm(SimilarityAlgorithm):
    def __init__(self, col_a: str, col_b: str, output_name: str, thresholds: list, **nargs):
        super().__init__(col_a, col_b, output_name, thresholds, **nargs)
        self.output_derived_col_p = f"{output_name}_dm_p"
        self.output_derived_col_s = f"{output_name}_dm_s"
        self.dm_sim_udf_name = f"dm_sim_{_normalize_spark_identifier(output_name)}"

    def _add_dmetaphone_columns(
        self, df: Union[DataFrame, TracedDataFrame], text_col: str, col_p: str, col_s: str
    ) -> Union[DataFrame, TracedDataFrame]:
        @pandas_udf("string")
        def dmetaphone_primary_udf(texts: pd.Series) -> pd.Series:
            return pd.Series([_dmetaphone_codes(t)[0] for t in texts.tolist()])

        @pandas_udf("string")
        def dmetaphone_secondary_udf(texts: pd.Series) -> pd.Series:
            return pd.Series([_dmetaphone_codes(t)[1] for t in texts.tolist()])

        unique_texts_df = df.select(F.col(text_col)).distinct()
        unique_with_codes = (
            unique_texts_df.withColumn(col_p, dmetaphone_primary_udf(F.col(text_col)))
            .withColumn(col_s, dmetaphone_secondary_udf(F.col(text_col)))
        )
        return df.alias("a").join(unique_with_codes.alias("b"), on=text_col, how="left").select(
            "a.*", f"b.{col_p}", f"b.{col_s}"
        )

    def pre_process_data(self, df_a: DataFrame, df_b: Optional[DataFrame] = None) -> Tuple[DataFrame, Optional[DataFrame]]:
        df_a, df_b = super().pre_process_data(df_a, df_b)
        spark = df_a.sparkSession
        udf_name = self.dm_sim_udf_name

        @pandas_udf("double")
        def dm_sim_udf(s1: pd.Series, s2: pd.Series) -> pd.Series:
            return pd.Series([_dmetaphone_similarity(a, b) for a, b in zip(s1.tolist(), s2.tolist())])

        spark.udf.register(udf_name, dm_sim_udf)

        df_a = self._add_dmetaphone_columns(df_a, self.output_name, self.output_derived_col_p, self.output_derived_col_s)
        if df_b is not None:
            df_b = self._add_dmetaphone_columns(df_b, self.output_name, self.output_derived_col_p, self.output_derived_col_s)
        return df_a, df_b

    def get_splink_configuration(self) -> List[Any]:
        p_col = self.output_derived_col_p
        s_col = self.output_derived_col_s
        levels = [
            {
                "sql_condition": (
                    f"{p_col}_l = {p_col}_r OR {p_col}_l = {s_col}_r OR "
                    f"{s_col}_l = {p_col}_r OR {s_col}_l = {s_col}_r"
                ),
                "label_for_charts": "Double Metaphone exact/cross match",
            }
        ]
        levels.extend(
            {
                "sql_condition": f"{self.dm_sim_udf_name}({self.output_name}_l, {self.output_name}_r) >= {thr}",
                "label_for_charts": f"Double Metaphone Similarity >= {thr}",
            }
            for thr in self.thresholds
        )
        return levels


@registry_to_portada_similarity_algorithms
class NgramAlgorithm(SimilarityAlgorithm):
    def __init__(self, col_a: str, col_b: str, output_name: str, thresholds: list, n: int = 2, **nargs):
        super().__init__(col_a, col_b, output_name, thresholds, **nargs)
        self.n = n
        self.udf_name = f"ngram_jaccard_{n}_{_normalize_spark_identifier(output_name)}"

    def pre_process_data(self, df_a: DataFrame, df_b: Optional[DataFrame] = None) -> Tuple[DataFrame, Optional[DataFrame]]:
        df_a, df_b = super().pre_process_data(df_a, df_b)
        spark = df_a.sparkSession
        n = self.n
        udf_name = self.udf_name

        @pandas_udf("double")
        def ngram_jaccard_udf(s1: pd.Series, s2: pd.Series) -> pd.Series:
            return pd.Series([_ngram_jaccard(a, b, n) for a, b in zip(s1.tolist(), s2.tolist())])

        spark.udf.register(udf_name, ngram_jaccard_udf)
        return df_a, df_b

    def get_splink_configuration(self) -> List[Any]:
        return [
            {
                "sql_condition": f"{self.udf_name}({self.output_name}_l, {self.output_name}_r) >= {thr}",
                "label_for_charts": f"Ngram({self.n}) Jaccard >= {thr}",
            }
            for thr in self.thresholds
        ]


class AbstractEmbeddingModelAlgorithm(SimilarityAlgorithm):
    def __init__(self, col_a: str, col_b: str, output_name: str, thresholds: list, model_name: str, **nargs):
        super().__init__(col_a, col_b, output_name, thresholds, **nargs)
        self.model_name = model_name
        self.output_vector_col = None

    def pre_process_data(self, df_a: DataFrame, df_b: Optional[DataFrame] = None) -> Tuple[DataFrame, Optional[DataFrame]]:
        df_a, df_b = super().pre_process_data(df_a, df_b)
        spark = df_a.sparkSession

        # Register the UDF using the unified name that Splink SQL will request
        @pandas_udf("double")
        def cosine_similarity_udf(v1: pd.Series, v2: pd.Series) -> pd.Series:
            def cos_dist(x, y):
                if x is None or y is None: return 0.0
                ax, ay = np.array(x), np.array(y)
                norm_x, norm_y = np.linalg.norm(ax), np.linalg.norm(ay)
                return float(np.dot(ax, ay) / (norm_x * norm_y)) if norm_x and norm_y else 0.0

            return pd.Series([cos_dist(i, j) for i, j in zip(v1, v2)])

        spark.udf.register(f"cos_sim_{self.output_vector_col}", cosine_similarity_udf)

        # 2. CALCULATE THE REAL VECTORS FOR EACH DATAFRAME AND COLUMN
        print(f">> Generant embeddings reals amb el model: {self.model_name}")
        df_a_enriched = self._compute_embeddings_distributed(df_a, self.output_name, self.output_vector_col)

        df_b_enriched = None
        if df_b is not None:
            df_b_enriched = self._compute_embeddings_distributed(df_b, self.output_name, self.output_vector_col)

        return df_a_enriched, df_b_enriched

    @abstractmethod
    def _compute_embeddings_distributed(self, df: Union[DataFrame, TracedDataFrame], text_col: str, vector_col: str) -> Union[DataFrame, TracedDataFrame]:
        pass

    @staticmethod
    def normalize_spark_name(name: str) -> str:
        return _normalize_spark_identifier(name)

@registry_to_portada_similarity_algorithms
class SentenceModelAlgorithm(AbstractEmbeddingModelAlgorithm):
    def __init__(self, col_a: str, col_b: str, output_name: str, thresholds: list, model_name = "shibing624/text2vec-base-multilingual", **nargs):
        super().__init__(col_a=col_a, col_b=col_b, output_name=output_name, thresholds=thresholds,  model_name= model_name, **nargs)
        # Vector column names during preprocessing
        mn = AbstractEmbeddingModelAlgorithm.normalize_spark_name(model_name)
        self.output_vector_col = f"{output_name}_sm_{mn}"


    def _compute_embeddings_distributed(self, df: Union[DataFrame, TracedDataFrame], text_col: str, vector_col: str) -> Union[DataFrame, TracedDataFrame]:
        # We capture the model name in a local variable so that it is available in Spark closure
        model_name = self.model_name

        # We define a Pandas UDF based on Iterators for maximum performance in Spark
        @pandas_udf("array<double>")
        def get_embeddings_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
            # --- THIS IS EXECUTED WITHIN SPARK WORKERS ---
            # We load the HuggingFace/SentenceTransformers model HERE.
            # Since it is outside the 'for' loop, it will only be loaded ONCE per Spark partition.
            from text2vec import SentenceModel
            model = SentenceModel(model_name)

            for text_batch in iterator:
                # Replace nulls with empty strings to avoid tokenizer errors
                texts = text_batch.fillna("").tolist()

                # We generate the vectors in blocks (thanks to PyTorch/Numpy it is done in parallel)
                embeddings = model.encode(texts, show_progress_bar=False)

                # Convert the numpy array to a series of ordinary Python lists
                # this way Spark can map it correctly to the "array<double>" type
                yield pd.Series(embeddings.tolist())

        # We apply the UDF generating the actual vector column
        # We apply the unique JOIN logic
        unique_texts_df = df.select(F.col(text_col)).distinct()
        # noinspection PyTypeChecker
        unique_vectors_df = unique_texts_df.withColumn(vector_col, get_embeddings_udf(F.col(text_col)))

        return df.alias("a").join(unique_vectors_df.alias("b"), on=text_col, how="left").select("a.*",  f"b.{vector_col}")


    def get_splink_configuration(self) -> List[Any]:
        udf_name = f"cos_sim_{self.output_vector_col}"
        r = [{"sql_condition": f"{udf_name}({self.output_vector_col}_l, {self.output_vector_col}_r) >= {thr}", "label_for_charts": f"SentenceModel({self.model_name}): semantic match >= {thr}"} for thr in self.thresholds]
        return r


@registry_to_portada_similarity_algorithms
class FastTextAlgorithm(AbstractEmbeddingModelAlgorithm):
    def __init__(
        self,
        col_a: str,
        col_b: str,
        output_name: str,
        thresholds: list,
        model_path: str,
        **nargs,
    ):
        super().__init__(
            col_a=col_a,
            col_b=col_b,
            output_name=output_name,
            thresholds=thresholds,
            model_name=model_path,
            **nargs,
        )
        self.model_path = model_path
        mn = AbstractEmbeddingModelAlgorithm.normalize_spark_name(os.path.basename(model_path))
        self.output_vector_col = f"{output_name}_ft_{mn}"

    def _compute_embeddings_distributed(
        self, df: Union[DataFrame, TracedDataFrame], text_col: str, vector_col: str
    ) -> Union[DataFrame, TracedDataFrame]:
        model_path = self.model_path

        @pandas_udf("array<double>")
        def get_fasttext_embeddings_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
            import fasttext

            model = fasttext.load_model(model_path)
            for text_batch in iterator:
                texts = text_batch.fillna("").tolist()
                embeddings = [model.get_sentence_vector(text).tolist() for text in texts]
                yield pd.Series(embeddings)

        unique_texts_df = df.select(F.col(text_col)).distinct()
        unique_vectors_df = unique_texts_df.withColumn(
            vector_col, get_fasttext_embeddings_udf(F.col(text_col))
        )
        return df.alias("a").join(unique_vectors_df.alias("b"), on=text_col, how="left").select(
            "a.*", f"b.{vector_col}"
        )

    def get_splink_configuration(self) -> List[Any]:
        udf_name = f"cos_sim_{self.output_vector_col}"
        return [
            {
                "sql_condition": (
                    f"{udf_name}({self.output_vector_col}_l, {self.output_vector_col}_r) >= {thr}"
                ),
                "label_for_charts": f"FastText({self.model_path}): semantic match >= {thr}",
            }
            for thr in self.thresholds
        ]


@registry_to_portada_similarity_algorithms
class SentenceTransformerAlgorithm(AbstractEmbeddingModelAlgorithm):
    def __init__(self, col_a: str, col_b: str, output_name: str, thresholds: list, model_name="paraphrase-multilingual-mpnet-base-v2", **nargs):
        super().__init__(col_a=col_a, col_b=col_b, output_name=output_name, thresholds=thresholds,  model_name=model_name, **nargs)
        # Vector column names during preprocessing
        mn = AbstractEmbeddingModelAlgorithm.normalize_spark_name(model_name)
        self.output_vector_col = f"{output_name}_st_{mn}"


    def _compute_embeddings_distributed(self, df: DataFrame, text_col: str, vector_col: str) -> DataFrame:
        # We capture the model name in a local variable so that it is available in Spark closure
        model_name = self.model_name

        # We define a Pandas UDF based on Iterators for maximum performance in Spark
        @pandas_udf("array<double>")
        def get_embeddings_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
            # --- THIS IS EXECUTED WITHIN SPARK WORKERS ---
            # We load the HuggingFace/SentenceTransformers model HERE.
            # Since it is outside the 'for' loop, it will only be loaded ONCE per Spark partition.
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer(model_name)

            for text_batch in iterator:
                # Replace nulls with empty strings to avoid tokenizer errors
                texts = text_batch.fillna("").tolist()

                # We generate the vectors in blocks (thanks to PyTorch/Numpy it is done in parallel)
                embeddings = model.encode(texts, show_progress_bar=False)

                # Convert the numpy array to a series of ordinary Python lists
                # this way Spark can map it correctly to the "array<double>" type
                yield pd.Series(embeddings.tolist())

        # We apply the UDF generating the actual vector column
        # We apply the unique JOIN logic
        unique_texts_df = df.select(F.col(text_col)).distinct()
        # noinspection PyTypeChecker
        unique_vectors_df = unique_texts_df.withColumn(vector_col, get_embeddings_udf(F.col(text_col)))

        return df.alias("a").join(unique_vectors_df.alias("b"), on=text_col, how="left").select("a.*", f"b.{vector_col}")

    def get_splink_configuration(self) -> List[Any]:
        udf_name = f"cos_sim_{self.output_vector_col}"
        r = [{"sql_condition": f"{udf_name}({self.output_vector_col}_l, {self.output_vector_col}_r) >= {thr}",
             "label_for_charts": f"SentenceTransformer({self.model_name}): semantic match >= {thr}"} for thr in self.thresholds]
        return r

@registry_to_portada_similarity_algorithms
class ByT5Algorithm(AbstractEmbeddingModelAlgorithm):
    def __init__(self, col_a: str, col_b: str, output_name: str, thresholds: list, model_name: str ="google/byt5-small", **nargs):
        super().__init__(col_a, col_b, output_name, thresholds=thresholds, model_name=model_name, **nargs)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        # Vector column names during preprocessing
        mn = AbstractEmbeddingModelAlgorithm.normalize_spark_name(model_name)
        self.output_vector_col = f"{output_name}_byt5_vector_{mn}"

    def _compute_embeddings_distributed(self, df: DataFrame, text_col: str, vector_col: str) -> DataFrame:
        # Extract configuration to local variables to serialize only what is necessary
        model_name = self.model_name
        device = self.device

        @pandas_udf("array<double>")
        def get_byt5_embeddings_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
            # 1. We load the model once per partition
            from transformers import AutoTokenizer, T5EncoderModel
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            model = T5EncoderModel.from_pretrained(model_name).to(device)
            model.eval()

            # 2. Processing with the encapsulated encoding function
            for text_batch in iterator:
                texts = text_batch.fillna("").tolist()

                # Let's encapsulate the inference logic right here
                inputs = tokenizer(texts, return_tensors="pt", padding=True, truncation=True)
                inputs = {k: v.to(device) for k, v in inputs.items()}

                with torch.no_grad():
                    out = model(**inputs)

                mask = inputs["attention_mask"].unsqueeze(-1).expand(out.last_hidden_state.size()).float()
                vecs = (out.last_hidden_state.float() * mask).sum(1) / mask.sum(1).clamp(min=1e-9)

                #vecs = outputs.last_hidden_state.mean(dim=1).cpu().numpy()
                vecs = vecs.cpu().numpy()
                norms = np.linalg.norm(vecs, axis=1, keepdims=True)
                vecs = vecs / np.maximum(norms, 1e-10)

                yield pd.Series(vecs.tolist())

        # We apply the unique JOIN logic
        unique_texts_df = df.select(F.col(text_col)).distinct()
        # noinspection PyTypeChecker
        unique_vectors_df = unique_texts_df.withColumn(vector_col, get_byt5_embeddings_udf(F.col(text_col)))

        return df.alias("a").join(unique_vectors_df.alias("b"), on=text_col, how="left").select("a.*", f"b.{vector_col}")

    def get_splink_configuration(self) -> List[Any]:
        udf_name = f"cos_sim_{self.output_vector_col}"
        r = [{"sql_condition": f"{udf_name}({self.output_vector_col}_l, {self.output_vector_col}_r) >= {thr}",
                 "label_for_charts": f"ByT5({self.model_name}): Semantic Match >= {thr}"} for thr in self.thresholds]
        return r
