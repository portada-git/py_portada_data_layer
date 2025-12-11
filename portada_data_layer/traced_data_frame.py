from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from collections import namedtuple


TRANSFORMING_METHODS = {
    "select",
    "selectExpr",
    "drop",
    "withColumn",
    "withColumns",
    "withColumnRenamed",
    "withColumnsRenamed",
    "filter",
    "where",
    "orderBy",
    "sort",
    "join",
    "dropDuplicates",
    "distinct",
    "limit",
    "repartition",
    "coalesce",
    "sample",
    "union",
    "unionByName",
    "dropna",
    "fillna",
    "replace",
    "withWatermark",
    "hint",
    "na",
}

TracedDataFrameNames = namedtuple('TracedDataFrameNames', ['name', 'large_name'])

class TracedDataFrame:
    """
    This class is used as a proxy of a dataframe of spark.sql.Dataframe, and it encapsulates a dataframe. For practical
    purposes it acts like a dataframe, but it contains the source_name_path and the source version where is data is stored
    """
    # def __init__(self, df: DataFrame, source_name: str, source_version: int = -1, name=None, large_name=None,transformations=None):
    def __init__(self, df: DataFrame, source_name: str, source_version: int=-1, name=None, **kwargs):
        self._df = df
        self._name = name
        self._large_name = kwargs["large_name"] if "large_name" in kwargs else None
        self.source_name = source_name
        self.source_version = source_version
        self.transformations = list(kwargs["transformations"]) if "transformations" in kwargs else []

    # --- Forwarding per accedir directament a les funcions del DataFrame ---
    def __getattr__(self, name):
        # 1. Si l’atribut és de la pròpia classe, no interceptis.
        if hasattr(TracedDataFrame, name):
            return object.__getattribute__(self, name)

        # Permet cridar data_layer.select(), data_layer.filter(), etc.
        attr = getattr(self._df, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                # Si el resultat és un DataFrame, retorna també un TracedDataFrame
                if isinstance(result, DataFrame):
                    result = self.update_result(self._df, result, name, args, kwargs)
                    return result
                return result
            return wrapper
        return attr

    @property
    def name(self):
        if self._name:
            n = self._name
        else:
            v = "" if self.source_version==-1 else f"_{self.source_version}"
            n = f"{self.source_name}{v}"
        if not (n.startswith("DF") or n.startswith("NEW_FROM")):
            n = f"DF({n})"
        if len(self.transformations) > 0:
            n = f"TR({n})"
        return n

    @property
    def large_name(self):
    #     if self._large_name:
    #         n = self._large_name
    #     elif self._name:
    #         n = self._name
    #     else:
    #         v = "" if self.source_version == -1 else f"_{self.source_version}"
    #         n = f"{self.source_name}{v}"
    #     tn = ""
    #     for t in self.transformations:
    #         tn = f"{tn}.{t["operation"]}({t["arguments"]})"
    #     if not n.startswith("DF"):
    #         n = f"DF({n})"
    #     return f"{n}{tn}"
        return self.get_partial_large_name()

    def get_partial_large_name(self, depth:int = None):
        if self._large_name:
            n = self._large_name
        elif self._name:
            n = self._name
        else:
            v = "" if self.source_version == -1 else f"_{self.source_version}"
            n = f"{self.source_name}{v}"
        tn = ""
        for i, t in enumerate(self.transformations):
            if depth and depth < i:
                break
            tn = f"{tn}.{t["operation"]}({t["arguments"]})"
        if not n.startswith("DF"):
            n = f"DF({n})"
        return f"{n}{tn}"


    def toSparkDataFrame(self):
        return self._df

    def update_result(self, old_df, new_df, op_name, args, kwargs):
        new_df = TracedDataFrame(new_df, self.source_name, self.source_version, name=self._name, large_name=self._large_name, transformations=self.transformations)

        old_cols = set(old_df.columns)
        new_cols = set(new_df.columns)

        added = new_cols - old_cols
        removed = old_cols - new_cols
        common = new_cols.intersection(old_cols)
        dataframes =  self._get_dataframe_list_from_arg(args, kwargs)
        io_columns = self._get_columns_list_from_arg(args, kwargs)

        transformation = {
            "operation": op_name,
            "source_dataframes": list(dataframes),
            "involved_columns": list(io_columns),
            "added_columns": list(added),
            "common_columns": list(common),
            "removed_columns": list(removed),
            "arguments": [self.summarize_arg(a) for a in args]
        }
        new_df.transformations.append(transformation)
        if len(dataframes)>1:
            new_df._large_name = new_df.large_name
            new_df._name = f"NEW_FROM([{",".join(x.name for x in dataframes)}])"
            new_df.transformations = []

        return new_df

    def _get_dataframe_list_from_arg(self, args, kwargs):
        ret = set()
        def process(arg):
            if isinstance(arg, TracedDataFrame):
                ret.add(TracedDataFrameNames(arg.name, arg.large_name))
            if isinstance(arg, DataFrame):
                ret.add(TracedDataFrameNames("UNKNOWN", "UNKNOWN"))
        ret.add(TracedDataFrameNames(self.name, self.large_name))
        for arg in args:
            process(arg)
        for arg in kwargs.values():
            process(arg)
        return ret

    @staticmethod
    def _get_columns_list_from_arg(args, kwargs):
        cols = set()
        def process(arg):
            from pyspark.sql import Column

            # 1. Column (expressió)
            if isinstance(arg, Column):
                try:
                    refs = arg._jc.references().toList()
                    cols.update([r.toString() for r in refs])
                except:
                    pass
                return

            # 2. string simple → nom de columna
            if isinstance(arg, str):
                if arg.isidentifier():
                    cols.add(arg)
                else:
                    tmp_col = expr(arg)
                    refs = tmp_col._jc.references().toList()
                    cols.update([r.toString() for r in refs])
                return

            # 3. llista/tuple → processar recursivament
            if isinstance(arg, (list, tuple)):
                for elem in arg:
                    process(elem)
                return

            # 4. diccionaris (fill/replace/etc.)
            if isinstance(arg, dict):
                for key in arg.keys():
                    if isinstance(key, str):
                        cols.add(key)
                return

            # 5. altres no aporten informació
            return

        # Processar args i kwargs
        for a in args:
            process(a)
        for a in kwargs.values():
            process(a)

        return cols

    @staticmethod
    def summarize_arg(arg):
        from pyspark.sql import Column

        # Column: expressió derivada d'altres columnes
        if isinstance(arg, Column):
            try:
                return arg._jc.toString()
            except:
                return str(arg)

        # LITERAL
        if hasattr(arg, "__class__") and "Literal" in arg.__class__.__name__:
            return f"lit({arg})"

        # Python literal
        if isinstance(arg, (int, float, str, bool)):
            return repr(arg)

        # TracedDataFrame → ignora o resumeix
        if isinstance(arg, TracedDataFrame):
            return arg.large_name

        if isinstance(arg, DataFrame):
            return "DF[UNKNOWN]"

        # catch-all
        return str(arg)
