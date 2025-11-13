from pyspark.sql import DataFrame

class TracedDataFrame:
    """
    This class is used as a proxy of a dataframe of spark.sql.Dataframe, and it encapsulates a dataframe. For practical
    purposes it acts like a dataframe, but it contains the source_name_path and the source version where is data is stored
    """
    def __init__(self, df: DataFrame, source_name: str, source_version: int=-1):
        self._df = df
        self.source_name = source_name
        self.source_version = source_version

    # --- Forwarding per accedir directament a les funcions del DataFrame ---
    def __getattr__(self, name):
        # Permet cridar data_layer.select(), data_layer.filter(), etc.
        attr = getattr(self._df, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                # Si el resultat és un DataFrame, retorna també un TracedDataFrame
                if isinstance(result, DataFrame):
                    return TracedDataFrame(result, self.source_name, self.source_version)
                return result
            return wrapper
        return attr

    def toSparkDataFrame(self):
        return self._df
