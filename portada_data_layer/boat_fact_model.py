import datetime
import json
import logging
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StructField

logger = logging.getLogger("portada_data.boat_fact_data_model")

class BoatFactDataModel(dict):
    CALCULATED_VALUE_FIELD = "calculated_value"
    DEFAULT_VALUE_FIELD = "default_value"
    ORIGINAL_VALUE_FIELD = "original_value"
    STACKED_VALUE_FIELD = "stacked_value"
    DETAILED_VALUE_FIELD = "detailed_value"
    EXTRACTION_SOURCE_METHOD_FIELD = "extraction_source_method"
    # MORE_EXTRACTED_VALUES_FIELD = "more_extracted_values"
    PUBLICATION_DATE_FIELD = "publication_date"
    PUBLICATION_NAME_FIELD = "publication_name"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        if self.__is_json_structured_field(key):
            if BoatFactDataModel.STACKED_VALUE_FIELD in super().__getitem__(key):
                super().__getitem__(key).__getitem__(BoatFactDataModel.STACKED_VALUE_FIELD).append(value)
            else:
                super().__setitem__(key, {BoatFactDataModel.STACKED_VALUE_FIELD: [value]})
        else:
            super().__setitem__(key, value)

    def __getitem__(self, key):
        if self.__is_json_structured_field(key):
            if BoatFactDataModel.STACKED_VALUE_FIELD in super().__getitem__(key):
                return super().__getitem__(key).__getitem__(BoatFactDataModel.STACKED_VALUE_FIELD)[-1]
            elif BoatFactDataModel.CALCULATED_VALUE_FIELD in super().__getitem__(key):
                return  super().__getitem__(key).__getitem__(BoatFactDataModel.CALCULATED_VALUE_FIELD)
            elif BoatFactDataModel.ORIGINAL_VALUE_FIELD in super().__getitem__(key):
                return  super().__getitem__(key).__getitem__(BoatFactDataModel.ORIGINAL_VALUE_FIELD)
            elif BoatFactDataModel.DEFAULT_VALUE_FIELD in super().__getitem__(key):
                return super().__getitem__(key).__getitem__(BoatFactDataModel.DEFAULT_VALUE_FIELD)
            else:
                return ""
        return super().__getitem__(key)

    def get(self, key, default=None):
        if key in self:
            return self.__getitem__(key)
        else:
            return default

    def reformat(self, id):
        ret = {}
        for key in self:
            if self.__is_json_structured_field(key):
                ret[key] = json.dumps(self[key]) if (isinstance(self[key], dict) or isinstance(self[key], list))  else self[key]
                ret[f"{key}_{self.EXTRACTION_SOURCE_METHOD_FIELD}"] = self.get_obtaining_method_for(super().__getitem__(key))
                if self.DEFAULT_VALUE_FIELD in super().__getitem__(key):
                    dv = super().__getitem__(key).__getitem__(self.DEFAULT_VALUE_FIELD)
                    dv = json.dumps(dv) if (isinstance(dv, dict) or isinstance(dv, list)) else dv
                else:
                    dv = None
                if self.ORIGINAL_VALUE_FIELD in super().__getitem__(key):
                    ov = super().__getitem__(key).__getitem__(self.ORIGINAL_VALUE_FIELD)
                    ov = json.dumps(ov) if (isinstance(ov, dict) or isinstance(ov, list)) else ov
                else:
                    ov = None
                if self.CALCULATED_VALUE_FIELD in super().__getitem__(key):
                    cv = super().__getitem__(key).__getitem__(self.CALCULATED_VALUE_FIELD)
                    cv = json.dumps(cv) if (isinstance(cv, dict) or isinstance(cv, list)) else cv
                else:
                    cv = None
                ret[f"{key}_{self.DETAILED_VALUE_FIELD}"]={
                    self.DEFAULT_VALUE_FIELD: dv,
                    self.ORIGINAL_VALUE_FIELD: ov,
                    self.CALCULATED_VALUE_FIELD:cv,
                }
                # ret[f"{key}_{self.MORE_EXTRACTED_VALUES_FIELD}"] = []
                # for f in [BoatFactDataModel.DEFAULT_VALUE_FIELD, BoatFactDataModel.ORIGINAL_VALUE_FIELD, BoatFactDataModel.CALCULATED_VALUE_FIELD]:
                #     if f == ret[f"{key}_{self.EXTRACTION_SOURCE_METHOD_FIELD}"]:
                #         break
                #     if f in super().__getitem__(key):
                #         ret[f"{key}_{self.MORE_EXTRACTED_VALUES_FIELD}"].append(
                #             {
                #                 "value":super().__getitem__(key).__getitem__(f),
                #                 BoatFactDataModel.EXTRACTION_SOURCE_METHOD_FIELD:f
                #             }
                #         )
            else:
                ret[key] = json.dumps(self[key]) if (isinstance(self[key], dict) or isinstance(self[key], list))  else self[key]
                ret[f"{key}_{self.EXTRACTION_SOURCE_METHOD_FIELD}"] = BoatFactDataModel.ORIGINAL_VALUE_FIELD
                ret[f"{key}_{self.DETAILED_VALUE_FIELD}"] = {
                    self.DEFAULT_VALUE_FIELD: None,
                    self.ORIGINAL_VALUE_FIELD: ret[key],
                    self.CALCULATED_VALUE_FIELD: None,
                }
                # ret[f"{key}_{self.MORE_EXTRACTED_VALUES_FIELD}"] =[]
        if BoatFactDataModel.PUBLICATION_DATE_FIELD in ret and (
                isinstance(ret[BoatFactDataModel.PUBLICATION_DATE_FIELD], int)
                or isinstance(ret[BoatFactDataModel.PUBLICATION_DATE_FIELD], str) and re.match("^[-\d]\d*?\.?\d*?$", ret[BoatFactDataModel.PUBLICATION_DATE_FIELD])):
            # ret[f"{BoatFactDataModel.PUBLICATION_DATE_FIELD}_{self.MORE_EXTRACTED_VALUES_FIELD}"].append(
            #     {
            #         "value":ret[BoatFactDataModel.PUBLICATION_DATE_FIELD],
            #         BoatFactDataModel.EXTRACTION_SOURCE_METHOD_FIELD: ret[f"{BoatFactDataModel.PUBLICATION_DATE_FIELD}_{self.EXTRACTION_SOURCE_METHOD_FIELD}"]
            #     }
            # )
            ret[BoatFactDataModel.PUBLICATION_DATE_FIELD] = datetime.datetime.fromtimestamp(int(ret[BoatFactDataModel.PUBLICATION_DATE_FIELD])/1000.0).strftime('%Y-%m-%d')
            ret[f"{BoatFactDataModel.PUBLICATION_DATE_FIELD}_{self.EXTRACTION_SOURCE_METHOD_FIELD}"] = "boat_fact_model"
        ret["entry_id"] = f"{ret[BoatFactDataModel.PUBLICATION_NAME_FIELD]}_{id}"
        ret[f"entry_id_{self.EXTRACTION_SOURCE_METHOD_FIELD}"] = "boat_fact_model"
        ret[f"entry_id_{self.DETAILED_VALUE_FIELD}"] = {
            self.DEFAULT_VALUE_FIELD: None,
            self.ORIGINAL_VALUE_FIELD: None,
            self.CALCULATED_VALUE_FIELD: None,
        }
        return ret

    def __is_json_structured_field(self, key):
        return key in self and BoatFactDataModel.is_structured_value(super().__getitem__(key))

    @staticmethod
    def is_structured_value(value):
        return (isinstance(value, dict) and
                (BoatFactDataModel.DEFAULT_VALUE_FIELD in value or
                 BoatFactDataModel.ORIGINAL_VALUE_FIELD in value or
                 BoatFactDataModel.CALCULATED_VALUE_FIELD in value))

    @staticmethod
    def get_single_value_of(value):
        if value is None:
            ret = ""
        elif BoatFactDataModel.is_structured_value(value):
            if BoatFactDataModel.CALCULATED_VALUE_FIELD in value:
                ret = value[BoatFactDataModel.CALCULATED_VALUE_FIELD]
            elif BoatFactDataModel.ORIGINAL_VALUE_FIELD in value:
                ret = value[BoatFactDataModel.ORIGINAL_VALUE_FIELD]
            elif BoatFactDataModel.DEFAULT_VALUE_FIELD in value:
                ret = value[BoatFactDataModel.DEFAULT_VALUE_FIELD]
            else:
                ret = ""
        else:
            ret = value
        return str(ret)

    @staticmethod
    def get_obtaining_method_for(value):
        if value is None:
            ret = None
        elif BoatFactDataModel.is_structured_value(value):
            if BoatFactDataModel.CALCULATED_VALUE_FIELD in value:
                ret = BoatFactDataModel.CALCULATED_VALUE_FIELD
            elif BoatFactDataModel.ORIGINAL_VALUE_FIELD in value:
                ret = BoatFactDataModel.ORIGINAL_VALUE_FIELD
            elif BoatFactDataModel.DEFAULT_VALUE_FIELD in value:
                ret = BoatFactDataModel.DEFAULT_VALUE_FIELD
            else:
                ret = "unknown_obtaining_method"
        else:
            ret = BoatFactDataModel.ORIGINAL_VALUE_FIELD
        return str(ret)


    # @staticmethod
    # def get_struct_value_of(value):
    #     if value is None:
    #         ret = {BoatFactDataModel.DEFAULT_VALUE_FIELD:None, BoatFactDataModel.ORIGINAL_VALUE_FIELD:None, BoatFactDataModel.CALCULATED_VALUE_FIELD:None}
    #     elif BoatFactDataModel.is_structured_value(value):
    #         ret = {
    #             BoatFactDataModel.DEFAULT_VALUE_FIELD: value[BoatFactDataModel.DEFAULT_VALUE_FIELD] if BoatFactDataModel.DEFAULT_VALUE_FIELD in value else None,
    #             BoatFactDataModel.ORIGINAL_VALUE_FIELD: value[BoatFactDataModel.ORIGINAL_VALUE_FIELD] if BoatFactDataModel.ORIGINAL_VALUE_FIELD in value else None,
    #             BoatFactDataModel.CALCULATED_VALUE_FIELD: value[BoatFactDataModel.CALCULATED_VALUE_FIELD] if BoatFactDataModel.CALCULATED_VALUE_FIELD in value else None,
    #         }
    #     else:
    #         ret = {BoatFactDataModel.DEFAULT_VALUE_FIELD:None, BoatFactDataModel.ORIGINAL_VALUE_FIELD:value, BoatFactDataModel.CALCULATED_VALUE_FIELD:None}
    #     return ret
    #




    # @staticmethod
    # def process_value_from_column(column, df_schema):
    #     def process(c):
    #         dtype = df_schema[column].dtype
    #         if c.isNull():
    #             ret = F.Lit("")
    #         elif isinstance(dtype, StructField):
    #             fnames = dtype.fieldNames()
    #             if BoatFactDataModel.STACKED_VALUE_FIELD in fnames:
    #                 ret = c[BoatFactDataModel.STACKED_VALUE_FIELD].getItem(F.size(c[BoatFactDataModel.STACKED_VALUE_FIELD])-1)
    #             elif BoatFactDataModel.CALCULATED_VALUE_FIELD in fnames:
    #                 ret = c[BoatFactDataModel.CALCULATED_VALUE_FIELD]
    #             elif  BoatFactDataModel.ORIGINAL_VALUE_FIELD in fnames:
    #                 ret = c[BoatFactDataModel.ORIGINAL_VALUE_FIELD]
    #             elif BoatFactDataModel.DEFAULT_VALUE_FIELD in fnames:
    #                 ret = c[BoatFactDataModel.DEFAULT_VALUE_FIELD]
    #             else:
    #                 ret = c
    #         else:
    #             ret = c
    #         return ret
    #     ret = F.transform(
    #         F.col(column),
    #         process
    #     )
    #     return ret

