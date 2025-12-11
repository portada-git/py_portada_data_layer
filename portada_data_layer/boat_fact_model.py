import logging

logger = logging.getLogger("portada_data.boat_fact_data_model")

class BoatFactDataModel(dict):
    CALCULATED_VALUE_FIELD = "calculated_value"
    DEFAULT_VALUE_FIELD = "default_value"
    ORIGINAL_VALUE_FIELD = "original_value"
    STACKED_VALUE = "stacked_value"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        if self.__is_json_structured_filed(key):
            if BoatFactDataModel.STACKED_VALUE in super().__getitem__(key):
                super().__getitem__(key).__getitem__(BoatFactDataModel.STACKED_VALUE).append(value)
            else:
                super().__setitem__(key, {BoatFactDataModel.STACKED_VALUE: [value]})
        else:
            super().__setitem__(key, value)

    def __getitem__(self, key):
        if self.__is_json_structured_filed(key):
            if BoatFactDataModel.STACKED_VALUE in super().__getitem__(key):
                return super().__getitem__(key).__getitem__(BoatFactDataModel.STACKED_VALUE)[-1]
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


    def __is_json_structured_filed(self, key):
        return key in self and BoatFactDataModel.is_structured_value(super().__getitem__(key))

    @staticmethod
    def is_structured_value(value):
        return (isinstance(value, dict) and
                (BoatFactDataModel.DEFAULT_VALUE_FIELD in value or
                 BoatFactDataModel.ORIGINAL_VALUE_FIELD in value or
                 BoatFactDataModel.CALCULATED_VALUE_FIELD in value or
                 BoatFactDataModel.STACKED_VALUE in value))

    @staticmethod
    def get_value_of(value):
        if value is None:
            ret = ""
        elif BoatFactDataModel.is_structured_value(value):
            if BoatFactDataModel.STACKED_VALUE in value:
                ret = value[BoatFactDataModel.STACKED_VALUE][-1] if value[BoatFactDataModel.STACKED_VALUE] else None
            elif BoatFactDataModel.CALCULATED_VALUE_FIELD in value:
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
            if BoatFactDataModel.STACKED_VALUE in value:
                ret = f"{BoatFactDataModel.STACKED_VALUE}_{len(value[BoatFactDataModel.STACKED_VALUE])-1}"
            elif BoatFactDataModel.CALCULATED_VALUE_FIELD in value:
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
