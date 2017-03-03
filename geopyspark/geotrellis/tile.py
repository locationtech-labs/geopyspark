import numpy as np

class TileArray(np.ndarray):
    def __new__(cls, input_array, no_data_value=None):
        obj = np.asarray(input_array).view(cls)

        if no_data_value:
            obj.no_data_value = no_data_value
        else:
            obj.no_data_value = None

        return obj

    def __array_finalize__(self, obj):
        if obj is None: return
        self.no_data_value = getattr(obj, 'no_data_value', None)

    def __array_wrap__(self, out_arr, context=None):
        return np.ndarray.__array_wrap__(self, out_arr, context)
