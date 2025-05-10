import numpy as np

import pandas as pd


def maxDTypeValue(tipo):
    try:
        VALNAN=np.iinfo(tipo).max
    except ValueError:
        VALNAN=np.finfo(tipo).max

    return VALNAN

def DF2maxValues(df:pd.DataFrame,colList=None):

    cols2wrk = df.columns if colList is None else colList

    result = df[cols2wrk].dtypes.apply( lambda t: maxDTypeValue(t.type))

    return result
