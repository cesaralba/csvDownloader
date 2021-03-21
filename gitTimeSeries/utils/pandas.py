import pandas as pd
import numpy as np

def maxDTypeValue(tipo):
    try:
        VALNAN=np.iinfo(tipo).max
    except ValueError as exc:
        VALNAN=np.finfo(tipo).max

    return VALNAN

def DF2maxValues(df:pd.DataFrame,colList=None):

    cols2wrk = df.columns if colList is None else colList

    result = df[cols2wrk].dtypes.apply( lambda t: maxDTypeValue(t.type))

    return result






