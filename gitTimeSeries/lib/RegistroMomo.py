import numpy as np
import pandas as pd
from git import Repo
from sklearn.preprocessing import StandardScaler

from .miscDataFrames import leeCSVdataset, indexFillNAs

DEFAULTCOMMIT = [0]

COLIDX = ['fecha_defuncion', 'ambito', 'nombre_ambito', 'nombre_sexo', 'nombre_gedad']
COLS2DROP = ['cod_ambito', 'cod_ine_ambito', 'cod_sexo', 'cod_gedad']
INDEXNAREPLACER = {'nombre_ambito': 'España'}

ESTADSCAMBIO = {'contCambObs': ['defunciones_observadas'],
                'contCambEstads': ['defunciones_observadas_lim_inf', 'defunciones_observadas_lim_sup',
                                   'defunciones_esperadas', 'defunciones_esperadas_q01', 'defunciones_esperadas_q99']}


def leeDatosMomoFila(fname, columna='defunciones_observadas'):
    """
    Lee un fichero diario de Momo
    ( https://momo.isciii.es/public/momo/data, https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html#datos )
    y lo convierte en un dataframe usando como columna todas las columnas de clasificación y una de las de
    datos.
    :param fname: Nombre de fichero o handle de lectura de fichero
    :param columna: columna que se va a usar como datos. Una de
      defunciones_observadas: el número de defunciones observadas (incluye la corrección por retraso).
      defunciones_observadas_lim_inf: el límite inferior del invervalo de confianza de las defunciones observadas (debido a la corrección).
      defunciones_observadas_lim_sup: de forma equivalente, el límite superior.
      defunciones_esperadas: el número de defunciones esperadas, resultantes del modelo.
      defunciones_esperadas_q01: el límite inferior del intervalo de confianza de las defunciones esperadas, correspondiente al percentil 1 de la distribución.
      defunciones_esperadas_q99: de forma equivalente, el límite superior, al percentil 99.
    :return: dataframe con las siguientes columnas
         df.columns = MultiIndex([('2018-05-10',     'ccaa', 'Andalucía', 'hombres', 'edad 65-74'),
            ...
            ('2020-06-05', 'nacional',    'España', 'mujeres', 'edad 65-74')],
           names=['fecha_defuncion', 'ambito', 'nombre_ambito', 'nombre_sexo', 'nombre_gedad'], length=181920)

    """
    COLLIST = COLIDX + [columna]

    myDF = pd.read_csv(fname, parse_dates=['fecha_defuncion'], infer_datetime_format=True, usecols=COLLIST)
    myDF.nombre_ambito = myDF.nombre_ambito.fillna("España")
    myDF = myDF.set_index(COLIDX)

    return myDF.T.reset_index(drop=True).astype('int64')


def leeDatosMomoDF(fname_or_handle, **kwargs):
    """
    """
    # COLLIST = COLIDX + [columna]

    myDF = leeCSVdataset(fname_or_handle, colIndex=COLIDX, cols2drop=COLS2DROP, parse_dates=['fecha_defuncion'],
                         infer_datetime_format=True)
    myDF.index = indexFillNAs(myDF.index, replacementValues=INDEXNAREPLACER)

    return myDF


def iterateOverGitRepo(REPOLOC, fname, readFunction=leeDatosMomoFila, **kwargs):
    """
    A partir de un repositorio GIT descargado en REPOLOC, atraviesa todos los commit buscando el archivo fname
    que lee con la función readFunction para devolver un dataFrameTemporal con la versión de cada commit en cada
    fila indexada por la fecha del commit

    :param REPOLOC: lugar donde está descargado el repositorio
    :param fname: nombre del fichero a leer. Debe ser relativo a la raíz del repositorio
    :param readFunction: función que lee el fichero fname y devuelve un dataframe de una sóla fila.
    :param readFunction: parámetros adicionales, con nombre, que se pasan a readFunction
    :return: Dataframe con índice temporal con cada lectura.
    """
    repo = Repo(REPOLOC)

    auxDict = dict()

    for commitID in repo.iter_commits():
        commit = repo.commit(commitID)
        commitDate = commitID.committed_datetime.date()

        commitTree = commit.tree

        for blob in commitTree.blobs:
            if blob.path == fname:
                fileHandle = blob.data_stream
                myDF = readFunction(fileHandle, **kwargs)
                if not myDF.empty:
                    auxDict[commitDate] = myDF

                break

    if not auxDict:
        return None

    result = pd.concat(auxDict, sort=True).droplevel(1).sort_index()
    result.index = pd.DatetimeIndex(result.index, freq='D', name='fechaCommit')

    return result


def filtraColumnasDF(df, colDict=None, conv2ts=False):
    """
    Devuelve las columnas de un dataframe que cumplen determinadas condiciones

    :param df: Dataframe
    :param colDict: diccionario con condiciones a cumplir. Las cumple todas y la condición es igualdad
        dada una columna cuyos índices son col={ kc:vc } para cada {k:v} se deben cumplir todas las condiciones
        col[k] == v. k puede ser la posición en la tupla o el nombre en el índice si están definidos.
    :param conv2ts: trata de convertir el DF resultante a una serie temporal (si se puede). El
       requisito es que el campo fecha de las columnas no se repita. Si se puede, se pierde el resto
       de los campos de las columnas (ámbito, ccaa, edad, sexo)

    :return: Dataframe con las columnas que cumplen las condiciones

        Ejemplo:
         df.columns = MultiIndex([('2018-05-10',     'ccaa', 'Andalucía', 'hombres', 'edad 65-74'),
            ...
            ('2020-06-05', 'nacional',    'España', 'mujeres', 'edad 65-74')],
           names=['fecha_defuncion', 'ambito', 'nombre_ambito', 'nombre_sexo', 'nombre_gedad'], length=181920)

        filtraColumnasDF(df, { 1:'nacional': 'nombre_sexo': 'mujeres' }) devuelve

        df[[('2020-06-05', 'nacional',    'España', 'mujeres', 'edad 65-74')]]

    """

    if not colDict:
        return df

    dfColumns = df.columns

    esMultiCol = all([isinstance(c, tuple) for c in dfColumns.to_list()])

    numClaves = max([len(c) for c in dfColumns.to_list()]) if esMultiCol else 1
    nomClaves = list(dfColumns.names)
    clave2i = dict(zip(nomClaves, range(numClaves)))

    checkConds = [k < numClaves if isinstance(k, (int)) else (k in nomClaves) for k in colDict.keys()]

    if not all(checkConds):
        failedConds = [cond for cond, check in zip(colDict.items(), checkConds) if not check]
        print(failedConds)
        condsMsg = ",".join(map(lambda x: '"' + str(x) + '"', failedConds))
        raise ValueError("filtraColumnasDF: condiciones incorrectas: %s" % condsMsg)

    funcCheckMulti = lambda x: all([x[k if isinstance(k, int) else clave2i[k]] == v for k, v in colDict.items()])
    funcCheckSingle = lambda x: (x == list(colDict.values())[0])

    colsOk = [c for c in dfColumns.to_list() if (funcCheckMulti if esMultiCol else funcCheckSingle)(c)]

    if not colsOk:
        raise KeyError(
            "filtraColumnasDF: ninguna columna cumple las condiciones (%s). Columnas: %s " % (
                str(colDict), dfColumns.to_list()))

    result = df[colsOk]
    if not conv2ts:  # Don't want conversion, nothing else to do
        return result

    fechasOk = [c[0] for c in colsOk]
    if len(set(fechasOk)) == len(colsOk):
        tsCols = pd.DatetimeIndex(fechasOk)
        result.columns = tsCols
        return result

    return result


def filtraFilasSerie(serie, indDict=None, conv2ts=False):
    """
    Devuelve las filas de un serie que cumplen determinadas condiciones

    :param serie: Serie
    :param indDict: diccionario con condiciones a cumplir. Las cumple todas y la condición es igualdad
        dada una columna cuyos índices son col={ kc:vc } para cada {k:v} se deben cumplir todas las condiciones
        col[k] == v. k puede ser la posición en la tupla o el nombre en el índice si están definidos.
    :param conv2ts: trata de convertir el DF resultante a una serie temporal (si se puede). El
       requisito es que el campo fecha de las columnas no se repita. Si se puede, se pierde el resto
       de los campos de las columnas (ámbito, ccaa, edad, sexo)

    :return: Serie con las fila que cumplen las condiciones

        Ejemplo:
         serie.index = MultiIndex([('2018-05-10',     'ccaa', 'Andalucía', 'hombres', 'edad 65-74'),
            ...
            ('2020-06-05', 'nacional',    'España', 'mujeres', 'edad 65-74')],
           names=['fecha_defuncion', 'ambito', 'nombre_ambito', 'nombre_sexo', 'nombre_gedad'], length=181920)

        filtraFilasSerie(serie, { 1:'nacional': 'nombre_sexo': 'mujeres' }) devuelve

        serie[[('2020-06-05', 'nacional',    'España', 'mujeres', 'edad 65-74')]]

    """

    if not indDict:
        return serie

    serieIndex = serie.index

    esMultiInd = all([isinstance(c, tuple) for c in serieIndex.to_list()])

    numClaves = max([len(c) for c in serieIndex.to_list()]) if esMultiInd else 1
    nomClaves = list(serieIndex.names)
    clave2i = dict(zip(nomClaves, range(numClaves)))

    checkConds = [k < numClaves if isinstance(k, (int)) else (k in nomClaves) for k in indDict.keys()]

    if not all(checkConds):
        failedConds = [cond for cond, check in zip(indDict.items(), checkConds) if not check]
        print(failedConds)
        condsMsg = ",".join(map(lambda x: '"' + str(x) + '"', failedConds))
        raise ValueError("filtraFilasSerie: condiciones incorrectas: %s" % condsMsg)

    funcCheckMulti = lambda x: all([x[k if isinstance(k, int) else clave2i[k]] == v for k, v in indDict.items()])
    funcCheckSingle = lambda x: (x == list(indDict.values())[0])

    filassOk = [c for c in serieIndex.to_list() if (funcCheckMulti if esMultiInd else funcCheckSingle)(c)]

    if not filassOk:
        raise KeyError(
            "filtraFilasSerie: ninguna fila cumple las condiciones (%s). Filas: %s " % (
                str(indDict), serieIndex.to_list()))

    result = serie[filassOk]
    if not conv2ts:  # Don't want conversion, nothing else to do
        return result

    fechasOk = [c[0] for c in filassOk]
    if len(set(fechasOk)) == len(filassOk):
        tsFilas = pd.DatetimeIndex(fechasOk)
        result.index = tsFilas
        return result

    return result


def ponMedidaPrefijo(col, sigColNanV=None, defaultNan=False):
    colName = col.name

    sigNan = defaultNan if sigColNanV is None else sigColNanV[colName]

    primNonNan = col.reset_index(drop=True).first_valid_index()
    primVal = col.iloc[primNonNan] if (primNonNan == 0 and not sigNan) else 0
    result = col.shift(1)
    result.iloc[primNonNan] = primVal

    return result


def cambiosDelDiaGrupo(df):
    sigColNan = df.head(1).isna().shift(-1, axis=1, fill_value=True).iloc[0]

    dfRef = df.apply(ponMedidaPrefijo, sigColNanV=sigColNan)

    return df - dfRef


def cambiosDelDia(df):
    """
    Calcula la diferencia con la entrada del día anterior
    :param df:
    :return:
    """
    grupos = df.columns.to_frame().reset_index(drop=True)[
        ['nombre_ambito', 'nombre_sexo', 'nombre_gedad']].drop_duplicates().T.to_dict().values()

    auxResult = []
    for filtro in grupos:
        dfReduc = filtraColumnasDF(df, colDict=filtro)
        difGrupo = cambiosDelDiaGrupo(dfReduc)

        auxResult.append(difGrupo)

    return pd.concat(auxResult, axis=1)


def reordenaColumnas(df, dfRef):
    """
    Devuelve un DF con las columnas ordenadas según las columnas de otro.

    :param df:
    :param dfRef:
    :return:
    """
    return df[dfRef.columns]


def operaRespetandoNA(df, func):
    """
    Aplica una función a cada elemento de un dataframe si NO es na. Si lo es pone el resultado será NA

    :param df: dataframe a tratar
    :param func: función a aplicar en los elementos que no son NA
    :return:
    """
    result = df.applymap(func=lambda x: np.nan if np.isnan(x) else func(x))

    return result


def primValorColumna(df):
    return df.apply(lambda x: x[x.first_valid_index()])


def ultValorColumna(df):
    return df.apply(lambda x: x[x.last_valid_index()])


def applyScaler(dfTS, year=2019, scalerCls=StandardScaler):
    scaler = scalerCls()
    valTrain = dfTS.loc[dfTS.index.year == year]
    scaler.fit(valTrain)

    result = pd.DataFrame(scaler.transform(dfTS), columns=dfTS.columns, index=dfTS.index)

    return result
