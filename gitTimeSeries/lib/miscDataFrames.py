import csv
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from os import path
from time import time
from types import FunctionType

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from utils.misc import listize
from .GitIterator import fileFromCommit, GitIterator


def applyScaler(dfTS, year=2019, scalerCls=StandardScaler):
    scaler = scalerCls()
    valTrain = dfTS.loc[dfTS.index.year == year]
    scaler.fit(valTrain)

    result = pd.DataFrame(scaler.transform(dfTS), columns=dfTS.columns, index=dfTS.index)

    return result


def cambiosDelDia(df):
    """
    Calcula la diferencia con la entrada del día anterior
    :param df:
    :return:
    """
    columnasFecha = indiceDeTipo(df.columns, "datetime")
    grupos = df.columns.to_frame().reset_index(drop=True).drop(
        columns=columnasFecha).drop_duplicates().T.to_dict().values()

    auxResult = []
    for filtro in grupos:
        dfReduc = filtraColumnasDF(df, colDict=filtro)
        difGrupo = cambiosDelDiaGrupo(dfReduc)

        auxResult.append(difGrupo)

    return pd.concat(auxResult, axis=1)


def cambiosDelDiaGrupo(df):
    sigColNan = df.head(1).isna().shift(-1, axis=1, fill_value=True).iloc[0]

    dfRef = df.apply(ponMedidaPrefijo, sigColNanV=sigColNan)

    return df - dfRef


def changeCounters2changedDataStats(dfOld, dfNew, changeCounters=None, **kwargs):
    statMsg = dict()
    resultDF = dfNew

    changeCounters = {} if changeCounters is None else changeCounters

    for counterName, counterConf in changeCounters.items():
        # Valores por defecto
        funcionCuenta = cuentaFilasCambiadas

        if isinstance(counterConf, list):
            kwargs['columnasObj'] = counterConf
        elif isinstance(counterConf, FunctionType):
            funcionCuenta = counterConf
        elif isinstance(counterConf, dict):
            kwargs.update(counterConf)
            funcionCuenta = counterConf.get('funcionCuenta', cuentaFilasCambiadas)

        resultCuenta, indiceCambiadas = funcionCuenta(counterName, dfOld, dfNew, **kwargs)

        if isinstance(resultCuenta, dict):
            for k in sorted(resultCuenta):
                finalK = f"{counterName}.{k}"
                valorK = resultCuenta[k]
                statMsg[finalK] = valorK
        else:
            statMsg[counterName] = resultCuenta
            if indiceCambiadas is not None:
                resultDF[counterName] = dfOld[counterName] + indiceCambiadas

    return resultDF, statMsg


def changeCounters2newColumns(dfNewlines, changeCounters=None):
    changeCounters = {} if changeCounters is None else changeCounters

    for counterName, counterConf in changeCounters.items():
        if isinstance(counterConf, dict):
            if counterConf.get('creaColumna', False):
                nombreColumna = counterConf.get('nombreColumna', counterName)
                dfNewlines[nombreColumna] = 0
        else:
            dfNewlines[counterName] = 0

    return dfNewlines


def changeCounters2ReqColNames(changeCounters: dict = None):
    """
    Dado un diccionario con estadistacas de contador de cambios, extrae las columnas que aparecerían en el DF histórico
    (columnas que se buscan + las que se añaden)
    :param changeCounters:
    :return:
    """
    if changeCounters is None:
        return []

    result = []
    for counterName, counterConf in changeCounters.items():
        if isinstance(counterConf, dict):
            if counterConf.get('creaColumna', False):
                nombreColumna = counterConf.get('nombreColumna', counterName)
                result.append(nombreColumna)
            if 'columnasObj' in counterConf:
                columnasAmirar = listize(counterConf['columnasObj'])
                result.extend(columnasAmirar)
        elif isinstance(counterConf, list):
            result.append(counterName)
            result.extend(counterConf)

    return result


def columnasCambiadasParaEstadistica(counterName, dfCambiadoOld, dfCambiadoNew, columnasObj=None):
    if len(dfCambiadoOld) != len(dfCambiadoNew):
        raise ValueError(f"cuentaFilas: longitudes difieren Old:{len(dfCambiadoOld)} != New:{len(dfCambiadoNew)}")
    if len(dfCambiadoOld) == 0:
        return 0

    auxColsObj = listize(columnasObj)
    counterCols = dfCambiadoOld.columns
    if columnasObj:
        missingColsNew = set(auxColsObj).difference(dfCambiadoNew.columns)

        if missingColsNew:
            print(
                f"columnasCambiadasParaEstadistica: {counterName}: columnas desconocidas: {sorted(missingColsNew)}. ",
                f"Columnas existentes: {sorted(list(dfCambiadoOld.columns))}. Ignorando contador.")
            return None
        counterCols = auxColsObj

    areRowsDifferent = (dfCambiadoOld[counterCols] != dfCambiadoNew[counterCols]).any(axis=1)
    return areRowsDifferent


def compareDataFrameIndexes(new: pd.DataFrame, old: pd.DataFrame = None):
    """
    Compares indexes of two dataframe and returns the removed,shared and new entries
    :param new: dataframe with new entries
    :param old: existing dataframe
    :return:
    """

    oldIndex = new.index.take([]) if old is None else old.index
    newIndex = new.index

    removed = oldIndex.difference(newIndex)
    shared = oldIndex.intersection(newIndex)
    added = newIndex.difference(oldIndex)

    return removed, shared, added


def compareDataFrames(new: pd.DataFrame, old: pd.DataFrame = None):
    """
    Dados dos dataframes, compara los valores de uno y otro y devuelve las claves del índice de datos nuevos,
    cambiados o eliminados. Las columnas que usa para comparar son las del nuevo.
    :param new: dataframe con datos nuevos
    :param old: dataframe con datos viejos
    :return: tupla de claves eliminadas del viejo, que han cambiado, que no aparecen en el viejo
    """
    removed, shared, added = compareDataFrameIndexes(new, old)

    if len(shared):
        oldData = old[new.columns]

        areRowsDifferent = (oldData.loc[shared, :] != new.loc[shared, :]).any(axis=1)
        changed = areRowsDifferent.loc[areRowsDifferent].index
    else:
        changed = shared.take([])

    return removed, changed, added


def cuentaFilasCambiadas(counterName, dfCambiadoOld, dfCambiadoNew, columnasObj=None, **kwargs):
    areRowsDifferent = columnasCambiadasParaEstadistica(counterName, dfCambiadoOld, dfCambiadoNew,
                                                        columnasObj=columnasObj)

    if (areRowsDifferent is None) or areRowsDifferent.empty:
        print("cuentaFilasCambiadas EMPTY or None", columnasObj)
        return 0, None

    return areRowsDifferent.sum(), areRowsDifferent


def DFVersionado2DFmerged(repoPath: str, filePath: str, readFunction, DFcurrent: pd.DataFrame = None,
                          minDate: datetime = None, changeCounters: dict = None, **kwargs):
    """
    Atraviesa un repositorio git, leyendo las versiones de un fichero susceptible de ser leído con pandas y hace
    anotaciones de los cambios y las adiciones (cuántos, en qué versión se ha hecho la última modificación).
    Opcionalmente puede contar cambios en subconjuntos de columnas
    :param repoPath: directorio raíz del repositorio git
    :param filePath: ubicación DENTRO DEL REPOSITORIO del fichero a estudiar
    :param readFunction: función de lectura del fichero a estudiar. Debe aceptar un nombre de fichero o un handle y
                         devuelve un dataframe de pandas
    :param DFcurrent: (Opcional) dataframe del estado actual. Sirve para hacer una lectura incremental. Sería el
                      resultado de la ejecución anterior de la función. Si no se suministra se consideran los datos
                      procedente de la primera versión como todos nuevos
    :param minDate: (Opcional) Fecha mínima del commit de la primera versión que se va a procesar.
    :param changeCounters: (Opcional). Configuración de contadores adicionales de cambios. Espera un diccionario con
                           pares {nombreContador:[lista de columnas a comparar]}. Añade columnas nombreContador al DF
                           resultado.
    :param kwargs: (Opcional) Parámetros adicionales a la función de lectura
    :return: Dataframe con el último valor leído para cada valor del índice. Tiene las siguientes características:
            * Índice: mísmo que el devuelto por la función de lectura
            * Columnas del DF devuelto: por la función de lectura
            * Columnas adicionales (metadata del último cambio referido al repo que lo contiene)
            * Contadores de cambios: general y si se han indicado contadores específicos
    """
    formatoLog = "DFVersionado2DFmerged: {dur:7.3f}s: commitDate: {commitDate} added: {added:6} changed: {changed:6}{contParciales}"
    fechaUltimaActualizacion = None

    if minDate:
        fechaUltimaActualizacion = minDate
    elif DFcurrent is not None:  # Hecha la comprobación de is not None porque pandas se queja
        fechaUltimaActualizacion = DFcurrent['fechaCommit'].max()

    repoIterator = GitIterator(repoPath=repoPath, reverse=True, minDate=fechaUltimaActualizacion)

    for commit in repoIterator:
        timeStart = time()
        commitSHA = commit.hexsha
        commitDate = commit.committed_datetime
        estadCambios = defaultdict(int)

        handle = BytesIO(fileFromCommit(filePath, commit).read())
        newDF = readFunction(handle, **kwargs)

        _, changed, added = compareDataFrames(newDF, DFcurrent)
        newDF['shaCommit'] = commitSHA
        newDF['fechaCommit'] = pd.to_datetime(commitDate)

        if len(added):
            newData = newDF.loc[added, :]
            newData['contCambios'] = 0

            newData = changeCounters2newColumns(dfNewlines=newData, changeCounters=changeCounters)

            if DFcurrent is None:
                DFcurrent = newData
                timeStop = time()
                print(formatoLog.format(dur=timeStop - timeStart, commitDate=commitDate, changed=len(changed),
                                        contParciales="", added=len(added)))
                continue  # No hay cambiadas porque no hay viejas. Son todas nuevas

        if len(changed):
            dfCambiadoOld = DFcurrent.loc[changed]
            dfCambiadoNew = newDF.loc[changed]
            dfCambiadoNew['contCambios'] = dfCambiadoOld['contCambios'] + 1

            restoArgs = {'columnasObj': None, 'fechaReferencia': commitDate}

            newConStats, msgStats = changeCounters2changedDataStats(dfCambiadoOld, dfCambiadoNew, changeCounters,
                                                                    **restoArgs)
            DFcurrent.loc[changed] = newConStats
            estadCambios.update(msgStats)

        if len(added):
            DFcurrent = pd.concat([DFcurrent, newData], axis=0)

        timeStop = time()
        strContParciales = ""
        if changeCounters:
            strContParciales = " [" + ",".join([f"{name}={estadCambios[name]:5}" for name in estadCambios]) + "]"
        print(
            formatoLog.format(dur=timeStop - timeStart, commitDate=commitDate, changed=len(changed), added=len(added),
                              contParciales=strContParciales))

    return DFcurrent


def DFVersionado2TSofTS(repoPath: str, filePath: str, readFunctionFila, columnaObj, minDate: datetime = None, **kwargs):
    fname = path.join(repoPath, filePath)
    formatoLog = "DFVersionado2TSofTS: {dur:7.3f}s: commitDate: {commitDate}"
    reqFreq = kwargs.get('freq', None)
    if 'freq' in kwargs:
        kwargs.pop('freq')

    fechaUltimaActualizacion = None
    if minDate:
        fechaUltimaActualizacion = minDate

    repoIterator = GitIterator(repoPath=repoPath, reverse=True, minDate=fechaUltimaActualizacion)

    auxDict = dict()

    for commit in repoIterator:
        timeStart = time()
        commitSHA = commit.hexsha
        commitDate = commit.committed_datetime

        handle = BytesIO(fileFromCommit(filePath, commit).read())
        newDF = readFunctionFila(handle, columnaObj, **kwargs)

        if newDF is None:
            print(f"DFVersionado2TSofTS: {fname}. Commit {commitSHA}[{commitDate}] Problemas leyendo dataframe")
            continue

        if newDF.empty:
            print(f"DFVersionado2TSofTS: {fname}. Commit {commitSHA}[{commitDate}] DF empty")
            continue

        auxDict[commitDate] = newDF

    if not auxDict:
        return None

    auxDF = pd.concat(auxDict, sort=True)
    result = auxDF.droplevel(1).sort_index()

    result.index = pd.DatetimeIndex(pd.to_datetime(result.index, utc=True).date, name='fechaCommit', freq=reqFreq)

    timeStop = time()
    strContParciales = ""
    print(formatoLog.format(dur=timeStop - timeStart, commitDate=commitDate))

    return result


def estadisticaCategoricals(counterName, dfCambiadoOld, dfCambiadoNew, columnaIndiceObj, columnasObj=None,
                            valoresAgrupacion=None, valoresDescribe=None, **kwargs):
    areRowsDifferent = columnasCambiadasParaEstadistica(counterName, dfCambiadoOld, dfCambiadoNew,
                                                        columnasObj=columnasObj)

    if areRowsDifferent is None:
        print("estadisticaCategoricals: {counterName}: problemas tras la invocacion a columnasCambiadasParaEstadistica")
        return {}, None

    if (len(areRowsDifferent) == 0) or (areRowsDifferent.sum() == 0):
        colData = ("en columna(s) " + ",".join(listize(columnasObj))) if columnasObj else ""
        print(f"estadisticaCategoricals: {counterName}: no ha habido cambios {colData}")
        return {}, None

    columnasIndice = dfCambiadoOld.index.names
    if columnaIndiceObj not in columnasIndice:
        print(
            f"estadisticaCategoricals: {counterName}: columna indice desconocida: {columnaIndiceObj}. ",
            f"Columnas existentes: {sorted(list(columnasIndice))}. Ignorando contador.")
        return None

    IDXfilter = False
    if valoresAgrupacion:
        IDXfilter = dfCambiadoNew.reset_index()[columnasIndice].isin(valoresAgrupacion).any(axis=1)
        IDXfilter.index = dfCambiadoNew.index

    filasAContar = ~IDXfilter & areRowsDifferent

    registrosAContar = filasAContar.loc[filasAContar].reset_index()[columnaIndiceObj].astype('category', copy=False)

    resultDesc = registrosAContar.describe()
    if valoresDescribe:
        resultDesc = resultDesc[valoresDescribe]

    result = resultDesc.to_dict()

    return result, None


def estadisticaFechaCambios(counterName, dfCambiadoOld, dfCambiadoNew, columnaIndiceObj, fechaReferencia,
                            columnasObj=None,
                            valoresAgrupacion=None, **kwargs):
    areRowsDifferent = columnasCambiadasParaEstadistica(counterName, dfCambiadoOld, dfCambiadoNew,
                                                        columnasObj=columnasObj)

    if areRowsDifferent is None:
        print("estadisticaFechaCambios: {counterName}: problemas tras la invocacion a columnasCambiadasParaEstadistica")
        return {}, None

    if (len(areRowsDifferent) == 0) or (areRowsDifferent.sum() == 0):
        colData = ("en columna(s) " + ",".join(listize(columnasObj))) if columnasObj else ""
        print(f"estadisticaFechaCambios: {counterName}: no ha habido cambios {colData}")
        return {}, None

    columnasIndice = dfCambiadoOld.index.names
    if columnaIndiceObj not in columnasIndice:
        print(
            f"estadisticaFechaCambios: {counterName}: columna indice desconocida: {columnaIndiceObj}. ",
            f"Columnas existentes: {sorted(list(columnasIndice))}. Ignorando contador.")
        return None

    IDXfilter = False
    if valoresAgrupacion:
        IDXfilter = dfCambiadoNew.reset_index()[columnasIndice].isin(valoresAgrupacion).any(axis=1)
        IDXfilter.index = dfCambiadoNew.index

    filasAContar = ~IDXfilter & areRowsDifferent

    registrosAContar = filasAContar.loc[filasAContar].reset_index()[columnaIndiceObj]

    descCateg = registrosAContar.astype('category', copy=False).describe().loc[['unique']]
    descFechas = pd.to_datetime(registrosAContar.describe(datetime_is_numeric=True).loc[['min', 'max']]).dt.strftime(
        "%Y-%m-%d")
    descFechas.index = pd.Index(['Fmin', 'Fmax'])
    descDiff = ((fechaReferencia.date() - registrosAContar.dt.date).dt.days).describe().loc[
        ['mean', 'std', '50%', 'min', 'max']].map('{:,.2f}'.format)
    descDiff.index = pd.Index(['Dmean', 'Dstd', 'Dmedian', 'Dmin', 'Dmax', ])

    resultDesc = pd.concat([descCateg, descFechas, descDiff])
    result = resultDesc.to_dict()

    return result, None


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


def grabaDatosHistoricos(df, fname):
    df.to_csv(fname, sep=';', header=True, index=True, quoting=csv.QUOTE_ALL)


def indexFillNAs(indexdata: pd.MultiIndex, replacementValues: dict):
    """
    Reemplaza los NAs de niveles de índice por valores configurables por nivel.
    :param indexdata: Indice a tratar
    :param replacementValues: diccionario con "nivel":"valor de reemplazo"
    :return:
    """
    newData = []
    for name in indexdata.names:
        dataLevel = indexdata.get_level_values(name).fillna(
            replacementValues[name]) if name in replacementValues else indexdata.get_level_values(name)
        newData.append(dataLevel)

    result = pd.MultiIndex.from_arrays(newData, names=indexdata.names)

    return result


def indiceDeTipo(dfIndex: pd.MultiIndex, prefijo):
    def getMatcher(prefijos):
        def coincideCadena(c: str) -> bool:
            return any([c.startswith(p) for p in prefijos])

        return coincideCadena

    nombresTipo = nombreTipoCamposIndice(dfIndex)

    listaPrefs = listize(prefijo)

    matcher = getMatcher(listaPrefs)

    resultSerie = nombresTipo.apply(matcher)

    result = resultSerie.loc[resultSerie].index.to_list()

    return result


def leeCSVdataset(fname_or_handle, colIndex=None, cols2drop=None, colDates=None, **kwargs) -> pd.DataFrame:
    """
    Lee un dataframe (en CSV) y le hace un tratamiento mínimo: fija columnas de índice, elmina columnas y convierte en fecha
    :param fname_or_handle: nombre de fichero o handle para acceder al dataframe
    :param colIndex: columnas que formarán el índice (nombre o lista)
    :param cols2drop: columnas que se van a eliminar (lista)
    :param colDates: columnas que se van a convertir en fechas (nombre de la columna, lista de nombres, diccionario con
                     pares: nombre:formatoEsperado
    :param kwargs: parámetros que se le pasan a pd.read_csv
    :return: dataframe
    """
    myDF = pd.read_csv(fname_or_handle, **kwargs)

    if colDates:
        if isinstance(colDates, str):
            conversorArgs = {colDates: {'arg': myDF[colDates], 'infer_datetime_format': True, 'utc': True}}
        elif isinstance(colDates, (list, set)):
            conversorArgs = {colName: {'arg': myDF[colName], 'infer_datetime_format': True, 'utc': True} for colName in
                             colDates}
        elif isinstance(colDates, dict):
            conversorArgs = {colName: {'arg': myDF[colName], 'format': colFormat, 'utc': True} for colName, colFormat in
                             colDates.items()}
        else:
            raise TypeError(
                f"leeCSVdataset: there is no way to process argument colDates '{colDates}' of type {type(colDates)}")

        for colName, args in conversorArgs.items():
            myDF[colName] = pd.to_datetime(**args)

    resultDropped = myDF.drop(columns=cols2drop) if cols2drop else myDF
    resultIndex = resultDropped.set_index(colIndex) if colIndex else resultDropped

    result = resultIndex

    return result


def leeDatosHistoricos(fname, extraCols, colsIndex, colsDate, changeCounters):
    requiredCols = extraCols + changeCounters2ReqColNames(changeCounters)

    result = leeCSVdataset(fname, colIndex=colsIndex, colDates=colsDate, sep=';', header=0)
    missingCols = set(requiredCols).difference(result.columns)
    if missingCols:
        raise ValueError(f"Archivo '{fname}': faltan columnas: {sorted(missingCols)}.")
    return result


def nombreTipoCamposIndice(dfIndex: pd.MultiIndex) -> pd.Series:
    """
    Dado un multiíndice devuelve los tipos de los campos que lo componen (cadena de texto)
    :param dfIndex: multiíndice de dataframe
    :return: Serie de cadenas con los tipos de cada campo del índice
    """
    result = dfIndex.to_frame(index=False).dtypes.map(str)

    return result


def operaRespetandoNA(df, func):
    """
    Aplica una función a cada elemento de un dataframe si NO es na. Si lo es pone el resultado será NA

    :param df: dataframe a tratar
    :param func: función a aplicar en los elementos que no son NA
    :return:
    """
    result = df.applymap(func=lambda x: np.nan if np.isnan(x) else func(x))

    return result


def ponMedidaPrefijo(col, sigColNanV=None, defaultNan=False):
    colName = col.name

    sigNan = defaultNan if sigColNanV is None else sigColNanV[colName]

    primNonNan = col.reset_index(drop=True).first_valid_index()
    primVal = col.iloc[primNonNan] if (primNonNan == 0 and not sigNan) else 0
    result = col.shift(1)
    result.iloc[primNonNan] = primVal

    return result


def primValorColumna(df):
    return df.apply(lambda x: x[x.first_valid_index()])


def reordenaColumnas(df, dfRef):
    """
    Devuelve un DF con las columnas ordenadas según las columnas de otro.

    :param df:
    :param dfRef:
    :return:
    """
    return df[dfRef.columns]


def ultValorColumna(df):
    return df.apply(lambda x: x[x.last_valid_index()])
