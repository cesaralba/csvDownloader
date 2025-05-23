import csv
import gc
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from os import path
from time import time
from types import FunctionType

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from utils.GitIterator import fileFromCommit, GitIterator, saveTempFileCondition
from utils.misc import listize
from utils.pandasUtils import DF2maxValues

CHGCOUNTERCOLNAME = 'contCambios'
COMMITHASHCOLNAME = 'shaCommit'
COMMITDATECOLNAME = 'fechaCommit'

COLSADDEDMERGED = [COMMITHASHCOLNAME, COMMITDATECOLNAME, CHGCOUNTERCOLNAME]


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
    statMsg = {}

    changeCounters = {} if changeCounters is None else changeCounters

    resultCounters = changeCounters2resultDF(data=dfNew.index, changeCounters=changeCounters)

    for counterName, counterConf in changeCounters.items():
        # Valores por defecto
        funcionCuenta = countChangedRows

        if isinstance(counterConf, list):
            kwargs['columnasObj'] = counterConf
        elif isinstance(counterConf, FunctionType):
            funcionCuenta = counterConf
        elif isinstance(counterConf, dict):
            kwargs.update(counterConf)
            funcionCuenta = counterConf.get('funcionCuenta', countChangedRows)

        resultCuenta, indiceCambiadas = funcionCuenta(counterName, dfOld, dfNew, **kwargs)

        if isinstance(resultCuenta, dict):
            for k in sorted(resultCuenta):
                finalK = f"{counterName}.{k}"
                valorK = resultCuenta[k]
                statMsg[finalK] = valorK
        else:
            statMsg[counterName] = resultCuenta
            if indiceCambiadas is not None:
                resultCounters[counterName] = indiceCambiadas

    return resultCounters, statMsg


def changeCounters2resultColumns(changeCounters=None):
    changeCounters = {} if changeCounters is None else changeCounters

    result = []

    for counterName, counterConf in changeCounters.items():
        if isinstance(counterConf, dict):
            if counterConf.get('creaColumna', False):
                nombreColumna = counterConf.get('nombreColumna', counterName)
                result.append(nombreColumna)
        else:
            result.append(counterName)

    return result


def changeCounters2resultDF(data, changeCounters=None):
    changeCounters = {} if changeCounters is None else changeCounters

    if isinstance(data, (pd.DataFrame, pd.Series)):
        auxIndex = data.index
    elif isinstance(data, (pd.Index, pd.MultiIndex, pd.DatetimeIndex, pd.RangeIndex)):
        auxIndex = data
    else:
        raise TypeError(
            f"changeCounters2resultDF: don't know how to handle '{type(data)}': "
            f"expected a DataFrame, a Series or some kind of index")

    result = pd.DataFrame([], index=auxIndex)

    for colname in changeCounters2resultColumns(changeCounters):
        result[colname] = 0

    return result


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
    for counterConf in changeCounters.values():
        if isinstance(counterConf, dict):
            if 'columnasObj' in counterConf:
                columnasAmirar = listize(counterConf['columnasObj'])
                result.extend(columnasAmirar)
        elif isinstance(counterConf, list):
            result.extend(counterConf)

    result = result + changeCounters2resultColumns(changeCounters)

    return result


def colDates2ReqColNames(colDates=None):
    """
    Devuelve la lista de columnas que se van a convertir en fecha
    :param colDates:
    :return:
    """
    result = set()
    if colDates:
        if isinstance(colDates, str):
            result = {colDates}
        elif isinstance(colDates, (list, set)):
            result = set(colDates)
        elif isinstance(colDates, dict):
            result = set(colDates.keys())
        else:
            raise TypeError(
                f"colDates2ReqColNames: there is no way to process argument colDates '{colDates}' of type {type(colDates)}")
    return result


def changedColumnsForStats(counterName, dfChangedOld, dfChangedNew, targetCols=None):
    if len(dfChangedOld) != len(dfChangedNew):
        raise ValueError(f"cuentaFilas: longitudes difieren Old:{len(dfChangedOld)} != New:{len(dfChangedNew)}")
    if len(dfChangedOld) == 0:
        return 0

    auxColsObj = listize(targetCols)
    counterCols = dfChangedNew.columns.difference(COLSADDEDMERGED)

    if targetCols:
        missingColsNew = set(auxColsObj).difference(dfChangedNew.columns)

        if missingColsNew:
            print(
                f"changedColumnsForStats: {counterName}: columnas desconocidas: {sorted(missingColsNew)}. ",
                f"Columnas existentes: {sorted(list(dfChangedOld.columns))}. Ignorando contador.")
            return None
        counterCols = auxColsObj

    NAfiller = DF2maxValues(dfChangedOld, counterCols)

    areRowsDifferent = (dfChangedOld.fillna(value=NAfiller)[counterCols] != dfChangedNew.fillna(value=NAfiller)[
        counterCols]).any(axis=1)
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
        targetCols = new.columns.intersection(old.columns).difference(COLSADDEDMERGED)
        oldData = old[targetCols]

        NAfiller = DF2maxValues(old, targetCols)

        areRowsDifferent = (oldData.fillna(value=NAfiller).loc[shared, targetCols] != new.fillna(value=NAfiller).loc[
            shared, targetCols]).any(axis=1)
        changed = areRowsDifferent.loc[areRowsDifferent].index
    else:
        changed = shared.take([])

    return removed, changed, added


def countChangedRows(counterName, dfCambiadoOld, dfCambiadoNew, columnasObj=None, **kwargs):
    areRowsDifferent = changedColumnsForStats(counterName, dfCambiadoOld, dfCambiadoNew,
                                              targetCols=columnasObj)

    _ = kwargs
    if (areRowsDifferent is None) or areRowsDifferent.empty:
        print(f"countChangedRows: {counterName}:{columnasObj} EMPTY or None", columnasObj)
        return 0, None

    return areRowsDifferent.sum(), areRowsDifferent


def DFversioned2DFmerged(repoPath: str, filePath: str, readFunction, DFcurrent: pd.DataFrame = None,
                         minDate: datetime = None, changeCounters: dict = None, backupFile: str = None,
                         backupStep: int = 0, usePrevDF: bool = True, skipBadCommits: bool = False, **kwargs):
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
    :param backupFile: name of the file storing intermediate result of the iterator on git repo
    :param backupStep: safe 'backupFile' every 'backupStep' iterations (commits)
    :param usePrevDF: TRUE: compares newly read dataframe against previously read dataframe; FALSE: compares agains historic data

    :param kwargs: (Opcional) Parámetros adicionales a la función de lectura
    :return: Dataframe con el último valor leído para cada valor del índice. Tiene las siguientes características:
            * Índice: mísmo que el devuelto por la función de lectura
            * Columnas del DF devuelto: por la función de lectura
            * Columnas adicionales (metadata del último cambio referido al repo que lo contiene)
            * Contadores de cambios: general y si se han indicado contadores específicos
    """
    formatoLog = "DFversioned2DFmerged: {dur:7.3f}s: commitDate: {commitDate} added: {added:6} changed: {changed:6} removed:{removed:6} {contParciales}"

    maxCommitDateCurrent = DFcurrent[COMMITDATECOLNAME].max() if DFcurrent is not None else None
    lastUpdateDate = minDate if minDate else maxCommitDateCurrent

    repoIterator = GitIterator(repoPath=repoPath, reverse=True, minDate=lastUpdateDate, strictMinimum=False)
    iterCounter = 0

    prevDF = None
    for commit in repoIterator:
        timeStart = time()
        commitSHA = commit.hexsha
        commitDate = commit.committed_datetime
        estadCambios = defaultdict(int)

        try:
            newDF = read_VersionedFile(commit, repoPath, filePath, readFunction, kwargs)
        except ValueError as exc:
            if skipBadCommits:
                continue
            raise exc

        colDateRef = newDF.index.to_frame().select_dtypes(exclude=['object']).columns.to_list()
        maxFecha = newDF.index.to_frame()[colDateRef].max().iloc[0]

        # Check if the first DF we are using has already been processed. If so set it as the reference to compare
        # and take next. We assume there can only be a commit at a certain time
        if (prevDF is None) and (DFcurrent is not None) and (commitDate == maxCommitDateCurrent):
            prevDF = newDF
            continue  # Nothing to do, but now we have the reference for next executions

        newDFwrk = newDF.copy()
        newDFwrk[COMMITHASHCOLNAME] = commitSHA
        newDFwrk[COMMITDATECOLNAME] = pd.to_datetime(commitDate, utc=True, format='ISO8601')

        DFref = prevDF if usePrevDF else DFcurrent

        eliminadas, cambiadas, nuevas = compareDataFrames(newDF, DFref)

        if len(eliminadas):
            print(f"Removed entries {len(eliminadas)}")
            print(eliminadas.to_frame())

        if len(cambiadas):
            dfCurrentChanged = DFcurrent.loc[cambiadas].copy()
            dfPrevChanged = DFref.loc[cambiadas]
            dfNewChanged = newDF.loc[cambiadas]
            dfNewChangedWrk = newDFwrk.loc[cambiadas]

            dfNewChangedWrk[CHGCOUNTERCOLNAME] = dfCurrentChanged[CHGCOUNTERCOLNAME] + 1

            restoArgs = {'columnasObj': None, 'fechaReferencia': maxFecha}

            dfCountCambiosCurr, msgStatsCurr = changeCounters2changedDataStats(dfPrevChanged, dfNewChanged,
                                                                               changeCounters, **restoArgs)

            newConStats = dfCurrentChanged[dfCountCambiosCurr.columns] + dfCountCambiosCurr

            dfChangedData = pd.concat([dfNewChangedWrk, newConStats], axis=1)
            DFcurrent.loc[cambiadas] = dfChangedData
            estadCambios.update(msgStatsCurr)

        if len(nuevas):
            auxNewData = newDFwrk.loc[nuevas, :]
            auxNewData[CHGCOUNTERCOLNAME] = 0

            counterDF = changeCounters2resultDF(data=auxNewData.index, changeCounters=changeCounters)
            newData = pd.concat([auxNewData, counterDF], axis=1, join='outer')

            if DFcurrent is None:
                DFcurrent = newData
                timeStop = time()
                print(formatoLog.format(dur=timeStop - timeStart, commitDate=commitDate, changed=len(cambiadas),
                                        contParciales="", added=len(nuevas), removed=len(eliminadas)))
                prevDF = newDF
                iterCounter += 1
                continue  # No hay cambiadas porque no hay viejas. Son todas nuevas

            DFcurrent = pd.concat([DFcurrent, newData], axis=0)

        saveIntermediateResults(DFcurrent, backupFile, backupStep, commitDate, commitSHA, iterCounter)

        timeStop = time()
        strContParciales = " [" + ",".join([f"{name}={estadCambios[name]:5}" for name in estadCambios]) + "]"
        print(formatoLog.format(dur=timeStop - timeStart, commitDate=commitDate, changed=len(cambiadas),
                                added=len(nuevas), removed=len(eliminadas), contParciales=strContParciales))
        prevDF = newDF
        iterCounter += 1
        gc.collect()

    return DFcurrent


def read_VersionedFile(commitEntry, repoPath, filePath, readFunction, kwargs):
    commitSHA = commitEntry.hexsha
    handle = BytesIO(fileFromCommit(filePath, commitEntry).read())
    try:
        newDF = readFunction(handle, **kwargs)
    except ValueError as exc:
        print(f"DFversioned2DFmerged: problemas leyendo {repoPath}/{filePath} ({commitSHA})")
        raise exc
    return newDF


def saveIntermediateResults(dataframe, filename, eachIter, commitDate, commitSHA, iterCounter):
    if saveTempFileCondition(filename=filename, step=eachIter):
        if (iterCounter > 0) & ((iterCounter % eachIter) == 0):
            print(
                f"Iter: {iterCounter}. Saving temp file {filename}. Commit date: {commitDate} Hash: {commitSHA}")
            saveHistoricData(dataframe, filename)


def DFVersionado2TSofTS(repoPath: str, filePath: str, readFunctionFila, columnaObj, minDate: datetime = None, **kwargs):
    fname = path.join(repoPath, filePath)
    formatoLog = "DFVersionado2TSofTS: {fname} {dur:7.3f}s"
    reqFreq = kwargs.get('freq', None)
    if 'freq' in kwargs:
        kwargs.pop('freq')

    timeStart = time()

    fechaUltimaActualizacion = None
    if minDate:
        fechaUltimaActualizacion = minDate

    repoIterator = GitIterator(repoPath=repoPath, reverse=True, minDate=fechaUltimaActualizacion)

    auxDict = {}

    for commit in repoIterator:
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

    result.index = pd.DatetimeIndex(pd.to_datetime(result.index, utc=True).date, name=COMMITDATECOLNAME, freq=reqFreq)

    timeStop = time()
    print(formatoLog.format(dur=timeStop - timeStart, fname=fname))

    return result


def DFVersionado2DictOfTS(repoPath: str, filePath: str, readFunction, minDate: datetime = None, **kwargs):
    timeStart = time()

    result = {}
    fname = path.join(repoPath, filePath)
    formatoLog = "DFversioned2DFmerged: {fname} {dur:7.3f}s: "
    fechaUltimaActualizacion = None

    if minDate:
        fechaUltimaActualizacion = minDate

    repoIterator = GitIterator(repoPath=repoPath, reverse=True, minDate=fechaUltimaActualizacion)

    for commit in repoIterator:
        commitSHA = commit.hexsha
        commitDate = commit.committed_datetime

        handle = BytesIO(fileFromCommit(filePath, commit).read())

        try:
            newDF = readFunction(handle, **kwargs)
            newDF[COMMITHASHCOLNAME] = commitSHA
            newDF[COMMITDATECOLNAME] = pd.to_datetime(commitDate, utc=True)
        except ValueError as exc:
            print(f"DFversioned2DFmerged: problemas leyendo {repoPath}/{filePath}")
            raise exc

        result[commitDate] = newDF

    timeStop = time()

    print(formatoLog.format(dur=timeStop - timeStart, fname=fname))

    return result


def estadisticaCategoricals(counterName, dfCambiadoOld, dfCambiadoNew, columnaIndiceObj, columnasObj=None,
                            valoresAgrupacion=None, valoresDescribe=None, **kwargs):
    areRowsDifferent = changedColumnsForStats(counterName, dfCambiadoOld, dfCambiadoNew,
                                              targetCols=columnasObj)

    _ = kwargs
    if areRowsDifferent is None:
        print("estadisticaCategoricals: {counterName}: problemas tras la invocacion a changedColumnsForStats")
        return {}, None

    if (len(areRowsDifferent) == 0) or (areRowsDifferent.sum() == 0):
        colData = ("en columna(s) " + ",".join(listize(columnasObj))) if columnasObj else ""
        print(f"estadisticaCategoricals: {counterName}: no ha habido cambios {colData}")
        return {}, None

    columnasIndice = list(dfCambiadoOld.index.names)
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

    return resultDesc.to_dict(), None


def estadisticaFechaCambios(counterName, dfCambiadoOld, dfCambiadoNew, columnaIndiceObj, fechaReferencia,
                            columnasObj=None,
                            valoresAgrupacion=None, **kwargs):
    _ = kwargs
    areRowsDifferent = changedColumnsForStats(counterName, dfCambiadoOld, dfCambiadoNew,
                                              targetCols=columnasObj)

    if areRowsDifferent is None:
        print("estadisticaFechaCambios: {counterName}: problemas tras la invocacion a changedColumnsForStats")
        return {}, None

    if (len(areRowsDifferent) == 0) or (areRowsDifferent.sum() == 0):
        colData = ("en columna(s) " + ",".join(listize(columnasObj))) if columnasObj else ""
        print(f"estadisticaFechaCambios: {counterName}: no ha habido cambios {colData}")
        return {}, None

    columnasIndice = list(dfCambiadoOld.index.names)
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
    descFechas = pd.to_datetime(registrosAContar.describe().loc[['min', 'max']]).dt.strftime("%Y-%m-%d")
    descFechas.index = pd.Index(['Fmin', 'Fmax'])
    descDiff = pd.to_timedelta(fechaReferencia.date() - registrosAContar.dt.date, unit='D').dt.days.describe().loc[
        ['mean', 'std', '50%', 'min', 'max']].map(lambda x: f'{x:,.2f}')
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

    esMultiCol = all(isinstance(c, tuple) for c in df.columns.to_list())

    numClaves = max(len(c) for c in df.columns.to_list()) if esMultiCol else 1
    clave2i = dict(zip(list(df.columns.names), range(numClaves)))

    checkConds = [k < numClaves if isinstance(k, int) else (k in list(df.columns.names)) for k in colDict.keys()]

    if not all(checkConds):
        failedConds = [cond for cond, check in zip(colDict.items(), checkConds) if not check]
        print(failedConds)
        condsMsg = ",".join(map(lambda x: '"' + str(x) + '"', failedConds))
        raise ValueError(f"filtraColumnasDF: condiciones incorrectas: {condsMsg}")

    def funcCheckMulti(x):
        return all(x[k if isinstance(k, int) else clave2i[k]] == v for k, v in colDict.items())

    def funcCheckSingle(x):
        return x == list(colDict.values())[0]

    colsOk = [c for c in df.columns.to_list() if (funcCheckMulti if esMultiCol else funcCheckSingle)(c)]

    if not colsOk:
        raise KeyError(
            f"filtraColumnasDF: ninguna columna cumple las condiciones ({str(colDict)}). "
            f"Columnas: {df.columns.to_list()} ")

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

    esMultiInd = all(isinstance(c, tuple) for c in serie.index.to_list())

    numClaves = max(len(c) for c in serie.index.to_list()) if esMultiInd else 1
    clave2i = dict(zip(list(serie.index.names), range(numClaves)))

    checkConds = [k < numClaves if isinstance(k, int) else (k in list(serie.index.names)) for k in indDict.keys()]

    if not all(checkConds):
        failedConds = [cond for cond, check in zip(indDict.items(), checkConds) if not check]
        print(failedConds)
        condsMsg = ",".join(map(lambda x: '"' + str(x) + '"', failedConds))
        raise ValueError(f"filtraFilasSerie: condiciones incorrectas: {condsMsg}")

    def funcCheckMulti(x):
        return all(x[k if isinstance(k, int) else clave2i[k]] == v for k, v in indDict.items())

    def funcCheckSingle(x):
        return (x == list(indDict.values())[0])

    filassOk = [c for c in serie.index.to_list() if (funcCheckMulti if esMultiInd else funcCheckSingle)(c)]

    if not filassOk:
        raise KeyError(f"filtraFilasSerie: ninguna fila cumple las condiciones ({str(indDict)}). "
                       f"Filas: {serie.index.to_list()} ")

    result = serie[filassOk]
    if not conv2ts:  # Don't want conversion, nothing else to do
        return result

    fechasOk = [c[0] for c in filassOk]
    if len(set(fechasOk)) == len(filassOk):
        tsFilas = pd.DatetimeIndex(fechasOk)
        result.index = tsFilas
        return result

    return result


def saveHistoricData(df, fname):
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
            return any(c.startswith(p) for p in prefijos)

        return coincideCadena

    nombresTipo = nombreTipoCamposIndice(dfIndex)

    listaPrefs = listize(prefijo)

    matcher = getMatcher(listaPrefs)

    resultSerie = nombresTipo.apply(matcher)

    result = resultSerie.loc[resultSerie].index.to_list()

    return result


def readCSVdataset(fname_or_handle, colIndex=None, cols2drop=None, colDates=None, **kwargs) -> pd.DataFrame:
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

    if 'chunksize' in kwargs and kwargs.get('chunksize'):
        auxList = []
        for chunk in pd.read_csv(fname_or_handle, **kwargs):
            auxList.append(chunk)

        myDF = pd.concat(auxList, axis=0)
    else:
        myDF = pd.read_csv(fname_or_handle, **kwargs)

    columnasDispo = set(myDF.columns)

    columnProblems = readCSV_column_checking(colDates, colIndex, cols2drop, columnasDispo)
    if columnProblems:
        raise ValueError(
            f"readCSVdataset: ha habido errores: {', '.join(columnProblems)}. Columnas disponibles: {sorted(columnasDispo)}")

    if colDates:
        conversorArgs = readCSV_prepare_date_conversion(colDates, myDF)

        for colName, args in conversorArgs.items():
            myDF[colName] = pd.to_datetime(**args)

    resultDropped = myDF.drop(columns=(cols2drop or []))
    resultIndex = resultDropped.set_index(colIndex) if colIndex else resultDropped

    return resultIndex


def readCSV_addCommitDateColumn2colsDate(colDates):
    """
    Given the existing colDates param, adds 'fechaCommit' in case it doesn't exist
    :param colDates: current colDates param
    :return: new version of colDates with 'fechaCommit' added (if required)
    """

    result = colDates

    if isinstance(colDates, str):
        if colDates != COMMITDATECOLNAME:
            result = [colDates, COMMITDATECOLNAME]
    elif isinstance(colDates, (list, set)):
        if COMMITDATECOLNAME not in colDates:
            if isinstance(colDates, list):
                result.append(COMMITDATECOLNAME)
            else:
                result.add(COMMITDATECOLNAME)
    elif isinstance(colDates, dict):
        if COMMITDATECOLNAME not in colDates:
            result[COMMITDATECOLNAME] = None
    else:
        raise TypeError(
            f"readCSVdataset: there is no way to process argument colDates '{colDates}' of type {type(colDates)}")

    return result


def readCSV_column_checking(colDates, colIndex, cols2drop, columnasDispo):
    errors = []
    if set(colIndex or set()).difference(columnasDispo):
        missingCols = set(colIndex).difference(columnasDispo)
        errorMsg = f"Columns for Index. Missing: {sorted(missingCols)}"
        errors.append(errorMsg)
    if set(cols2drop or set()).difference(columnasDispo):
        missingCols = set(cols2drop).difference(columnasDispo)
        errorMsg = f"Columns to ignore. Missing: {sorted(missingCols)}"
        errors.append(errorMsg)
    if colDates2ReqColNames(colDates).difference(columnasDispo):
        missingCols = colDates2ReqColNames(colDates).difference(columnasDispo)
        errorMsg = f"Columns to transform into time. Missing: {sorted(missingCols)}"
        errors.append(errorMsg)
    return errors


def readCSV_prepare_date_conversion(colDates, myDF):
    if isinstance(colDates, str):
        conversorArgs = {colDates: {'arg': myDF[colDates], 'infer_datetime_format': True, 'utc': True}}
    elif isinstance(colDates, (list, set)):
        conversorArgs = {colName: {'arg': myDF[colName], 'format': 'ISO8601', 'utc': True} for colName in
                         colDates}
    elif isinstance(colDates, dict):
        conversorArgs = {colName: {'arg': myDF[colName], 'format': colFormat, 'utc': True} for colName, colFormat in
                         colDates.items()}
    else:
        raise TypeError(
            f"readCSVdataset: there is no way to process argument colDates '{colDates}' of type {type(colDates)}")
    return conversorArgs


def readHistoricData(fname, extraCols, colsIndex, colsDate, changeCounters, chunkSize: int = 0):
    requiredCols = extraCols + changeCounters2ReqColNames(changeCounters)

    auxColsDate = readCSV_addCommitDateColumn2colsDate(colsDate)
    try:
        result = readCSVdataset(fname, colIndex=colsIndex, colDates=auxColsDate, sep=';', header=0, chunksize=chunkSize)
    except ValueError as exc:
        print(f"readHistoricData: error reading '{fname}': exc")
        raise exc

    missingCols = set(requiredCols).difference(result.columns)
    if missingCols:
        raise ValueError(f"Archivo '{fname}': faltan columnas: {sorted(missingCols)}.")

    return result.sort_index()


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
