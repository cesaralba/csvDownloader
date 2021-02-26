from collections import defaultdict
from datetime import datetime
from time import time
from types import FunctionType

import pandas as pd

from utils.misc import listize
from .GitIterator import GitIterator, fileFromCommit


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
            conversorArgs = {colDates: {'arg': myDF[colDates], 'infer_datetime_format': True}}
        elif isinstance(colDates, (list, set)):
            conversorArgs = {colName: {'arg': myDF[colName], 'infer_datetime_format': True} for colName in colDates}
        elif isinstance(colDates, dict):
            conversorArgs = {colName: {'arg': myDF[colName], 'format': colFormat} for colName, colFormat in
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

        newDF = readFunction(fileFromCommit(filePath, commit), **kwargs)

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


def cuentaFilasCambiadas(counterName, dfCambiadoOld, dfCambiadoNew, columnasObj=None, **kwargs):
    areRowsDifferent = columnasCambiadasParaEstadistica(counterName, dfCambiadoOld, dfCambiadoNew,
                                                        columnasObj=columnasObj)

    return areRowsDifferent.sum(), areRowsDifferent


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
