from time import time

import pandas as pd

from gitTimeSeries.lib.GitIterator import GitIterator, fileFromCommit


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
    removed, shared, added = compareDataFrameIndexes(new, old)

    if len(shared):
        oldData = old[new.columns]

        areRowsDifferent = (oldData.loc[shared, :] != new.loc[shared, :]).any(axis=1)
        changed = areRowsDifferent.loc[areRowsDifferent].index
    else:
        changed = shared.take([])

    return removed, changed, added


def leeCSVdataset(fname_or_handle, colIndex=None, cols2drop=None, **kwargs) -> pd.DataFrame:
    myDF = pd.read_csv(fname_or_handle, **kwargs)
    resultIndex = myDF.set_index(colIndex) if colIndex else myDF
    resultDropped = resultIndex.drop(columns=cols2drop) if cols2drop else resultIndex

    result = resultDropped

    return result


def DFVersionado2DFmerged(repoPath: str, filePath: str, readFunction, DFcurrent: pd.DataFrame = None,
                          changeCounters: dict = None):
    fechaUltimaActualizacion = DFcurrent['fechaCommit'].max() if DFcurrent is not None else None
    repoIterator = GitIterator(repoPath=repoPath, reverse=True, minDate=fechaUltimaActualizacion)

    for commit in repoIterator:
        timeStart = time()
        commitSHA = commit.hexsha
        commitDate = commit.committed_datetime
        print(commitSHA, commitDate)

        newDF = readFunction(fileFromCommit(filePath, commit))

        _, changed, added = compareDataFrames(newDF, DFcurrent)

        if len(added):
            newData = newDF.loc[added, :]
            newData['shaCommit'] = commitSHA
            newData['fechaCommit'] = pd.to_datetime(commitDate)
            newData['contCambios'] = 0
            if changeCounters:
                for counterName, counterCols in changeCounters.items():
                    missingCols = set(counterCols).difference(newDF.columns)
                    if missingCols:
                        print(
                            f"DFVersionado2DFmerged: {counterName}: columnas desconocidas: {sorted(missingCols)}. ",
                            f"Columnas existentes: {sorted(list(newDF.columns))}. Ignorando contador.")
                        continue
                    newData[counterName] = 0

            if DFcurrent is None:
                DFcurrent = newData
                timeStop = time()
                print(
                    f"DFVersionado2DFmerged: {timeStop - timeStart}: commitDate: {commitDate} changed: {len(changed)} added: {len(added)}")
                continue  # No hay cambiadas porque no hay viejas. Son todas nuevas

        if len(changed):
            DFcurrent.loc[changed, newDF.columns] = newDF.loc[changed, :]
            DFcurrent.loc[changed, 'shaCommit'] = commitSHA
            DFcurrent.loc[changed, 'fechaCommit'] = commitDate
            DFcurrent.loc[changed, 'contCambios'] += 1
            if changeCounters:
                for counterName, counterCols in changeCounters.items():
                    missingCols = set(counterCols).difference(newDF.columns)
                    if missingCols:
                        print(
                            f"DFVersionado2DFmerged: {counterName}: columnas desconocidas: {sorted(missingCols)}. ",
                            f"Columnas existentes: {sorted(list(newDF.columns))}. Ignorando contador.")
                        continue
                    areRowsDifferent = (DFcurrent.loc[changed, counterCols] != newDF.loc[changed, counterCols]).any(axis=1)
                    DFcurrent.loc[changed, counterName] += areRowsDifferent


        if len(added):
            DFcurrent = pd.concat([DFcurrent, newData], axis=0)

        timeStop = time()
        print(
            f"DFVersionado2DFmerged: {timeStop - timeStart}: commitDate: {commitDate} changed: {len(changed)} added: {len(added)}")

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
