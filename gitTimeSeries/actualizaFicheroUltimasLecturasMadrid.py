import pandas as pd
from configargparse import ArgumentParser

from lib.ComunidadMadrid import COLDATES, COLIDX, ESTADSCAMBIO, leeDatosMadDF
from lib.miscDataFrames import COLSADDEDMERGED, DFversioned2DFmerged, saveHistoricData, readHistoricData
from utils.misc import listize


# TODO: Logging como dios manda

def leeFicheroHistEntrada(fname, colzona, create=False):
    try:
        colsIndex = sorted(set(COLIDX + listize(colzona)))
        result = readHistoricData(fname, extraCols=COLSADDEDMERGED, colsIndex=colsIndex, colsDate=COLDATES,
                                  changeCounters=ESTADSCAMBIO)

    except FileNotFoundError as exc:
        if create:
            return None
        else:
            print(f"Archivo no encontrado '{fname}' y parámetro de creación (-c) no indicado. Adios")
            exit(1)
    except pd.errors.EmptyDataError as exc:
        print(
            f"Archivo '{fname}' vacío:{exc}. Borrar y volver a ejecutar programa con parámetro de creación (-c). Adios")
        exit(1)
    except UnicodeError as exc:
        print(
            f"Archivo '{fname}' con problemas:{exc}. Borrar y volver a ejecutar programa con parámetro de creación (-c). Adios")
        exit(1)
    except ValueError as exc:
        print(
            f"Archivo '{fname}' con problemas:{exc}. Borrar y volver a ejecutar programa con parámetro de creación (-c). Adios")
        exit(1)

    return result


def parse_arguments():
    descriptionTXT = "Lee un dataset versionado (fichero CSV en GIT) y genera un CSV con los últimos valores de cada datos"

    parser = ArgumentParser(description=descriptionTXT)

    parser.add('-v', dest='verbose', action="count", env_var='GTS_VERBOSE', required=False, help='', default=0)
    parser.add('-d', dest='debug', action="store_true", env_var='GTS_DEBUG', required=False, help='', default=False)

    parser.add('-i', dest='infile', type=str, env_var='GTS_INFILE', help='Fichero de entrada', required=False)
    parser.add('-o', dest='outfile', type=str, env_var='GTS_OUTFILE', help='Fichero de salida', required=True)

    parser.add('-r', dest='repoPath', type=str, env_var='GTS_REPOPATH', help='Directorio base del repositorio GIT',
               required=True)
    parser.add('-f', dest='csvPath', type=str, env_var='GTS_CSVPATH',
               help='Ubicación del fichero de dataset dentro del repositorio GIT', required=True)

    parser.add('-t', dest='colIndice', type=str, env_var='GTS_INDEXCOL',
               choices=['zona_basica_salud', 'municipio_distrito'],
               help='Columnas se van a usar como indice del dataframe', required=True)

    parser.add('-c', dest='create', action="store_true", env_var='GTS_CREATE', required=False,
               help='Inicializa el fichero si no existe ya', default=False)

    args = parser.parse_args()

    return args


def main(args):
    momoActual = leeFicheroHistEntrada(args.infile, colzona=args.colIndice,
                                       create=args.create) if 'infile' in args and args.infile else None

    try:
        result = DFversioned2DFmerged(args.repoPath, args.csvPath, readFunction=leeDatosMadDF, DFcurrent=momoActual,
                                      changeCounters=ESTADSCAMBIO, extraIndexes=args.colIndice)
    except ValueError as exc:
        print(exc)
        exit(1)

    saveHistoricData(result, args.outfile)


if __name__ == '__main__':
    args = parse_arguments()
    main(args)
