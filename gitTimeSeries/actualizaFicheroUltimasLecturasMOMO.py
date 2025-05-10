import gc
import sys

import pandas as pd
from configargparse import ArgumentParser

from lib.RegistroMomo import COLIDX, DATECOLS, ESTADSCAMBIO, leeDatosMomoDF
from lib.miscDataFrames import COLSADDEDMERGED, DFversioned2DFmerged, saveHistoricData, readHistoricData


# TODO: Logging como dios manda

def leeFicheroEntrada(fname: str, create: bool = False, chunkSize: int = 0):
    try:
        result = readHistoricData(fname, extraCols=COLSADDEDMERGED, colsIndex=COLIDX, colsDate=DATECOLS,
                                  changeCounters=ESTADSCAMBIO, chunkSize=chunkSize)
    except FileNotFoundError:
        if create:
            return None
        print(f"Archivo no encontrado '{fname}' y parámetro de creación (-c) no indicado. Adios")
        sys.exit(1)
    except pd.errors.EmptyDataError as exc:
        print(
            f"Archivo '{fname}' vacío:{exc}. Borrar y volver a ejecutar programa con parámetro "
            f"de creación (-c). Adios")
        sys.exit(1)
    except UnicodeError as exc:
        print(f"Archivo '{fname}' con problemas:{exc}. Borrar y volver a ejecutar programa "
              f"con parámetro de creación (-c). Adios")
        sys.exit(1)
    except ValueError as exc:
        print(
            f"Archivo '{fname}' con problemas:{exc}. Borrar y volver a ejecutar programa con parámetro "
            f"de creación (-c). Adios")
        sys.exit(1)

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

    parser.add('-c', dest='create', action="store_true", env_var='GTS_CREATE', required=False,
               help='Inicializa el fichero si no existe ya', default=False)
    parser.add('-x', dest='skipBadCommits', action="store_true", env_var='GTS_SKIPERRORS', required=False,
               help='Ignora commits incorrectos', default=False)
    parser.add('-z', dest='chunkSize', action="store", env_var='GTS_CHUNKSIZE', required=False, type=int,
               help='Number of lines per read', default=None)

    parser.add('--tempFile', dest='tempFile', type=str, env_var='TEMP_FILE', required=False,
               help='Store intermediate results', default=None)
    parser.add('--tempStep', dest='tempStep', type=int, env_var='TEMP_STEP', required=False,
               help='Store every n commits', default=0)

    args = parser.parse_args()

    return args


def main(args):
    gc.enable()
    momoActual = leeFicheroEntrada(fname=args.infile, create=args.create,
                                   chunkSize=args.chunkSize) if 'infile' in args and args.infile else None

    result = DFversioned2DFmerged(args.repoPath, args.csvPath, readFunction=leeDatosMomoDF, DFcurrent=momoActual,
                                  changeCounters=ESTADSCAMBIO, backupFile=args.tempFile, backupStep=args.tempStep,
                                  skipBadCommits=args.skipBadCommits, usePrevDF=False, chunksize=args.chunkSize)

    saveHistoricData(result, args.outfile)


if __name__ == '__main__':
    args = parse_arguments()
    main(args)
