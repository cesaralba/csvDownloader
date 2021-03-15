import pandas as pd
from configargparse import ArgumentParser

from lib.RegistroMomo import COLSADDED, COLIDX, DATECOLS, ESTADSCAMBIO, leeDatosMomoDF
from lib.miscDataFrames import DFVersionado2DFmerged, leeDatosHistoricos, grabaDatosHistoricos


# TODO: Logging como dios manda

def leeFicheroEntrada(fname, create=False):
    try:
        result = leeDatosHistoricos(fname, extraCols=COLSADDED, colsIndex=COLIDX, colsDate=DATECOLS,
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

    parser.add('-c', dest='create', action="store_true", env_var='GTS_CREATE', required=False,
               help='Inicializa el fichero si no existe ya', default=False)

    args = parser.parse_args()

    return args


def main(args):
    momoActual = leeFicheroEntrada(args.infile, args.create) if 'infile' in args and args.infile else None

    result = DFVersionado2DFmerged(args.repoPath, args.csvPath, readFunction=leeDatosMomoDF, DFcurrent=momoActual,
                                   changeCounters=ESTADSCAMBIO)

    grabaDatosHistoricos(result, args.outfile)


if __name__ == '__main__':
    args = parse_arguments()
    main(args)
