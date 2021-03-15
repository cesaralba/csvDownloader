import csv

from lib.miscDataFrames import leeCSVdataset

COLDATES = ['fecha_informe']
COLIDX = ['zona_basica_salud', 'fecha_informe']
COLS2DROP = ['codigo_geometria']

ESTADSCAMBIO = {'c_conf_act_u14': ['casos_confirmados_activos_ultimos_14dias'],
                'c_conf_u14': ['casos_confirmados_ultimos_14dias'],
                'c_conf_tot': ['casos_confirmados_totales'],
                'ti_acum_u14': ['tasa_incidencia_acumulada_ultimos_14dias'],
                'ti_acum_act_u14': ['tasa_incidencia_acumulada_activos_ultimos_14dias'],
                'ti_acum_tot': ['tasa_incidencia_acumulada_total']}

csv.register_dialect('IDA', delimiter=';', lineterminator='\r\n')


def leeDatosMadDF(fname_or_handle, **kwargs):
    """
    """
    # COLLIST = COLIDX + [columna]
    kwargs.update({'encoding': 'latin-1'})

    myDF = leeCSVdataset(fname_or_handle, colIndex=COLIDX, cols2drop=COLS2DROP, colDates=COLDATES,
                         decimal=',', dialect='IDA', **kwargs)

    return myDF


def leeDatosMadFila(fname_or_handle, columnaObj, **kwargs):
    """
    """
    myDF = leeDatosMadDF(fname_or_handle, **kwargs)

    if columnaObj not in myDF.columns:
        colNamesStr = ",".join(sorted(myDF.columns.tolist()))
        print(f"leeDatosDFfila: Columna '{columnaObj}' no existe. Posibles: {colNamesStr}")
        return None

    result = myDF[[columnaObj]].reset_index()

    result[COLDATES[0]] = result[COLDATES[0]].dt.date

    return result.set_index(COLIDX).sort_index().T
