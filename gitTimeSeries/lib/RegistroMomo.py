import pandas as pd

from .miscDataFrames import estadisticaCategoricals, estadisticaFechaCambios, indexFillNAs, readCSVdataset

DEFAULTCOMMIT = [0]

COLIDX = ['fecha_defuncion', 'ambito', 'nombre_ambito', 'nombre_sexo', 'nombre_gedad']
COLS2DROP = ['cod_ambito', 'cod_ine_ambito', 'cod_sexo', 'cod_gedad']
INDEXNAREPLACER = {'nombre_ambito': 'España'}

VALORESAGRUP = {'nacional', 'todos', 'ccaa'}

ESTADSCAMBIO = {'cambObs': ['defunciones_observadas'],
                'cambEst': ['defunciones_estimadas_base', 'defunciones_estimadas_base_q01',
                            'defunciones_estimadas_base_q99', 'defunciones_atrib_exc_temp',
                            'defunciones_atrib_def_temp'],
                'provEst': {'columnaIndiceObj': 'nombre_ambito', 'columnasObj': 'defunciones_observadas',
                            'funcionCuenta': estadisticaCategoricals, 'valoresAgrupacion': VALORESAGRUP,
                            'valoresDescribe': ['unique', 'top', 'count']},
                'fechaEst': {'columnaIndiceObj': 'fecha_defuncion', 'columnasObj': 'defunciones_observadas',
                             'funcionCuenta': estadisticaFechaCambios, 'valoresAgrupacion': VALORESAGRUP},

                }
DATECOLS = ['fecha_defuncion', 'fechaCommit']


def leeDatosMomoFila(fname, columna):
    """
    Lee un fichero diario de Momo
    ( https://momo.isciii.es/public/momo/data, https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html#datos )
    y lo convierte en un dataframe usando como columna todas las columnas de clasificación y una de las de
    datos.
    :param fname: Nombre de fichero o handle de lectura de fichero
    :param columna: columna que se va a usar como datos. Una de
      defunciones_observadas: el número de defunciones observadas (incluye la corrección por retraso).
      defunciones_observadas_lim_inf: el límite inferior del invervalo de confianza de las defunciones observadas
                                      (debido a la corrección).
      defunciones_observadas_lim_sup: de forma equivalente, el límite superior.
      defunciones_esperadas: el número de defunciones esperadas, resultantes del modelo.
      defunciones_esperadas_q01: el límite inferior del intervalo de confianza de las defunciones esperadas,
                                 correspondiente al percentil 1 de la distribución.
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
    myDF = readCSVdataset(fname_or_handle, colIndex=COLIDX, cols2drop=COLS2DROP, colDates=['fecha_defuncion'], **kwargs)
    myDF.index = indexFillNAs(myDF.index, replacementValues=INDEXNAREPLACER)

    result = myDF[~myDF['defunciones_observadas'].isna()]

    return result.sort_index()
