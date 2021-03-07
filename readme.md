# Versionado de datasets de series temporales

El contenido de este repositorio contiene herramientas para la descarga y almacenamiento en un sistema de control de versiones (tipo GIT) de datasets que contienen series temporales. El resultado final es una serie temporal de series temporales.

## Orígenes

Uno de los datasets que alcanzaron notoriedad de entre todo el aluvión de datasets públicos que aparecieron o se hicieron conocidos durante la pandemia de COVID-19 en 2020 fue el de [MOMO](https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html#nacional).

El MOMO [es](https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html#que-es-momo)

![Definición de MOMO (2020-02-20)](doc/gfx/DefinicionMOMO.20210220.png)

A efectos prácticos es una base de datos circular con datos de 749 días en la que cada día entran 240 datos correspondientes al día anterior a la descarga y desaparecen otros 240. Con tomas regulares (con una vez al día es suficiente)
se puede obtener información para periodos mayores a la rotación original, observar la actualización de los datos que corresponden a determinada fecha o la evolución agregada de los datos en el sistema.

El dataset permitía, con sus limitaciones, hacerse una idea de la magnitud de la tragedia (ver cuánta gente estaba muriendo que no estaba previsto que lo hiciese)
y su evolución (las olas). Con el paso del tiempo la curiosidad se amplió a observar cómo cambiaban los datos e imaginar cuál era la logística asociada al dataset.

Tengo la política de _si se va a hacer una cosa 3 veces, se automatiza_ y experiencia de problemas similares de descarga periódica de datos así que el contenido de este repositorio era la respuesta esperable. Con el tiempo he ido automatizando las cosas y generalizando para poder tratar datasets diferentes (como los de la
[comunidad de Madrid](https://datos.comunidad.madrid/catalogo/dataset/covid19_tia_muni_y_distritos) relativos al COVID).

### ¿Qué información se puede obtener con un dataset almacenado así?

Un dataset almacenado así añade la siguiente información al original:

* Serie temporal más larga. Por tratarse de una base de datos rotatoria, los datos de los primeros días de cada fichero habrán desaparecido en tomas posteriores. Al tener muchas tomas, se puede obtener una serie temporal que exceda los 749 días originales.
* Observar cómo evolucionan los datos de correspondientes a una fecha en el tiempo y cómo converge el valor. Aunque existen estadísticas [aquí](https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html#notificacion) se pueden hacer estudios diferentes.

## Requisitos generales e inconvenientes

La intención al crear este tipo de herramientas es automatizar las partes mecánicas para poder desentenderse de ese tipo de tareas y centrarse en mirar los datasets que es lo interesante.

Para ello se necesita una plataforma que pueda correr los programas contenidos en este repositorio de manera periódica (idealmente), desatendida y con acceso al dataset por internet.

Los scripts de descarga y versionado usan _shell scripts_ de _bash_, _wget_
(sí, soy un clásico) y _git_. Las herramientas de postprocesado están hechas en
_python_ con _pandas_. Una máquina corriendo un Unix (Linux, MacOS...) serviría tal cual si tienen disponibles esas herramientas. Otros entornos podrían requerir más trabajo para ponerlo en marcha.

Si bien el procedimiento de descarga y almacenamiento de los dataset es relativamente rápido. La acumulación de datos puede hacer que su tratamiento de los mismos lleve tiempo puede ser necesario que el tratamiento posterior de los datos se realice también como parte de las tareas periódicas o cambios en la estrategia del tratamiento de los datos ya que puede verse limitado por las capacidades (RAM) del sistema que hace el tratamiento (el uso de Big Data se debe empezar a considerar cuando el PC
del analista no da más de sí). Se agradecen estrategias alternativas o mejoras a través de las secciones de "Issues" y "Pull Requests" que tiene el repositorio.

Ejemplos (el tamaño del dataset del MOMO es 179760 filas y 6 columnas ocupando unos 17MB, desde que se empezó a usar esta herramienta se han descargado 250 días):

* El proceso de la diferencia entre los datos acumulados y los descargados para actualizar el último valor correspondiente a cada fecha y contar cuántos registros han cambiado en cada descarga lleva unos 7 segundos de media (se están comparando datasets de que hay que cargar y comparar). El tratamiento de 250 días descargados de MOMO supondría aproximadamente media hora.
* Para estudiar la evolución de los datos de fallecidos que corresponden a una fecha habría que leer todas las versiones (se puede limitar para tener en cuenta la rotación). Si se quieren almacenar todas las entradas en un dataframe, este tendría (749 + 250) * 240 columnas y 250 filas. Si suponemos que de las columnas del dataframe original sólo nos interesa la columna de los fallecidos y no las cinco restantes de estadísticas, son 60 millones de enteros de los que 15 millones son NAs de
  triángulos en los lados de datos que aparecen y salen de la BD por la rotación.

## Instrucciones de uso

### Funcionamiento general

En el cron se configura un script [lanzador](bin/actualizaDatosCron.sh) que tiene como parámetro un fichero con la lista de datasets que se van a descargar. El lanzador hace lo siguiente 
* descarga el repositorio del código en un directorio de trabajo
* ejecuta el programa de [descarga](bin/descargaDatosGen.sh) con cada una de las líneas del fichero que se pasa como parámetro. Este fichero contiene ficheros de _entorno_ en los que se pasan los parámetros de ejecución.

El comportamiento general del lanzador se controla con otro fichero con variables de entorno que debe estar alojado en */etc/sysconfig/GitTimeSeries*. Hay un [ejemplo](etc/sysconfigSAMPLE)

El programa asume que tiene acceso y permisos para hacer todo lo que necesita.

### Flujo general

* Localizar un dataset susceptible de ser versionado (de texto para poder aprovechar lo que ofrece git para control de versiones, con una variación de datos relativamente baja) y guardar su URL (que se almacenará en la variable _URLFILE_) para descargarlo con wget (petición GET normal).
* Crear un directorio (que se almacenará en la variable _DATADIR_) en la máquina que va a alojar la serie descargada e inicializarlo como repositorio _git_. Si se desea puede emplearse el programa [bin/creaRepoGen.sh](bin/creaRepoGen.sh).
* Decidir un nombre para el fichero (que se almacenará, path completo en la variable _DATAFILE_ ). El nombre no tiene por qué ser el del fichero descargado.
* Opcionalmente se puede crear un repositorio en un SCM remoto (_github.com_ por ejemplo) al que se subirá el local. Se deberá configurar en un destino _origin_ y para que la subida se haga sin necesidad de interactividad (sugerencia, clave SSH autorizada y uso de la variable de entorno _GIT_SSH_ para controlar la conexión a GIT)
* Localizar un directorio para dejar ficheros temporales (cuya ubicación se almacenará en la variable _WRKDIR_) y elegir un nombre de fichero dentro de ese directorio para el fichero que se descargue (cuya ubicación se almacenará, path completo, en la variable _WRKDIR_)  
* Crear un fichero de entorno que contendrá todas las variables antes mencionadas (más información, [aquí](#fichero-de-entorno))
* Si es necesario realizar postprocesado, en caso de que haya habido cambios en el repositorio, se invocará el script indicado en la variable de entorno _FOLLOWUPSCRIPT_.

### Configuración del lanzador

El contenido del fichero en */etc/sysconfig/GitTimeSeries* es
~~~
export GTS_REPO="https://github.com/cesaralba/csvDownloader"
export GTS_CODEDIR=DONDEHAREELGITCLONE
~~~

* *GTS_REPO* es la ubicación del repositorio en _github.com_. Modificar si se hace un fork o se hace desde otro sitio.
* *GTS_CODEDIR* es el lugar en el disco donde se hará el clonado del repositorio. Este directorio se creará en cada ejecución por lo que no debería ser usado para desarrollo u otros fines.

### Programa de descargas

El programa de descargas [bin/descargaDatosGen.sh](bin/descargaDatosGen.sh) hace lo siguiente (las ubicaciones de todo se controlan mediante variables contenidas en un fichero cuya ubicación se pasa como parámetro):
* Crea el directorio de trabajo (temporal) si no existe ya
* Borra el último fichero descargado del directorio de trabajo
* Descarga el dataset (fichero único) en el directorio de trabajo
* Compara el fichero descargado con el fichero versionado (si lo hay). Si hay diferencias (o no hay fichero versionado) lo copia a la ubicación versionada y prepara el commit.
* Si el repositorio tiene una ubicación remota, hace el push al SCM remoto.
* Si hay configurado un programa de postprocesado, lo ejecuta.

Para todo (descarga de ficheros, acceso al SCM remoto, ejecución del script...) asume que tiene permisos.

###  Fichero de entorno

Es un fichero en el que se fijan los valores que controlan una descarga. La sintaxis es la de un fichero de variables de entorno de _bash_ y los programas que lo emplean lo cargarán con _source_.

Hay un ejemplo en [etc/envSAMPLE](etc/envSAMPLE) y el contenido es el siguiente.

~~~
DATADIR=WhereDataWillBeStored/data
WRKDIR=WhereDataWillBeStored/new
NEWFILE=${WRKDIR}/newData.csv
DATAFILE=${DATADIR}/data.csv

#La URL debe ser un único fichero DE TEXTO
URLFILE="http://WhereIGetDataFrom"

#REMOTEGITURL=NoSeUsaDeMomento

# Si se va a usar una acción de postprocesado
#FOLLOWUPSCRIPT="ScriptThatWillRunAfterCommit"
#GTS_SCRIPTFILE="PythonScriptThatWouldBeUsedIfFOLLOWUPSCRIPTis_bin/pythonPostAction.sh"

#export GTS_VENV=DONDECREAREELVENVPARAELANALISIS
#export GTS_INFILE="MiArchivoDeCacheEjecucionAnterior"
#export GTS_OUTFILE="FicheroResultadoDelPostprocesado"
~~~
Las variables que es necesario incluir son:
* *DATADIR* Ubicación del fichero de datos (realmente no pero por comodidad general mejor que lo sea). *DEBE* estar dentro de un repositorio _git_. Si se quieren usar  [bin/creaRepoGen.sh](bin/creaRepoGen.sh) o [bin/pythonPostAction.sh](bin/pythonPostAction.sh), la base del repositorio será el contenido de esta variable.
* *DATAFILE* Nombre (path completo para simplificar las cosas) del fichero que contiene el dataset. Debe estar dentro de _DATADIR_ (para evitar problemas sin carpetas intermedias).
* *WRKDIR* Ubicación del fichero temporal. No es demasiado importante el sitio, basta con que tenga espacio y permisos.
* *NEWFILE* Fichero que se descargará de la fuente y se comparará con el último válido. Este fichero se borrará en cada ejecución de [bin/descargaDatosGen.sh](bin/descargaDatosGen.sh).
* *URLFILE* URL completa del fichero con el dataset. El programa hace un wget sin autenticación.
  
Las siguientes variables son opcionales y sirven para ejecutar un script de postprocesado si se produce una actualización en el dataset. La invocación del script incluye como parámetro este mismo fichero de entorno por lo que las variables de entorno que rijan el comportamiento deberían ir incluidas en este mismo fichero. 

Las variables que siguen a _FOLLOWUPSCRIPT_ son relevantes a [bin/pythonPostAction.sh](bin/pythonPostAction.sh). Scripts diferentes podrían necesitar variables diferentes. El flujo de trabajo de [bin/pythonPostAction.sh](bin/pythonPostAction.sh) se explica más adelante.   
* *FOLLOWUPSCRIPT* [Opcional] Ubicación del script. Se suministra [bin/pythonPostAction.sh](bin/pythonPostAction.sh) para ejecutar los scripts en Python 
* *GTS_SCRIPTFILE* Programa python que va a procesar el repositorio _git_
* *GTS_INFILE* Fichero que es la entrada del script indicado en *GTS_SCRIPTFILE*. Para que el conjunto sea eficiente, debería ser la salida de una ejecución anterior y que el programa sea incremental.   
* *GTS_OUTFILE* Fichero que es la salida del script indicado en *GTS_SCRIPTFILE*.  
* *GTS_VENV* es la ubicación del _virtual environment_ de _python_ que contiene las librerías necesarias para usar los scripts de postprocesado. Este _venv_ lo deberá  crear y actualizar el administrador cuando sea neceario. Se pueden encontrar instrucciones para crear el _venv_ [aquí](https://docs.python.org/3/library/venv.html). El listado de librerías necesarias para ejecutar el programa de postprocesado se puede encontrar en [gitTimeSeries/requirements.txt](gitTimeSeries/requirements.txt). En [gitTimeSeries/requirements-dev.txt](gitTimeSeries/requirements-dev.txt) se encuentran librerías que se pueden usar para un estudio de los datos descargados _offline_.

### Ejecución de postprocesado
Como se indica en la sección [anterior](#fichero-de-entorno), es posible configurar el descargador para que ejecute un programa en caso de que haya habido una actualización en los datos. El patrón de ejecución es similar al del programa de descargas: un script/programa al que se le pasa como parámetro el mismo fichero (su ubicación) que se le pasó al programa de descargas.

Se suministra un [script operativo](bin/pythonPostAction.sh) y que puede servir de punto de partida si se quieren hacer alteraciones. El script lanza un programa _python_ que recorre el repositorio para anotar los cambios. 

El flujo de este programa es el siguiente:

* Carga el fichero de variables de entorno que se ha pasado como parámetro.
* Hace una comprobación de que todas las variables que necesita están definidas y son válidas.
* Si el virtual environment no está creado (no hay una instancia de _python_ donde debía), lo crea.
* Repasa la lista de módulos python necesarios (en [gitTimeSeries/requirements.txt](gitTimeSeries/requirements.txt)) y los carga/actualiza si es necesario.
* Calcula la ubicación del fichero de datos relativo (sacada de _DATAFILE_ y que trae ruta absoluta) a la raíz del repositorio (sacada de _DATADIR_) que serán parámetros del programa que se ejecute (sacado de _GTS_SCRIPTFILE_).
* Ejecuta el programa. El programa usa las variables _GTS_INFILE_ y _GTS_OUTFILE_ como datos de entrada y salida, respectivamente.   
* Si la ejecución es exitosa, pone a salvo el fichero (le añade la extensión _.prev_) que se usó de entrada (si el fichero existía) y copia el fichero de salida para que sea el fichero de entrada de la siguiente ejecución.

### Programación de la ejecución.

Para la ejecución de este programa la frecuencia debe estar acorde con la actualización del dataset y la frecuencia deseada de muestreo. En lo posible, hay que tratar de no sobrecargar el servidor remoto. Ejemplos: 
* el MOMO en principio sólo cambia una vez al día (en el casi un año de ejecución sólo ha un caso de más de una actualización el mismo día) e interesa tener muestra de todos los días. Se puede sondear dos veces al día para prevenir (a las 9 y a las 21, por ejemplo) si hay un problema con la primera ejecución.   

* para datos que se actualizan semanalmente con una vez al día sería suficiente

El mecanismo de programar dependerá del sistema. El venerable [_cron_](https://opensource.com/article/17/11/how-use-cron-linux) y su variante _anacron_ (tambien cubierto en el artículo) pueden resolver el 
problema.
