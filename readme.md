# Versionado de datasests de series temporales

El contenido de este repositorio contiene herramientas para la descarga y 
almacenamiento en un sistema de control de versiones (tipo GIT) de datasets que 
contienen series temporales. El resultado final es una serie temporal de series 
temporales.

## Orígenes

Uno de los datasets que alcanzaron notoriedad de entre todo el aluvión de 
datasets públicos que aparecieron o se hicieron conocidos durante la pandemia de 
COVID-19 en 2020 fue el de [MOMO](https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html#nacional). 

El MOMO [es](https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html#que-es-momo) 

![Definición de MOMO (2020-02-20)](doc/gfx/DefinicionMOMO.20210220.png)

A efectos prácticos es una base de datos circular con datos de 749 días en la que 
cada día entran 240 datos correspondientes al día anterior a la descarga y 
desaparecen otros 240. Con tomas regulares (con una vez al día es suficiente) 
se puede obtener información para periodos mayores a la rotación original, 
observar la actualización de los datos que corresponden a determinada fecha o la 
evolución agregada de los datos en el sistema.

El dataset permitía, con sus limitaciones, hacerse una idea de la magnitud de la
tragedia (ver cuánta gente estaba muriendo que no estaba previsto que lo hiciese)
y su evolución (las olas). Con el paso del tiempo la curiosidad se amplió a 
observar cómo cambiaban los datos e imaginar cuál era la logística asociada al
dataset. 

Tengo la política de _si se va a hacer una cosa 3 veces, se automatiza_ y 
experiencia de problemas similares de descarga periódica de datos así que este 
repositorio era la respuesta esperable. Con el tiempo he ido automatizando las 
cosas y generalizando para poder tratar datasets diferentes (como los de la 
[comunidad de Madrid](https://datos.comunidad.madrid/catalogo/dataset/covid19_tia_muni_y_distritos) relativos al COVID).
  
### ¿Qué información se puede obtener con un dataset almacenado así?

Un dataset almacenado así aporta la siguiente información al original:
* Serie temporal más larga. Por tratarse de una base de datos rotatoria, los 
  datos de los primeros días de cada fichero habrán desaparecido en tomas 
  posteriores. Al tener muchas tomas, se puede obtener una serie temporal que 
  exceda los 749 días originales.
* ¿Cómo evolucionan los datos de correspondientes a una fecha en el tiempo? Aunque
  existen estadísticas [aquí](https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html#notificacion) se pueden hacer estudios diferentes.

## Requisitos generales e inconvenientes

La intención al crear este tipo de herramientas es automatizar las partes 
mecánicas para poder desentenderse de ese tipo de tareas y centrarse en mirar los
datasets que es lo interesante.

Para ello se necesita una plataforma que pueda correr los programas contenidos en
este repositorio de manera periódica (idealmente), desatendida y con acceso al 
dataset por internet.

Los scripts de descarga y versionado usan _shell scripts_ de _bash_, _wget_
(sí, soy un clásico) y _git_. Las herramientas de postprocesado están hechas en 
_python_ con _pandas_.

Si bien el procedimiento de descarga y almacenamiento de los dataset es 
relativamente rápido. La acumulación de datos puede hacer que su tratamiento de 
los mismos lleve tiempo puede ser necesario que el tratamiento posterior de los
datos se realice también como parte de las tareas periódicas o cambios en la 
estrategia del tratamiento de los datos ya que puede verse limitado por las 
capacidades (RAM) del sistema que hace el tratamiento (el uso de Big Data se debe
empezar a considerar cuando el PC del analista no da más de sí). Se agradecen 
estrategias alternativas o mejoras a través de las secciones de "Issues" y "Pull 
Requests" que tiene el repositorio.

Ejemplos (el tamaño del dataset del MOMO es 179760 filas y 6 columnas ocupando 
unos 17MB, desde que se empezó a usar esta herramienta se han descargado 250 días):
* El proceso de la diferencia entre los datos acumulados y los descargados para 
  actualizar el último valor correspondiente a cada fecha y contar cuántos 
  registros han cambiado en cada descarga lleva unos 7 segundos de media (se 
  están comparando datasets de  que hay que cargar y comparar). El tratamiento 
  de 250 días descargados de MOMO supondría aproximadamente media hora.
* Para estudiar la evolución de los datos de fallecidos que corresponden a una 
fecha habría que leer todas las versiones (se puede limitar para tener en cuenta 
  la rotación). Si se quieren almacenar todas las entradas en un dataframe, este
  tendría (749 + 250) * 240 columnas y 250 filas. Si suponemos que de las 
  columnas del dataframe original sólo nos interesa la columna de los fallecidos 
  y no las cinco restantes de estadísticas, son 60 millones de enteros de los que 
  15 millones son NAs de triángulos en los lados de datos que aparecen y salen 
  de la BD por la rotación.
  

## 