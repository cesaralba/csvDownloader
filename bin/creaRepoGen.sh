#!/bin/bash

function adiosMundoCruel {
  MSG=${1:-No msg}
  echo ${MSG}
  exit 1
}


if [ "x$1" = "x" ]
then
  adiosMundoCruel "No se ha especificado parametro con variables de entorno"
else
  ENVFILE=$1
fi

[ -f "${ENVFILE}" ] || adiosMundoCruel "Fichero con entorno '${ENVFILE}' no existe"

source ${ENVFILE}

[ -n "${DATADIR}" ] || adiosMundoCruel "No se ha especificado la variable DATADIR"

GITDIR="${DATADIR}/.git"

[ -d ${DATADIR} ] || mkdir -p ${DATADIR} || adiosMundoCruel "Problemas creando ${DATADIR}. Bye"

[ -d ${GITDIR} ] || git init ${DATADIR} || adiosMundoCruel "Problemas creando repo en ${DATADIR}. Bye"

cat <<FIN

Repo creado. La vinculación con un repo externo queda fuera del alcance de este script


No obstante, el procedimiento es:

* Crear el repo remoto (vacío, sin readmes ni mierdas) en un servicio de repositorios (github.com o similar)
* Elegir la URL para usar (SSH o HTTPS) para enviar cosas que llamaremos URLGITHUB
* Ir a ${DATADIR}

  git remote add origin URLGITHUB


Tenga en cuenta que el programa de descarga está pensado para no ser interactivo. Eso también tendrá que 
arreglarlo.

FIN


