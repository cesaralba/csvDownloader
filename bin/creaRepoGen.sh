#!/bin/bash

function soLong {
  MSG=${1:-No msg}
  echo ${MSG}
  exit 1
}


if [ "x$1" = "x" ]
then
  echo "Parameter with name of file containing env vars not provided. I will assume they have been declared"
else
  ENVFILE=$1
  [ -f "${ENVFILE}" ] || soLong "Provided file containing env vars '${ENVFILE}' does not exist"
  source ${ENVFILE}
fi

[ -n "${DATADIR}" ] || soLong "DATADIR env var not specified"

GITDIR="${DATADIR}/.git"

[ -d ${DATADIR} ] || mkdir -p ${DATADIR} || soLong "Problems creting ${DATADIR}. Bye"

[ -d ${GITDIR} ] || git init ${DATADIR} || soLong "Problems creting repo in ${DATADIR}. Bye"

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


