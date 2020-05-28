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




