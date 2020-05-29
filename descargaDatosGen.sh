#!/bin/bash

DATE=$( date +%Y%m%d%H%M )

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
[ -n "${WRKDIR}" ] || adiosMundoCruel "No se ha especificado la variable WRKDIR"
[ -n "${NEWFILE}" ] || adiosMundoCruel "No se ha especificado la variable NEWFILE"
[ -n "${DATAFILE}" ] || adiosMundoCruel "No se ha especificado la variable DATAFILE"
[ -n "${URLFILE}" ] || adiosMundoCruel "No se ha especificado la variable URLFILE"

DOCOMMIT=0

[ -d ${WRKDIR} ] || mkdir -p ${WRKDIR} || adiosMundoCruel "Problemas creando ${WRKDIR}. Bye"

[ -f ${NEWFILE} ] || rm -f ${NEWFILE} || adiosMundoCruel "Problemas borrando ${NEWFILE}. Bye"

wget -q -O ${NEWFILE} ${URLFILE} || adiosMundoCruel "Problemas descargando ${URLFILE}. Bye"

if [ -f ${DATAFILE} ]
then
  diff -q ${NEWFILE} ${DATAFILE}
  RES=$?
  if [ ${RES} != 0 ]
  then
    echo "Descarga: ${DATE}"
    diff ${NEWFILE} ${DATAFILE}
    cp ${NEWFILE} ${DATAFILE} || adiosMundoCruel "Problemas copiando de ${NEWFILE} a  ${DATAFILE}. Bye"
    (cd $DATADIR ; git add ${DATAFILE} || adiosMundoCruel "No puedo añadir ${DATAFILE} a repo. Bye")
    DOCOMMIT=1
  fi
else
  cp ${NEWFILE} ${DATAFILE} || adiosMundoCruel "Problemas copiando de ${NEWFILE} a  ${DATAFILE}. Bye"
  (cd $DATADIR ; git add ${DATAFILE} || adiosMundoCruel "No puedo añadir ${DATAFILE} a repo. Bye")
  DOCOMMIT=1
fi

if [ ${DOCOMMIT} != 0 ]
then
  MSG="Fecha:${DATE} fuente ${URLFILE}"
  (cd $DATADIR ; git commit -q ${DATAFILE} -m "${MSG}" || adiosMundoCruel "No puedo añadir ${DATAFILE} a repo. Bye")

  (cd $DATADIR ; git remote  | grep -q origin)
  RES=$?
  if [ $RES = 0 ]
  then
    (cd $DATADIR ; git push -u -q origin master || adiosMundoCruel "No puedo hacer push a remoto $(git remote -v ). Bye")
  fi
fi




