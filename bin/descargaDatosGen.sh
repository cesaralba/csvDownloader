#!/bin/bash

DATE=$(date +%Y%m%d%H%M)

function adiosMundoCruel() {
  MSG=${1:-No msg}
  echo ${MSG}
  exit 1
}

if [ "x$1" != "x" ]; then
  ENVFILE=$1
  [ -f "${ENVFILE}" ] || adiosMundoCruel "Fichero con entorno '${ENVFILE}' no existe"

  source ${ENVFILE}
fi

[ -n "${DATADIR}" ] || adiosMundoCruel "No se ha especificado la variable DATADIR"
[ -n "${WRKDIR}" ] || adiosMundoCruel "No se ha especificado la variable WRKDIR"
[ -n "${NEWFILE}" ] || adiosMundoCruel "No se ha especificado la variable NEWFILE"
[ -n "${DATAFILE}" ] || adiosMundoCruel "No se ha especificado la variable DATAFILE"
[ -n "${URLFILE}" ] || [ -n "${SRCFILE}" ] || adiosMundoCruel "Se necesita especificar o la variable URLFILE o la variable SRCFILE"

BRANCHDEF=${REMOTEBRANCH:-master}
NAMEDEF=${REMOTENAME:-origin}

DOCOMMIT=0

[ -d ${WRKDIR} ] || mkdir -p ${WRKDIR} || adiosMundoCruel "Problemas creando ${WRKDIR}. Bye"

[ -f ${NEWFILE} ] || rm -f ${NEWFILE} || adiosMundoCruel "Problemas borrando ${NEWFILE}. Bye"

if [ -n "${URLFILE}" ]; then
  MSG="Fecha:${DATE} fuente ${URLFILE}"
  wget -q -O ${NEWFILE} ${URLFILE} || adiosMundoCruel "Problemas descargando ${URLFILE}. Bye"
else
  MSG="Fecha:${DATE} fuente ${SRCFILE}"
  cp -f ${SRCFILE} ${NEWFILE} || adiosMundoCruel "Problemas copiando ${SRCFILE}. Bye"
fi

if [ -f ${DATAFILE} ]; then
  diff -q ${NEWFILE} ${DATAFILE}
  RES=$?
  if [ ${RES} != 0 ]; then
    echo "Descarga: ${MSG}"
    cp ${NEWFILE} ${DATAFILE} || adiosMundoCruel "Problemas copiando de ${NEWFILE} a  ${DATAFILE}. Bye"

    (
      cd $DATADIR
      git diff --shortstat
    )
    (
      cd $DATADIR
      git add ${DATAFILE} || adiosMundoCruel "No puedo añadir ${DATAFILE} a repo. Bye"
    )
    DOCOMMIT=1
  fi
else
  cp ${NEWFILE} ${DATAFILE} || adiosMundoCruel "Problemas copiando de ${NEWFILE} a  ${DATAFILE}. Bye"
  (
    cd $DATADIR
    git add ${DATAFILE} || adiosMundoCruel "No puedo añadir ${DATAFILE} a repo. Bye"
  )
  DOCOMMIT=1
fi

PREVCOMMIT=$(
  cd $DATADIR
  git rev-parse HEAD
)
if [ ${DOCOMMIT} != 0 ]; then
  (
    cd $DATADIR
    git commit -q ${DATAFILE} -m "${MSG}" || adiosMundoCruel "No puedo añadir ${DATAFILE} a repo. Bye"
  )

  (
    cd $DATADIR
    git remote | grep -q ${NAMEDEF}
  )
  RES=$?
  if [ $RES = 0 ]; then
    (
      cd $DATADIR
      git push -q ${NAMEDEF} ${BRANCHDEF} || adiosMundoCruel "No puedo hacer push a remoto ${NAMEDEF}-> ($(git remote -v | grep ${NAMEDEF}). Bye"
    )
  fi
fi
CURRCOMMIT=$(
  cd $DATADIR
  git rev-parse HEAD
)

if [ "${PREVCOMMIT}" != "${CURRCOMMIT}" ]; then
  if [ "x${FOLLOWUPSCRIPT}" != "x" ]; then
    [ -e ${FOLLOWUPSCRIPT} ] || adiosMundoCruel "Script de continuación ${FOLLOWUPSCRIPT} no existe"
    [ -x ${FOLLOWUPSCRIPT} ] || adiosMundoCruel "Script de continuación ${FOLLOWUPSCRIPT} no ejecutable"
    ${FOLLOWUPSCRIPT} ${ENVFILE}
  fi
fi
