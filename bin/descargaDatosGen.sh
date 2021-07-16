#!/bin/bash

DATE=$(date +%Y%m%d%H%M)

function soLong() {
  MSG=${1:-No msg}
  echo ${MSG}
  exit 1
}

if [ "x$1" != "x" ]; then
  ENVFILE=$1
  [ -f "${ENVFILE}" ] || soLong "Provided filename '${ENVFILE}' with env variable settings does not exist"
  source ${ENVFILE}
fi

#Parameters sanity check
[ -n "${DATADIR}" ] || soLong "Variable DATADIR not set"
[ -n "${WRKDIR}" ] || soLong "Variable WRKDIR not set"
[ -n "${NEWFILE}" ] || soLong "Variable NEWFILE not set"
[ -n "${DATAFILE}" ] || soLong "Variable DATAFILE not set"
[ -n "${URLFILE}" ] || [ -n "${SRCFILE}" ] || soLong "Either variable URLFILE or SRCFILE must be set"

BRANCHDEF=${REMOTEBRANCH:-master}
NAMEDEF=${REMOTENAME:-origin}

DOCOMMIT=0

#Create destination repo if it doesn't exist
[ -d "${DATADIR}" ] || mkdir -p ${DATADIR} || soLong "Problems creating ${DATADIR}. Bye"

GITDIR="${DATADIR}/.git"

if [ ! -d ${GITDIR} ]
then
  # Clone remote or create from blank
  if [ -n "${REMOTEGITURL}" ]
  then
    echo "Remote: -> ${REMOTEGITURL}"
    git clone ${REMOTEGITURL} ${DATADIR}  || soLong "Problems cloning ${REMOTEGITURL}. Bye"
    (         #-b
      cd ${DATADIR}  || soLong "Problemas changing to ${DATADIR}. Bye"
      git checkout ${BRANCHDEF} || git checkout -b ${BRANCHDEF} || soLong "Problems creating/switching to ${BRANCHDEF}. Bye"
    ) || soLong "Problems cloning repo (${DATADIR})"

  else
    git init -b ${BRANCHDEF} ${DATADIR} || soLong "Problems creating repo in ${DATADIR}. Bye"
  fi

  # Config user and email
  (
    cd ${DATADIR}  || soLong "Problemas changing to ${DATADIR}. Bye"
    git config user.name "${USERNAME:-$(whoami)}" || soLong "Problems configuring user.name . Bye"
    git config user.email "${USERMAIL:-"my@email"}" || soLong "Problems configuring user.mail . Bye"
  ) || soLong "Problems configuring user data (${DATADIR})"
fi


#Preparing work area
[ -d ${WRKDIR} ] || mkdir -p ${WRKDIR} || soLong "Problems creating ${WRKDIR}. Bye"
[ -f ${NEWFILE} ] || rm -f ${NEWFILE} || soLong "Problems creating ${NEWFILE}. Bye"

if [ -n "${URLFILE}" ]; then
  MSG="Date:${DATE} source ${URLFILE}"
  wget -q -O ${NEWFILE} ${URLFILE} || soLong "Problems downloading ${URLFILE}. Bye"
else
  MSG="Date:${DATE} source ${SRCFILE}"
  cp -f ${SRCFILE} ${NEWFILE} || soLong "Problems copying ${SRCFILE}. Bye"
fi


ABSDATADIR=$(readlink -e ${DATADIR})

if [ -f ${DATAFILE} ]; then
  ABSDATAFILE=$(readlink -e ${DATAFILE})
  RELDATAFILE=${ABSDATAFILE#${ABSDATADIR}/}

  diff -q ${NEWFILE} ${DATAFILE}
  RES=$?
  if [ ${RES} != 0 ]; then
    echo "Download: ${MSG}"
    cp ${NEWFILE} ${DATAFILE} || soLong "Problems copying ${NEWFILE} to ${DATAFILE}. Bye"

    (
      cd $DATADIR
      git diff --shortstat
    )
    (
      cd $DATADIR
    git add ${RELDATAFILE} || soLong "Can't add ${ABSDATAFILE} to repo. Bye"
    )  || soLong "Problems on add. Bye (${DATADIR})"
    DOCOMMIT=1
  fi
else
  cp ${NEWFILE} ${DATAFILE} || soLong "Problems copying ${NEWFILE} to ${DATAFILE}. Bye"
  ABSDATAFILE=$(readlink -e ${DATAFILE})
  RELDATAFILE=${ABSDATAFILE#${ABSDATADIR}/}
  (
    cd $DATADIR
    git add ${RELDATAFILE} || soLong "Can't add ${ABSDATAFILE} to repo. Bye"
  ) || soLong "Problems on add. Bye (${DATADIR})"
  DOCOMMIT=1
fi

PREVCOMMIT=$(
  cd $DATADIR
  git rev-parse HEAD
)
if [ ${DOCOMMIT} != 0 ]; then
  (
    cd $DATADIR
    git commit -q ${RELDATAFILE} -m "${MSG}" || soLong "Can't commit ${ABSDATAFILE}. Bye"
  ) || soLong "Problems on commit. Bye (${DATADIR})"

  if [ -n "${REMOTEGITURL}" ]; then
    (
      cd $DATADIR
      git remote -v | grep -q ${REMOTEGITURL}
    )
    RES=$?
    if [ $RES != 0 ]; then
      (
        cd ${DATADIR}  || soLong "Problemas changing to ${DATADIR}. Bye"
        git remote add ${NAMEDEF} ${REMOTEGITURL} || soLong "Can't create remote ${NAMEDEF} -> ${REMOTEGITURL}. Bye"
      ) || soLong "Problems creating remote '${NAMEDEF}' -> '${REMOTEGITURL}' (${DATADIR})"
    fi
    (
      cd $DATADIR
      NAMEFROMREMOTE=$(git remote -v | grep ${REMOTEGITURL} | cut -f1  | sort | uniq)
      git push --set-upstream -q ${NAMEFROMREMOTE} ${BRANCHDEF} || soLong "Can't push to remote ${NAMEFROMREMOTE}-> ($(git remote -v | grep ${REMOTEGITURL}). Bye"
    ) || soLong "Problems pushing to remote '${REMOTEGITURL}' (${DATADIR})"
  fi


fi
CURRCOMMIT=$(
  cd $DATADIR
  git rev-parse HEAD
)

if [ "${PREVCOMMIT}" != "${CURRCOMMIT}" ]; then
  if [ "x${FOLLOWUPSCRIPT}" != "x" ]; then
    [ -e ${FOLLOWUPSCRIPT} ] || soLong "Script de continuación ${FOLLOWUPSCRIPT} no existe"
    [ -x ${FOLLOWUPSCRIPT} ] || soLong "Script de continuación ${FOLLOWUPSCRIPT} no ejecutable"
    ${FOLLOWUPSCRIPT} ${ENVFILE}
  fi
fi
