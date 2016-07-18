#!/bin/bash
#-------------------------------------------------------------------------------
# @author Elvin-Alin Sindrilaru - CERN
# @brief Script used by Jenkins to build EOS rpms
#-------------------------------------------------------------------------------

#************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************/

#-------------------------------------------------------------------------------
# Print help
#-------------------------------------------------------------------------------
function printHelp()
{
  echo "Usage:                                                               " 1>&2
  echo "${0} <branch_or_tag> <xrootd_tag> <build_number> <dst_path>          " 1>&2
  echo "  <branch_or_tag> branch name in the form of \"origin/master\" or tag" 1>&2
  echo "                  name e.g. 1.0.0 for which to build the project     " 1>&2
  echo "  <xrootd_tag>    XRootD tag version used for this build             " 1>&2
  echo "  <build_number>  build number value passed in by Jenkins            " 1>&2
  echo "  <platform>      build platform e.g. slc-6, el-7, fc-24             " 1>&2
  echo "  <architecture>  build architecture e.g. x86_64, i386               " 1>&2
  echo "  <dst_path>      destination path for the rpms built                " 1>&2
}

#-------------------------------------------------------------------------------
# Get the local branch name and dist tag for the rpms. For example local branch
# name of branch 'origin/master' is master. The dist tag for Scientific Linux 5
# can be 'slc5' or 'el5'.
# Function sets two global variables BRANCH and DIST.
#-------------------------------------------------------------------------------
function getLocalBranchAndDistTag()
{
  if [[ ${#} -ne 2 ]]; then
    echo "Usage:                                                               " 1>&2
    echo "${0} <branch_or_tag> <platform>                                      " 1>&2
    echo "  <branch_or_tag> branch name in the form of \"origin/master\" or tag" 1>&2
    echo "                  name e.g. 1.0.0 for which to build the project     " 1>&2
    echo "  <platform>      build platform e.g. slc-6, el-7, fc-24             " 1>&2
    exit 1
  fi

  local BRANCH_OR_TAG=${1}
  local PLATFORM=${2}
  local TAG_REGEX="^[04]+\..*$"
  local TAG_REGEX_AQUAMARINE="^0.3.*$"
  local TAG_REGEX_CITRINE="^4.*$"

  # If this is a tag get the branch it belogs to
  if [[ "${BRANCH_OR_TAG}" =~ ${TAG_REGEX} ]]; then
      if [[ "${BRANCH_OR_TAG}" =~ ${TAG_REGEX_AQUAMARINE} ]]; then
	  BRANCH="aquamarine"
      elif [[ "${BRANCH_OR_TAG}" =~ ${TAG_REGEX_CITRINE} ]]; then
	  BRANCH="citrine"
      fi
  else
      BRANCH=$(basename ${BRANCH_OR_TAG})
      # For beryl_aquamarine use aquamarine as release
      if [[ "${BRANCH}" == "beryl_aquamarine" ]]; then
	  BRANCH="aquamarine"
      elif [[ "${BRANCH}"  == "master" ]]; then
	  BRANCH="citrine"
      fi
  fi

  # For aquamarine still use the old ".slc-*" dist tag for SLC5/6
  if [[ "${BRANCH}" == "aquamarine" ]] ; then
      if [[ "${PLATFORM}" == "el-5" ]] || [[ "${PLATFORM}" == "el-6" ]]; then
	  DIST=".slc${PLATFORM: -1}"
      else
	  DIST=".${PLATFORM}"
      fi
  else
      # For any other branch use the latest XRootD release
      XROOTD_TAG="v4.3.0"
      DIST=".${PLATFORM}"
  fi

  # Remove any "-" from the dist tag
  DIST="${DIST//-}"

  echo "Local branch:         ${BRANCH}"
  echo "Dist tag:             ${DIST}"
}

#-------------------------------------------------------------------------------
# Main - when we are called the current BRANCH_OR_TAG is already checked-out and
#        the script must be run from the **same directory** where it resides.
#-------------------------------------------------------------------------------
if [[ ${#} -ne 6 ]]; then
    printHelp
    exit 1
fi

BRANCH_OR_TAG=${1}
XROOTD_TAG=${2}
BUILD_NUMBER=${3}
PLATFORM=${4}
ARCHITECTURE=${5}
DST_PATH=${6}

echo "Build number:         ${BUILD_NUMBER}"
echo "Branch or tag:        ${BRANCH_OR_TAG}"
echo "XRootD tag:           ${XROOTD_TAG}"
echo "Build platform:       ${PLATFORM}"
echo "Build architecture:   ${ARCHITECTURE}"
echo "Destination path:     ${DST_PATH}"
echo "Running in directory: $(pwd)"

# Exit script immediately if a command exists with a non-zero status
set -e

# Get local branch and dist tag for the RPMS
getLocalBranchAndDistTag ${BRANCH_OR_TAG} ${PLATFORM}

# Create cmake build directory and build without dependencies
cd ..; mkdir build; cd build
# Use cmake3 if installed, otherwise fallback to cmake command
CMAKE_EX="$(which cmake3)"
if test x"${CMAKE_EX}" == x -o ! -x "${CMAKE_EX}"; then
  CMAKE_EX=cmake
fi
${CMAKE_EX} .. -DPACKAGEONLY=1
# Create tarball
make dist
# Build the source RPMs
make srpm
SRC_RPM=$(ls *.src.rpm | grep -v clientsonly)
SRC_RPM_CLT=$(ls *.src.rpm | grep clientsonly)

# Get the mock configurations from gitlab
git clone ssh://git@gitlab.cern.ch:7999/dss/dss-ci-mock.git ../dss-ci-mock
# Prepare the mock configuration
head -n -1 ../dss-ci-mock/eos-templates/${PLATFORM}-${ARCHITECTURE}.cfg.in | sed "s/__XROOTD_TAG__/$XROOTD_TAG/" | sed "s/__BUILD_NUMBER__/${BUILD_NUMBER}/" > eos.cfg
# Add eos dependencies repos
echo -e '\n[eos-depend]\nname=EOS Dependencies\nbaseurl=http://dss-ci-repo.web.cern.ch/dss-ci-repo/eos/'${BRANCH}'-depend/'$PLATFORM'-'$ARCHITECTURE'/\ngpgcheck=0\nenabled=1 \nexclude=xrootd*\n' >> eos.cfg
echo -e '"""' >> eos.cfg

echo "Content of eos.cfg:"
cat eos.cfg

## Build the RPMs (with yum repo rpms)
#mock --yum --init --uniqueext="eos01" -r ./eos.cfg --rebuild ./eos-*.src.rpm --resultdir ../rpms -D "dist ${DIST}" -D "yumrpm 1"
# Build the RPMs (without yum repo rpms)
mock --yum --init --uniqueext="eos01" -r ./eos.cfg --rebuild ./${SRC_RPM} --resultdir ../rpms -D "dist ${DIST}"
# move the clients only srpm to the results dir and rename it after the one mock by rpmbuild as it is wrongly named by CPack
mv ./${SRC_RPM_CLT} ../rpms/$( cd ../rpms && ls *.src.rpm | sed 's/eos\-/eos\-clientsonly\-/g' )


# List of branches for CI YUM repo
BRANCH_LIST=('aquamarine' 'citrine')

# If building one of the production branches then push rpms to YUM repo
if [[ ${BRANCH_LIST[*]} =~ ${BRANCH} ]]; then
  cd ../rpms/
  # Get the release string length
  RELEASE_LEN=$(find . -name "eos-*.src.rpm" -print0 | awk -F "-" '{print $3;}' | awk -F "." '{print length($1);}')
  COMMIT_LEN=24

  # For not tagged builds the release string is 24 characters i.e date + git + commit_hash
  if [[ ${RELEASE_LEN} -eq ${COMMIT_LEN} ]]; then
    BUILD_TYPE="commit"
  else
    BUILD_TYPE="tag"
  fi

  # Make sure the directories are created and rebuild the YUM repo
  YUM_REPO_PATH="${DST_PATH}/${BRANCH}/${BUILD_TYPE}/${PLATFORM}/${ARCHITECTURE}"
  echo "Save RPMs in YUM repo: ${YUM_REPO_PATH}"
  aklog
  mkdir -p ${YUM_REPO_PATH}
  cp -f *.rpm ${YUM_REPO_PATH}
  createrepo --update -q ${YUM_REPO_PATH}
else
  echo "RPMs for branch ${BRANCH} are NOT saved in any YUM repository!"
fi
