#!/usr/bin/env bash
# Script to fetch all dependencies of JupyterHub for offline installation

# Set python and pip version
PIP='pip2.7'
PYTHON='python2.7'
PACKAGE_DIR='/home/kentt/tmp/'
INSTALL_PAC='pandas'

##########################################

$PIP install virtualenv

set -ex

if [ "${PYTHON//[A-Z]/}" = "2.7" ]
then
    VENV_CMD='virtualenv'
else
	VENV_CMD='venv'
fi

$PYTHON -m $VENV_CMD $PACKAGE_DIR$INSTALL_PAC/wheel-env
source $PACKAGE_DIR$INSTALL_PAC/wheel-env/bin/activate

$PIP install --upgrade setuptools pip wheel
mkdir $PACKAGE_DIR$INSTALL_PAC && cd $PACKAGE_DIR$INSTALL_PAC

$PIP install $INSTALL_PAC 

# record all the packages we have installed, including dependencies in a new requirements.txt
$PIP freeze | grep -v "pkg-resources" > $PACKAGE_DIR$INSTALL_PAC/$INSTALL_PAC-requirements.txt
# Build wheel directory from requirements file:
$PIP wheel -r $PACKAGE_DIR$INSTALL_PAC/$INSTALL_PAC-requirements.txt -w $PACKAGE_DIR$INSTALL_PAC/wheels


tar -czvf $PACKAGE_DIR$INSTALL_PAC.tar.gz -C $PACKAGE_DIR $INSTALL_PAC
rm -rf $PACKAGE_DIR$INSTALL_PAC
