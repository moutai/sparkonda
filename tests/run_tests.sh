#!/usr/bin/env bash

#conda create -n sparkonda-test-env --yes pip nose coverage setuptools numpy scipy scikit-learn python=2.7
#pip install py4j
#export SPARK_HOME=path_to_spark_home
#export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
#export PYTHONPATH=path_to/sparkonda:$PYTHONPATH

source activate sparkonda-test-env

nosetests -s --nologcapture
