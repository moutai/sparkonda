# # create a new conda env to test with if you don't have one:
# # conda create -n sparkonda-test python pandas scikit-learn
#
# #Add the sparkonda file to the cluster workers
"""
sc.addPyFile('dev/sparkonda/sparkonda/sparkonda.py')

import sparkonda as skon
try: import sparkonda.sparkonda as skon
except:print 'shell version using the distributed pyfile'
from os.path import expanduser
home_dir = expanduser("~")

#Edit to match your conda env name
skon.CONDA_ENV_NAME = 'sparkonda-test'
#Edit this path to match your conda env location
skon.CONDA_ENV_LOCATION = home_dir+'/miniconda/envs/'+skon.CONDA_ENV_NAME
#Edit to match your cluster size
skon.SC_NUM_EXECUTORS = 2

skon.zip_conda_env()
skon.distribute_conda_env(sc)
skon.list_cwd_files(sc)
skon.install_conda_env(sc)
skon.set_workers_python_interpreter(sc)

#This assumes that pandas and sklearn are installed in the conda env you specified
def d(x): import pandas as pd; return pd.__version__
sc.parallelize([1]).map(d).collect()

def d(x): import sklearn as sk; return sk.__version__
sc.parallelize([1]).map(d).collect()

skon.remove_conda_env(sc)

skon.list_cwd_files(sc)

sc.parallelize([1]).map(lambda x: x+1).collect()

#Check that the package is not accessible anymore(should get an error)
#ImportError: No module named sklearn
def d(x): import sklearn as sk; return sk.__version__

sc.parallelize([1]).map(d).collect()
"""
#
#
#
