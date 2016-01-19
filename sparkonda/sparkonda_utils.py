# -*- coding: utf-8 -*-
import subprocess
import os
import os.path
import os
import zipfile
import shutil
import logging

from pyspark import SparkFiles

"""
Name of the Conda environment to be used for the installation
"""
CONDA_ENV_NAME = None

"""
Location of the Conda environment on the driver node to be used for the installation
"""
CONDA_ENV_LOCATION = None

"""
Number of Executors used for the Spark job
"""
SC_NUM_EXECUTORS = 2


logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def __ls(broadcast_vars, iterator):
    """
    Get the list of files in the worker-local directory
    """
    return os.listdir(SparkFiles.getRootDirectory())


def __parse_worker_local_path(path):
    """
    Get the top level folder of the conda env after unzipping
    """
    return path[1:].split('/')[0]


def __unzip_conda_env(broadcast_vars, iterator):
    """
    Command to install the conda env in the local worker
    """

    # transform '/path/to/conda' to 'path'
    try:
        shutil.rmtree(broadcast_vars['CONDA_ENV_LOCATION'])
    except:
        # pass if a previous conda env does not exist
        pass

    conda_env_zip_file = '%s.zip' % broadcast_vars['CONDA_ENV_NAME']

    with open(conda_env_zip_file, 'rb') as conda_env_zip_file_handle:
        with zipfile.ZipFile(conda_env_zip_file_handle) as z:
            for name in z.namelist():
                # Extract in the current working directory
                target = z.extract(name)
                os.chmod(target, 777)

    # import subprocess
    # cmd = 'rm -rf %s' % worker_conda_folder
    # subprocess.check_output(cmd.split(' '))

    # cmd = 'unzip %s.zip' % broadcast_vars['CONDA_ENV_NAME']
    # subprocess.check_output(cmd.split(' '))

    # cmd = 'rm -rf %s.zip' % broadcast_vars['CONDA_ENV_NAME']
    # subprocess.check_output(cmd.split(' '))

    return [__get_hostname(), 'done']


def __rm_conda_env(broadcast_vars, iterator):
    """
    Command to remove the conda env from the local worker
    """
    import subprocess
    worker_conda_folder = __parse_worker_local_path(broadcast_vars['CONDA_ENV_LOCATION'])

    cmd = 'rm -rf %s' % worker_conda_folder
    return [subprocess.check_output(cmd.split(' '))]


def __get_hostname():
    """
    Get the hostname of the worker
    """
    import socket
    return socket.gethostname()


def __get_broadcast_vars():
    """
    Build a dict of the broadcast vars for convenience
    """
    return dict(CONDA_ENV_NAME=CONDA_ENV_NAME,
                CONDA_ENV_LOCATION=CONDA_ENV_LOCATION,
                SC_NUM_EXECUTORS=SC_NUM_EXECUTORS)


def __get_local_module_file_location():
    """Discover the location of the sparkonda module
    """
    module_file = os.path.abspath(__file__).replace('pyc', 'py')
    return module_file


def __get_sc_executors_instances(sc):
    """Discover the location of the sparkonda module
    """
    sc.getConf()


def __create_conda_zip():
    zip_full_path = '/tmp/%s.zip' % CONDA_ENV_NAME

    with zipfile.ZipFile(zip_full_path, 'w') as zip_handle:
        __zipdir(CONDA_ENV_LOCATION, zip_handle)

    # subprocess.check_output(('zip -r /tmp/%s.zip %s'
    #                          % (CONDA_ENV_NAME,
    #                             CONDA_ENV_LOCATION))
    #                         .split(' '))
    return zip_full_path


def __zipdir(path, zip_handle):
    for root, dirs, files in os.walk(path):
        for file in files:
            zip_handle.write(os.path.join(root, file))


def prun(sc, cmd, include_broadcast_vars=True, debug=False):
    """
    Run a function on all spark executors.
    The number of executors needs to be defined with SC_NUM_EXECUTORS
    """
    num_workers = SC_NUM_EXECUTORS
    if debug:
        logger.debug('Number of executors/partitions:' + str(SC_NUM_EXECUTORS))

    if include_broadcast_vars:
        from functools import partial
        broadcast_vars = sc.broadcast(__get_broadcast_vars())
        return (sc.parallelize(range(1))
                .repartition(num_workers)
                .mapPartitions(partial(cmd, broadcast_vars.value))
                .collect())
    else:
        return (sc.parallelize(range(1))
                .repartition(num_workers)
                .mapPartitions(cmd)
                .collect())


def zip_conda_env(overwrite=True):
    """
    Zip the local conda env located at CONDA_ENV_LOCATION
    """
    if overwrite:
        zip_location = __create_conda_zip()
    else:
        if not os.path.exists('/tmp/%s.zip' % (CONDA_ENV_NAME)):
            logger.debug('Zip does not exist, creating new zip file...')
            zip_location = __create_conda_zip()
        else:
            zip_location = None
    return zip_location


def distribute_conda_env(sc):
    """
    Distributes the conda env in zip format given a sparkcontext
    """
    return sc.addFile('/tmp/%s.zip' % CONDA_ENV_NAME)


def list_cwd_files(sc, debug=False):
    """
    List the files in the temporary directory of each executor given a sparkcontext
    """

    return prun(sc, __ls, debug=debug)


def install_conda_env(sc):
    """
    Unzip and install the conda zip file in each executor given a sparkcontext
    """
    return prun(sc, __unzip_conda_env)


def remove_conda_env(sc):
    """
    Remove the conda env from each executor given a sparkcontext
    """
    return prun(sc, __rm_conda_env)


def set_workers_python_interpreter(sc):
    """
    Set the interpreter for each executor given a sparkcontext
    """
    sc.pythonExec = CONDA_ENV_LOCATION[1:] + "/bin/python2.7"


def reset_workers_python_interpreter(sc):
    """
    Reset the interpreter to the default for each executor given a sparkcontext
    """
    sc.pythonExec = '/usr/bin/python2.7'
