# -*- coding: utf-8 -*-
import os
import tarfile
import logging
import subprocess

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

"""Unpacking Error Level"""
UNPACKING_ERROR_LEVEL = 0

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


def __unpack_conda_env(broadcast_vars, iterator):
    """
    Command to install the conda env in the local worker
    """
    # transform '/path/to/conda' to 'path'
    worker_conda_folder = __parse_worker_local_path(broadcast_vars['CONDA_ENV_LOCATION'])
    # delete existing unpacked env root 'path' folder
    cmd = 'rm -rf %s' % worker_conda_folder
    subprocess.check_output(cmd.split(' '))

    # unpack the env
    with tarfile.open(broadcast_vars['CONDA_ENV_NAME'] + '.tar') as tar_handle:
        tar_handle.errorlevel = UNPACKING_ERROR_LEVEL
        tar_handle.extractall()

    # remove the env tar file
    cmd = 'rm -rf %s.tar' % broadcast_vars['CONDA_ENV_NAME']
    subprocess.check_output(cmd.split(' '))

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


def __tar_env():
    """Untar the conda env tar file
    """
    with tarfile.open('/tmp/' + CONDA_ENV_NAME + '.tar', 'w') as tar_handle:
        for root, dirs, files in os.walk(CONDA_ENV_LOCATION):
            for cur_file in files:
                tar_handle.add(os.path.join(root, cur_file))


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


def pack_conda_env(overwrite=True):
    """
    Pack the local conda env located at CONDA_ENV_LOCATION
    """
    if overwrite:
        print('Overwriting tar file if exists at:' + '/tmp/' + CONDA_ENV_NAME + '.tar')
        __tar_env()
    else:
        if not os.path.exists('/tmp/%s.tar' % CONDA_ENV_NAME):
            print('Tar file does not exist, creating new tar file at:' + '/tmp/' + CONDA_ENV_NAME + '.tar')
            __tar_env()


def distribute_conda_env(sc):
    """
    Distributes the conda env in tar format given a sparkcontext
    """
    return sc.addFile('/tmp/%s.tar' % CONDA_ENV_NAME)


def list_cwd_files(sc, debug=False):
    """
    List the files in the temporary directory of each executor given a sparkcontext
    """

    return prun(sc, __ls, debug=debug)


def install_conda_env(sc):
    """
    Unpack and install the conda package file in each executor given a sparkcontext
    """
    return prun(sc, __unpack_conda_env)


def remove_conda_env(sc):
    """
    Remove the conda env from each executor given a sparkcontext
    """
    return prun(sc, __rm_conda_env)


def set_workers_python_interpreter(sc):
    """
    Set the interpreter for each executor given a sparkcontext
    """
    # Remove the first path separator from the conda env location
    sc.pythonExec = CONDA_ENV_LOCATION[1:] + "/bin/python2.7"


def reset_workers_python_interpreter(sc):
    """
    Reset the interpreter to the default for each executor given a sparkcontext
    """
    sc.pythonExec = '/usr/bin/python2.7'
