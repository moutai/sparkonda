# -*- coding: utf-8 -*-

import subprocess
import os.path

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


def __ls(broadcast_vars, iterator):
    """
    Get the list of files in the worker-local directory
    """
    import subprocess
    return [subprocess.check_output('ls'.split(' '))]


def __parse_worker_local_path(path):
    """
    Get the top level folder of the conda env after unzipping
    """
    return path[1:].split('/')[0]


def __unzip_conda_env(broadcast_vars, iterator):
    """
    Command to install the conda env in the local worker
    """
    import subprocess

    # transform '/path/to/conda' to 'path'
    worker_conda_folder = __parse_worker_local_path(broadcast_vars['CONDA_ENV_LOCATION'])

    cmd = 'rm -rf %s' % worker_conda_folder
    subprocess.check_output(cmd.split(' '))

    cmd = 'unzip %s.zip' % broadcast_vars['CONDA_ENV_NAME']
    subprocess.check_output(cmd.split(' '))

    cmd = 'rm -rf %s.zip' % broadcast_vars['CONDA_ENV_NAME']
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


def prun(sc, cmd, include_broadcast_vars=True):
    """
    Run a function on all spark executors.
    The number of executors needs to be defined with SC_NUM_EXECUTORS
    """
    num_workers = SC_NUM_EXECUTORS
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
    conda_env_location = CONDA_ENV_LOCATION
    if overwrite:
        subprocess.check_output(('zip -r /tmp/%s.zip %s'
                                 % (CONDA_ENV_NAME,
                                    conda_env_location))
                                .split(' '))
    else:
        if not os.path.exists('/tmp/%s.zip' % (CONDA_ENV_NAME)):
            print('Zip does not exist, creating new zip file:')
            subprocess.check_output(('zip -r /tmp/%s.zip %s'
                                     % (CONDA_ENV_NAME,
                                        conda_env_location))
                                    .split(' '))


def distribute_conda_env(sc):
    """
    Distributes the conda env in zip format given a sparkcontext
    """
    return sc.addFile('/tmp/%s.zip' % CONDA_ENV_NAME)


def list_cwd_files(sc):
    """
    List the files in the temporary directory of each executor given a sparkcontext
    """
    return prun(sc, __ls)


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
