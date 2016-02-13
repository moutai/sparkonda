========
Usage
========

Currently this uses the sparkcontext addPyFile method to ship this module to the workers.


To ship and install a conda environment the following steps are needed:

.. code-block:: python

    # Create a new conda env to test with, if you don't have one:
    conda create -n sparkonda-test-env python=2.7 pip pandas scikit-learn numpy numba
    source activate sparkonda-test-env
    pip install sparkonda
    ###########################################################
    ###########################################################
    # In the following example sc represents a running pyspark context
    # Please see the tests folder for examples
    ###########################################################

    # Adding the sparkonda_utils module to the spark workers:
    ###########################################################
    ###########################################################
    ####OPTION 1(requires knowledge of the installation location):
    sc.addPyFile('path/to/sparkonda_utils.py')


    ####OPTION 2(tries to detect the installation folder):
    # Declare these two helper functions to help with sys.modules cache manipulations
    # and to add the sparkonda_utils.py file to the pyspark workers SparkFiles
    def add_sparkonda_utils_to_workers(sc):
        # Helper to add sparkonda_utils module to the workers
        # and clean up the sys.modules cache afterward
        import sparkonda
        sparkonda.module_helper.add_module_to_workers(sc)

    def import_sparkonda_utils():
        # Helper to import the sparkonda_utils module
        # Try-Catch trick used for IDEs, to provide autocomplete
        try:
            import sparkonda_utils as skon
        except ImportError:
            import sparkonda.sparkonda_utils as skon
        return skon
    add_sparkonda_utils_to_workers(sc)
    skon = import_sparkonda_utils()
    ###########################################################
    ###########################################################


    # Make sure that the skon module points to the SparkFile
    # and not to the local sparkonda installation
    skon.__file__

    # Configure your skon object and use it to deploy your conda env to
    # the spark workers
    from os.path import expanduser
    home_dir = expanduser("~")

    # Edit to match your conda env name
    skon.CONDA_ENV_NAME = 'sparkonda-test-env'
    # Edit this path to match your conda env location
    skon.CONDA_ENV_LOCATION = ''.join([home_dir,'/miniconda/envs/',skon.CONDA_ENV_NAME])
    # Edit to match the number of spark workers
    skon.SC_NUM_EXECUTORS = 2
    # Edit to match the number of cores per spark worker
    skon.SC_NUM_CORE_PER_EXECUTOR = 2

    # Pack, ship, list worker dirs, install the conda env and set the workers python interpreter
    skon.pack_conda_env()
    skon.distribute_conda_env(sc)
    skon.list_cwd_files(sc)
    skon.install_conda_env(sc)
    skon.set_workers_python_interpreter(sc)

    # Test your setup
    # This assumes that pandas and sklearn are installed in the conda env you specified
    def check_pandas(x): import pandas as pd; return [pd.__version__]
    skon.prun(sc, check_pandas, include_broadcast_vars=False)

    def check_sklearn(x): import sklearn as sk; return [sk.__version__]
    skon.prun(sc, check_sklearn, include_broadcast_vars=False)

To remove the custom conda env from the workers and reset the interpreter:

.. code-block:: python

    skon.remove_conda_env(sc)

    skon.list_cwd_files(sc)

    skon.reset_workers_python_interpreter(sc)

    # Check that the package is not accessible anymore
    # User should get an ImportErrror:
    #   ImportError: No module named sklearn
    def check_sklearn(x): import sklearn as sk; return [sk.__version__]
    skon.prun(sc, check_sklearn, include_broadcast_vars=False)
