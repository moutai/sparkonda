#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_sparkonda
----------------------------------

Tests for `sparkonda` module.
"""
from __future__ import print_function
import unittest
from nose.tools import assert_true, assert_equal
from nose.plugins.attrib import attr
import sys
import ConfigParser
import os.path
from os.path import expanduser
import logging

from pyspark import SparkContext
from pyspark import SparkConf


class SparkondaTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        class_name = cls.__name__
        conf = SparkConf()
        conf.set('spark.app.name', 'class_name')

        # Read the spark configuration and update the spark conf
        test_spark_config = ConfigParser.ConfigParser()
        test_spark_config.read('test_config.cfg')
        test_spark_config.sections()
        configs = dict(test_spark_config.items('spark_conf_test_generic'))
        for k, v in configs.items():
            conf.set(k, v)
        cls.spark_test_configs = configs
        # Create the spark context
        cls.sc = SparkContext(conf=conf)
        if 'PYSPARK_DRIVER_PYTHON' in configs.keys():
            cls.sc.pythonExec = configs['PYSPARK_DRIVER_PYTHON']
        else:
            cls.sc.pythonExec = 'python2.7'

        logger = cls.sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

        logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s')
        cls.logger = logging.getLogger(__name__)
        cls.logger.setLevel(logging.DEBUG)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        cls.sc._jvm.System.clearProperty("spark.driver.port")

    def test_01_spark_context_started(self):
        self.logger.debug("Running test_01_spark_context_started")
        assert_true(self.sc is not None, 'sc should be initialized')
        assert_true(self.sc.range(10).count() == 10, 'sc.range(10).count() should return 10')

    def test_02_adding_sparkonda_utils_file_to_workers(self):
        from os.path import expanduser
        home_dir = expanduser("~")
        self.logger.debug("Running test_02_adding_sparkonda_utils_file_to_workers")
        sparkonda_utils_filename = self.add_sparkonda_utils_to_workers()
        self.logger.debug('sparkonda_utils_filename: %s', sparkonda_utils_filename)
        assert_true('sparkonda' not in sys.modules.keys(),
                    'sparkonda should be cleaned from the sys.modules cache')
        assert_true('sparkonda.module_helper' not in sys.modules.keys(),
                    'sparkonda should be cleaned from the sys.modules cache')

        skon = self.import_sparkonda_utils()
        self.logger.debug("self.spark_test_configs['spark.executor.instances']: %s"
                          % str(self.spark_test_configs['spark.executor.instances']))
        self.logger.debug("self.spark_test_configs['spark.executor.cores']: %s"
                          % str(self.spark_test_configs['spark.executor.cores']))

        skon.SC_NUM_EXECUTORS = int(self.spark_test_configs['spark.executor.instances'])
        skon.SC_NUM_CORES_PER_EXECUTOR = int(self.spark_test_configs['spark.executor.cores'])
        skon.CONDA_ENV_NAME = 'sparkonda-test-env'
        skon.CONDA_ENV_LOCATION = ''.join([home_dir, '/miniconda/envs/', skon.CONDA_ENV_NAME])

        file_list = skon.list_cwd_files(self.sc, debug=True)
        self.logger.debug('file_list: %s' % str(file_list))
        assert_true(sparkonda_utils_filename in str(file_list),
                    ' '.join([sparkonda_utils_filename, ' should be in workers directories']))

    def test_03_pack_conda_env_on_the_spark_driver(self):
        self.logger.debug("Running test_03_pack_conda_env_on_the_spark_driver")
        skon = self.init_sparkonda()
        skon.pack_conda_env(overwrite=True)

        pack_location = '/tmp/' + skon.CONDA_ENV_NAME + '.tar'
        assert_true(pack_location is not None, 'Pack file location should not be None')
        assert_true('/tmp/' in pack_location, 'Pack file location should be in the /tmp directory')

        pack_location = skon.pack_conda_env(overwrite=False)
        assert_equal(pack_location, None, 'Pack file location should be None, if it already exist')

    def test_04_distribute_conda_env_on_the_spark_workers(self):
        self.logger.debug("Running test_04_distribute_conda_env_on_the_spark_workers")
        skon = self.init_sparkonda()
        skon.pack_conda_env(overwrite=True)
        pack_file_location = '/tmp/' + skon.CONDA_ENV_NAME + '.tar'
        skon.distribute_conda_env(self.sc)
        workers_files = skon.list_cwd_files(self.sc)
        self.logger.debug('workers_files:%s' % str(workers_files))
        pack_file_name = os.path.split(pack_file_location)[-1]
        count_files = 0
        for fh in workers_files:
            for f in fh:
                if pack_file_name == f:
                    count_files += 1
        self.logger.debug(
            'count_files, int(skon.SC_NUM_EXECUTORS): %s' % str([count_files, int(skon.SC_NUM_EXECUTORS)]))
        assert_equal(count_files, int(skon.SC_NUM_EXECUTORS),
                     'Number of pack files should be equal to the executors count')

    def test_05_install_conda_env_on_the_spark_workers(self):
        self.logger.debug("Running test_05_install_conda_env_on_the_spark_workers")
        skon = self.init_sparkonda()
        skon.pack_conda_env(overwrite=True)
        pack_file_location = '/tmp/' + skon.CONDA_ENV_NAME + '.tar'
        skon.distribute_conda_env(self.sc)
        workers_files = skon.list_cwd_files(self.sc)
        self.logger.debug('Before unpack workers_files:%s' % str(workers_files))
        skon.install_conda_env(self.sc)
        workers_files = skon.list_cwd_files(self.sc)
        self.logger.debug('After unpack workers_files:%s' % str(workers_files))
        self.logger.debug('Looking for top level folder in workers_files:%s' % skon.CONDA_ENV_LOCATION)
        top_level_conda_env_location = skon.CONDA_ENV_LOCATION.split('/')[1]
        count_files = 0
        for fh in workers_files:
            for f in fh:
                if top_level_conda_env_location == f:
                    count_files += 1
        self.logger.debug(
            'count_files, int(skon.SC_NUM_EXECUTORS): %s' % str([count_files, int(skon.SC_NUM_EXECUTORS)]))
        assert_equal(count_files, int(skon.SC_NUM_EXECUTORS),
                     'Number of conda env folders should be equal to the executors count')

    def test_06_conda_installed_python_on_the_spark_workers(self):
        self.logger.debug("Running test_06_conda_installed_python_on_the_spark_workers")
        skon = self.init_sparkonda()
        skon.pack_conda_env(overwrite=True)
        skon.distribute_conda_env(self.sc)
        skon.install_conda_env(self.sc)
        workers_files = skon.list_cwd_files(self.sc, debug=True)
        self.logger.debug('workers_files:%s' % str(workers_files))
        self.logger.debug(skon.CONDA_ENV_LOCATION)
        skon.set_workers_python_interpreter(self.sc)

        def check_pandas(x):
            import pandas as pd
            return [pd.__version__]

        pandas_versions = skon.prun(self.sc, check_pandas, include_broadcast_vars=False)
        self.logger.debug('pandas_versions %s' % str(pandas_versions))

        def check_sklearn(x):
            import sklearn as sk
            return [sk.__version__]

        sklearn_versions = skon.prun(self.sc, check_sklearn, include_broadcast_vars=False)
        self.logger.debug('sklearn_versions %s' % str(pandas_versions))

        assert_equal(len(pandas_versions), int(skon.SC_NUM_EXECUTORS),
                     'Number of responses should be equal to the executors count')

        assert_equal(len(sklearn_versions), int(skon.SC_NUM_EXECUTORS),
                     'Number of responses should be equal to the executors count')


    def test_07_num_executors_match_num_hosts_returned_from_prun(self):
        self.logger.debug("Running test_07_num_executors_match_num_hosts_returned_from_prun")
        skon = self.init_sparkonda()
        workers_files = skon.list_cwd_files(self.sc, debug=True)
        num_hosts = len(set(workers_files[::2]))
        assert_equal(num_hosts, int(skon.SC_NUM_EXECUTORS),
                     'Number of hosts should be equal to the executors count')


    def add_sparkonda_utils_to_workers(self):
        # Helper to add sparkonda_utils module to the workers
        # and clean up the sys.modules cache afterward
        import sparkonda
        return sparkonda.module_helper.add_module_to_workers(self.sc)

    def import_sparkonda_utils(self):
        # Helper to import the sparkonda_utils module
        # Try-Catch trick used for IDEs, to provide autocomplete
        try:
            import sparkonda_utils as skon
        except ImportError:
            import sparkonda.sparkonda_utils as skon
        return skon

    def init_sparkonda(self):
        from os.path import expanduser
        home_dir = expanduser("~")
        self.add_sparkonda_utils_to_workers()
        skon = self.import_sparkonda_utils()
        skon.SC_NUM_EXECUTORS = int(self.spark_test_configs['spark.executor.instances'])
        skon.SC_NUM_CORE_PER_EXECUTOR = int(self.spark_test_configs['spark.executor.cores'])
        skon.CONDA_ENV_NAME = 'sparkonda-test-env'
        skon.CONDA_ENV_LOCATION = ''.join([home_dir, '/miniconda/envs/', skon.CONDA_ENV_NAME])
        return skon
