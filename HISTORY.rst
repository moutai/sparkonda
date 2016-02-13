.. :changelog:

History
-------

0.2.2 (2016-02)
---------------------
Fix bug when number of cores per executor is larger than 1

Fix list_cwd to include hostname in output

Fix tests to use pack files naming instead of zip files

Fix usage docs to include new config for number of cores

Add tests for correct prun to num partition mapping

0.2.1 (2016-01)
---------------------
Fix documentation, setup and usage methods

Fix travis and setup.py configs

Move zip to pack as an action

Replace zip with tar to better preserve acls on conda env files

Add configuration of error level for untar

0.2.0 (2016-01)
---------------------
Additional tests for the distributed version of remote package delivery

Changed os files management to python based support (zip, rm)

Use SparkFiles to detect files distributed to the workers.

0.1.0 (2015-11)
---------------------
Initial version to manage the conda environments on pyspark cluster workers without involving your cluster admins too heavily.


