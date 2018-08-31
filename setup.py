#!/usr/bin/env python

#from distutils.core import setup
from setuptools import setup
from setuptools.command.install import install
from AGLOW.version import __version__
import os

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        # PUT YOUR POST-INSTALL SCRIPT HERE or CALL A FUNCTION
        dirs =  get_airflow_directories()
        install.run(self)


setup(name='AGLOW',
      packages=['AGLOW','AGLOW/airflow','AGLOW/airflow/dags','AGLOW/airflow/operators','AGLOW/airflow/sensors','AGLOW/airflow/utils', 'AGLOW/airflow/subdags'],
      version=__version__,
      setup_requires=[
          ],
      tests_require=[
          'pytest', 
          ],
      install_requires=[
          'snakebite',
          'tzlocal==1.5.1',   
          'apache-airflow',
          'GRID_LRT',
          'six'
          ],
      include_package_data=True,
      data_files = [ 
          ],
      description='AGLOW: Automated Grid-enabled LOFAR Workflows',
      long_description="Distributing, Automating and Accelerating LOFAR data processing using an industry standard workflow orchestration software on a High Throughput cluster at the Dutch Grid location.",
      author='Alexandar Mechev',
      author_email='LOFAR@apmechev.com',
      url='https://www.github.com/apmechev/AGLOW/',
      download_url = 'https://github.com/apmechev/AGLOW/archive/v0.0.9x.tar.gz',
      keywords = ['surfsara', 'distributed-computing', 'LOFAR', 'workflow','airflow'],
      classifiers = ['Development Status :: 2 - Pre-Alpha',
      'Intended Audience :: Developers',
      'Intended Audience :: Science/Research',
      "License :: OSI Approved :: GNU General Public License v3 (GPLv3)" ,
      "Natural Language :: English",
      "Topic :: Scientific/Engineering :: Astronomy",
      "Topic :: System :: Distributed Computing",
#      'Programming Language :: Python :: 2',
      'Programming Language :: Python :: 2.6',
      'Programming Language :: Python :: 2.7',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.2',
      'Programming Language :: Python :: 3.3',
      'Programming Language :: Python :: 3.4'] 
     )

def get_airflow_directories():
    import airflow
    airflow_file = airflow.__file__
    airflow_dirs = {}
    airflow_dirs['airflow_base_dir'] = airflow_file.split('__init__')[0]
    base_dir = airflow_dirs['airflow_base_dir']
    airflow_dirs['airflow_contrib_dir'] = base_dir + 'contrib/'
    airflow_dirs['airflow_operators_dir'] = base_dir + 'contrib/operators/'
    airflow_dirs['airflow_sensors_dir'] = base_dir + 'contrib/sensors/'       
    airflow_dirs['airflow_utils_dir'] = base_dir + 'utils/'
    for key in airflow_dirs:
        if not os.path.exists(airflow_dirs[key]):
            raise RuntimeError("Directory %s doesn't exist" % airflow_dirs[key])
    return airflow_dirs
