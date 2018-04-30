#!/usr/bin/env python

#from distutils.core import setup
from setuptools import setup
import os

setup(name='AGLOW',
      packages=['AGLOW','AGLOW/airflow','AGLOW/airflow/dags','AGLOW/airflow/operators','AGLOW/airflow/sensors','AGLOW/airflow/utils'], #'GRID_LRT', 'GRID_LRT/Staging', 'GRID_LRT/Application', 'GRID_LRT/couchdb'],
      version='v0.0.9.2',
      setup_requires=[
          ],
      tests_require=[
        'pytest', 
          ],
      install_requires=[
          'apache-airflow',
          'grid-lrt'
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

