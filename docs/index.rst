.. AGLOW documentation master file, created by
   sphinx-quickstart on Mon Feb  5 09:40:38 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to the AGLOW documentation!
====================================

This package is built by Alexandar Mechev and the LOFAR e-infra group at Leiden University with the support of SURFsara. The goals of this package is to enable High Throughput processing of LOFAR data on the Dutch Grid infrastructure. We do this by making a set of tools designed to wrap around several different LOFAR processing strategies. These tools are responsible for staging data at the LOFAR Long Term Archives, creating and launching Grid jobs on the Gina cluster, as well as managing intermediate data on the SURFsara dCache storage. 

This software combines `Apache Ariflow <https://airflow.apache.org/>`_.. and the `Grid LOFAR Tools <http://grid-lrt.rtfd.io/>`_.. 


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installing
   utils
   sensors
   operators
   subdags
   dags

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
