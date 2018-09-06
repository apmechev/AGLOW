# AGLOW
[![Documentation Status](https://readthedocs.org/projects/aglow/badge/?version=latest)](http://aglow.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://travis-ci.org/apmechev/AGLOW.svg?branch=master)](https://travis-ci.org/apmechev/AGLOW)

Automated Grid-Enabled LOFAR Workflows
====

AGLOW is a combination of the GRID LOFAR Reduction Tools and Apache Airflow. In order to efficiently use the Computational Resources provided to us at SURFsara, the AGLOW package includes custom Airflow Operators. These operators can be combined to build LOFAR workflows, from a single NDPPP run to full Direction Dependent Imaging. 

Setup
=====
The AGLOW package is best set up in a conda environment. The included environment.yml package will create a conda env named 'AGLOW' and set up all the prerequisites to run an AGLOW server. The usave is as such:

```bash
conda env create -f environment.yml 
```

