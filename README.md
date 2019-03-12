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


```bash
mkdir ~/AGLOW_tutorial{,/dags}
export AIRFLOW_HOME=~/AGLOW_tutorial

conda  create -n AGLOW_tutorial python=3.6
source activate AGLOW_tutorial

export SLUGIFY_USES_TEXT_UNIDECODE=yes
pip install aglow

#To install postgress in Userspace:
./AGLOW/scripts/setup_postgres.sh
## If launching fails, check the log, you might need to change the configurations at 
## ${AIRFLOW_HOME}/postgres/database/postgresql.conf

#Run each of these command inside a screen: 
airflow scheduler
airflow webserver
```
