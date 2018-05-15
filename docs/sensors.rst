

AGLOW sensors 
~~~~~~~~~~~~~~~~~~~~~
Derived from the airflow sensor class, the AGLOW sensors can track long-running events using GRID and LOFAR tools

gliteSensor
------
The purpose of the gliteSensor is to monitor a job submitted with glite-wms-job-submit. Given a URL pointing to the job, this sensor polls the jobs status at a regular interval using the airflow sensor's poke feature. It parses the output and exits when all of the jobs have exited (whether completed successfully or not)


.. autoclass:: AGLOW.airflow.sensors.glite_wms_sensor.gliteSensor
    :members: 
    :undoc-members:

