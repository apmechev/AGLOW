${AIRFLOW_HOME}/postgres/bin/pg_ctl -o "-F -p 5433"  -D ${AIRFLOW_HOME}/postgres/database/ -l ${AIRFLOW_HOME}/postgres/logfile start

