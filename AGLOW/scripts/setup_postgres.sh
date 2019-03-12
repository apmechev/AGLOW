echo "Setting up postgresql"
cd ${AIRFLOW_HOME}
wget https://ftp.postgresql.org/pub/source/v10.1/postgresql-10.1.tar.gz
tar -zxvf postgresql-10.1.tar.gz 
rm postgresql-10.1.tar.gz
cd postgresql-10.1/
mkdir ${AIRFLOW_HOME}/postgres
./configure --prefix=${AIRFLOW_HOME}/postgres --without-readline
make
make install
cd ${AIRFLOW_HOME}
rm -rf postgresql-10.1/
ls postgres/
ls postgres/bin/
ls postgres/lib/
ls postgres/share/
ls ${AIRFLOW_HOME}/postgres/bin/

echo "Setting up psycopg"
cd ${AIRFLOW_HOME}
pip install psycopg2 --upgrade

python setup.py build_ext  --pg-config ${AIRFLOW_HOME}/postgres/bin/pg_config build 
python setup.py install

cd ${AIRFLOW_HOME}
${AIRFLOW_HOME}/postgres/bin/initdb -D ${AIRFLOW_HOME}/postgres/database/

./scripts/start_postgress.sh 

#####YOU NEED TO DO THIS IN POSTGRESS:
# ${AIRFLOW_HOME}/postgres/bin/psql -d postgres
#>psql (10.1)
#>Type "help" for help.
#>postgres=# CREATE DATABASE AGLOW_DB;

###Then, insife airflow.cfg:
###change the Executor to Local Executor and the sqlAlchemy to
#sql_alchemy_conn = postgresql+psycopg2://apmechev:password@localhost/aglow_db
#
#Then run airflow initdb
