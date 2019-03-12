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
wget https://pypi.python.org/packages/dd/47/000b405d73ca22980684fd7bd3318690cc03cfa3b2ae1c5b7fff8050b28a/psycopg2-2.7.3.2.tar.gz#md5=8114e672d5f23fa5329874a4314fbd6f
tar -zxvf psycopg2-2.7.3.2.tar.gz
rm psycopg2-2.7.3.2.tar.gz
cd psycopg2-2.7.3.2/
python setup.py build_ext  --pg-config ${AIRFLOW_HOME}/postgres/bin/pg_config build 
python setup.py install

cd ${AIRFLOW_HOME}
${AIRFLOW_HOME}/postgres/bin/initdb -D ${AIRFLOW_HOME}/postgres/database/

./start_postgress.sh 

#YOU NEED TO DO THIS IN POSTGRESS:
# ${AIRFLOW_HOME}/postgres/bin/psql -d postgres
#>psql (10.1)
#>Type "help" for help.
#>postgres=# CREATE DATABASE database_name;

#Then change the Executor to Local Executor and the sqlAlchemy to
#sql_alchemy_conn = postgresql+psycopg2://apmechev:password@localhost/database_name
#Then run airflow initdb
