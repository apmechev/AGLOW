import airflow.www as www
import airflow.__version__


WWW_LOC = www.__file__.split('__init__.py')[0]
TEMPLATES_LOC = "{}/templates/airflow".format(WWW_LOC)
FILE_LENGTHS={'dag.html':{'1.10.0':436,
                          '1.10.1':450,
                          '1.10.2':529}}

def test_dag_patched():
    dagfile = TEMPLATES_LOC+"/dag.html"
    with open(dagfile,'r') as _f:
        dag_data = _f.read()
    assert "SKSP_field" in dag_data
#    assert len(dag_data.split('\n')) == 436 #This is different for different airflow versions...
