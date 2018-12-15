import airflow.www as www


WWW_LOC = www.__file__.split('__init__.py')[0]
TEMPLATES_LOC = "{}/templates/airflow".format(WWW_LOC)

def test_dag_patched():
    dagfile = TEMPLATES_LOC+"/dag.html"
    with open(dagfile,'r') as _f:
        dag_data = _f.read()
    assert "SKSP_field" in dag_data
#    assert len(dag_data.split('\n')) == 436 #This is different for different airflow versions...
