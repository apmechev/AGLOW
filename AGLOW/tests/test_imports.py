#Most basic test will check if all classes can be improted

def test_base_imports():
    import AGLOW
    import AGLOW.airflow
    import AGLOW.airflow.subdags
    import AGLOW.airflow.operators
    import AGLOW.airflow.sensors
    import AGLOW.airflow.utils
    import AGLOW.airflow.postgres

def test_subdags_imports():
    import AGLOW.airflow.subdags.SKSP_calibrator
    import AGLOW.airflow.subdags.SKSP_juelich
    import AGLOW.airflow.subdags.SKSP_target

def test_sensors_imports():
    import AGLOW.airflow.sensors.glite_wms_sensor

def test_operators_imports():
    import AGLOW.airflow.operators.data_staged
    import AGLOW.airflow.operators.lofar_staging
    import AGLOW.airflow.operators.LRT_Sandbox
    import AGLOW.airflow.operators.LRT_storage_to_srm
    import AGLOW.airflow.operators.LRT_submit
    import AGLOW.airflow.operators.LRT_token
    import AGLOW.airflow.operators.LTA_staging

def test_utils_imports():
    import AGLOW.airflow.utils.AGLOW_MySQL_utils
    import AGLOW.airflow.utils.AGLOW_utils
