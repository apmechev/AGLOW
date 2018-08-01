import sshtunnel
import logging
import socket as sock
#import MySQLdb as msqldb
#import MySQLdb.cursors as mdbcursors
import os
import datetime
from collections import namedtuple
from time import sleep
#sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

LocationTuple = namedtuple('LocationTuple',['location','count'])

def count_runs_at_locations(query_results):
    locations = {'Sara':0,'Juelich':0, 'Poznan':0}
    for i in query_results:
        if i['location'] in locations.keys():
                locations[i['location']]+=1
    sorted_locations = sorted(locations.items(), key=lambda kv: kv[1])
    results = []
    for location in sorted_locations:
        if not skip_location(location[0]):
            results.append(LocationTuple(*location))
    return results

def get_next_at_location(location="Juelich"):
    sdb = SurveysDB(readonly=True)
    sdb.cur.execute('select observations.id,observations.field,fields.priority,observations.status,observations.priority,observations.location from observations left join fields on (observations.field=fields.id) where observations.status="Observed" and observations.location = "%s" order by observations.priority desc ,fields.priority desc limit 2'%(location))
    results = sdb.cur.fetchone()
    sdb.close()
    return results

def get_running_observations():
    sdb=SurveysDB(readonly=True)
    sdb.cur.execute('select location,status from observations where observations.status!="Observed" and observations.status!="DI_Processed" and observations.status!="Preprocessed";')
    results=sdb.cur.fetchall()
    return results


def get_next_obsrvation():
    # return the name of the top-priority field with appropriate status
    sdb=SurveysDB(readonly=True)
#    sdb.cur.execute('select fields.id as id,sum(integration) as s,count(observations.id) as c,fields.priority from fields left join observations on (observations.field=fields.id) where fields.status="Not started" and observations.status="DI_processed" group by fields.id having s>7 order by fields.priority desc,ra')
    sdb.cur.execute('select observations.id,observations.field,fields.priority,observations.priority from observations left join fields on (observations.field=fields.id) where observations.status="Observed" order by fields.priority desc,observations.priority desc')
    results = sdb.cur.fetchone()
    sdb.close()
    return results[0]['field']

def skip_location(location='Juelich'):
    """
    Decides if we should skip runs at location if all OBS have Priority <0
    """
    sdb=SurveysDB(readonly=True)
    sdb.cur.execute('select max(priority) from observations where location = \"%s\" '%location)
    results = sdb.cur.fetchone()
    sleep(1)
    sdb.close()
    print("Maximum priority at location %s is %f " % (location, results['max(priority)']))
    if results['max(priority)']== -1:
        return True
    return False

def get_next_pref(**kwargs):
    # return the name of the top-priority field balancing all locations
    running = get_running_observations()
    running_locs = count_runs_at_locations(running)
    next_run_location = running_locs[0].location
    results = get_next_at_location(location=next_run_location)
    return {'field_name':results['field'], 
            'sanitized_field_name':results['field'].replace('+','_')}



def get_AGLOW_field_properties(field_step, **context):
    name = context['ti'].xcom_pull(field_step)['field_name']
    props = get_one_observation(name)
    return {'target_OBSID':'L' + str(int(props['id'])),
            'targ_freq_resolution':int(props['nchan']),
            'targ_time_resolution':int(props['dt']),
            'calib_OBSID':'L'+str(int(props['calibrator_id'])),
            'calib_freq_resolution':int(props['calibrator_nchan']),
            'calib_time_resolution':int(props['calibrator_dt']),
            'field_name':str(props['field']),
            'baseline_filter':str(props['bad_baselines']),
            'calibrator_nsb':str(props['calibrator_nsb']),
            'target_nsb':str(props['nsb'])}

def get_one_observation(field_name):
    sdb=SurveysDB()
    idd=sdb.get_observation(field_name)
    sdb.close()
    return idd

def update_status(field_name, obsid,status,table,time=None):
    # utility function to just update the status of an observation
    # name can be None (work it out from cwd), or string (field name)

    sdb=SurveysDB()
    if table=='fields':
        idd=sdb.get_field(id)
        idd['status']=status
        tag_field(sdb,idd)
        sdb.set_field(idd)
    elif table == 'observations':
        idd=sdb.get_observation(field_name, obsid)
        idd['status']=status
        sdb.set_observation(idd)
    sdb.close()

def update_field_status_from_taskid(task_id, status,  **context):
    field_data=context['ti'].xcom_pull(task_id)
    field_name = field_data['field_name']
    update_status(field_name,status,'fields',time=datetime.datetime.now())

def update_OBSID_status_from_taskid(field_task_id,OBSID_task_id, status, **context):
    field_data = context['ti'].xcom_pull(field_task_id)
    field_name = field_data['field_name']
    obsid_data = context['ti'].xcom_pull(OBSID_task_id)
    obsid = obsid_data['target_OBSID'][1:]
    update_status(field_name, obsid,status,'observations',time=datetime.datetime.now())



def tag_field(sdb,idd):
    # Add location and user tags
    idd['clustername']=get_cluster()
    idd['location']=os.getcwd()
    idd['username']=get_user()
    idd['nodename']=sdb.hostname
    
def get_id():
    dir=os.getcwd()
    dname=dir.split('/')[-1]
    return dname

def get_user():
    return os.getenv('USER')

def get_cluster():
    cluster=os.getenv('DDF_PIPELINE_CLUSTER')
    if cluster:
        return cluster
    else:
        return 'Unknown'

def use_database():
    return 'DDF_PIPELINE_DATABASE' in os.environ

class SurveysDB(object):
    ''' Provides low-level and high-level interfaces to the surveys database '''

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def __init__(self,localport=33306,readonly=False):
        import MySQLdb as msqldb
        import MySQLdb.cursors as mdbcursors

        # get the config file -- this must exist
        home=os.getenv("HOME")
        cfg=open(home+'/.surveys').readlines()
        self.password=cfg[0].rstrip()
        try:
            self.ssh_user=cfg[1].rstrip()
        except:
            self.ssh_user=None
        
        # read only use
        self.readonly=readonly

        # set up an ssh tunnel if not running locally
        self.usetunnel=True
        self.hostname=sock.gethostname()
        if self.hostname=='lofar-server':
            self.con = msqldb.connect('127.0.0.1', 'survey_user', self.password, 'surveys')
        else:
            try:
                dummy=sock.gethostbyname('lofar-server.data')
            except:
                self.usetunnel=True

            if self.usetunnel:
                self.tunnel=sshtunnel.SSHTunnelForwarder('lofar.herts.ac.uk',
                                                         ssh_username=self.ssh_user,
                                                         ssh_pkey=home+'/.ssh/id_rsa_martin',
                                                         remote_bind_address=('127.0.0.1',3306),
                                                         local_bind_address=('127.0.0.1',localport))
                self.tunnel.start()
                self.con = msqldb.connect('127.0.0.1', 'survey_user', self.password, 'surveys', port=localport)
            else:
                self.con = msqldb.connect('lofar-server.data', 'survey_user', self.password, 'surveys')
        self.cur = self.con.cursor(cursorclass=mdbcursors.DictCursor)
        if not self.readonly:
            self.cur.execute('lock table fields write, observations write')
        self.closed=False

    def close(self):
        if not self.closed:
            if not self.readonly:
                self.cur.execute('unlock tables')
            self.con.close()
            if self.usetunnel:
                self.tunnel.stop()
            self.closed=True # prevent del from trying again
    
    def __del__(self):
        self.close()
        
    def get_field(self,id):
        self.cur.execute('select * from fields where id=%s',(id,))
        result=self.cur.fetchall()
        if len(result)==0:
            return None
        else:
            return result[0]

    def set_field(self,sd):
        assert not self.readonly
        id=sd['id'];
        for k in sd:
            if k=='id':
                continue
            if sd[k] is not None:
                self.cur.execute('update fields set '+k+'=%s where id=%s',(sd[k],id))

    def create_field(self,id):
        self.cur.execute('insert into fields(id) values (%s)',(id,))
        return self.get_field(id)

    def get_observation(self,field_id, obsid=None):
        if not obsid:
            self.cur.execute('select * from observations where field=%s',(field_id,))
        else:
            self.cur.execute('select * from observations where field=%s and id=%s',(field_id,obsid))
        result=self.cur.fetchall()
        if len(result)==0:
            return None
        else:
            return result[0]

    def set_observation(self,sd):
        assert not self.readonly
        id=sd['id'];
        for k in sd:
            if k=='id':
                continue
            if sd[k] is not None:
                self.cur.execute('update observations set '+k+'=%s where id=%s',(sd[k],id))

    def create_observation(self,id):
        self.cur.execute('insert into observations(id) values (%s)',(id,))
        return self.get_field(id)

def force_close_tunnel(localport=33306):
    tunnel=sshtunnel.SSHTunnelForwarder('lofar.herts.ac.uk',ssh_username='apmechev',ssh_pkey=os.getenv("HOME")+'/.ssh/id_rsa_martin',remote_bind_address=('127.0.0.1',3306),local_bind_address=('127.0.0.1',localport))
    tunnel.start()
    tunnel.stop()


if __name__=='__main__':
    sdb=SurveysDB()
    result=sdb.get_field('P35Hetdex10')
    #result['location']='Never Never Land'
    #sdb.set_id(result)
    print result
    sdb.close()

