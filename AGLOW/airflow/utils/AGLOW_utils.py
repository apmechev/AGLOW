# -*- coding: utf-8 -*-                                                                                  
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from functools import wraps

import math

import logging                                                                                           
import subprocess
import os
import re
from os import fdopen, remove
from shutil import move

import pdb

from airflow.utils.decorators import apply_defaults                                                      
from airflow.models import Variable
from airflow import settings
from airflow.utils.log.logging_mixin import LoggingMixin
from GRID_LRT.Staging.srmlist import srmlist
from GRID_LRT import Token
from GRID_LRT.get_picas_credentials import picas_cred
import tempfile
from tempfile import mkstemp
log = LoggingMixin().log

Field_Statuses={'AAA':'Starting Processing',
                'SSS':'Staging done',
                'CCC':'Calibrator Done',
                'TTT':'Target Done',
                'RRR':'Error',
                'ZZZ':'Completed'}


def get_task_instance(context, key, parent_dag=False ):
    if not parent_dag:
        return context['ti'].xcom_pull(task_ids=key)
    else:
        dag = context['dag']
        return context['ti'].xcom_pull(dag_id=dag.parent_dag.dag_id, task_ids=key)


def get_user_proxy(username):
    """
    Gets the X509 file by the user in order to perform authorized operations by them
    as opposed as by the DAG Executor
    """
    pass

def make_srmfile_from_step_results(prev_step_token_task):
    pass

def check_if_enoug_output_files(outp_task):
    pass


def launch_processing_subdag(prev_task, **context):
    task_dict = context['ti'].xcom_pull(prev_task)

def archive_tokens_from_task(token_task, delete=False, **context):
    """ Determines whic tokens to archive and saves them. delete if necessary
    """
    task_dict = context['ti'].xcom_pull(token_task)
    t_type = task_dict['token_type']
    archive_location = task_dict['output_dir']
    archive_all_tokens(t_type, archive_location, delete=delete)


def archive_all_tokens(token_type, archive_location, delete=False):
    pc = picas_cred()
    th = Token.Token_Handler(t_type=token_type, uname=pc.user, pwd=pc.password, dbn=pc.database)
    token_archive = th.archive_tokens(delete_on_save=delete, compress=True)
    logging.info("Archived tokens from " + token_type + " and made an archive: " + token_archive)
    logging.info(token_archive + " size is " + str(os.stat(token_archive).st_size))
    subprocess.call(['globus-url-copy '+token_archive+" "+archive_location+"/"+token_archive.split('/')[-1]],shell=True)
    logging.info("Resulting archive is at "+archive_location+"/"+token_archive.split('/')[-1])


def modify_parset(parset_path, freq_res, time_res, OBSID, flags ):
    """Takes in a base_parset path and changes the time and frequency resolution parameters 
    of this parset. Saves it into a tempfile. Returns the tempfile_path"""
    fh, abs_path = mkstemp()
    with open(parset_path, 'r') as file:
        filedata = file.read()
        filedata = re.sub(r'\! avg_timestep\s+=\s\S?',
                                      "! avg_timestep         = "+str(int(time_res)), filedata)
        filedata = re.sub(r'\! avg_freqstep\s+=\s\S?',
                                              "! avg_freqstep         = "+str(int(freq_res)), filedata)
        filedata = re.sub(r'\! flag_baselines\s+=\s+\W\s+\S+\s+\W',
                                              "! flag_baselines    = "+flags+ "\n", filedata)
        file_ending=".MS"
        if 'gsmcal_solve' in filedata:
            file_ending='.ms'
        filedata = re.sub(r'\! target_input_pattern\s+=\s\S+',
                                  "! target_input_pattern    = " + str(OBSID) + "*" + file_ending,  filedata)
        if 'calib_cal' in filedata:
            file_ending=".MS"
        if 'h5imp_cal' in filedata:
            file_ending=".ndppp_prep_cal"
        filedata = re.sub(r'\! cal_input_pattern\s+=\s\S+',
                              "! cal_input_pattern    = "+str(OBSID)+"*"+file_ending,
                            filedata)

    with open(abs_path, 'w') as pars_file:
        pars_file.write(filedata)
        os.chmod(abs_path, 0o666 )
    return(abs_path)


def modify_parset_from_fields_task(parsets_dict={}, fields_task=None, time_avg=8, freq_avg=2, flags=None,  **context):
    """Takes a dict of 'original' parsets and the task with the fields information
    which will be used to update the averaging paremetes. Returns a dict of the 'name':'location' of the 
    modified parsets, so they can be used by the upload tokens task"""
    field_dict = context['ti'].xcom_pull(fields_task)
    targ_freq = int(field_dict['targ_freq_resolution'])/freq_avg
    targ_time = time_avg/int(math.floor(float(field_dict['targ_time_resolution'])))
    cal_freq = int(field_dict['calib_freq_resolution'])/freq_avg
    cal_time = time_avg/int(math.floor(float(field_dict['calib_time_resolution'])))
    cal_OBSID = field_dict['calib_OBSID']
    targ_OBSID = field_dict['target_OBSID']
    resulting_parsets = {}
    if not flags:
        flags = ", ".join(field_dict['baseline_filter'].split(" ")) 
    for p_name, p_path in parsets_dict.items():
        if "Calib" in p_name:
            resulting_parsets[p_name] = modify_parset(p_path, cal_freq, cal_time, cal_OBSID, flags)
        if "Targ" in p_name:
            resulting_parsets[p_name] = modify_parset(p_path, targ_freq, targ_time, targ_OBSID, flags)
    return resulting_parsets


def set_user_proxy_var(proxy_location):
    os.environ['X509_USER_PROXY']=proxy_location


def set_field_status(fields_file, cal_OBSID, targ_OBSID, field_name, status):
    fh, abs_path = mkstemp()
    with fdopen(fh,'w') as tmp_f:
        with open(fields_file,'r') as f:
            for line in f:
                if line.split(',')[11] == field_name and "L"+line.split(',')[1] == targ_OBSID and "L"+line.split(',')[6] == cal_OBSID:
                    logging.info("Setting field"+str(field_name)+" to status "+str(status))
                    tmp_f.write(str(','.join([str(status)]+line.split(',')[1:])))
                else:
                    tmp_f.write(line)
    remove(fields_file)
    move(abs_path, fields_file)
    os.chmod(fields_file, 0o666 )


def set_field_status_from_taskid(fields_file, task_id, status, **context):
    """sets the field status as the status input variable"""
    field_data=context['ti'].xcom_pull(task_id)
    field_name = field_data['field_name']
    cal_OBSID = field_data['calib_OBSID']
    targ_OBSID = field_data['target_OBSID']
    set_field_status(fields_file, cal_OBSID, targ_OBSID, field_name, status)


def set_field_status_from_task_return(fields_file, task_id, status_task, **context):
    """Sets the field status based on the (String) reuturned by the status_task variable

    """
    field_data=context['ti'].xcom_pull(task_id)
    return_data=context['ti'].xcom_pull(status_task)
    field_name = field_data['field_name']
    cal_OBSID = field_data['calib_OBSID']
    targ_OBSID = field_data['target_OBSID']
    set_field_status(fields_file, cal_OBSID, targ_OBSID, field_name, return_data)


def get_field_location_from_srmlist(srmlist_task, srmfile_key='targ_srmfile', **context):
    """Gets the srmlist from a task and returns the location of the field
    IE the LTA location where the raw data is stored"""
    field_loc="U" #U=unknown
    srm_data=context['ti'].xcom_pull(srmlist_task)
    srmfile=srm_data[srmfile_key]
    _s_list=srmlist()
    for i in  open(srmfile,'r').read().split():
        _s_list.append(i)
    if _s_list.LTA_location == 'juelich':
        return "juelich"
    if _s_list.LTA_location == 'sara':
        return "sara"
    if _s_list.LTA_location == 'poznan':
        return "poznan"
    return "UNK"


def check_if_running_field(fields_file):
    """
    Checks if there are fields that are running (ie not completed, or Error)
    Returns True if running, False if not
    """
    for i in open(fields_file,'r').read().split():
        status=i.split(',')[0]
        if status=='AAA' or status=='SSS' or status=='CCC':
            return True
    return False

def get_srmfile_from_dir(srmdir,field_task, var_calib="SKSP_Prod_Calibrator_srm_file",
        var_targ="SKSP_Prod_Target_srm_file", **context):
    field_data=context['ti'].xcom_pull(field_task)
    cal_OBSID=field_data['calib_OBSID']
    targ_OBSID=field_data['target_OBSID']
    calib_srmfile=srmdir+'srm'+str(cal_OBSID.split('L')[-1])+".txt"
    targ_srmfile=srmdir+'srm'+str(targ_OBSID.split('L')[-1])+".txt"
    Variable.set(var_calib, calib_srmfile)
    Variable.set(var_targ, targ_srmfile)
    logging.info("Calib_srmfile is "+calib_srmfile)
    logging.info("Targ_srmfile is "+targ_srmfile)
    return {'cal_srmfile':calib_srmfile,
            'targ_srmfile':targ_srmfile, 
            "CAL_OBSID": cal_OBSID, 
            "TARG_OBSID": targ_OBSID}


def get_next_field(fields_file, indicator='SND', **context):
    """Determines the next field to be processed,
    uses the set_field_status function to set its status to
    started().
    By default it locks SARA tokens (marked SND in the first column of the
    fields_file), however using the indicator variable, other files can be locked

    """
    for i in open(fields_file,'r').read().split('\n'):
        if not i.split(',')[0] == indicator:
            continue
        l=i.split(',')
        break
    field_information = {'target_OBSID':'L'+l[1],
            'targ_freq_resolution':l[3],
            'targ_time_resolution':l[4],
            'calib_OBSID':'L'+l[6],
            'calib_freq_resolution':l[8],
            'calib_time_resolution':l[9],
            'field_name':l[11],
            'sanitized_field_name':l[11].replace('+','_'),
            'baseline_filter':l[12]}
 
    logging.info("Field Information is: "+str(field_information))
    return field_information


def count_files_uberftp(directory):
    num_files=0
    logging.info(directory)
    c=subprocess.Popen(['uberftp','-ls', directory],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out=c.communicate()
    if out[1]!='':
        logging.warning("srmls failed to find a directory!! ")
        return 0
    if 'lofsksp' not in out[0]:
        logging.warning("no link found in folder!, returning 0 files")
        return 0
    file_list=[directory+"/"+str(i.split()[-1])
                            for i in out[0].strip().split("\r\n")]
    return len(file_list)

def get_var_from_task_decorator(Cls, upstream_task_id="", upstream_return_key="",u_task=None):
    """wrapper for functions that require an fixed input, which is here provided
    by a previous task"""
#    upstream_return=context['ti'].xcom_pull(task_ids=upstream_task_id)
    upstream_return={upstream_return_key:"test"}
    return_value=upstream_return[upstream_return_key]
    Cls.srmfile=return_value
    return Cls


def count_from_task(srmlist_task, srmfile_name, task_if_less,
                    task_if_more, pipeline="SKSP",step='pref_cal2',
                    min_num_files=1, parent_dag=False, **context):
    srmlist_task = get_task_instance(context, srmlist_task, parent_dag=parent_dag )
    srmlist = srmlist_task[srmfile_name]
    return(count_grid_files(srmlist, task_if_less,task_if_more, pipeline=pipeline,step=step, min_num_files=min_num_files))


def check_folder_for_files(folder,number=1):
    """Raises an exception (IE FAILS) if the (gsiftp) folder has less than
    'number' files
    
    By default fails if folder is empty"""
    num_files = count_files_uberftp(folder)
    if num_files < number:
        raise ValueError("Not enough Files ("+
                str(num_files)+"<"+str(number)+") in "+ str(folder))
    return

def check_folder_for_files_from_task(taskid, xcom_key, number, **context):
    """Either uses number to see how many files should be there 
    or checks the number of tokens in the view TODO:make this the right way"""
    xcom_results = context['ti'].xcom_pull(task_ids=taskid)
    path = xcom_results[xcom_key]
    if isinstance(number,int):
        check_folder_for_files(path,number)
    else:
        if 'view' in xcom_results.keys():
            view = xcom_results['view']
        if 'token_type' in xcom_results.keys():
            t_type = xcom_results['token_type']
        pc = picas_cred()
        th = Token.Token_Handler(t_type=t_type, uname=pc.user, pwd=pc.password, dbn=pc.database)
        number = len(th.list_tokens_from_view(view))
        check_folder_for_files(path,number)
    

def count_grid_files(srmlist_file, task_if_less,
        task_if_more, pipeline="SKSP",step='pref_cal1',min_num_files=1):
    """
    An airflow function that branches depending on whether there are calibrator or target
    solutions matching the files in the srmlist. 
    """ 
    srml=srmlist()
    for line in open(srmlist_file).readlines():
        srml.append(line)
    num_files = count_files_uberftp('gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/pipelines/'+str(pipeline)+'/'+str(step)+'/'+str(srml.OBSID))
    if num_files >= min_num_files:
        return task_if_more
    else:
        return task_if_less


def stage_if_needed(stage_task, run_if_staged, run_if_not_staged,
                     **context):
    """This function takes the check_if_staged task and stages the files
    if None is returned. Otherwise it passes the srmlist to the 'join' task
    and the processing continues
    """
    staged_status=context['ti'].xcom_pull(task_ids=stage_task)
    logging.info(staged_status)
    if staged_status['staged']:
        return run_if_staged
    else:
        return run_if_not_staged

def delete_gsiftp_files(gsiftp_directory):
    del_task = subprocess.Popen(['uberftp','rm',gsiftp_directory+"/*"], stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)
    output = del_task.communicate()
    if output[1] != '':
        logging.warn("Error deleting files:")
        logging.warn(output[1])
    return gsiftp_directory

def delete_gsiftp_from_task(root_dir, OBSID_task, **context):
    """A task that returns OBSID, and a root directory come together in this
    function to gracefully delete all files in this here directory

    """
    task_results=context['ti'].xcom_pull(task_ids=OBSID_task)
    OBSID=task_result['OBSID']
    logging.debug('Deleting files from directory '+str(root_dir)+"/"+str(OBSID))
    delete_gsiftp_files(root_dir+"/"+OBSID_task)
