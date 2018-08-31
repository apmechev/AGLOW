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


from builtins import bytes
import os
import os.path
import signal
from time import sleep
import logging
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile, mkdtemp
import tarfile
import shutil

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.state import State
from AGLOW.airflow.utils.AGLOW_utils import get_task_instance

#import progressbar
from GRID_LRT import Token
from GRID_LRT import get_picas_credentials
from GRID_LRT.Staging.srmlist import srmlist
from GRID_LRT.Staging.srmlist import slice_dicts
import yaml

import pdb

class TokenCreator(BaseOperator):
    """
    Using a Token template input, this class creates the tokens for a LOFAR job
    The tokens are a set of documents that contain the metadata for each processing
    job as well as the job's progress, step completion times, and etc. 

    :param sbx_task: The name of the sandbox task which passes the sbx_loc to the tokens
    :type sbx_task: string
    :param srms: a list of the srms that need to be staged
    :type srms: list
    :param stageID: In case staging was already done
    :type stageID: string
    :type output_encoding: output encoding of bash command
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#f3f92c'

    @apply_defaults
    def __init__(
            self,
            tok_config,
            sbx_task,
            staging_task,
            srms_task = None,
            fields_task = None,
            pc_database=None,
            subband_prefix = 'SB',
            subband_suffix = '_',
            token_type = 'test_',
            files_per_token = 10,
            output_encoding = 'utf-8',
            *args, **kwargs):

        super(TokenCreator, self).__init__(*args, **kwargs)
        self.pc_database = pc_database
        self.tok_config  =  tok_config  
        self.sbx_task = sbx_task
        self.fields_task = fields_task
        self.subband_prefix = subband_prefix
        self.subband_suffix = subband_suffix
        self.staging_task = staging_task
        self.srms_task = srms_task
        self.files_per_token = files_per_token
        self.output_encoding  =  output_encoding
        self.t_type = token_type
        self.state = State.QUEUED

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        srms = self.get_staged_srms(context)
        if not srms:
            print("Could not get the list of staged srms!")
        pc = get_picas_credentials.picas_cred()
        if self.pc_database:
            pc.database = self.pc_database
        if self.fields_task:
            task_name = self.fields_task['name']
            task_parent_dag = self.fields_task['parent_dag']
            try:
                app = get_task_instance(context, task_name, task_parent_dag)['sanitized_field_name']
            except KeyError:
                app = get_task_instance(context, task_name, task_parent_dag)['field_name']
        else:
            app = srms.OBSID
        self.t_type= self.t_type+app
        tok_settings = yaml.load(open(self.tok_config,'rb'))['Token']
        pipe_type = tok_settings['PIPELINE_STEP']
        th = Token.Token_Handler(t_type=self.t_type,
                    uname=pc.user,pwd=pc.password,dbn=pc.database)
        th.add_overview_view()
        th.add_status_views()
        th.add_view(view_name = pipe_type, cond='doc.PIPELINE_STEP == "{0}" '.format(pipe_type), emit_value2='doc.status')
        
        logging.info('Token type is '+self.t_type)
        logging.info('Tokens are available at https://picas-lofar.grid.surfsara.nl:6984/_utils/database.html?'+pc.database+'/_design/'+self.t_type+'/_view/overview_total')
        logging.info("Token settings are :")
        for i in tok_settings.items():
            logging.info(str(i))

        self.tokens = Token.TokenSet(th=th,tok_config=self.tok_config)
        self.upload_tokens(self.tokens)
        logging.debug(srms)
        if self.files_per_token != 1:
            d = slice_dicts(srms.sbn_dict(pref=self.subband_prefix, 
                                    suff=self.subband_suffix)
                                    ,self.files_per_token)
        else:
            d = {}
            for i in srms.sbn_dict(pref=self.subband_prefix, suff=self.subband_suffix):
                d[i[0]] = i[1]
        self.tokens.create_dict_tokens(iterable=d,
                id_append=pipe_type,key_name='STARTSB',
                file_upload='srm.txt')
        self.tokens.add_keys_to_list("OBSID",srms.OBSID)
        
        if 'CAL_OBSID' in tok_settings.keys():
            task_name = self.srms_task['name']
            task_parent_dag = self.srms_task['parent_dag']
            cal_results = get_task_instance(context, task_name, task_parent_dag)
            CAL_OBSID = cal_results['CAL_OBSID']
            self.tokens.add_keys_to_list("CAL_OBSID",CAL_OBSID)
        logging.info(str(self.sbx_task) )
        task_name = self.sbx_task['name']
        task_parent_dag = self.sbx_task['parent_dag']
        sbx_xcom = get_task_instance(context, task_name, task_parent_dag)
        sbx_name = sbx_xcom["SBX_location"]
        self.tokens.add_keys_to_list('SBXloc',"gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/sandbox/"+sbx_name)
        results = dict()
        results['num_jobs'] = len(d.keys())
        results['output_dir'] = tok_settings['RESULTS_DIR']+"/"+ str(srms.OBSID)
        results['token_type'] = str(self.t_type)
        results['view'] = pipe_type
        results['OBSID'] = srms.OBSID
        return results

    def upload_tokens(self,tokens):
        pass

    def upload_attachments(self,attachment):
        pass

    def modify_fields(self,field_dict):
        for k in field_dict.keys():
            self.tokens.add_keys_to_list(k,field_dict[k])

    def get_staged_srms(self,context):
        task_name = self.staging_task['name']
        task_parent_dag = self.staging_task['parent_dag']
        srm_xcom = get_task_instance(context, task_name, task_parent_dag)
        srmfile = srm_xcom['srmfile']
        logging.info("Srmfile is "+srmfile)
        if srmfile == None:
            raise RuntimeError("Could not get the srm list from the "+str(self.staging_task) +" task")
        self.srmlist = srmlist()
        for link in open(srmfile,'rb').readlines():
            self.srmlist.append(link.strip('\n'))
        return self.srmlist

    def success(self):
        self.status = State.SUCCESS
        logging.info("Successfully uploaded " +
                    str(self.progress['Percent done']) + " % of the tokens.")
    
    def on_kill(self):
        logging.warn('Sending SIGTERM signal to staging group')
        self.state = State.SHUTDOWN
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)


class TokenUploader(BaseOperator):
    """
    Using a Token template input, this class creates the tokens for a LOFAR job
    The tokens are a set of documents that contain the metadata for each processing
    job as well as the job's progress, step completion times, and etc. 

    :param sbx_task: The name of the sandbox task which passes the sbx_loc to the tokens
    :type sbx_task: string
    :param srms: a list of the srms that need to be staged
    :type srms: list
    :param stageID: In case staging was already done
    :type stageID: string
    :type output_encoding: output encoding of bash command
    """ 
    template_fields = ()
    template_ext = ()
    ui_color = '#f0ede4'
            
    @apply_defaults
    def __init__(
            self,
            token_task, 
            upload_file=None, 
            parset_task=None,
            pc_database=None,
            parent_dag=False,
            output_encoding='utf-8',
            *args, **kwargs):
        
        super(TokenUploader, self).__init__(*args, **kwargs)
        self.pc_database = pc_database
        self.token_task = token_task
        self.parset_task = parset_task
        self.output_encoding = output_encoding
        self.upload_file = upload_file
        self.parent_dag = parent_dag
        self.state = State.QUEUED

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        if self.upload_file == None and self.parset_task == None: 
            raise(Exception("No Parset task nor upload file specified!"))
        pc=get_picas_credentials.picas_cred()
        if self.pc_database:
            pc.database = self.pc_database
        tok_dict=context['task_instance'].xcom_pull(task_ids=self.token_task)
        token_id=tok_dict['token_type']
        view=tok_dict['view']
        filename=self.find_upload_file(context)
        self.upload(token_id, view, filename )

    def find_upload_file(self, context):
        """ Checks whether the file exists (if fed a filename as a parameter)
        Otherwise it looks for the xcom of the parset task and takes the 
         key of the dictionary that is named after self.upload_file.split('/')[-1]
         (I.E. the filename) of that xcom. checks if it exists and returns it

        """
        if not self.parset_task and os.path.exists(self.upload_file): 
            return self.upload_file # no parset_task, just get file parameter
        parset_xcom = get_task_instance(context, self.parset_task, self.parent_dag)
        parset_filename=self.upload_file.split('/')[-1]
        parset_file_loc = parset_xcom[parset_filename] 
        if os.path.exists(parset_file_loc): 
            return parset_file_loc
        raise(Exception("Cannot find the parset file"))

    def upload(self, token_id, view, file_name):
        pc=get_picas_credentials.picas_cred()
        if self.pc_database:                                                                                                                                                                                                                                                              
            pc.database = self.pc_database
        th=Token.Token_Handler(t_type=token_id,
                    uname=pc.user,pwd=pc.password,dbn=pc.database)
        self.tokens=th.list_tokens_from_view(view)
        for token in self.tokens:
            th.add_attachment(token.id,open(file_name,'rb'),self.upload_file.split('/')[-1])
        

    def success(self):
        self.status=State.SUCCESS
        logging.info("Successfully uploaded " +
                    str(self.progress['Percent done']) + " % of the tokens.")

    def on_kill(self):
        logging.warn('Sending SIGTERM signal to staging group')
        self.state=State.SHUTDOWN
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)



class ModifyTokenStatus(BaseOperator):
    """
    Using a Token template input, this class creates the tokens for a LOFAR job
    The tokens are a set of documents that contain the metadata for each processing
    job as well as the job's progress, step completion times, and etc. 

    :param sbx_task: The name of the sandbox task which passes the sbx_loc to the tokens
    :type sbx_task: string
    :param srms: a list of the srms that need to be staged
    :type srms: list
    :param stageID: In case staging was already done
    :type stageID: string
    :type output_encoding: output encoding of bash command
    """ 
    template_fields = ()
    template_ext = () 
    ui_color = '#f0ede4'
             
    @apply_defaults
    def __init__(
            self,
            token_task,
            pc_database=None,
            modification={'reset':'todo'}, #Dictionary of list of modifications
            output_encoding='utf-8',
            *args, **kwargs):
            
        super(ModifyTokenStatus, self).__init__(*args, **kwargs)
        self.pc_database = pc_database
        self.token_task=token_task
        self.output_encoding = output_encoding
        self.modification=modification
        self.state=State.QUEUED
        
    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        pc=get_picas_credentials.picas_cred()
        if self.pc_database:
            pc.database = self.pc_database
        tok_dict=context['task_instance'].xcom_pull(task_ids=self.token_task)
        token_id=tok_dict['token_type']
        th=Token.Token_Handler(t_type=token_id,
                    uname=pc.user,pwd=pc.password,dbn=pc.database)        
        for operation, view in self.modification.iteritems():
            if operation=='reset':
                th.reset_tokens(view)
            if operation=='delete':
                th.delete_tokens(view)
            if operation=='set_to_status': # {'set_to_status':{"view":'view_name',"status":'status'}}
                th.set_view_to_status(view['view'],view['status'])

    def success(self):
        self.status=State.SUCCESS
        logging.info("Successfully uploaded " +
                    str(self.progress['Percent done']) + " % of the tokens.")
                    
    def on_kill(self):
        logging.warn('Sending SIGTERM signal to staging group')
        self.state=State.SHUTDOWN
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
        
class ModifyTokenField(BaseOperator):
    """
    Using a Token template input, this class creates the tokens for a LOFAR job
    The tokens are a set of documents that contain the metadata for each processing
    job as well as the job's progress, step completion times, and etc. 

    :param sbx_task: The name of the sandbox task which passes the sbx_loc to the tokens
    :type sbx_task: string
    :param srms: a list of the srms that need to be staged
    :type srms: list
    :param stageID: In case staging was already done
    :type stageID: string
    :type output_encoding: output encoding of bash command
    """
    template_fields = ()
    template_ext = () 
    ui_color = '#f0ede4'

    @apply_defaults 
    def __init__(
            self,
            token_task,
            pc_database=None,
            keyval=['CAL_OBSID','L123456'], #Dictionary of list of modifications
            output_encoding='utf-8',
            *args, **kwargs):
            
        super(ModifyTokenStatus, self).__init__(*args, **kwargs)
        self.pc_database = pc_database
        self.token_task=token_task
        self.output_encoding = output_encoding
        self.modification=modification
        self.state=State.QUEUED
        
    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        pc=get_picas_credentials.picas_cred()
        tok_dict=context['task_instance'].xcom_pull(task_ids=self.token_task)
        token_id=tok_dict['token_type']
        pc=get_picas_credentials.picas_cred()
        th=Token.Token_Handler(t_type=token_id,
                    uname=pc.user,pwd=pc.password,dbn=pc.database)
        ts=Token.Tokenset(th)
        ts.add_keys_to_list(key=keyval[0],val=keyval[1])

    def success(self):
        self.status=State.SUCCESS
        logging.info("Successfully modified the tokens.")
                    
    def on_kill(self):
        logging.warn('Sending SIGTERM signal to staging group')
        self.state=State.SHUTDOWN
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)


class SrmlistFromTokenView(BaseOperator):
    """
    A Token View returns a list of keys/values of (job description) documents
    that match the condition of the view. Using the data returned, we can create
    a srmlist of files stored by these tokens. This will srmlist can be passed
    to a future task to create tokens in chunks as required by the user

    """
    def __init__(
            self,
            token_task,
            view=['test'], 
            pattern='%s_%s', #this will make a srmlist of key_val
            output_encoding='utf-8',
            *args, **kwargs):
        
        super(ModifyTokenStatus, self).__init__(*args, **kwargs)
        self.token_task=token_task
        self.view=view
        self.pattern=pattern
        self.output_encoding = output_encoding
        self.state=State.QUEUED
        
    def execute(self, context):  
        tokens=self.get_tokens_from_view()
        f=NamedTemporaryFile(delete=False)
        surl_list=self.make_srmlist_from_token_list(tokens,
                pattern=self.pattern)
        for i in surl_list:
            f.write(i)

        return f.name

    def get_tokens_from_view(self):
        tokens=[]
        tok_dict=context['task_instance'].xcom_pull(task_ids=self.token_task)
        token_id=tok_dict['token_type']
        token_view=tok_dict['view'] #TODO: Decide if use view here or in initializer
        pc=get_picas_credentials.picas_cred()
        th=Token.Token_Handler(t_type=token_id,
                uname=pc.user,pwd=pc.password,dbn=pc.database)
        for t in th.list_tokens_from_view(token_view):
            tokens.append(t)
        return tokens

    def make_srmlist_from_token_list(self,token_list=[],pattern="%s_%s"):
        surl_list=srmlist.srmlist()
        for token in token_list:
            surl_list.append(pattern % (token['key'], token['value']))
        return surl_list


class TokenArchiver(BaseOperator):
    """
    Archives all data from a certain run, given token_type and output directory
                                                                                                         
    :param token_type: The task that returns the token_type
    :type token_type: string
    :type output_encoding: output encoding of bash command
    """ 
    template_fields = ()
    template_ext = ()
    ui_color = '#c563ff'
            
    @apply_defaults
    def __init__(
            self,
            token_type_task,
            base_archive_location = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/distrib/SKSP',
            output_encoding = 'utf-8',
            *args, **kwargs):
        
        super(TokenArchiver, self).__init__(*args, **kwargs)
        self.output_encoding = output_encoding
        self.base_archive_location = base_archive_location
        self.t_task = token_type_task
        self.state = State.QUEUED
        
    def execute(self, context):
        self.tmpdir=mkdtemp() #Makes temporary directory
        self.oldpwd=os.getcwd()
        tok_dict=context['task_instance'].xcom_pull(task_ids=self.t_task)
        token_type = tok_dict['token_type']              
        self.OBSID = tok_dict['OBSID']
        pc=get_picas_credentials.picas_cred()
        self.th=Token.Token_Handler(t_type=token_type,
            uname=pc.user,pwd=pc.password,dbn=pc.database)
        archive_file=self.archive_tokens()
        self.upload_archive_to_storage(archive_file)
        self.th.purge_tokens()

    def upload_archive_to_storage(self,archive):
        dest_dir = self.base_archive_location+"/"+str(self.OBSID)
        upload = Popen(['globus-url-copy', archive, dest_dir+"/"+"token_archive.tar.gz"], stdout=PIPE, stderr=PIPE)
        execute = upload.communicate()
        if execute[1] != '':
            logging.warn("Upload error?")
            logging.warn(execute[1])

    def archive_tokens(self):
        os.chdir(self.tmpdir)
        self.th.archive_tokens(delete_on_save=True)
        pc=get_picas_credentials.picas_cred()
        self.tarfile = "tokens_"+pc.user+"_"+pc.database+"_"+self.th.t_type+".tar.gz"
        with tarfile.open(self.tmpdir+"/"+self.tarfile, "w:gz") as tar:
            tar.add(self.tmpdir)
        os.chdir(self.oldpwd)
        return self.tmpdir+"/"+self.tarfile

    def remove_tmpdir(self):
        shutil.rmtree(self.tmpdir)
        

