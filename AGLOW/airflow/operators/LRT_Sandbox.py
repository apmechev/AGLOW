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
from tempfile import gettempdir, NamedTemporaryFile


from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.state import State
#import progressbar
#logging.info(progressbar.__file__)
import GRID_LRT.sandbox as Sandbox

import pdb

class LRTSandboxOperator(BaseOperator):
    """
    Operator that uploads a LOFAR 'sandbox' to GRID storage

    :param srmfile: the name of the file holding a list of srm files to stage
    :type srmfile: string
    :param srms: a list of the srms that need to be staged
    :type srms: list
    :param stageID: In case staging was already done
    :type stageID: string
    :type output_encoding: output encoding of bash command
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#42f4a4'

    @apply_defaults
    def __init__(
            self,
            sbx_config,
            tok_config=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(LRTSandboxOperator, self).__init__(*args, **kwargs)
        self.sbx_config = sbx_config
        self.tok_config = tok_config  
        self.SBX=Sandbox.Sandbox(cfgfile=sbx_config)
        self.output_encoding = output_encoding
        self.state=State.QUEUED
#        self.p_bar=progressbar.ProgressBar()

    def execute(self, context):
        """
        Launches the task. Leverages the sandbox class to build
        a sandbox using a configuration file. Sandbox is uploaded to 
        the respective storage. Xcom returned is the location of the
        Sandbox
        """
        SBXlocs=[]
        self.SBX.build_sandbox(self.sbx_config)        
        self.SBX.upload_sandbox()
        self.SBX.cleanup()
        return {"SBX_location":self.SBX.sbxloc}


    def success(self):
        self.status=State.SUCCESS
        logging.info("Successfully staged " +
                    str(self.progress['Percent done']) + " % of the files.")

    def get_SBXloc(self):
        return self.SBX.SBXloc

    def on_kill(self):
        logging.warn('Sending SIGTERM signal to staging group')
        self.state=State.SHUTDOWN
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
