import os
import json
import shutil
import logging
import hashlib

from cwltool.draft2tool import CommandLineTool
from cwltool.pathmapper import MapperEnt, PathMapper, dedup
from cwltool.stdfsaccess import StdFsAccess
from cwltool.workflow import defaultMakeTool

from pipeline import Pipeline, PipelineJob
from poll import PollThread
from pprint import pformat

from schema_salad.ref_resolver import file_uri, uri_file_path

try:
    import requests
except ImportError:
    pass

log = logging.getLogger('tes-backend')


class TESService:

    def __init__(self, addr):
        if not addr.startswith("http"):
            addr = "http://" + addr

        if addr.endswith("/"):
            addr = addr[:-1]

        self.addr = addr

    def submit(self, task):
        r = requests.post('%s/v1/tasks' % (self.addr), json=task)
        data = r.json()
        if 'Error' in data:
            raise Exception('Request Error: %s' % (data['Error']))
        return data['id']

    def get_job(self, task_id):
        r = requests.get('%s/v1/tasks/%s' % (self.addr, task_id))
        return r.json()


class TESPipeline(Pipeline):

    def __init__(self, url, kwargs):
        super(TESPipeline, self).__init__()
        self.kwargs = kwargs
        self.service = TESService(url)
        if kwargs.get('basedir') is not None:
            self.basedir = kwargs.get('basedir')
        else:
            self.basedir = os.getcwd()
        self.fs_access = StdFsAccess(self.basedir)

    def make_exec_tool(self, spec, **kwargs):
        return TESPipelineTool(spec, self, fs_access=self.fs_access, **kwargs)

    def make_tool(self, spec, **kwargs):
        if 'class' in spec and spec['class'] == 'CommandLineTool':
            return self.make_exec_tool(spec, **kwargs)
        else:
            return defaultMakeTool(spec, **kwargs)

class TESPipelineTool(CommandLineTool):

    def __init__(self, spec, pipeline, fs_access, **kwargs):
        super(TESPipelineTool, self).__init__(spec, **kwargs)
        self.spec = spec
        self.pipeline = pipeline
        self.fs_access = fs_access

    def makeJobRunner(self):
        return TESPipelineJob(self.spec, self.pipeline, self.fs_access)

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        return PathMapper(reffiles, kwargs['basedir'], stagedir)

class TESPipelineJob(PipelineJob):

    def __init__(self, spec, pipeline, fs_access):
        super(TESPipelineJob, self).__init__(spec, pipeline)
        self.running = True
        self.docker_workdir = '/var/spool/cwl'
        self.fs_access = fs_access

    def create_parameters(self, puts, output=False):
        parameters = []
        for put, path in puts.items():
            if not output:
                ent = self.pathmapper.mapper(path)
                if ent is not None:
                    parameter = {
                        'name': put,
                        'description': 'cwl_input:%s' % (put),
                        'url': ent.resolved,
                        'path': ent.target
                    }
                    parameters.append(parameter)
            else:
                parameter = {
                    'name': put,
                    'description': 'cwl_output:%s' % (put),
                    'url': self.output2url(path),
                    'path': self.output2path(path)
                }

        return parameters

    def create_task(self):
        inputs = {}
        for k, v in self.joborder.items():
            if isinstance(v, dict):
                inputs[k] = v['location']
            elif isinstance(v, list):
                for i in range(len(v)):
                    try:
                        inputs["{}[{}]".format(k, i)] = v[i]['location']
                    except:
                        continue

        input_parameters = self.create_parameters(inputs)

        # manage InitialWorkDirRequirement file generation
        for listing in self.generatefiles['listing']:
            if listing['class'] == 'File':
                loc = self.fs_access.join(self.tmpdir, listing['basename'])
                with self.fs_access.open(loc, 'wb') as gen:
                    gen.write(listing['contents'])
                parameter = {
                    'name': listing['basename'],
                    'description': 'cwl_generated_input:%s' % (loc),
                    'url': file_uri(loc),
                    'path': self.fs_access.join(self.docker_workdir, listing['basename'])
                }
                input_parameters.append(parameter)

        docid = self.spec.get('id')
        outputs = {output['id'].replace(docid + '#', ''): output['outputBinding']['glob']
                   for output in self.spec['outputs'] if 'outputBinding' in output}

        output_parameters = self.create_parameters(outputs, True)

        if self.stdout is not None:
            parameter = {
                'name': 'stdout',
                'url': self.output2url(self.stdout),
                'path': self.output2path(self.stdout),
                'type': 'FILE'
            }
            output_parameters.append(parameter)

        if self.stderr is not None:
            parameter = {
                'name': 'stderr',
                'url': self.output2url(self.stderr),
                'path': self.output2path(self.stderr),
                'type': 'FILE'
            }
            output_parameters.append(parameter)

        output_parameters.append({
            'name': 'workdir',
            'url': self.output2url(''),
            'path': self.docker_workdir,
            'type': 'DIRECTORY',
        })

        container = self.find_docker_requirement()

        cpus = None
        ram = None
        disk = None
        # reqs = self.spec.get('requirements', []) + self.spec.get('hints', [])
        for i in self.requirements:
            if i.get('class', 'NA') == 'ResourceRequirement':
                cpus = i.get('coresMin', i.get('coresMax', None))
                ram = i.get('ramMin', i.get('ramMax', None))
                disk = i.get('outdirMin', i.get('outdirMax', None))

        resources = {}
        if cpus is not None:
            resources['cpu_cores'] = cpus

        if ram is not None:
            resources['ram_gb'] = ram

        if disk is not None:
            resources['size_gb'] = disk

        create_body = {
            'name': self.name,
            'description': self.spec.get('doc', ''),
            'executors': [{
                'cmd': self.command_line,
                'image_name': container,
                'workdir': self.docker_workdir,
                'stdout': self.output2path(self.stdout),
                'stderr': self.output2path(self.stderr),
                'stdin': self.stdin,
                'environ': self.environment
            }],
            'inputs': input_parameters,
            'outputs': output_parameters,
            'resources': resources
        }

        return create_body


    def run(self, pull_image=True, rm_container=True, rm_tmpdir=True,
            move_outputs='move', **kwargs):
        log.debug('DIR JOB ----------------------')
        log.debug(pformat(self.__dict__))

        task = self.create_task()

        log.debug('CREATED TASK MSG----------------------')
        log.debug(pformat(task))

        task_id = self.pipeline.service.submit(task)
        log.debug('SUBMITTED TASK ----------------------')

        operation = self.pipeline.service.get_job(task_id)

        def callback(operation):
            try:
                outputs = self.collect_outputs(self.outdir)
                log.debug('FINAL OUTPUTS ------------------')
                log.debug(pformat(outputs))
                self.output_callback(outputs, 'success')
            except Exception as e:
                raise e
            finally:
                self.running = False

        poll = TESPipelinePoll(
            service=self.pipeline.service,
            operation=operation,
            callback=callback
        )

        self.pipeline.add_thread(poll)
        poll.start()

        while True:
            if not self.running:
                log.debug('STARTING CLEAN UP ------------------')
                break

        if self.stagedir and os.path.exists(self.stagedir):
            log.debug('[job %s] Removing input staging directory %s', self.name, self.stagedir)
            shutil.rmtree(self.stagedir, True)

        if rm_tmpdir:
            log.debug('[job %s] Removing temporary directory %s', self.name, self.tmpdir)
            shutil.rmtree(self.tmpdir, True)

        log.debug('JOB COMPLETE------------------')

    def output2url(self, path):
        if path is not None:
            return file_uri(self.fs_access.join(self.outdir, os.path.basename(path)))
        return None

    def output2path(self, path):
        if path is not None:
            return self.fs_access.join(self.docker_workdir, path)
        return None


class TESPipelinePoll(PollThread):

    def __init__(self, service, operation, callback):
        super(TESPipelinePoll, self).__init__(operation)
        self.service = service
        self.callback = callback

    def poll(self):
        return self.service.get_job(self.operation['id'])

    def is_done(self, operation):
        terminal_states = ['COMPLETE', 'CANCELED', 'ERROR', 'SYSTEM_ERROR']
        return operation['state'] in terminal_states

    def complete(self, operation):
        self.callback(operation)
