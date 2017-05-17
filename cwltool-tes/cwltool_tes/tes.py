import os
import json
import shutil
import logging
import hashlib
from pprint import pformat

import cwltool.draft2tool
from cwltool.pathmapper import MapperEnt, dedup
from cwltool.stdfsaccess import StdFsAccess

from pipeline import Pipeline, PipelineJob
from poll import PollThread

from schema_salad.ref_resolver import file_uri

try:
    import requests
except ImportError:
    pass

log = logging.getLogger('tes-backend')


class TESService:

    def __init__(self, addr):
        self.addr = addr

    def submit(self, task):
        r = requests.post("%s/v1/tasks" % (self.addr), json=task)
        data = r.json()
        if 'Error' in data:
            raise Exception("Request Error: %s" % (data['Error']))
        return data['id']

    def get_job(self, task_id):
        r = requests.get("%s/v1/tasks/%s" % (self.addr, task_id))
        return r.json()


class TESPipeline(Pipeline):

    def __init__(self, url, kwargs):
        super(TESPipeline, self).__init__()
        self.kwargs = kwargs
        self.service = TESService(url)
        if kwargs.get("basedir") is not None:
            self.basedir = kwargs.get("basedir")
        else:
            self.basedir = os.getcwd()
        self.fs_access = StdFsAccess(self.basedir)

    def make_exec_tool(self, spec, **kwargs):
        return TESPipelineTool(spec, self, fs_access=self.fs_access, **kwargs)

    def make_tool(self, spec, **kwargs):
        if 'class' in spec and spec['class'] == 'CommandLineTool':
            return self.make_exec_tool(spec, **kwargs)
        else:
            return cwltool.workflow.defaultMakeTool(spec, **kwargs)

class TESPipelineTool(cwltool.draft2tool.CommandLineTool):

    def __init__(self, spec, pipeline, fs_access, **kwargs):
        super(TESPipelineTool, self).__init__(spec, **kwargs)
        self.spec = spec
        self.pipeline = pipeline
        self.fs_access = fs_access

    def makeJobRunner(self):
        return TESPipelineJob(self.spec, self.pipeline, self.fs_access)

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        return cwltool.pathmapper.PathMapper(
            reffiles, kwargs['basedir'], stagedir
        )

class TESPipelineJob(PipelineJob):

    def __init__(self, spec, pipeline, fs_access):
        super(TESPipelineJob, self).__init__(spec, pipeline)
        self.running = True
        self.docker_workdir = "/var/spool/cwl"
        self.fs_access = fs_access

    def create_parameters(self, puts, output=False):
        parameters = []
        for put, path in puts.items():
            if not output:
                ent = self.pathmapper.mapper(path)
                if ent is not None:
                    parameter = {
                        'name': put,
                        'description': "cwl_input:%s" % (put),
                        'url': ent.resolved,
                        'path': ent.target
                    }
                    parameters.append(parameter)
            else:
                parameter = {
                    'name': put,
                    'description': "cwl_output:%s" % (put),
                    'url': self.output2url(path),
                    'path': self.output2path(path)
                }

        return parameters

    def create_task(self, command, inputs, outputs):

        input_parameters = self.create_parameters(inputs)
        output_parameters = self.create_parameters(outputs, True)

        stdout_path = self.spec.get('stdout', None)
        stderr_path = self.spec.get('stderr', None)

        if stdout_path is not None:
            parameter = {
                'name': 'stdout',
                'description': 'tool stdout',
                'url': self.output2url(stdout_path),
                'path': self.output2path(stdout_path)
            }
            output_parameters.append(parameter)

        if stderr_path is not None:
            parameter = {
                'name': 'stderr',
                'description': 'tool stderr',
                'url': self.output2url(stderr_path),
                'path': self.output2path(stderr_path)
            }
            output_parameters.append(parameter)

        container = self.find_docker_requirement()

        reqs = self.spec.get("requirements", []) + self.spec.get("hints", [])
        for i in reqs:
            if i.get("class", "NA") == "ResourceRequirement":
                cpus = i.get("coresMin", i.get("coresMax", None))
                ram = i.get("ramMin", i.get("ramMax", None))
                disk = i.get("outdirMin", i.get("outdirMax", None))

        resources = {}
        if cpus is not None:
            resources["cpu_cores"] = cpus

        if ram is not None:
            resources["ram_gb"] = ram

        if disk is not None:
            resources["size_gb"] = disk

        create_body = {
            'name': self.spec.get("name", self.spec.get("id", "cwltool-tes task")),
            'description': self.spec.get("doc", ""),
            'executors': [{
                'cmd': command,
                'image_name': container,
                'workdir': self.docker_workdir,
                'stdout': self.output2path(stdout_path),
                'stderr': self.output2path(stderr_path)
            }],
            'inputs': input_parameters,
            'outputs': output_parameters,
            'resources': resources
        }

        return create_body


    def run(self, pull_image=True, rm_container=True, rm_tmpdir=True,
            move_outputs="move", **kwargs):
        docid = self.spec.get("id")

        log.debug('DIR JOB ----------------------')
        log.debug(pformat(self.__dict__))

        # prep the inputs
        inputs = {}
        for k, v in self.joborder.items():
            if isinstance(v, dict):
                inputs[k] = v['location']

        for listing in self.generatefiles['listing']:
            if listing['class'] == 'File':
                with self.fs_access.open(listing['basename'], 'wb') as gen:
                    gen.write(listing['contents'])

        log.debug('SPEC_OUTPUTS ----------------------')
        log.debug(pformat(self.spec['outputs']))

        outputs = {output['id'].replace(docid + '#', ''): output['outputBinding']['glob']
                   for output in self.spec['outputs'] if 'outputBinding' in output}

        log.debug('PRE_OUTPUTS----------------------')
        log.debug(pformat(outputs))

        task = self.create_task(
            command=self.command_line,
            inputs=inputs,
            outputs=outputs
        )

        log.debug('CREATED TASK MSG----------------------')
        log.debug(pformat(task))

        task_id = self.pipeline.service.submit(task)
        log.debug('SUBMITTED TASK ----------------------')

        operation = self.pipeline.service.get_job(task_id)

        poll = TESPipelinePoll(
            service=self.pipeline.service,
            operation=operation,
            callback=self.jobCleanup
        )

        self.pipeline.add_thread(poll)
        poll.start()

        while True:
            if not self.running:
                log.debug('TASK COMPLETE ------------------')
                break

    def jobCleanup(self, operation):
        log.debug('COLLECTING OUTPUTS ------------------')

        final = {}
        for output in self.spec['outputs']:
            if output['type'] == 'File':
                outid = output['id'].replace(self.spec['id'] + '#', '')
                binding = output['outputBinding']['glob']
                log.debug('BINDING: ' + self.fs_access.join(self.outdir, binding))
                glob = self.fs_access.glob(self.fs_access.join(self.outdir, binding))
                log.debug('GLOB: ' + pformat(glob))
                if len(glob) == 0:
                    self.running = False
                    raise WorkflowException(
                        "Output processing failed. File not found: %s" %
                        self.fs_access.join(self.outdir, binding)
                    )
                # with self.fs_access.open(glob[0], 'rb') as handle:
                #     contents = handle.read()
                #     checksum = hashlib.sha1(contents)
                #     hex = "sha1$%s" % checksum.hexdigest()
                p = self.fs_access._abs(glob[0])
                collect = {
                    'basename': os.path.basename(p),
                    'dirname': os.path.dirname(p),
                    'location': file_uri(p),
                    'path': p,
                    'class': 'File',
                    'size': os.path.getsize(p),
                }
                final[outid] = collect

        output_manifest = self.fs_access.join(self.outdir, "cwl.output.json")
        with open(output_manifest, 'w') as fh:
            cwl_output = json.dumps(final)
            fh.write(cwl_output)

        log.debug('COLLECTED OUTPUTS ------------------')
        log.debug(pformat(final))

        self.output_callback(final, 'success')
        self.running = False

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
        terminal_states = ['COMPLETE', 'CANCELED', 'ERROR', "SYSTEM_ERROR"]
        return operation['state'] in terminal_states

    def complete(self, operation):
        self.callback(operation)
