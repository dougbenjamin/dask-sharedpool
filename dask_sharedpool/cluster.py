import logging

from collections import ChainMap
import warnings
import dask
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob
import os
import re
import sys
import socket
from pathlib import Path


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def get_free_dask_scheduler_port():
    for port in range(31000,32000):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            res = sock.connect_ex(('localhost', port))
            if res == 111:
                return port

def get_free_dask_dashboard_port():
    for port in range(41000,42000):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            res = sock.connect_ex(('localhost', port))
            if res == 111:
                return port


def merge(*args):
    # This will merge dicts, but earlier definitions win
    return dict(ChainMap(*filter(None, args)))


def check_job_script_prologue(var, job_script_prologue):
    """
    Check if an environment variable is set in job_script_prologue.

    Parameters
    ----------
    var : str
        Name of the environment variable to check.

    job_script_prologue: list of k=v strings

    Returns
    -------
    bool
        True if the environment variable is set.
    """
    if not job_script_prologue:
        return False
    matches = list(filter(lambda x: re.match(f"\W*export {var}\s*=.*", x), job_script_prologue))
    if matches:
        return True
    return False


def get_xroot_url(eos_path):
    """
    Return the xroot url for a given eos path.

    Parameters
    ----------
    eos_path : str
        Path in eos, ie /eos/user/b/bejones/SWAN_projects

    Returns
    -------
    str
        The xroot url for the file ie root://eosuser.cern.ch//eos/user/b/bejones/SWAN_projects
    """
    # note we have to support the following eos paths:
    # /eos/user/b/bejones/foo/bar
    # /eos/home-b/bejones/foo/bar
    # /eos/home-io3/b/bejones/foo/bar
    # Also note this only supports eoshome.cern.ch at this point
    eos_match = re.match("^/eos/(?:home|user)(?:-\w+)?(?:/\w)?/(?P<username>\w+)(?P<path>/.+)$", eos_path)
    if not eos_match:
        return None
    return f"root://eosuser.cern.ch//eos/user/{eos_match.group('username')[:1]}/{eos_match.group('username')}{eos_match.group('path')}"


class Bnlt3Job(HTCondorJob):
    config_name = "bnlt3"

    def __init__(self,
                 scheduler=None,
                 name=None,
                 disk=None,
                 **base_class_kwargs
                 ):

        if disk is None:
            num_cores = base_class_kwargs.get("cores", 1)
            disk = f'{int(num_cores) * 20} GB'

        warnings.simplefilter(action='ignore', category=FutureWarning)

        super().__init__(scheduler=scheduler, name=name, disk=disk, **base_class_kwargs)

        warnings.resetwarnings()

        if hasattr(self, "log_directory"):
                self.job_header_dict.pop("Stream_Output", None)
                self.job_header_dict.pop("Stream_Error", None)


class Bnlt3Cluster(HTCondorCluster):
    __doc__ = (
        HTCondorCluster.__doc__
    +"""
    A customized :class:`dask_jobqueue.HTCondorCluster` subclass for spawning Dask workers in the SNL SDCC HTCondor pool for US ATLAS T1

    It provides the customizations and submit options required for the SDCC Shared pool.
    
    Additional SDCC  parameters:
    worker_image: The container to run the Dask workers inside. Defaults to same container as JupyterHub SCONTAINER variable 
    container_runtime: ``singularity`` (the default)
    batch_name: The HTCondor JobBatchName assigned to the worker jobs. The default ends up as ``"dask-worker"``
    """
    )
    config_name = "bnlt3"
    job_cls = Bnlt3Job

    # note this is where we'd specify a SDCC override of the job definition
    # job_cls = Bnlt3Job

    def __init__(self,
                 *,
                 worker_image = None,
                 image_type = None,
                 container_runtime = 'singularity',
                 gpus = None,
                 batch_name = None,
                 **base_class_kwargs,
                 ):
        """
        :param: worker_image: The container image to run the Dask workers inside.
        Defaults to the singularity image as defined in the SCONTAINER variable
        :param: container_runtime: The container runtime to run the Dask workers inside, defaults to singularity.
        :param: disk: The amount of disk to request. Defaults to 20 GiB / core
        :param: gpus: The number of GPUs to request.
        Defaults to ``0``.
        :param batch_name: The HTCondor JobBatchName ro assign to the worker jobs.
        The default ends up as ``"dask-worker"``
        :param kwargs: Additional keyword arguments like ``cores`` or ``memory`` to pass
        to `dask_jobqueue.HTCondorCluster`.
        """

        base_class_kwargs = Bnlt3Cluster._modify_kwargs(
            base_class_kwargs,
            worker_image=worker_image,
            container_runtime=container_runtime,
            gpus=gpus,
            batch_name=batch_name,
        )

        warnings.simplefilter(action='ignore', category=FutureWarning)

        super().__init__(**base_class_kwargs)

        warnings.resetwarnings()

    @classmethod
    def _modify_kwargs(cls,
                       kwargs,
                       *,
                       worker_image = None,
                       container_runtime = None,
                       gpus = None,
                       batch_name = None,
                       lcg = False,
                       ):
        """
        This method implements the special modifications to adapt dask-jobqueue to run on the SDCC Sharedpool cluster.

        See the class __init__ for the details of the arguments.
        """
        modified = kwargs.copy()

        container_runtime = container_runtime or dask.config.get(f"jobqueue.{cls.config_name}.container-runtime")
        if os.getenv('SCONTAINER'):
            worker_image = os.getenv('SCONTAINER')
        worker_image = worker_image or dask.config.get(f"jobqueue.{cls.config_name}.worker-image")

        logdir = modified.get("log_directory", dask.config.get(f"jobqueue.{cls.config_name}.log-directory", None))
        if logdir:
            modified["log_directory"] = logdir
        xroot_url = get_xroot_url(modified["log_directory"]) if logdir and modified["log_directory"].startswith("/eos/") else None

        modified["job_extra_directives"] = merge(
            {"MY.SingularityImage": f'"{worker_image}"'} if container_runtime == "singularity" else None,
            {"request_gpus": str(gpus)} if gpus is not None else None,
            {"+want_gpus": "True"} if gpus is not None else None,
            {"PeriodicRemove": "(JobStatus == 1 && NumJobStarts > 1) || JobStatus == 5"},
            {"Requirements": '(IsJupyterSlot =!= True) && (IsIcSlot =!= True) && Machine=="spool0805.sdcc.bnl.gov"'},
            {"MY.IsDaskWorker": "True"},
            # getenv justified in case of LCG as both sides have to be the same environment
            {"GetEnv": "True"},
            {"AcctGroup": "group_atlas.tier3"},
            # add singularity BIND mounts
            {"SINGULARITY_BIND_EXPR": "/u0b/software /cvmfs /direct/u0b/software /usatlas/atlas01 /etc/condor /direct/condor"},
            # need to set output destination  see how it is done with SDCC batchspawner
            #{"output_destination": f"{xroot_url}"} if xroot_url else None,
            {"Output": f"{Path.home()}/.dask-worker-$(ClusterId).$(ProcId).out"},
            {"Error": f"{Path.home()}/.dask-worker-$(ClusterId).$(ProcId).err"},
            {"Log": f"{Path.home()}/.dask-worker-$(ClusterId).$(ProcId).log"},
            # extra user input
            kwargs.get("job_extra_directives", dask.config.get(f"jobqueue.{cls.config_name}.job_extra_directives")),
            kwargs.get("job_extra", dask.config.get(f"jobqueue.{cls.config_name}.job_extra")),
            {"JobBatchName": f'"{batch_name or dask.config.get(f"jobqueue.{cls.config_name}.batch-name")}"'},
        )

        submit_command_extra = kwargs.get("submit_command_extra", [])
        #if "-spool" not in submit_command_extra:
        #    submit_command_extra.append('-spool')
        #    modified["submit_command_extra"] = submit_command_extra

        modified["worker_extra_args"] = [
                *kwargs.get("worker_extra_args", dask.config.get(f"jobqueue.{cls.config_name}.worker_extra_args")),
            "--worker-port",
            "40000:41000",
        ]
        return modified


