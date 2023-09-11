# dask-sharedpool

Builds on top of Dask-Jobqueue to enable jobs to run on the BNL SDCC Shared Pool  HTCondor cluster via jupyterhub setup.

## Summary

```python
from distributed import Client 
from dask_sharedpool import Bnlt3Cluster, get_free_dask_scheduler_port
import socket

port_number=get_free_dask_scheduler_port()

cluster = Bnlt3Cluster(
    cores = 1,
    memory = '2000MB',
    disk = '10GB',
    death_timeout = '60',
    nanny = False,
    container_runtime = 'none',
    log_directory = '/eos/user/b/ben/condor/log',
    scheduler_options = {
        'port': port_number,
        'host': socket.gethostname(),
    },
    job_extra = {
        'MY.JobFlavour': '"longlunch"',
    },
)
```

## BNL SDCC extras
There are a few changes in the wrapper to address some of the particular features of the SDCC Shared Pool 
HTCondor cluster, but there are also a few changes to detail here.

### Options
DASK  normally requires that both the scheduler and the worker is the same python versions and libraries. 
At SDCC this means that we use the enviroment as defined in the container being used by BNL US ATLAS T3 federated JupyterHub instanmce.

`container_runtime`: Is set to `"singularity"`, since a runtime is needed 
for the worker, this selects which will be used for the `HTCondor` job the worker runs. The default `"singularity"`.

`worker_image`: The image that will be used if `container_runtime` is defined to use one. The default 
is defined in `jobqueue-bnlt3.yaml`.

`batch_name`: Optionally set a string that will identify the jobs in `HTCondor`. The default is 
`"dask-worker"`
