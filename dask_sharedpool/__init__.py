import logging as _logging

_logger = _logging.getLogger(__name__)
_logger.setLevel(_logging.DEBUG)
_logger.addHandler(_logging.NullHandler())

from .cluster import Bnlt3Cluster, get_free_dask_scheduler_port
from .config import _ensure_user_config_file, _set_base_config

_ensure_user_config_file()
_set_base_config()