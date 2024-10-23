import importlib.util

from fastapi import Request

from transcriptomics_data_service.authz.middleware_base import BaseAuthzMiddleware
from transcriptomics_data_service.config import get_config
from transcriptomics_data_service.logger import get_logger

__all__ = ["authz_plugin"]


def import_module_from_path(path):
    spec = importlib.util.spec_from_file_location("authz_plugin", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# TODO find a way to allow plugin writers to specify additional dependencies to be installed

AUTHZ_MODULE_PATH = "/tds/lib/authz.module.py"
authz_plugin_module = import_module_from_path(AUTHZ_MODULE_PATH)

# Get the concrete authz middleware from the provided plugin module
authz_plugin: BaseAuthzMiddleware = authz_plugin_module.authz_middleware
