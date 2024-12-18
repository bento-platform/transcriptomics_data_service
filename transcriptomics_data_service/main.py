from contextlib import asynccontextmanager
from bento_lib.apps.fastapi import BentoFastAPI
from fastapi import FastAPI

from transcriptomics_data_service.db import get_db
from transcriptomics_data_service.routers.experiment_results import experiment_router
from transcriptomics_data_service.routers.ingest import ingest_router
from transcriptomics_data_service.routers.normalization import normalization_router
from transcriptomics_data_service.routers.query import query_router
from . import __version__
from .config import get_config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .logger import get_logger

BENTO_SERVICE_INFO = {
    "serviceKind": BENTO_SERVICE_KIND,
    "dataService": True,
    "workflowProvider": True,
    "gitRepository": "https://github.com/bento-platform/transcriptomics_data_service",
}

config_for_setup = get_config()
logger_for_setup = get_logger(config_for_setup)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db = get_db(config_for_setup, logger_for_setup)

    await db.close()

    yield


app = BentoFastAPI(
    authz_middleware=None,
    config=config_for_setup,
    logger=logger_for_setup,
    bento_extra_service_info=BENTO_SERVICE_INFO,
    service_type=SERVICE_TYPE,
    version=__version__,
    lifespan=lifespan,
)

app.include_router(ingest_router)
app.include_router(experiment_router)
app.include_router(normalization_router)
app.include_router(query_router)
