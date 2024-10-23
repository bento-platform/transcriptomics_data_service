from fastapi import APIRouter, status

from transcriptomics_data_service.authz.plugin import authz_plugin
from transcriptomics_data_service.db import DatabaseDependency

__all__ = ["expression_router"]

expression_router = APIRouter(prefix="/expressions", dependencies=authz_plugin.dep_expression_router())


@expression_router.get("", status_code=status.HTTP_200_OK, dependencies=[authz_plugin.dep_authz_expressions_list()])
async def expressions_list(db: DatabaseDependency):
    return await db.fetch_expressions()
