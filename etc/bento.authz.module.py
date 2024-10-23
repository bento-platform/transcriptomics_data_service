from typing import Annotated
from bento_lib.auth.middleware.fastapi import FastApiAuthMiddleware
from bento_lib.auth.permissions import P_INGEST_DATA, P_DELETE_DATA, P_QUERY_DATA, Permission
from bento_lib.auth.resources import RESOURCE_EVERYTHING, build_resource
from fastapi import Depends, Request

from transcriptomics_data_service.config import get_config
from transcriptomics_data_service.logger import get_logger
from transcriptomics_data_service.authz.middleware_base import BaseAuthzMiddleware

import re

config = get_config()
logger = get_logger(config)


class BentoAuthzMiddleware(FastApiAuthMiddleware, BaseAuthzMiddleware):
    """
    Concrete implementation of BaseAuthzMiddleware to authorize with Bento's authorization service/model.
    Extends the bento-lib FastApiAuthMiddleware, which includes all the middleware lifecycle and authorization logic.

    Notes:
        - This middleware plugin will only work with a Bento authorization-service.
        - TDS should be able to perform HTTP requests on the authz service url: `config.bento_authz_service_url`
    """

    def _build_resource_from_id(self, experiment_result_id: str):
        # Injectable for endpoints that use the 'experiment_result_id' param to create the authz Resource
        # Ownsership of an experiment is baked-in the ExperimentResult's ID in Bento
        # e.g. "<project-id>--<dataset-id>--<experiment_id>"
        # TODO: come up with better delimiters
        [project, dataset, experiment] = re.split("--", experiment_result_id)
        print(project, dataset, experiment)
        self._logger.debug(
            f"Injecting resource: project={project} dataset={dataset} experiment_result_id={experiment_result_id}"
        )
        return build_resource(project, dataset)

    def _dep_require_permission_injected_resource(
        self,
        permission: Permission,
    ):
        # Given a permission and the injected resource, will evaluate if operation is allowed
        async def inner(
            request: Request,
            resource: Annotated[dict, Depends(self._build_resource_from_id)],  # Inject resource
        ):
            await self.async_check_authz_evaluate(request, frozenset({permission}), resource, set_authz_flag=True)

        return Depends(inner)

    def _dep_perm_data_everything(self, permission: Permission):
        return self.dep_require_permissions_on_resource(
            permissions=frozenset({permission}),
            resource=RESOURCE_EVERYTHING,
        )

    # INGESTION router paths

    def dep_authz_ingest(self):
        # User needs P_INGEST_DATA permission on the target resource (injected)
        return self._dep_require_permission_injected_resource(P_INGEST_DATA)

    def dep_authz_normalize(self):
        return self._dep_require_permission_injected_resource(P_INGEST_DATA)

    # EXPERIMENT RESULT router paths

    def dep_authz_get_experiment_result(self):
        return self._dep_require_permission_injected_resource(P_QUERY_DATA)

    def dep_authz_delete_experiment_result(self):
        return self._dep_require_permission_injected_resource(P_DELETE_DATA)

    # EXPRESSIONS router paths

    def dep_authz_expressions_list(self):
        return self._dep_perm_data_everything(P_QUERY_DATA)


authz_middleware = BentoAuthzMiddleware.build_from_fastapi_pydantic_config(config, logger)
