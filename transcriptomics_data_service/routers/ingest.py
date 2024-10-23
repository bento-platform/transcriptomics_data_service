from fastapi import APIRouter, File, Request, UploadFile, status
import csv
import json
from io import StringIO

from transcriptomics_data_service.db import DatabaseDependency
from transcriptomics_data_service.models import ExperimentResult, GeneExpression
from transcriptomics_data_service.authz.plugin import authz_plugin

__all__ = ["ingest_router"]

ingest_router = APIRouter(dependencies=authz_plugin.dep_ingest_router())

# TODO make configurable? an argument?
GENE_ID_KEY = "GeneID"


@ingest_router.post(
    "/ingest/{experiment_result_id}/assembly-name/{assembly_name}/assembly-id/{assembly_id}",
    status_code=status.HTTP_200_OK,
    # Injects the plugin authz middleware dep_authorize_ingest function
    dependencies=[authz_plugin.dep_authz_ingest()],
)
async def ingest(
    request: Request,
    db: DatabaseDependency,
    experiment_result_id: str,
    assembly_name: str,
    assembly_id: str,
    rcm_file: UploadFile = File(...),
):
    # Read and process rcm file
    file_bytes = rcm_file.file.read()
    buffer = StringIO(file_bytes.decode("utf-8"))
    rcm = {}
    for row in csv.DictReader(buffer):
        # print(row)
        rcm[row[GENE_ID_KEY]] = row
    # rcm["WASH6P"]  would return something like:
    # {'GeneID': 'WASH6P', '<BIOSAMPLE_ID_1>': '63', '<BIOSAMPLE_ID_2>: '0', ...}
    # TODO read counts as integers

    experiment_result = ExperimentResult(
        experiment_result_id=experiment_result_id, assembly_name=assembly_name, assembly_id=assembly_id
    )

    # Perform the ingestion in a transaction
    async with db.transaction_connection() as transaction_con:
        # For each matrix: create ONE row in ExperimentResult
        await db.create_experiment_result(experiment_result, transaction_con)

        # TODO For EACH cell in the matrix: create one row in GeneExpression
        # gene_expressions: list[GeneExpression]
        # await db.create_gene_expressions(gene_expressions, transaction_con)
    return


@ingest_router.post(
    "/normalize/{experiment_result_id}",
    status_code=status.HTTP_200_OK,
    dependencies=[authz_plugin.dep_authz_normalize()],
)
async def normalize(
    db: DatabaseDependency,
    experiment_result_id: str,
    features_lengths_file: UploadFile = File(...),
    status_code=status.HTTP_200_OK,
):
    features_lengths = json.load(features_lengths_file.file)
    # TODO validate shape
    # TODO validate experiment_result_id exists
    # TODO algorithm selection argument?
    # TODO perform the normalization in a transaction
    return
