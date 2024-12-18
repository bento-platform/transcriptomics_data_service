from fastapi import APIRouter, HTTPException, status, Query

from transcriptomics_data_service.db import DatabaseDependency
from transcriptomics_data_service.logger import LoggerDependency
from transcriptomics_data_service.models import (
    GeneExpressionData,
    GeneExpressionResponse,
    PaginationMeta,
    MethodEnum,
    QueryParameters,
)

query_router = APIRouter(prefix="/query")


async def get_expressions_handler(
    params: QueryParameters,
    db: DatabaseDependency,
    logger: LoggerDependency,
):
    """
    Handler for fetching and returning gene expression data.
    """
    logger.info(f"Received query parameters: {params}")

    expressions, total_records = await db.fetch_gene_expressions(
        genes=params.genes,
        experiments=params.experiments,
        sample_ids=params.sample_ids,
        method=params.method.value,
        page=params.page,
        page_size=params.page_size,
    )

    if not expressions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No gene expression data found for the given parameters.",
        )

    response_data = []
    method_field = f"{params.method.value}_count" if params.method != MethodEnum.raw else "raw_count"
    for expr in expressions:
        count = getattr(expr, method_field)
        response_item = GeneExpressionData(
            gene_code=expr.gene_code,
            sample_id=expr.sample_id,
            experiment_result_id=expr.experiment_result_id,
            count=count,
            method=method_field,
        )
        response_data.append(response_item)

    total_pages = (total_records + params.page_size - 1) // params.page_size
    pagination_meta = PaginationMeta(
        total_records=total_records,
        page=params.page,
        page_size=params.page_size,
        total_pages=total_pages,
    )

    return GeneExpressionResponse(expressions=response_data, pagination=pagination_meta)


@query_router.post(
    "/expressions",
    status_code=status.HTTP_200_OK,
    response_model=GeneExpressionResponse,
)
async def get_expressions_post(
    params: QueryParameters,
    db: DatabaseDependency,
    logger: LoggerDependency,
):
    """
    Retrieve gene expression data via POST request.

    Example JSON body:
    {
        "genes": ["gene1", "gene2"],
        "experiments": ["exp1"],
        "sample_ids": ["sample1"],
        "method": "tmm",
        "page": 1,
        "page_size": 100
    }
    """
    return await get_expressions_handler(params, db, logger)
