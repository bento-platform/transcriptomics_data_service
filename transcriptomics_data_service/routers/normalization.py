from fastapi import APIRouter, HTTPException, UploadFile, File, status
import pandas as pd
from io import StringIO

from transcriptomics_data_service.db import DatabaseDependency
from transcriptomics_data_service.models import (
    CountTypesEnum,
    GeneExpression,
    NormalizationMethodEnum,
)
from transcriptomics_data_service.normalization_utils import (
    getmm_normalization,
    tmm_normalization,
    tpm_normalization,
)


__all__ = ["normalization_router"]


REQUIRES_GENES_LENGHTS = [NormalizationMethodEnum.tpm, NormalizationMethodEnum.getmm]

normalization_router = APIRouter(prefix="/normalize")


@normalization_router.post(
    "/{experiment_result_id}/{method}",
    status_code=status.HTTP_200_OK,
)
async def normalize(
    db: DatabaseDependency,
    experiment_result_id: str,
    method: NormalizationMethodEnum,
    gene_lengths_file: UploadFile = File(None),
):
    """
    Normalize gene expressions using the specified method for a given experiment_result_id.
    """

    # Load gene lengths if required
    if method.lower() in [NormalizationMethodEnum.tpm, NormalizationMethodEnum.getmm]:
        if gene_lengths_file is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Gene lengths file is required for {method.upper()} normalization.",
            )
        gene_lengths = await _load_gene_lengths(gene_lengths_file)
    else:
        gene_lengths = None

    # Fetch raw counts from the database
    raw_counts_df = await _fetch_raw_counts(db, experiment_result_id)

    # Perform normalization
    if method is NormalizationMethodEnum.tpm:
        raw_counts_df, gene_lengths_series = _align_gene_lengths(raw_counts_df, gene_lengths)
        normalized_df = tpm_normalization(raw_counts_df, gene_lengths_series)
    elif method is NormalizationMethodEnum.tmm:
        normalized_df = tmm_normalization(raw_counts_df)
    elif method is NormalizationMethodEnum.getmm:
        raw_counts_df, gene_lengths_series = _align_gene_lengths(raw_counts_df, gene_lengths)
        normalized_df = getmm_normalization(raw_counts_df, gene_lengths_series)

    # Update database with normalized values
    await _update_normalized_values(db, normalized_df, experiment_result_id, method)

    return {"message": f"{method.upper()} normalization completed successfully"}


async def _load_gene_lengths(gene_lengths_file: UploadFile) -> pd.Series:
    """
    Load gene lengths from the uploaded file.
    """
    content = await gene_lengths_file.read()
    gene_lengths_df = pd.read_csv(StringIO(content.decode("utf-8")), index_col=0)
    if gene_lengths_df.shape[1] != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Gene lengths file should contain exactly one column of gene lengths.",
        )
    gene_lengths_series = gene_lengths_df.iloc[:, 0]
    gene_lengths_series = gene_lengths_series.apply(pd.to_numeric, errors="raise")
    return gene_lengths_series


async def _fetch_raw_counts(db: DatabaseDependency, experiment_result_id: str) -> pd.DataFrame:
    """
    Fetch raw counts from the database for the given experiment_result_id.
    Returns a DataFrame with genes as rows and samples as columns.
    """
    expressions, _ = await db.fetch_gene_expressions(
        experiments=[experiment_result_id], method=CountTypesEnum.raw, paginate=False
    )
    if not expressions:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment result not found.")

    # TODO: this extra loop is not needed, Ill modify db.fetch_gene_expressions 
    # so we can pass a lambda mapper to convert directly from DB records, 
    # with no intermediate GeneExpression deserialization.
    data = []
    for expr in expressions:
        data.append(
            {
                "GeneID": expr.gene_code,
                "SampleID": expr.sample_id,
                "RawCount": expr.raw_count,
            }
        )
    df = pd.DataFrame(data)
    raw_counts_df = df.pivot(index="GeneID", columns="SampleID", values="RawCount")

    raw_counts_df = raw_counts_df.apply(pd.to_numeric, errors="raise")

    return raw_counts_df


def _align_gene_lengths(raw_counts_df: pd.DataFrame, gene_lengths: pd.Series):
    """
    Align the gene lengths with the raw counts DataFrame based on GeneID.
    """
    common_genes = raw_counts_df.index.intersection(gene_lengths.index)
    if common_genes.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No common genes between counts and gene lengths.",
        )
    raw_counts_df = raw_counts_df.loc[common_genes]
    gene_lengths_series = gene_lengths.loc[common_genes]
    return raw_counts_df, gene_lengths_series


async def _update_normalized_values(
    db: DatabaseDependency,
    normalized_df: pd.DataFrame,
    experiment_result_id: str,
    method: NormalizationMethodEnum,
):
    """
    Update the normalized values in the database.
    """
    # Fetch existing expressions to get raw_count values
    existing_expressions, _ = await db.fetch_gene_expressions(
        experiments=[experiment_result_id], method=CountTypesEnum.raw, paginate=False
    )
    raw_count_dict = {(expr.gene_code, expr.sample_id): expr.raw_count for expr in existing_expressions}

    normalized_df = normalized_df.reset_index().melt(
        id_vars="GeneID", var_name="SampleID", value_name="NormalizedValue"
    )

    expressions = []
    for _, row in normalized_df.iterrows():
        gene_code = row["GeneID"]
        sample_id = row["SampleID"]
        raw_count = raw_count_dict.get((gene_code, sample_id))

        if raw_count is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Raw count not found for gene {gene_code}, sample {sample_id}",
            )

        # Create a GeneExpression object with the normalized value
        gene_expression = GeneExpression(
            gene_code=gene_code,
            sample_id=sample_id,
            experiment_result_id=experiment_result_id,
            raw_count=raw_count,
            tpm_count=row["NormalizedValue"] if method == NormalizationMethodEnum.tpm else None,
            tmm_count=row["NormalizedValue"] if method == NormalizationMethodEnum.tmm else None,
            getmm_count=row["NormalizedValue"] if method == NormalizationMethodEnum.getmm else None,
        )
        expressions.append(gene_expression)

    # Update expressions in the database
    await db.update_normalized_expressions(expressions, method)
