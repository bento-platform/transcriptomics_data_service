from fastapi import APIRouter, HTTPException, UploadFile, File, status
import pandas as pd
from io import StringIO

from transcriptomics_data_service.db import DatabaseDependency
from transcriptomics_data_service.models import GeneExpression
from transcriptomics_data_service.scripts.normalize import (
    read_counts2tpm,
    tmm_normalization,
    getmm_normalization,
)

# Constants for normalization methods
NORM_TPM = "tpm"
NORM_TMM = "tmm"
NORM_GETMM = "getmm"

# List of all valid normalization methods
VALID_METHODS = [NORM_TPM, NORM_TMM, NORM_GETMM]

__all__ = ["normalization_router"]

normalization_router = APIRouter(prefix="/normalize")


@normalization_router.post(
    "/{method}/{experiment_result_id}",
    status_code=status.HTTP_200_OK,
)
async def normalize(
    method: str,
    experiment_result_id: str,
    db: DatabaseDependency,
    gene_lengths_file: UploadFile = File(None),
):
    """
    Normalize gene expressions using the specified method for a given experiment_result_id.
    """
    # method validation
    if method not in VALID_METHODS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported normalization method: {method}"
        )

    # load gene lengths
    if method in [NORM_TPM, NORM_GETMM]:
        if gene_lengths_file is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Gene lengths file is required for {method.upper()} normalization.",
            )
        gene_lengths = await _load_gene_lengths(gene_lengths_file)

    raw_counts_df = await _fetch_raw_counts(db, experiment_result_id)

    # normalization
    if method == NORM_TPM:
        raw_counts_df, gene_lengths_series = _align_gene_lengths(raw_counts_df, gene_lengths)
        normalized_df = read_counts2tpm(raw_counts_df, gene_lengths_series)
    elif method == NORM_TMM:
        normalized_df = tmm_normalization(raw_counts_df)
    elif method == NORM_GETMM:
        raw_counts_df, gene_lengths_series = _align_gene_lengths(raw_counts_df, gene_lengths)
        normalized_df = getmm_normalization(raw_counts_df, gene_lengths_series)

    #  database update using normalized values
    await _update_normalized_values(db, normalized_df, experiment_result_id, method=method)

    return {"message": f"{method.upper()} normalization completed successfully"}


async def _load_gene_lengths(gene_lengths_file: UploadFile) -> pd.Series:
    """
    Load gene lengths from the uploaded file.
    """
    content = await gene_lengths_file.read()
    gene_lengths_df = pd.read_csv(StringIO(content.decode("utf-8")), index_col="GeneID")
    gene_lengths_series = gene_lengths_df["GeneLength"]
    gene_lengths_series = gene_lengths_series.apply(pd.to_numeric, errors="raise")
    return gene_lengths_series


async def _fetch_raw_counts(db, experiment_result_id: str) -> pd.DataFrame:
    """
    Fetch raw counts from the database for the given experiment_result_id.
    Returns a DataFrame with genes as rows and samples as columns.
    """
    expressions = await db.fetch_gene_expressions_by_experiment_id(experiment_result_id)
    if not expressions:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment result not found.")

    data = []
    for expr in expressions:
        data.append({"GeneID": expr.gene_code, "SampleID": expr.sample_id, "RawCount": expr.raw_count})
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
            status_code=status.HTTP_400_BAD_REQUEST, detail="No common genes between counts and gene lengths."
        )
    raw_counts_df = raw_counts_df.loc[common_genes]
    gene_lengths_series = gene_lengths.loc[common_genes]
    return raw_counts_df, gene_lengths_series


async def _update_normalized_values(db, normalized_df: pd.DataFrame, experiment_result_id: str, method: str):
    """
    Update the normalized values in the database
    """
    # Fetch existing expressions to get raw_count values
    existing_expressions = await db.fetch_gene_expressions_by_experiment_id(experiment_result_id)
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

        gene_expression = GeneExpression(
            gene_code=gene_code,
            sample_id=sample_id,
            experiment_result_id=experiment_result_id,
            raw_count=raw_count,
            tpm_count=row["NormalizedValue"] if method == NORM_TPM else None,
            tmm_count=row["NormalizedValue"] if method == NORM_TMM else None,
            getmm_count=row["NormalizedValue"] if method == NORM_GETMM else None,
        )
        expressions.append(gene_expression)

    # Update expressions in the database
    await db.update_normalized_expressions(expressions, method)
