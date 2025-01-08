from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum

__all__ = [
    "ExperimentResult",
    "GeneExpression",
    "GeneExpressionData",
    "GeneExpressionResponse",
    "NormalizationMethodEnum",
    "ExpressionQueryBody",
]


TPM = "tpm"
TMM = "tmm"
GETMM = "getmm"
RAW = "raw"


class NormalizationMethodEnum(str, Enum):
    tpm = TPM
    tmm = TMM
    getmm = GETMM


class CountTypesEnum(str, Enum):
    raw = RAW
    # normalized counts
    tpm = TPM
    tmm = TMM
    getmm = GETMM


class PaginatedRequest(BaseModel):
    page: int = Field(1, ge=1, description="Current page number")
    page_size: int = Field(100, ge=1, le=1000, description="Number of records per page")


class PaginatedResponse(PaginatedRequest):
    total_records: int = Field(..., ge=0, description="Total number of records")
    total_pages: int = Field(..., ge=1, description="Total number of pages")


class ExperimentResult(BaseModel):
    experiment_result_id: str = Field(..., min_length=1, max_length=255)
    assembly_id: Optional[str] = Field(None, max_length=255)
    assembly_name: Optional[str] = Field(None, max_length=255)


class GeneExpression(BaseModel):
    gene_code: str = Field(..., min_length=1, max_length=255, description="Feature identifier")
    sample_id: str = Field(..., min_length=1, max_length=255, description="Sample identifier")
    experiment_result_id: str = Field(..., min_length=1, max_length=255, description="ExperimentResult identifier")
    raw_count: int = Field(..., ge=0, description="The raw count for the given feature")
    tpm_count: Optional[float] = Field(None, ge=0, description="TPM normalized count")
    tmm_count: Optional[float] = Field(None, ge=0, description="TMM normalized count")
    getmm_count: Optional[float] = Field(None, ge=0, description="GETMM normalized count")


class GeneExpressionData(BaseModel):
    gene_code: str = Field(..., min_length=1, max_length=255, description="Gene code")
    sample_id: str = Field(..., min_length=1, max_length=255, description="Sample ID")
    experiment_result_id: str = Field(..., min_length=1, max_length=255, description="Experiment result ID")
    count: float = Field(..., description="Expression count")


class ExpressionQueryBody(PaginatedRequest):
    genes: Optional[List[str]] = Field(None, description="List of gene codes to retrieve")
    experiments: Optional[List[str]] = Field(None, description="List of experiment result IDs to retrieve data from")
    sample_ids: Optional[List[str]] = Field(None, description="List of sample IDs to retrieve data from")
    method: CountTypesEnum = Field(
        CountTypesEnum.raw,
        description="Data method to retrieve: 'raw', 'tpm', 'tmm', 'getmm'",
    )


class GeneExpressionResponse(PaginatedResponse):
    query: ExpressionQueryBody = Field(..., description="The query that produced this response")
    expressions: List[GeneExpressionData] = Field(..., description="List of gene expressions")
