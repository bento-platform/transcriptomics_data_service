from pydantic import BaseModel, Field, validator
from typing import List, Optional
from enum import Enum

__all__ = [
    "ExperimentResult",
    "GeneExpression",
    "GeneExpressionData",
    "PaginationMeta",
    "GeneExpressionResponse",
    "MethodEnum",
    "QueryParameters",
]


class ExperimentResult(BaseModel):
    experiment_result_id: str = Field(..., min_length=1, max_length=255)
    assembly_id: Optional[str] = Field(None, max_length=255)
    assembly_name: Optional[str] = Field(None, max_length=255)


class GeneExpression(BaseModel):
    gene_code: str = Field(..., min_length=1, max_length=255)
    sample_id: str = Field(..., min_length=1, max_length=255)
    experiment_result_id: str = Field(..., min_length=1, max_length=255)
    raw_count: int
    tpm_count: Optional[float] = None
    tmm_count: Optional[float] = None
    getmm_count: Optional[float] = None


class GeneExpressionData(BaseModel):
    gene_code: str = Field(..., min_length=1, max_length=255, description="Gene code")
    sample_id: str = Field(..., min_length=1, max_length=255, description="Sample ID")
    experiment_result_id: str = Field(..., min_length=1, max_length=255, description="Experiment result ID")
    count: float = Field(..., description="Expression count")
    method: str = Field(..., description="Method used to calculate the expression count")


class PaginationMeta(BaseModel):
    total_records: int = Field(..., ge=0, description="Total number of records")
    page: int = Field(..., ge=1, description="Current page number")
    page_size: int = Field(..., ge=1, le=1000, description="Number of records per page")
    total_pages: int = Field(..., ge=1, description="Total number of pages")


class GeneExpressionResponse(BaseModel):
    expressions: List[GeneExpressionData]
    pagination: PaginationMeta


class MethodEnum(str, Enum):
    raw = "raw"
    tpm = "tpm"
    tmm = "tmm"
    getmm = "getmm"


class QueryParameters(BaseModel):
    genes: Optional[List[str]] = Field(None, description="List of gene codes to retrieve")
    experiments: Optional[List[str]] = Field(None, description="List of experiment result IDs to retrieve data from")
    sample_ids: Optional[List[str]] = Field(None, description="List of sample IDs to retrieve data from")
    method: MethodEnum = Field(MethodEnum.raw, description="Data method to retrieve: 'raw', 'tpm', 'tmm', 'getmm'")
    page: int = Field(
        1,
        ge=1,
        description="Page number for pagination (must be greater than or equal to 1)",
    )
    page_size: int = Field(
        100,
        ge=1,
        le=1000,
        description="Number of records per page (between 1 and 1000)",
    )

    @validator("genes", "experiments", "sample_ids", each_item=True)
    def validate_identifiers(cls, value):
        if not (1 <= len(value) <= 255):
            raise ValueError("Each identifier must be between 1 and 255 characters long.")
        if not value.replace("_", "").isalnum():
            raise ValueError("Identifiers must contain only alphanumeric characters and underscores.")
        return value
