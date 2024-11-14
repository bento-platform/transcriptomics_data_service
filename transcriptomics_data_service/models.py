from pydantic import BaseModel

__all__ = [
    "ExperimentResult",
    "GeneExpression",
]


class ExperimentResult(BaseModel):
    experiment_result_id: str
    assembly_id: str | None = None
    assembly_name: str | None = None


class GeneExpression(BaseModel):
    gene_code: str
    sample_id: str
    experiment_result_id: str
    raw_count: int
    tpm_count: float | None = None
    tmm_count: float | None = None
    getmm_count: float | None = None
