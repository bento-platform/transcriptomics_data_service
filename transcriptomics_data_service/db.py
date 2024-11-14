import logging
from typing import Annotated, AsyncIterator, List, Tuple
import asyncpg
from bento_lib.db.pg_async import PgAsyncDatabase
from contextlib import asynccontextmanager
from fastapi import Depends
from functools import lru_cache
from pathlib import Path

from .config import Config, ConfigDependency
from .logger import LoggerDependency
from .models import ExperimentResult, GeneExpression


SCHEMA_PATH = Path(__file__).parent / "sql" / "schema.sql"


class Database(PgAsyncDatabase):
    def __init__(self, config: Config, logger: logging.Logger):
        self._config = config
        self.logger = logger
        super().__init__(config.database_uri, SCHEMA_PATH)

    async def _execute(self, *args):
        conn: asyncpg.Connection
        async with self.connect() as conn:
            await conn.execute(*args)

    ##########################
    # CRUD: experiment_results
    ##########################
    async def create_experiment_result(self, exp: ExperimentResult, transaction_conn: asyncpg.Connection | None = None):
        query = """
        INSERT INTO experiment_results (experiment_result_id, assembly_id, assembly_name)
        VALUES ($1, $2, $3)
        """
        execute_args = (query, exp.experiment_result_id, exp.assembly_id, exp.assembly_name)
        if transaction_conn is not None:
            # execute within transaction if a transaction_conn is passed
            await transaction_conn.execute(*execute_args)
        else:
            # auto-commit if not part of a transaction
            await self._execute(*execute_args)
        self.logger.info(
            f"Created experiment_results row: {exp.experiment_result_id} {exp.assembly_name} {exp.assembly_id}"
        )

    async def read_experiment_result(self, exp_id: str) -> ExperimentResult | None:
        conn: asyncpg.Connection
        async with self.connect() as conn:
            res = await conn.fetchrow("SELECT * FROM experiment_results WHERE experiment_result_id = $1", exp_id)

        if res is None:
            return None

        self.logger.debug(f"READ experiment_results ID={exp_id}")
        return ExperimentResult(
            experiment_result_id=res["experiment_result_id"],
            assembly_name=res["assembly_name"],
            assembly_id=res["assembly_id"],
        )

    async def update_experiment_result(self, exp: ExperimentResult):
        await self._execute(
            *(
                "UPDATE experiment_results SET assembly_id = $2, assembly_name = $3 WHERE experiment_result_id = $1",
                exp.experiment_result_id,
                exp.assembly_id,
                exp.assembly_name,
            )
        )

    async def delete_experiment_result(self, exp_id: str):
        await self._execute(*("DELETE FROM experiment_results WHERE experiment_result_id = $1", exp_id))
        self.logger.info(f"Deleted experiment_result row {exp_id}")

    ########################
    # CRUD: gene_expressions
    ########################
    async def create_gene_expressions(self, expressions: list[GeneExpression], transaction_conn: asyncpg.Connection):
        """
        Creates rows on gene_expression as part of an Atomic transaction
        Rows on gene_expressions can only be created as part of an RCM ingestion.
        Ingestion is all-or-nothing, hence the transaction.
        """
        # Prepare data for bulk insertion
        records = [
            (
                expr.gene_code,
                expr.sample_id,
                expr.experiment_result_id,
                expr.raw_count,
                expr.tpm_count,
                expr.tmm_count,
                expr.getmm_count,
            )
            for expr in expressions
        ]

        query = """
        INSERT INTO gene_expressions (
            gene_code, sample_id, experiment_result_id, raw_count, tpm_count, tmm_count, getmm_count
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """

        await transaction_conn.executemany(query, records)
        self.logger.info(f"Inserted {len(records)} gene expression records.")

    async def fetch_expressions(self) -> tuple[GeneExpression, ...]:
        return tuple([r async for r in self._select_expressions(None)])

    async def _select_expressions(self, exp_id: str | None) -> AsyncIterator[GeneExpression]:
        conn: asyncpg.Connection
        where_clause = "WHERE experiment_result_id = $1" if exp_id is not None else ""
        query = f"SELECT * FROM gene_expressions {where_clause}"
        async with self.connect() as conn:
            res = await conn.fetch(query, *(exp_id,) if exp_id is not None else ())
        for r in map(lambda g: self._deserialize_gene_expression(g), res):
            yield r

    def _deserialize_gene_expression(self, rec: asyncpg.Record) -> GeneExpression:
        return GeneExpression(
            gene_code=rec["gene_code"],
            sample_id=rec["sample_id"],
            experiment_result_id=rec["experiment_result_id"],
            raw_count=rec["raw_count"],
            tpm_count=rec["tpm_count"],
            tmm_count=rec["tmm_count"],
            getmm_count=rec["getmm_count"],
        )

    ############################
    # CRUD: gene_expression_norm
    ############################

    async def fetch_gene_expressions_by_experiment_id(self, experiment_result_id: str) -> Tuple[GeneExpression, ...]:
        """
        Fetch gene expressions for a specific experiment_result_id.
        """
        conn: asyncpg.Connection
        async with self.connect() as conn:
            query = """
            SELECT * FROM gene_expressions WHERE experiment_result_id = $1
            """
            res = await conn.fetch(query, experiment_result_id)
        return tuple([self._deserialize_gene_expression(record) for record in res])

    async def update_normalized_expressions(self, expressions: List[GeneExpression], method: str):
        """
        Update the normalized expressions in the database using batch updates.
        """
        conn: asyncpg.Connection
        async with self.connect() as conn:
            async with conn.transaction():
                if method == "tpm":
                    column = "tpm_count"
                elif method == "tmm":
                    column = "tmm_count"
                elif method == "getmm":
                    column = "getmm_count"
                else:
                    raise ValueError(f"Unsupported normalization method: {method}")

                # Prepare data for bulk update
                records = [
                    (
                        getattr(expr, column),
                        expr.experiment_result_id,
                        expr.gene_code,
                        expr.sample_id,
                    )
                    for expr in expressions
                ]

                await conn.execute(
                    f"""
                    CREATE TEMPORARY TABLE temp_updates (
                        value DOUBLE PRECISION,
                        experiment_result_id VARCHAR(255),
                        gene_code VARCHAR(255),
                        sample_id VARCHAR(255)
                    ) ON COMMIT DROP
                    """
                )

                await conn.copy_records_to_table(
                    "temp_updates",
                    records=records,
                    columns=["value", "experiment_result_id", "gene_code", "sample_id"],
                )

                # Update the main table
                await conn.execute(
                    f"""
                    UPDATE gene_expressions
                    SET {column} = temp_updates.value
                    FROM temp_updates
                    WHERE gene_expressions.experiment_result_id = temp_updates.experiment_result_id
                      AND gene_expressions.gene_code = temp_updates.gene_code
                      AND gene_expressions.sample_id = temp_updates.sample_id
                    """
                )
        self.logger.info(f"Updated normalized values for method '{method}'.")

    @asynccontextmanager
    async def transaction_connection(self):
        conn: asyncpg.Connection
        async with self.connect() as conn:
            async with conn.transaction():
                # operations must be made using this connection for the transaction to apply
                yield conn


@lru_cache()
def get_db(config: ConfigDependency, logger: LoggerDependency) -> Database:
    return Database(config, logger)


DatabaseDependency = Annotated[Database, Depends(get_db)]
