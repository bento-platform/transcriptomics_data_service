import logging
from typing import Annotated, AsyncIterator, Callable, List, Tuple, Optional
import asyncpg
from bento_lib.db.pg_async import PgAsyncDatabase
from contextlib import asynccontextmanager
from fastapi import Depends
from functools import lru_cache
from pathlib import Path


from .config import Config, ConfigDependency
from .logger import LoggerDependency
from .models import (
    CountTypesEnum,
    ExperimentResult,
    GeneExpression,
    GeneExpressionData,
    NormalizationMethodEnum,
    PaginatedRequest,
)

SCHEMA_PATH = Path(__file__).parent / "sql" / "schema.sql"

DEFAULT_PAGINATION: PaginatedRequest = PaginatedRequest(page=1, page_size=100)


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
        execute_args = (
            query,
            exp.experiment_result_id,
            exp.assembly_id,
            exp.assembly_name,
        )
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
            res = await conn.fetchrow(
                "SELECT * FROM experiment_results WHERE experiment_result_id = $1",
                exp_id,
            )

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

    def _deserialize_experiment_result(self, record: asyncpg.Record) -> ExperimentResult:
        return ExperimentResult(
            experiment_result_id=record["experiment_result_id"],
            assembly_id=record["assembly_id"],
            assembly_name=record["assembly_name"],
        )

    ############################
    # fetch experiment_results
    ############################

    def _paginated_query(
        self, base_query: str, base_params: List, pagination: PaginatedRequest | None
    ) -> Tuple[str, List]:
        # Ignore if None
        if pagination is None:
            return base_query, base_params

        # Parametrize pagination if provided
        offset = (pagination.page - 1) * pagination.page_size

        # take base query params into account, if provided
        params = [*base_params]
        params_count = len(params)
        params.append(pagination.page_size)
        params.append(offset)
        query = f"{base_query.strip()} LIMIT ${params_count + 1} OFFSET ${params_count + 2}"
        return query, params

    async def fetch_experiment_results(
        self,
        pagination: PaginatedRequest | None = DEFAULT_PAGINATION,
    ) -> Tuple[List[ExperimentResult], int]:
        base_query = "SELECT * FROM experiment_results ORDER BY experiment_result_id"
        count_query = "SELECT COUNT(*) FROM experiment_results"
        async with self.connect() as conn:
            total_records = await conn.fetchval(count_query)
            query, params = self._paginated_query(base_query, [], pagination)
            rows = await conn.fetch(query, *params)
        items = [self._deserialize_experiment_result(r) for r in rows]
        return items, total_records

    async def fetch_experiment_samples(
        self,
        experiment_result_id: str,
        pagination: PaginatedRequest | None = DEFAULT_PAGINATION,
    ) -> Tuple[List[str], int]:
        """
        Returns (list_of_sample_ids, total_records) for a single experiment_result_id.
        """
        count_query = """
            SELECT COUNT(DISTINCT sample_id)
            FROM gene_expressions
            WHERE experiment_result_id = $1
        """
        base_query = """
            SELECT DISTINCT sample_id
            FROM gene_expressions
            WHERE experiment_result_id = $1
            ORDER BY sample_id
        """
        async with self.connect() as conn:
            total_records = await conn.fetchval(count_query, experiment_result_id)
            query, params = self._paginated_query(base_query, [experiment_result_id], pagination)
            rows = await conn.fetch(query, *params)
        items = [r["sample_id"] for r in rows]
        return items, total_records

    async def fetch_experiment_features(
        self, experiment_result_id: str, pagination: PaginatedRequest | None = DEFAULT_PAGINATION
    ) -> Tuple[List[str], int]:
        """
        Returns (list_of_features, total_records) for a single experiment_result_id.
        """
        count_query = """
            SELECT COUNT(DISTINCT gene_code)
            FROM gene_expressions
            WHERE experiment_result_id = $1
        """
        base_query = """
            SELECT DISTINCT gene_code
            FROM gene_expressions
            WHERE experiment_result_id = $1
            ORDER BY gene_code
        """

        async with self.connect() as conn:
            total_records = await conn.fetchval(count_query, experiment_result_id)
            query, params = self._paginated_query(base_query, [experiment_result_id], pagination)
            rows = await conn.fetch(query, *params)

        items = [r["gene_code"] for r in rows]
        return items, total_records

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
    # Normalization Methods
    ############################

    async def update_normalized_expressions(self, expressions: List[GeneExpression], method: NormalizationMethodEnum):
        """
        Update the normalized expressions in the database using batch updates.
        """
        column = f"{method.value}_count"
        if not column:
            raise ValueError(f"Unsupported normalization method: {method}")
        conn: asyncpg.Connection
        async with self.transaction_connection() as conn:
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
                """
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

    async def fetch_gene_expressions(
        self,
        genes: Optional[List[str]] = None,
        experiments: Optional[List[str]] = None,
        sample_ids: Optional[List[str]] = None,
        method: CountTypesEnum = CountTypesEnum.raw,
        page: int = 1,
        page_size: int = 100,
        paginate: bool = True,
        mapping: GeneExpression | GeneExpressionData = GeneExpression,
    ) -> Tuple[List[GeneExpression] | List[GeneExpressionData], int]:
        """
        Fetch gene expressions based on genes, experiments, sample_ids, and method, with optional pagination.
        Returns a tuple of (expressions list, total_records count).
        """
        conn: asyncpg.Connection
        async with self.connect() as conn:
            # Query builder
            base_query = """
                SELECT gene_code, sample_id, experiment_result_id, raw_count, tpm_count, tmm_count, getmm_count
                FROM gene_expressions
                """
            count_query = "SELECT COUNT(*) FROM gene_expressions"
            conditions = []
            params = []
            param_counter = 1

            if genes:
                conditions.append(f"gene_code = ANY(${param_counter}::text[])")
                params.append(genes)
                param_counter += 1

            if experiments:
                conditions.append(f"experiment_result_id = ANY(${param_counter}::text[])")
                params.append(experiments)
                param_counter += 1

            if sample_ids:
                conditions.append(f"sample_id = ANY(${param_counter}::text[])")
                params.append(sample_ids)
                param_counter += 1

            if method.value != "raw":
                conditions.append(f"{method.value}_count IS NOT NULL")

            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

            order_clause = " ORDER BY gene_code, sample_id"

            query = base_query + where_clause + order_clause
            count_query += where_clause

            # Pagination
            if paginate:
                limit_offset_clause = f" LIMIT ${param_counter} OFFSET ${param_counter + 1}"
                params.extend([page_size, (page - 1) * page_size])
                query += limit_offset_clause

            total_records_params = params[:-2] if paginate else params
            total_records = await conn.fetchval(count_query, *total_records_params)

            res = await conn.fetch(query, *params)

        if mapping is GeneExpression:
            expressions = [self._deserialize_gene_expression(record) for record in res]
        else:
            # For the /expressions endpoint
            # Returns a lightweight representation of a GeneExpression as GeneExpressionData,
            # which only contains the requested count type.
            count_col = f"{method.value}_count"
            expressions = [
                GeneExpressionData(
                    gene_code=record["gene_code"],
                    sample_id=record["sample_id"],
                    experiment_result_id=record["experiment_result_id"],
                    count=record[count_col],
                )
                for record in res
            ]

        return expressions, total_records


@lru_cache()
def get_db(config: ConfigDependency, logger: LoggerDependency) -> Database:
    return Database(config, logger)


DatabaseDependency = Annotated[Database, Depends(get_db)]
