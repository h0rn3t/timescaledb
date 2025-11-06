#!/usr/bin/env python3
"""
Parallel Query Processor for TimescaleDB Large Joins

This script demonstrates how to efficiently process large JOIN queries
between a hypertable and dimension table by parallelizing queries
at the application level.

Each query gets proper chunk exclusion because it has explicit bounds.
"""

import asyncio
import asyncpg
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ActivePeriod:
    """Represents a time period to query"""
    sensor_id: int
    start_time: datetime
    end_time: datetime
    period_id: Optional[int] = None


@dataclass
class QueryResult:
    """Result from a single period query"""
    period: ActivePeriod
    row_count: int
    duration_ms: float
    success: bool
    error: Optional[str] = None


class ParallelQueryProcessor:
    """
    Processes large JOIN queries in parallel by breaking them into
    smaller queries that TimescaleDB can optimize with chunk exclusion.
    """

    def __init__(
        self,
        dsn: str,
        max_connections: int = 10,
        batch_size: int = 1000,
        query_timeout: float = 60.0
    ):
        """
        Initialize the processor.

        Args:
            dsn: PostgreSQL connection string
            max_connections: Maximum concurrent database connections
            batch_size: Number of OR conditions per batch query
            query_timeout: Timeout per query in seconds
        """
        self.dsn = dsn
        self.max_connections = max_connections
        self.batch_size = batch_size
        self.query_timeout = query_timeout
        self.pool: Optional[asyncpg.Pool] = None

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()

    async def connect(self):
        """Create connection pool"""
        logger.info(f"Creating connection pool with {self.max_connections} connections")
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=2,
            max_size=self.max_connections,
            command_timeout=self.query_timeout
        )

    async def disconnect(self):
        """Close connection pool"""
        if self.pool:
            logger.info("Closing connection pool")
            await self.pool.close()

    async def get_active_periods(
        self,
        sensor_id: Optional[int] = None,
        status: str = 'DONE',
        limit: Optional[int] = None
    ) -> List[ActivePeriod]:
        """
        Fetch active periods from the database.

        Args:
            sensor_id: Optional filter by sensor_id
            status: Filter by status
            limit: Optional limit on number of periods

        Returns:
            List of ActivePeriod objects
        """
        query = """
            SELECT id, sensor_id, start_time, end_time
            FROM bench.active_periods
            WHERE status = $1
        """
        params = [status]

        if sensor_id is not None:
            query += " AND sensor_id = $2"
            params.append(sensor_id)

        if limit is not None:
            query += f" LIMIT ${len(params) + 1}"
            params.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        periods = [
            ActivePeriod(
                sensor_id=row['sensor_id'],
                start_time=row['start_time'],
                end_time=row['end_time'],
                period_id=row['id']
            )
            for row in rows
        ]

        logger.info(f"Loaded {len(periods)} active periods")
        return periods

    async def query_single_period(
        self,
        period: ActivePeriod
    ) -> QueryResult:
        """
        Query data for a single period with explicit bounds.
        This enables proper chunk exclusion.

        Args:
            period: The time period to query

        Returns:
            QueryResult with execution details
        """
        query = """
            SELECT
                s.sensor_id,
                s.measurement_time,
                s.measurement_value,
                p.start_time,
                p.end_time
            FROM bench.sensor_data s
            JOIN bench.active_periods p
              ON s.sensor_id = p.sensor_id
             AND s.measurement_time >= p.start_time
             AND s.measurement_time < p.end_time
            WHERE s.sensor_id = $1
              AND s.measurement_time >= $2
              AND s.measurement_time < $3
              AND p.status = 'DONE'
        """

        start_time = asyncio.get_event_loop().time()

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    query,
                    period.sensor_id,
                    period.start_time,
                    period.end_time
                )

            duration_ms = (asyncio.get_event_loop().time() - start_time) * 1000

            return QueryResult(
                period=period,
                row_count=len(rows),
                duration_ms=duration_ms,
                success=True
            )

        except Exception as e:
            duration_ms = (asyncio.get_event_loop().time() - start_time) * 1000
            logger.error(f"Error querying period {period}: {e}")

            return QueryResult(
                period=period,
                row_count=0,
                duration_ms=duration_ms,
                success=False,
                error=str(e)
            )

    async def query_batch_periods(
        self,
        periods: List[ActivePeriod]
    ) -> QueryResult:
        """
        Query multiple periods using OR conditions for optimal chunk exclusion.

        Args:
            periods: List of periods to query in this batch

        Returns:
            QueryResult with aggregated details
        """
        if not periods:
            return QueryResult(
                period=None,
                row_count=0,
                duration_ms=0,
                success=True
            )

        # Build OR conditions
        or_conditions = []
        for i, p in enumerate(periods):
            or_conditions.append(
                f"(s.sensor_id = {p.sensor_id} "
                f"AND s.measurement_time >= '{p.start_time.isoformat()}' "
                f"AND s.measurement_time < '{p.end_time.isoformat()}')"
            )

        # Get global time bounds for additional constraint
        min_time = min(p.start_time for p in periods)
        max_time = max(p.end_time for p in periods)

        query = f"""
            SELECT
                s.sensor_id,
                s.measurement_time,
                s.measurement_value,
                p.start_time,
                p.end_time
            FROM bench.sensor_data s
            JOIN bench.active_periods p
              ON s.sensor_id = p.sensor_id
             AND s.measurement_time >= p.start_time
             AND s.measurement_time < p.end_time
            WHERE ({' OR '.join(or_conditions)})
              AND s.measurement_time >= $1
              AND s.measurement_time < $2
              AND p.status = 'DONE'
        """

        start_time = asyncio.get_event_loop().time()

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, min_time, max_time)

            duration_ms = (asyncio.get_event_loop().time() - start_time) * 1000

            logger.info(
                f"Batch query completed: {len(periods)} periods, "
                f"{len(rows)} rows, {duration_ms:.2f}ms"
            )

            return QueryResult(
                period=None,  # Multiple periods
                row_count=len(rows),
                duration_ms=duration_ms,
                success=True
            )

        except Exception as e:
            duration_ms = (asyncio.get_event_loop().time() - start_time) * 1000
            logger.error(f"Error in batch query: {e}")

            return QueryResult(
                period=None,
                row_count=0,
                duration_ms=duration_ms,
                success=False,
                error=str(e)
            )

    async def process_all_periods_parallel(
        self,
        periods: List[ActivePeriod],
        use_batching: bool = False
    ) -> List[QueryResult]:
        """
        Process all periods in parallel.

        Args:
            periods: List of all periods to process
            use_batching: If True, use batch queries with OR conditions.
                         If False, query each period individually.

        Returns:
            List of QueryResult objects
        """
        if use_batching:
            # Split into batches
            batches = [
                periods[i:i + self.batch_size]
                for i in range(0, len(periods), self.batch_size)
            ]

            logger.info(
                f"Processing {len(periods)} periods in {len(batches)} batches "
                f"of up to {self.batch_size} each"
            )

            tasks = [
                self.query_batch_periods(batch)
                for batch in batches
            ]

        else:
            logger.info(
                f"Processing {len(periods)} periods individually with "
                f"{self.max_connections} concurrent connections"
            )

            tasks = [
                self.query_single_period(period)
                for period in periods
            ]

        # Execute with concurrency limit via semaphore
        semaphore = asyncio.Semaphore(self.max_connections)

        async def bounded_task(task):
            async with semaphore:
                return await task

        results = await asyncio.gather(
            *[bounded_task(task) for task in tasks],
            return_exceptions=True
        )

        # Handle exceptions
        final_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed with exception: {result}")
                final_results.append(
                    QueryResult(
                        period=None,
                        row_count=0,
                        duration_ms=0,
                        success=False,
                        error=str(result)
                    )
                )
            else:
                final_results.append(result)

        return final_results

    def print_summary(self, results: List[QueryResult]):
        """Print summary statistics"""
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        total_rows = sum(r.row_count for r in successful)
        total_duration = sum(r.duration_ms for r in successful)
        avg_duration = total_duration / len(successful) if successful else 0

        print("\n" + "=" * 70)
        print("QUERY EXECUTION SUMMARY")
        print("=" * 70)
        print(f"Total queries:        {len(results)}")
        print(f"Successful:           {len(successful)}")
        print(f"Failed:               {len(failed)}")
        print(f"Total rows retrieved: {total_rows:,}")
        print(f"Total duration:       {total_duration / 1000:.2f}s")
        print(f"Average per query:    {avg_duration:.2f}ms")

        if successful:
            min_duration = min(r.duration_ms for r in successful)
            max_duration = max(r.duration_ms for r in successful)
            print(f"Min query time:       {min_duration:.2f}ms")
            print(f"Max query time:       {max_duration:.2f}ms")

        print("=" * 70 + "\n")


async def main():
    """Example usage"""
    # Configuration
    DSN = "postgresql://user:password@localhost:5432/benchmark"
    MAX_CONNECTIONS = 10
    BATCH_SIZE = 1000
    LIMIT_PERIODS = 100  # Limit for testing

    processor = ParallelQueryProcessor(
        dsn=DSN,
        max_connections=MAX_CONNECTIONS,
        batch_size=BATCH_SIZE
    )

    async with processor:
        # Load active periods
        periods = await processor.get_active_periods(
            status='DONE',
            limit=LIMIT_PERIODS  # Remove limit for production
        )

        if not periods:
            logger.error("No active periods found")
            return

        # Approach 1: Individual queries (better for many connections)
        print("\n--- Approach 1: Individual Queries ---")
        results_individual = await processor.process_all_periods_parallel(
            periods,
            use_batching=False
        )
        processor.print_summary(results_individual)

        # Approach 2: Batched queries (better for fewer connections)
        print("\n--- Approach 2: Batched Queries ---")
        results_batched = await processor.process_all_periods_parallel(
            periods,
            use_batching=True
        )
        processor.print_summary(results_batched)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
