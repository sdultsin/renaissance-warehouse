"""pipeline-supabase Postgres connector.

Direct psycopg2 connection to the pipeline-supabase Postgres pooler. Used by
``entities/pipeline_mirror.py`` to copy slim analytical tables into DuckDB.

Notes:
    * We use the pooler URL (`postgresql://postgres.<ref>:<pass>@aws-1-<region>.pooler.supabase.com:5432/postgres`).
      The pooler enforces a 30-min statement_timeout; our slim tables are far under that.
    * ARRAY columns come back as Python lists, jsonb comes back as Python dict/list — both
      get serialized to JSON strings by the mirror before insert.
    * For large tables we use a server-side cursor (named cursor) + itersize so we don't
      materialize 1M+ rows in Python memory at once.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("sources.pipeline_supabase")


class PipelineSupabase:
    """Thin psycopg2 wrapper. One connection per instance, opened lazily."""

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url
        self._conn: psycopg2.extensions.connection | None = None

    def _connect(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._db_url)
            # Read-only autocommit. Server-side cursors need a real transaction though,
            # so we leave autocommit off and just rollback at the end.
            self._conn.set_session(readonly=True, autocommit=False)
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            try:
                self._conn.rollback()
            except Exception:
                pass
            self._conn.close()

    def fetch_table(
        self,
        table_name: str,
        where_clause: str | None = None,
    ) -> list[dict[str, Any]]:
        """SELECT * FROM public.<table_name>; return all rows as list of dicts.

        Convenient for small tables. For larger tables use ``iter_table`` to stream
        in chunks via a server-side cursor.
        """
        return list(self.iter_table(table_name, where_clause=where_clause))

    def iter_table(
        self,
        table_name: str,
        where_clause: str | None = None,
        chunk_size: int = 5000,
    ) -> Iterator[dict[str, Any]]:
        """Stream rows via a server-side named cursor. Yields one dict per row.

        Server-side cursors avoid Python materializing the entire resultset in RAM —
        critical for tables like lead_events that may return 1M+ rows.

        Caller should iterate to completion (or close()) so we close the cursor.
        """
        sql = f'SELECT * FROM public."{table_name}"'
        if where_clause:
            sql += f" WHERE {where_clause}"
        logger.info("iter_table %s where=%s", table_name, where_clause or "<none>")
        conn = self._connect()
        # Named cursor = server-side cursor. Required for streaming.
        cur_name = f"warehouse_{table_name}_{uuid.uuid4().hex[:8]}"
        cur = conn.cursor(name=cur_name, cursor_factory=RealDictCursor)
        cur.itersize = chunk_size
        try:
            cur.execute(sql)
            for row in cur:
                yield dict(row)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            # Server-side cursors hold a transaction open; rollback to release.
            try:
                conn.rollback()
            except Exception:
                pass
