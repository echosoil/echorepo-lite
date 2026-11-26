"""Search and persist exact, country-scoped canonical English corrections."""

from contextlib import closing

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from .db import get_pg_conn

FIELDS = {
    "contamination_other": "samples",
    "observations": "samples",
    "metals_info": "samples",
    "image_description": "sample_images",
}
PAGE_SIZE = 50


def search_manual_en(query, page=1):
    if not query.strip():
        return [], False
    parts = []
    for field, table in FIELDS.items():
        parts.append(
            sql.SQL(
                "SELECT {field} AS field, upper(btrim(coalesce(country_code, ''))) AS country, "
                "{orig} AS original, {en} AS english FROM {table} "
                "WHERE NULLIF(btrim({orig}), '') IS NOT NULL "
                "AND lower(btrim({orig})) NOT IN ('nan', 'none', 'null', '<na>')"
            ).format(
                field=sql.Literal(field),
                orig=sql.Identifier(field + "_orig"),
                en=sql.Identifier(field + "_en"),
                table=sql.Identifier(table),
            )
        )
    # Match literal substrings, including % and _, without SQL wildcard semantics.
    statement = sql.SQL("""
        WITH texts AS ({sources}), grouped AS (
            SELECT field, country, original, count(*) AS occurrences,
                   count(DISTINCT english) AS distinct_en,
                   count(*) FILTER (WHERE english IS NULL OR btrim(english) = '') AS missing_en,
                   coalesce(
                       min(english) FILTER (WHERE position(lower(%s) in lower(english)) > 0),
                       min(english) FILTER (WHERE btrim(english) <> '')
                   ) AS current_en
            FROM texts
            GROUP BY field, country, original
            HAVING position(lower(%s) in lower(original)) > 0
                OR bool_or(position(lower(%s) in lower(coalesce(english, ''))) > 0)
        )
        SELECT grouped.*, overrides.english AS override
        FROM grouped LEFT JOIN manual_en_overrides AS overrides
          ON overrides.field = grouped.field AND overrides.country = grouped.country
         AND overrides.original_hash = md5(grouped.original)
         AND overrides.original = grouped.original
        ORDER BY grouped.original, grouped.country, grouped.field
        LIMIT %s OFFSET %s
    """).format(sources=sql.SQL(" UNION ALL ").join(parts))
    with closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(statement, (query, query, query, PAGE_SIZE + 1, (page - 1) * PAGE_SIZE))
        rows = cur.fetchall()
    return rows[:PAGE_SIZE], len(rows) > PAGE_SIZE


class ManualConflict(Exception):
    """The displayed override was changed or the source no longer exists."""


def change_manual_en(field, country, original, value, expected, actor):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("A nonempty English translation is required")
    table = FIELDS[field]
    orig, en = (sql.Identifier(field + suffix) for suffix in ("_orig", "_en"))
    where = sql.SQL(
        "{orig} = %s AND upper(btrim(coalesce(country_code, ''))) = %s"
    ).format(orig=orig)
    with closing(get_pg_conn()) as conn, conn, conn.cursor() as cur:
        # Serialize the short correction transaction with ingestion/translation
        # writes, including inserts of new matching rows. Ordinary reads continue.
        cur.execute(
            sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(sql.Identifier(table))
        )
        cur.execute(
            "SELECT english FROM manual_en_overrides "
            "WHERE field = %s AND country = %s AND original_hash = md5(%s) AND original = %s",
            (field, country, original, original),
        )
        old = cur.fetchone()
        if (old[0] if old else None) != expected:
            raise ManualConflict("Override changed; search again before saving.")
        cur.execute(
            sql.SQL("SELECT count(*) FROM {table} WHERE {where}").format(
                table=sql.Identifier(table), where=where
            ),
            (original, country),
        )
        count = cur.fetchone()[0]
        if not count:
            raise ManualConflict("Source no longer exists; search again.")
        cur.execute(
            """
            INSERT INTO manual_en_overrides (field, country, original, english, updated_by)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (field, country, original_hash) DO UPDATE
            SET english = EXCLUDED.english, updated_by = EXCLUDED.updated_by, updated_at = now()
            WHERE manual_en_overrides.original = EXCLUDED.original
            RETURNING english
            """,
            (field, country, original, value, actor),
        )
        if cur.fetchone() is None:
            raise ManualConflict("Source hash collision; no change made.")
        cur.execute(
            sql.SQL(
                "UPDATE {table} SET {en} = %s WHERE {where} AND {en} IS DISTINCT FROM %s"
            ).format(table=sql.Identifier(table), en=en, where=where),
            (value, original, country, value),
        )
        return {"matched": count, "updated": cur.rowcount, "override": value}
