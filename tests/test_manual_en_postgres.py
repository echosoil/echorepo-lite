"""Real PostgreSQL integration: run only against a dedicated disposable test database."""

import os
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from echorepo.services import manual_en

DSN = os.getenv("MANUAL_EN_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="Requires dedicated PostgreSQL test database")


def test_search_inject_and_protect(monkeypatch):
    schema = "manual_en_test_" + uuid4().hex
    root = Path(__file__).resolve().parents[1]
    with psycopg2.connect(DSN) as setup, setup.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    setup.close()

    def connect():
        return psycopg2.connect(DSN, options=f"-c search_path={schema}")

    monkeypatch.setattr(manual_en, "get_pg_conn", connect)
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE samples (
                    sample_id TEXT PRIMARY KEY, country_code TEXT,
                    contamination_other_orig TEXT, contamination_other_en TEXT,
                    observations_orig TEXT, observations_en TEXT,
                    metals_info_orig TEXT, metals_info_en TEXT,
                    soil_texture_en TEXT
                );
                CREATE TABLE sample_images (
                    sample_id TEXT, image_id INTEGER, country_code TEXT,
                    image_description_orig TEXT, image_description_en TEXT,
                    PRIMARY KEY (sample_id, image_id)
                );
            """)
            cur.execute(
                (root / "migrations/postgres/20261126_002_manual_en_overrides.sql").read_text()
            )
            cur.execute("""
                INSERT INTO sample_images VALUES
                    ('a', 1, 'IT', 'campione 3', 'champion 3'),
                    ('b', 2, 'IT', 'campione 3', 'specimen 3'),
                    ('c', 1, 'ES', 'campione 3', 'different'),
                    ('d', 1, 'IT', 'campione 3a', 'unchanged'),
                    ('e', 1, 'IT', '100% soil', NULL);
                INSERT INTO samples (sample_id, country_code, observations_orig, observations_en,
                    soil_texture_en)
                VALUES ('a', 'IT', 'campione 3', 'other field', 'loam');
            """)
        conn.close()
        rows, more = manual_en.search_manual_en("CHAMPION")
        assert not more and len(rows) == 1
        assert rows[0]["occurrences"] == 2
        assert rows[0]["current_en"] == "champion 3"
        assert len(manual_en.search_manual_en("100%")[0]) == 1
        assert not manual_en.search_manual_en("100_")[0]
        assert not manual_en.search_manual_en("")[0]

        result = manual_en.change_manual_en(
            "image_description", "IT", "campione 3", "sample 3", None, "editor"
        )
        assert result == {"matched": 2, "updated": 2, "override": "sample 3"}
        with connect() as conn, conn.cursor() as cur:
            cur.execute("UPDATE sample_images SET image_description_en = 'bad' WHERE sample_id = 'a'")
            cur.execute("INSERT INTO sample_images VALUES ('f', 1, 'IT', 'campione 3', NULL)")
            cur.execute("SELECT sample_id, image_description_en FROM sample_images ORDER BY sample_id")
            assert dict(cur.fetchall()) == {
                "a": "sample 3", "b": "sample 3", "c": "different",
                "d": "unchanged", "e": None, "f": "sample 3",
            }
            cur.execute("SELECT observations_en, soil_texture_en FROM samples")
            assert cur.fetchone() == ("other field", "loam")
            cur.execute("UPDATE sample_images SET image_description_orig = 'edited' WHERE sample_id = 'f'")
            cur.execute("SELECT image_description_en FROM sample_images WHERE sample_id = 'f'")
            assert cur.fetchone() == (None,)
        conn.close()

        with pytest.raises(manual_en.ManualConflict):
            manual_en.change_manual_en(
                "image_description", "IT", "campione 3", "different", None, "editor"
            )
        manual_en.change_manual_en(
            "image_description", "IT", "campione 3", None, "sample 3", "editor", clear=True
        )
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT image_description_en FROM sample_images WHERE sample_id = 'a'")
            assert cur.fetchone() == ("sample 3",)
            cur.execute("UPDATE sample_images SET image_description_en = 'new' WHERE sample_id = 'a'")
            cur.execute("SELECT image_description_en FROM sample_images WHERE sample_id = 'a'")
            assert cur.fetchone() == ("new",)
        conn.close()
    finally:
        with psycopg2.connect(DSN) as cleanup, cleanup.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        cleanup.close()
