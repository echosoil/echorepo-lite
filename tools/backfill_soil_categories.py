#!/usr/bin/env python3

import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from echorepo.services.db import get_pg_conn
from echorepo.services.soil_categories import (
    canonical_soil_value,
)


def main():
    unknown_texture = Counter()
    unknown_structure = Counter()

    updates = []

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    sample_id,
                    soil_texture_orig,
                    soil_structure_orig
                FROM samples
                ORDER BY sample_id
                """
            )

            rows = cur.fetchall()

    for (
        sample_id,
        texture_orig,
        structure_orig,
    ) in rows:

        texture_en = canonical_soil_value(
            texture_orig,
            "soil_texture",
        )

        structure_en = canonical_soil_value(
            structure_orig,
            "soil_structure",
        )

        if texture_orig and texture_en is None:
            unknown_texture[
                str(texture_orig)
            ] += 1

        if structure_orig and structure_en is None:
            unknown_structure[
                str(structure_orig)
            ] += 1

        updates.append(
            (
                sample_id,
                texture_en,
                structure_en,
            )
        )

    print(
        f"Samples inspected: {len(updates)}"
    )

    print("\nUnknown soil textures:")
    for value, count in unknown_texture.most_common():
        print(
            f"{count:6d}  {value}"
        )

    print("\nUnknown soil structures:")
    for value, count in unknown_structure.most_common():
        print(
            f"{count:6d}  {value}"
        )


if __name__ == "__main__":
    main()