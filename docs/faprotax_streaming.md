# Memory-bounded FAPROTAX input generation

The standard 16S preparation step builds files 6 and 7 from the *current*
structured raw PostgreSQL biodiversity data, after all sample-column parts
have imported successfully:

- `data/biodiversity/faprotax_work/6_otu_clean_counts_no_blanks.csv`: full
  dense OTU-by-sample CSV for R/microeco (zeros included);
- `data/biodiversity/faprotax_work/7_taxonomy_clean.csv`: canonical
  seven-rank taxonomy, semicolon-delimited, OTUs aligned with file 6.

Earlier implementations loaded every nonzero (sample, OTU, count) into pandas,
then used a dense `pivot_table`. A typical 98,729 OTU by 2,736
sample table needs about 2 GiB just for its final float64 dense array;
pivoting and holding 10+ million nonzero records adds substantial peak RAM.

The standard CLI now uses a **server-side PostgreSQL cursor**, streaming
eligible nonzero abundances in OTU/sample order into a single in-memory row
of sample counts, then immediately writing it to a temporary CSV. Python
does not allocate the dense matrix or materialize all nonzero counts.
Taxonomy is still loaded/reconciled with the existing rules; SQL still
applies prevalence and total-read filters against the complete current
dataset. The resulting CSV is deliberately dense to remain compatible
with `tools/faprotax_16S_functional_profile.R`.

## Run

First verify **every** biodiversity upload part has status `succeeded`.
Then from the ECHOREPO repository containing the intended .env:

```bash
# Use 0 overrides when .env enables guild-image generation.
GENERATE_FUNGAL_GUILDS=0 GENERATE_BACTERIAL_GUILDS=0 \
python3 -u tools/generate_biodiversity_piecharts.py \
  --marker 16S --build-faprotax-inputs

Rscript tools/faprotax_16S_functional_profile.R \
  data/biodiversity/faprotax_work/6_otu_clean_counts_no_blanks.csv \
  data/biodiversity/faprotax_work/7_taxonomy_clean.csv \
  data/biodiversity

BUILD_FAPROTAX_INPUTS=0 \
python3 -u tools/generate_biodiversity_piecharts.py --all-images --force
```

The Python phase prints progress after every 5,000 output OTUs, and creates
temporary .csv.tmp files before publishing final outputs.
If interrupted while streaming, it removes the temporary files and keeps
the previous final files. It still needs available disk space for the full
dense CSV and PostgreSQL may use temporary sort disk space.

**Important:** The subsequent R/microeco script still loads file 6 as a
dense data frame and can have significant RAM requirements. Optimizing
Python input preparation does **not** eliminate R's memory requirement.

## Development environment warning

The generator currently reads .env from the *current working directory*
and honors its `PROJECT_ROOT` setting for output paths. Running a script
from `../echorepo-lite-dev/tools/` while standing in `echorepo-lite/` can
therefore process the production-configured database and write under the
production-configured root. Confirm both the DB connection and root printed
in the log before large runs.

## Tests

```bash
python -m unittest discover -s tests -p "test_faprotax_streaming.py" -v
```

The pure writer tests check byte-for-byte equivalence to the legacy pandas
pivot, zero-only sample columns, quoting, validation failures, and a
synthetic sparse stream under a 25 MiB Python allocation budget. The
biodiversity API integration workflow also invokes the new builder against
an actual temporary PostgreSQL database populated by two disjoint
sample-column uploads, verifying output files 6 and 7.
