# FAPROTAX memory-bounded R functional profiling

For a 16S table of ~98,729 retained OTUs × ~2,736 samples, the previous
`faprotax_16S_functional_profile.R` read ~270 million dense abundance
cells into R/microeco, then created additional dense copies and was SIGKILLed
at ~10.6 GiB RSS on a memory-constrained server.

The new R implementation retains microeco's FAPROTAX annotation rules, but
generates the OTU-to-function membership table **only once**, using taxonomy
from file 7 and a dummy reference column (one positive count per OTU).
The reference column is never included in the output.

File 6 is then read **sequentially in OTU-row batches**. Each batch is
converted into a small numeric OTU-by-sample matrix and multiplied by a
sparse OTU-to-function map using the `Matrix` package.
Only sample totals, function totals, and the small current batch remain
in memory. The script neither creates the whole OTU-by-sample matrix nor
requires a database connection.

This means memory grows approximately with the number of samples,
possible functions, and `FAPROTAX_CHUNK_ROWS`, rather than with the number
of OTUs multiplied by the number of samples. The taxonomy assignment
itself still uses memory proportional to the number of OTUs and FAPROTAX
functions, so benchmark on the target server rather than assuming a
specific peak RAM usage.

## Running

After successfully generating the matched inputs 6 and 7:

```bash
FAPROTAX_CHUNK_ROWS=128 \
/usr/bin/time -v Rscript tools/faprotax_16S_functional_profile.R \
  data/biodiversity/faprotax_work/6_otu_clean_counts_no_blanks.csv \
  data/biodiversity/faprotax_work/7_taxonomy_clean.csv \
  data/biodiversity
```

If memory is still tight, reduce `FAPROTAX_CHUNK_ROWS=32` (more I/O and
calls, less peak RAM). To speed processing when memory allows, try 256.

The output remains:

- `7_taxonomy_NA.csv`: aligned cleaned taxonomy (semicolon delimiter).
- `8_faprotax_samples_x_functions.csv`: sample × function fractions used for bacterial guild plots.
- `9_faprotax_annotated_fraction.csv`: per-sample *read-weighted* annotation coverage.

The script checks that each OTU ID in file 6 matches file 7 in exactly
the same order, checks for negative/missing counts, logs progress, and
writes temporary outputs before replacing final files. Make sure no
other process is writing these same outputs concurrently.

## Scientific interpretation

The *original* script used microeco's default
`cal_spe_func_perc()` / `cal_func_FR()`, which on current microeco
is **unweighted OTU prevalence** (number of present OTUs possessing a
function divided by number of present OTUs) on a 0–1 scale, **not**
relative abundance of sequence reads. The new script preserves that
default, now explicitly at 6 decimal places. The output name and its
downstream usage are unchanged.

To intentionally produce **read-weighted** functional fractions instead,
set `FAPROTAX_ABUNDANCE_WEIGHTED=1`. This is a scientific
definition change; document it when comparing results and do not
mix files created with different modes. The annotation coverage in
file 9 always uses read proportions, matching the original script.

For the reference formula see the microeco `trans_func` documentation.
Some older microeco versions had different percentage and rounding
defaults; our 0–1 / 6-decimal output is explicit to keep ECHOREPO's
guild plots consistent with their stated expectations.

## Validate on a small dataset

```bash
Rscript tests/test_faprotax_r_streaming.R
```

Unit tests use small CSV inputs with known expected results for both
unweighted and weighted calculations, OTU-ID quoting and alignment
errors, and zero-only samples. If microeco is installed, they also compare
the sample-level fractions with microeco's own functional-redundancy
calculation on the same synthetic mapping.

If you want to run the old full-matrix script for comparison, do that only
on a small subset (e.g. dozens of OTUs and several samples); do not run
the old script against the full dataset on a RAM-limited server.
