# Microbial biodiversity and soil chemistry — User manual

**Notebook:** `ECHOREPO_soil_biodiversity.ipynb`  
**Level:** intermediate (basic ecology/statistics recommended)  
**Data sources:** `echo.samples()` and `echo.biodiversity()` through the canonical API

[← Back to the notebook guide](../README.md)

## Research aim

Explore whether **microbial taxonomic diversity** in ECHOREPO observations is associated with **field soil pH** and the **field-estimated organic percentage**. The notebook treats molecular markers such as **16S** and **ITS** separately, because they represent different biological targets and sequencing workflows.

**Important:** ECHOREPO's canonical biodiversity export contains **taxon-abundance summaries** (e.g., named taxa at genus, family, phylum level), not raw OTU/ASV abundance matrices. This notebook calculates *rank-specific descriptive taxonomic diversity indices*; it does not estimate raw ASV/OTU richness, perform rarefaction, or correct sequencing depth.

## Before you begin: configure the analysis

In **Section 1 — Imports and parameters**, review:

| Setting | Default | What it controls |
| --- | --- | --- |
| `TAXON_RANK` | `None` | Automatically chooses the first available rank in this order: genus, family, species, order, phylum, class; use e.g. `'phylum'` to fix the rank |
| `MIN_TOTAL_READS` | `10` | Minimum total read counts among **included named taxa at the selected rank**, per sample and marker |
| `EXCLUDE_UNASSIGNED` | `True` | Removes generic `unclassified`, `unknown`, `uncultured`, etc. names before the calculations |
| `MIN_N_FOR_CORRELATION` | `10` | Minimum number of valid pairs for a Spearman calculation |
| `MAX_MARKERS_IN_PLOT` | `4` | Maximum number of marker panels shown in each figure |
| `EXPORT_DIR` | `Path('exports')` | Directory for the output CSVs; created automatically |

For an initial comparison, consider explicitly selecting the same taxonomic rank across runs:

```python
TAXON_RANK = "phylum"  # Only works if this rank is present in the fetched data
MIN_TOTAL_READS = 10
EXCLUDE_UNASSIGNED = True
```

**Do not switch ranks midway through an analysis** and then compare diversity numbers as though they represented the same definition. After changes, use **Run → Run All Cells** to regenerate all downstream results.

## Guided workflow

### 1. Retrieve the sample and biodiversity tables

The notebook calls `EchoRepo()`, checks `ping()`, records a UTC time stamp, and downloads:

```python
samples_raw = echo.samples()
bio_raw = echo.biodiversity()
```

It prints row counts and the available biodiversity fields. If the biodiversity endpoint returns no rows, the notebook stops with an explanatory error. Multiple biodiversity rows may refer to the same sample or to different taxonomic levels; **row count is not sample count**.

### 2. Select one taxonomic rank

The notebook reads `taxon_rank` (new schema) or `taxonomic_level` (legacy), displays which ranks are present, then selects the configured rank. It keeps records with positive numeric `read_count` values and named taxa, optionally excludes generic unassigned names, and combines duplicate records by **sample ID + marker + scientific name**.

**Why one rank at a time?** The same underlying DNA reads can be represented at multiple classification levels. Counting taxa across ranks together would inflate or distort apparent diversity.

### 3. Calculate three rank-specific indices

For every **sample × marker** group after filtering, the notebook computes:

| Metric | Definition | Basic interpretation |
| --- | --- | --- |
| `observed_taxa` | Number of distinct named taxa at the selected rank | A descriptive rank-level richness count, **not OTU/ASV richness** |
| `shannon` | `-Σ pᵢ ln(pᵢ)`, where `pᵢ` is the proportion of included reads assigned to taxon *i* | Reflects both distribution and number of included named taxa |
| `effective_taxa` | `exp(shannon)` | Shannon diversity expressed as an *effective number* of equally common taxa |

The notebook additionally reports `total_reads`: **the sum of reads assigned to included taxa at the selected rank**. This is *not* the total raw sequencing depth and can change with taxonomic assignment completeness or `EXCLUDE_UNASSIGNED`.

A group passes the depth filter when its rank-specific included-read total is at least `MIN_TOTAL_READS`. This threshold is **not** a sequencing-depth normalisation, a rarefaction procedure, or a quality-control standard.

### 4. Join biodiversity with soil properties

The analysis joins each sample–marker diversity record to its soil sample on `sample_id`, keeping `country_code`, field pH, and organic percentage. The join explicitly handles current or legacy soil field names. Pairs with unavailable pH or organic measurements cannot contribute to correlations involving those properties.

Markers remain **separate rows**. One physical sample with both 16S and ITS can therefore contribute two marker-specific records without being treated as one pooled community.

### 5. Inspect sampling effort versus richness

A scatter plot compares `total_reads` against `observed_taxa`, with total reads on a logarithmic x-axis. A pronounced trend may indicate that richer-looking samples received more usable taxonomic reads or better assignment. This figure is a **diagnostic**, not a corrected richness model.

### 6. Explore associations with soil chemistry

For each available marker, the notebook computes Spearman rank correlations for:

- Field pH versus `observed_taxa`
- Field pH versus `shannon`
- Organic percentage versus `observed_taxa`
- Organic percentage versus `shannon`

Scatter plots show `shannon` against each soil property, separately for markers. The correlation table lists the marker, selected rank, tested fields, complete-pair count, Spearman ρ, and a **nominal p-value**.

A positive ρ indicates that higher values of the soil property tend to coincide with higher values of the diversity metric *within the analysed records*. It does **not** establish that the soil property caused a biodiversity change. P-values are unadjusted for geography, sequencing methods, library size, or multiple comparisons.

### 7. Examine geographic coverage

The notebook reports, for each `country_code`:

- `all_soil_samples`: number of unique sample IDs in the entire soil table
- `with_biodiversity`: number of unique sample IDs represented in the **filtered, joined biodiversity analysis**
- `coverage_pct`: share of the soil sample set included in this analysis

Where samples have both 16S and ITS, the numerator **counts the sample once**, not twice. A low coverage percentage may limit interpretation and cross-country comparisons. This is coverage of the analytical subset, not the proportion of soils with biodiversity in the country at large.

### 8. Export the results

Three CSV files are saved under `exports/`:

| Filename | Contents |
| --- | --- |
| `exports/biodiversity_alpha_diversity_and_soil.csv` | One row per retained **sample × marker**, with diversity metrics, included reads, soil properties, country, chosen rank, and fetch time |
| `exports/biodiversity_soil_associations.csv` | Marker-specific pH/organic × richness/Shannon correlations, sample size, Spearman ρ, and nominal p-value |
| `exports/biodiversity_country_coverage.csv` | Country-level soil-sample counts, unique biodiversity-subset counts, and coverage percentages |

The files are overwritten on rerun. The joined export contains `sample_id`, but the notebook does not include location coordinates in that export. Treat linked sample-level data as governed by ECHOREPO access and publication requirements.

## How to report a finding responsibly

A suitable wording is:

> Using the ECHOREPO canonical biodiversity export retrieved on **[UTC date]**, we quantified the diversity of **named [rank] taxa** for the **[16S/ITS]** marker, after excluding **[selection rules]**. Among **[n]** samples with paired field pH, we observed a **[direction/strength]** association with **[Shannon entropy / rank-level richness]** (Spearman ρ = **[value]**). These unadjusted descriptive results are sensitive to sequencing depth, taxonomic assignment and geographic sampling.

Always report the **rank**, **marker**, **excluded names**, **minimum included reads**, **paired sample size**, and the **field variable used**. Do not describe these results as raw ASV/OTU alpha diversity.

## Suggested exercises

1. Run the analysis once at `TAXON_RANK = 'phylum'`, and again at `'genus'` if both are present. How do the distributions change? Why is the comparison not a controlled estimate of biological change?
2. Raise `MIN_TOTAL_READS` and note the change in sample counts, richness, and correlations.
3. Compare 16S and ITS separately. Why is pooling their diversity scores inappropriate?
4. Check whether the apparent pH–diversity association is driven by a few countries with many samples.
5. For formal scientific analysis, obtain raw ASV/OTU matrices plus sequencing/batch metadata and apply appropriate diversity estimation and statistical models.

## Limitations and troubleshooting

- **`Requested rank unavailable`:** inspect the printed `Available ranks` output and set a rank that is actually present, or leave `TAXON_RANK = None`.
- **`No positive read counts at selected rank`:** try a different rank or inspect the unassigned-name filter; do not remove quality filters solely to force a result.
- **`No samples with ≥... rank-specific reads`:** review the meaning of `MIN_TOTAL_READS` and the available data. The default threshold is pedagogical, not a recommended sequencing quality standard.
- **No correlation value (`NaN`):** too few complete pairs or no variation in the variables at that marker.
- **Large richness–read relationship:** sequencing effort and classification efficiency may be influencing the diversity calculation.
- **All results are observational.** The notebook does not correct multiple-testing p-values, control for environmental confounders, harmonise lab protocols, or handle spatial autocorrelation.

These figures and CSVs should be treated as a **starting point for hypothesis generation**, not as publication-ready ecological inference.
