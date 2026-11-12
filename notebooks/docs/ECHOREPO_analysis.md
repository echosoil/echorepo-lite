# ECHOREPO data overview — User manual

**Notebook:** `ECHOREPO_analysis.ipynb`  
**Level:** beginner  
**Purpose:** learn the ECHOREPO data-access workflow and perform first-pass exploratory checks.

[← Back to the notebook guide](../README.md)

## What questions does this notebook answer?

- Is the ECHOREPO canonical API reachable from this Jupyter workspace?
- How many sample, laboratory-parameter, image, and microbial-biodiversity records are available now?
- Do laboratory and biodiversity records refer to sample identifiers present in the sample table?
- Which countries currently contribute the most sample records?
- What does the observed distribution of soil pH look like, and how does its mean/median differ by country?
- Which taxa have the largest combined read counts in the available biodiversity table?

This is a **quick data overview**, not a complete data-quality assessment or a representative statistical study of European soils.

## Before running

1. Open `ECHOREPO_analysis.ipynb` in JupyterLab.
2. Make sure `lib/echorepo.py` is importable and the Python kernel is selected.
3. Check the important compatibility note below: this particular notebook uses **older sample-column names**.
4. The last cell saves a CSV into `exports/`, so the directory must already exist. In a code cell before the export, run:

```python
from pathlib import Path
Path("exports").mkdir(parents=True, exist_ok=True)
```

### Important: current versus legacy sample fields

The provided example uses `samples["ph"]` and `organic_carbon_pct`. Newer canonical responses expose `soil_ph_field` and `soil_organic_matter_estimate_pct` instead.

If you see `KeyError: 'ph'`, change the pH references in the plotting and country-summary cells to the actual current field. You can see which fields are available with:

```python
print(samples.columns.tolist())
```

For example, if your API uses the current column names:

```python
# Replace the original pH histogram expression:
samples["soil_ph_field"].dropna().hist(bins=20)

# Replace the pH part of the final country aggregation:
country_stats = (
    samples.groupby("country_code")
    .agg(
        samples=("sample_id", "count"),
        mean_ph=("soil_ph_field", "mean"),
        mean_organic_matter_pct=("soil_organic_matter_estimate_pct", "mean"),
    )
    .reset_index()
)
```

You should similarly substitute `soil_ph_field` for `ph` in the country mean/median cell. Do **not** relabel organic matter as organic carbon; the quantities are not equivalent.

## Walkthrough of the eight code cells

### 1. Connect to the API

The notebook imports matplotlib and `EchoRepo`, constructs `echo = EchoRepo()`, and prints the URL and `echo.ping()` response. A successful ping establishes connectivity; it does **not** prove that every data endpoint is authorised or populated.

### 2. Download the four canonical datasets

```python
samples = echo.samples()
parameters = echo.parameters()
images = echo.images()
biodiversity = echo.biodiversity()
```

It prints the **number of rows** in each DataFrame. A row in `parameters`, `images`, or `biodiversity` is not necessarily a distinct soil sample; one sample can have multiple linked rows. The data are fetched live, so today's counts can differ from earlier displayed notebook outputs.

### 3. Check sample ID relationships

The notebook counts unique sample IDs and compares the IDs found in `parameters` and `biodiversity` with those in `samples`. A nonzero result indicates a linkage to investigate; zero does **not** establish that all data are correct, correctly geolocated, or complete.

### 4. Inspect sample counts by country

`value_counts().head(20)` lists the 20 most represented country codes. It reports **sampling effort**, not the prevalence of a soil type or condition within a country.

### 5. Plot field pH

The notebook displays a histogram with 20 bins. Its height is the number of **observations**, and visible peaks can reflect reporting increments or sampling concentration. For the current API, use `soil_ph_field` in place of `ph`.

### 6. Compare country-level pH statistics

The notebook groups data by `country_code`, then displays the count, mean, and median for countries with the most non-null pH records. Consider both the **number of observations** and whether a country is geographically well sampled before comparing countries. National population-level conclusions are **not** supported.

### 7. Inspect commonly reported microbial taxa

The notebook groups the biodiversity table by `marker` and `scientific_name`, sums `read_count`, and shows the top 30 combinations. This is a **read-abundance ranking** across the retrieved dataset—not a species inventory, a richness measure, or a fair comparison of 16S with ITS. It does not select one taxonomic rank or normalise by sample/sequencing effort, so interpret it as a simple exploration of available rows.

### 8. Export basic country statistics

The last code cell calculates total sample rows, mean pH, and mean organic percentage by country, and saves:

`exports/country_statistics.csv`

Create `exports/` before running it. The original code names the organic aggregate `mean_soc`, but this is potentially misleading if your API now returns a **soil organic matter estimate**. Use a name matching the actual measured/estimated variable.

## How to read the results

- **Large country count:** many submitted sample records, not necessarily broad geographic coverage.
- **Mean versus median pH:** differences may indicate skew, outliers, or uneven contributions from different soil environments.
- **Top biodiversity taxon:** high summed reads in this table, **not** necessarily the most widespread or biologically dominant taxon throughout Europe.
- **Missing linked sample IDs:** examine provenance, export selection, and whether different snapshots were mixed.

## Useful first exercises

1. Restrict the downloaded `samples` DataFrame to one `country_code` and regenerate the pH histogram.
2. Count how many samples have missing pH and compare that share across countries.
3. Check how many **unique** sample IDs appear in `images`, `parameters`, and `biodiversity` instead of simply counting rows.
4. Run the notebook on a different day, note the fetch date, and compare the counts. Do not interpret differences as data loss without checking updated records and filters.

## Limitations

The provided example has eight **code-only** cells; it was designed as a starting point. It has minimal error handling, limited QA checks, no provenance timestamp written into the exported table, and no automatic creation of the output directory. For cleaner field compatibility and richer analyses, proceed to the [Soil properties explorer](ECHOREPO_soil_properties.md) or [Microbial biodiversity explorer](ECHOREPO_soil_biodiversity.md).
