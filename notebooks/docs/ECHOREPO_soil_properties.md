# Soil properties explorer — User manual

**Notebook:** `ECHOREPO_soil_properties.ipynb`  
**Level:** beginner to intermediate  
**Data source:** `echo.samples()` through the ECHOREPO canonical API

[← Back to the notebook guide](../README.md)

## Research aim

Explore how **field soil pH** and the **citizen-estimated soil organic fraction** vary between samples and countries, and test whether the two variables show a **monotonic association**. The focus is exploratory description, not identifying causal relationships or estimating national soil conditions.

The notebook works with both `soil_ph_field`/`soil_organic_matter_estimate_pct` (current) and `ph`/`organic_carbon_pct` (legacy). It records which names are present and labels the organic variable accordingly.

## Before you begin

Open the notebook in the ECHOREPO JupyterLab environment. Confirm that you can import `EchoRepo`, that the kernel has `pandas`, `numpy`, `matplotlib`, and `scipy`, and that the API responds to `ping()`. The notebook downloads the current sample CSV from the API; running it again can produce different results.

In **Section 1 — Imports and configuration**, you can change:

| Setting | Default | Meaning |
| --- | --- | --- |
| `COUNTRY_CODES` | `[]` | Empty = all countries; e.g. `['ES', 'PT']` restricts to Spain and Portugal **after downloading** |
| `MIN_PAIRED_COUNTRY_N` | `30` | Minimum complete pH + organic-measure pairs required to include a country in the within-country correlation table |
| `MAX_COUNTRIES_IN_PLOT` | `15` | Maximum number of countries shown in the median-pH chart |
| `EXPORT_DIR` | `Path('exports')` | Directory receiving four output CSV tables; created automatically |

To study one country, for example, change:

```python
COUNTRY_CODES = ["ES"]
```

Then select **Run → Run All Cells** from the beginning. If you change a configuration variable, rerun downstream cells; otherwise old figures may still reflect previous settings.

## Guided workflow

### 1. Retrieve the live samples

The notebook creates `EchoRepo()`, checks `ping()`, records a UTC fetch timestamp, downloads `echo.samples()`, and prints the returned columns and row count. If connectivity fails, see the [main guide's troubleshooting section](../README.md#5-common-problems).

### 2. Check and standardise the measurements

- Verifies `sample_id`, `country_code`, and appropriate pH/organic columns.
- Keeps the first record for any duplicated `sample_id`.
- Accepts numeric values with `.` or `,` decimal separators.
- Marks **pH outside 0–14** and **percentages outside 0–100** as missing.
- Counts samples with valid pH, valid organic percentage, and **both values available**.

This is intentionally **minimal physical-range screening**, not a complete QA protocol. The screening flags are counted; unusual but physically possible values may still require checking.

### 3. Examine distributions

Two histograms display counts of observations against field pH and organic percentage. Look for peaks at whole or half-unit values, which can reflect how people record estimates. Missing values are not drawn in the histograms; inspect the completeness table first.

### 4. Explore the pH–organic relationship

A **hexbin plot** groups paired samples into coloured hexagons; more intense colour means more samples fall in that area of the plot. The notebook also calculates **Spearman's correlation coefficient (ρ)** and a nominal p-value.

| Spearman ρ | Basic interpretation |
| --- | --- |
| Close to `+1` | Strong increasing rank association |
| Close to `-1` | Strong decreasing rank association |
| Close to `0` | Little or no monotonic association |
| `NaN` | Not enough variation or valid paired observations |

A correlation does **not** show cause and effect. The p-value is **nominal**, with no adjustment for geographic confounding, repeated comparisons, or survey design.

### 5. Compare countries

A table shows total samples and the number, mean, and median of available field-pH/organic observations by country. A horizontal bar chart plots **median pH** for the countries with the most valid pH records; the chart labels include sample sizes.

**Do not call these national estimates.** ECHO samples may cluster in particular communities, locations, soil types, or land uses.

### 6. Calculate within-country associations

For each country with at least `MIN_PAIRED_COUNTRY_N` complete pairs, the notebook computes Spearman ρ for pH versus organic percentage and displays a comparison chart. A country omitted from this table may simply have too few complete pairs.

Correlations from countries with different sampling patterns, soil environments, or measurement practices should not be ranked as if they were controlled experiments.

### 7. Export results

The notebook creates **four CSV files**:

| Filename | Contents |
| --- | --- |
| `exports/soil_properties_completeness.csv` | Counts for unique samples, valid measurements, complete pairs, duplicates discarded, and screened values |
| `exports/soil_properties_country_summary.csv` | Per-country counts and field measurement summaries |
| `exports/soil_properties_country_correlations.csv` | Countries passing the complete-pair threshold, their sample sizes, Spearman ρ, and nominal p-value |
| `exports/soil_properties_sample_level.csv` | `sample_id`, `country_code`, normalised `ph`, and `organic_pct` for the analysed subset |

The exports overwrite matching earlier files. The output folder is relative to the Python kernel's working directory. The notebook prints the fetch timestamp; record that time and your configuration settings when publishing or sharing analyses.

## How to report a finding responsibly

A suitable exploratory sentence is:

> In the ECHOREPO observations retrieved on **[UTC date]**, we found a **[positive/negative/weak]** monotonic association between field soil pH and **[the correctly named organic measure]** among **[n]** samples with both values present (Spearman ρ = **[value]**). The result is descriptive and not adjusted for sampling geography or confounders.

Use the label printed by the notebook for the organic variable. The current API's `soil_organic_matter_estimate_pct` represents **field-estimated organic matter**, not measured soil organic carbon; no assumed conversion is performed.

## Suggested research exercises

1. Compare the all-country result with `COUNTRY_CODES = ['ES']`, then `['FI']`. Do you see the same association?
2. Compare the number of valid organic observations with the number of valid pH observations. What biases might missing values introduce?
3. Look at the lowest and highest pH samples (without sharing identifiers unnecessarily). Are values plausible and do they need further QA?
4. Use the `sample_id` link to investigate laboratory parameters with `echo.parameters()` as a separate extension. Parameter codes, units, and laboratory methods need to be checked before interpretation.

## Known limitations

- The data are **volunteer-contributed** and not designed as a random probability sample.
- The field organic fraction is an **estimate** and may represent different semantics across historical schemas.
- Outlier screening is minimal; the notebook does not automatically apply ECHOREPO `qa_status` or filter obfuscated locations.
- Correlation does not adjust for geography, soil texture, climate, land management, or reporting practices.
- One country can have many records but poor spatial coverage.
- The sample-level export contains identifiers. Share it only under applicable project access and publication rules.

For methodology and configuration details, see the Markdown explanations embedded in the notebook itself.
