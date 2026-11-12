# ECHOREPO Jupyter — Research notebooks: user guide

**Audience:** researchers, students, citizen-science coordinators, and other authorised users of the ECHOREPO JupyterLab research environment.

This guide covers three example notebooks that retrieve **live ECHOREPO canonical data** through the existing ECHOREPO Python API client. Start with `ECHOREPO_analysis.ipynb` if you are new to Jupyter, then choose one of the more specialised notebooks.

| Notebook | Main question | What you will obtain | Detailed guide |
| --- | --- | --- | --- |
| `ECHOREPO_analysis.ipynb` | What data are available and how are they distributed? | Dataset counts, sample-ID consistency checks, country counts, pH distribution, biodiversity taxa ranking, country-level statistics | [Getting started / data overview](docs/ECHOREPO_analysis.md) |
| `ECHOREPO_soil_properties.ipynb` | How do soil pH and citizen-estimated organic percentages vary and relate to each other? | Distributions, paired-property plot, country medians, within-country correlations, **4 CSV outputs** | [Soil properties explorer](docs/ECHOREPO_soil_properties.md) |
| `ECHOREPO_soil_biodiversity.ipynb` | Is microbial taxonomic diversity associated with soil pH or organic percentages? | Taxon richness, Shannon diversity, depth check, marker-specific correlations, country coverage, **3 CSV outputs** | [Microbial biodiversity explorer](docs/ECHOREPO_soil_biodiversity.md) |

> **Interpretation:** these are exploratory research templates, not population-representative European soil surveys or laboratory-validated inference pipelines. Their results depend on which records are in the live API at the time you run them.

## 1. Open and run a notebook

1. Open the ECHOREPO JupyterLab research environment and navigate to the `notebooks/` folder (the folder that contains `lib/`).
2. Open one of the `.ipynb` files and select the available **Python 3 / ipykernel** kernel if prompted.
3. Read the introduction and the configuration cell. You may adjust its options before running.
4. Run the cells **from top to bottom** with **Run → Run All Cells** (menu names vary slightly with JupyterLab versions), or use **Shift+Enter** to run the selected cell and advance.
5. Inspect the printed row counts, summary tables, figures, warnings, and (where applicable) CSV files saved under `exports/`.

A notebook consists of **Markdown cells** (explanations) and **code cells** (executable Python). Markdown inside a notebook is rendered when the cell is not being edited; stand-alone `.md` documentation uses a different preview command (below).

### Where the data come from

The notebooks use the existing helper at `lib/echorepo.py`:

```python
from lib.echorepo import EchoRepo

echo = EchoRepo()
print(echo.base_url)
print(echo.ping())

samples = echo.samples()
parameters = echo.parameters()
images = echo.images()
biodiversity = echo.biodiversity()
```

The intended internal API URL inside Docker is `http://echorepo-lite:8000`, normally configured through the `ECHOREPO_URL` environment variable. The notebooks do not need direct database credentials. In this environment, the API responses are loaded into pandas DataFrames in notebook memory, and returned data can change as ECHOREPO is updated.

For an explicit data-access check, run:

```python
from lib.echorepo import EchoRepo
print(EchoRepo().ping())
```

A working response typically includes `{'ok': True}`. If `echo.base_url` is not the intended address, check the notebook container's `ECHOREPO_URL` configuration.

### Main data fields

The ECHOREPO API has used two sets of names for some sample attributes. The two new specialised notebooks handle both:

| Meaning | Current canonical API field | Legacy field |
| --- | --- | --- |
| Field-measured / field-reported soil pH | `soil_ph_field` | `ph` |
| Field-estimated soil organic matter (%) | `soil_organic_matter_estimate_pct` | `organic_carbon_pct` (previously labelled organic carbon) |
| Identifier | `sample_id` | `sample_id` |
| Country | `country_code` | `country_code` |

**Important:** organic matter (%) and organic carbon (%) are distinct measurements. The specialised notebooks label the variable according to the actual column present; do not silently convert one into the other or compare them as the same quantity. The original `ECHOREPO_analysis.ipynb` still uses legacy fields and may need updating when run against the current API; see [its guide](docs/ECHOREPO_analysis.md).

## 2. Where results are saved

- Notebooks normally display **figures and tables inside JupyterLab** as soon as their cells execute.
- The original overview notebook writes `exports/country_statistics.csv` (but does **not** create `exports/` itself).
- The soil-properties notebook writes four CSVs, and the biodiversity notebook writes three CSVs. Both create `exports/` automatically.
- `exports/` is **relative to the current notebook kernel working directory**. To check the actual directory, run:

```python
from pathlib import Path
print(Path.cwd())
print((Path.cwd() / 'exports').resolve())
```

If you rerun a notebook with the same output filenames, its CSVs will be **overwritten**. Save a dated copy if you want a reproducible record of an earlier analysis. The specialised notebooks print the UTC fetch time, and include it in at least one relevant sample-level export.

## 3. View these Markdown (`.md`) manuals in JupyterLab

Double-clicking a `.md` file often opens its **source text**, so you see `#` and `**` characters. To see formatted text:

1. In JupyterLab's left file browser, **right-click** the `.md` filename.
2. Choose **Open With → Markdown Preview**.
3. To edit, open the same file with **Editor**; you can keep the editor and preview visible side by side.

If the `.md` file is already open in an editor, look for **Show Markdown Preview** in the editor context menu or command palette. No Markdown-rendering extension should be needed in standard JupyterLab.

Official references: [JupyterLab — File and Output Formats](https://jupyterlab.readthedocs.io/en/stable/user/file_formats.html) and [JupyterLab — Commands](https://jupyterlab.readthedocs.io/en/stable/user/commands.html).

## 4. Responsible interpretation and sharing

- **Counts are counts of ECHOREPO records**, not estimates of soil conditions in the general population; sampling intensity varies geographically and over time.
- **Associations are not causal effects.** Geography, soil type, land management, climate, sampling method, and laboratory protocol can affect apparent relationships.
- **Field and laboratory variables should remain distinguishable.** Use the actual API field name and units when reporting results.
- **Biodiversity is taxon-level summary data.** It is not a replacement for analysis of raw OTU/ASV sequencing tables.
- **Check QA and provenance.** These notebooks do only limited validation and do not automatically exclude all potentially problematic or geographically obfuscated records.
- **Be careful with sample IDs and joined records.** Specialist notebook CSVs can contain sample-level identifiers even if coordinates are excluded. Follow your project's data-access and publication rules.

## 5. Common problems

| Symptom | Likely cause | What to try |
| --- | --- | --- |
| `ModuleNotFoundError: No module named 'lib'` | Kernel started outside the notebook workspace, or `lib/` is missing | Start Jupyter from the ECHOREPO notebook workspace; verify `lib/echorepo.py` exists and check `Path.cwd()` |
| `NameResolutionError`, timeout, or connection error | Docker network or API address unreachable | Run `echo.ping()`; check `ECHOREPO_URL` and container networking with the platform administrator |
| Expected CSV but got an HTML response | Login page, proxy, or authentication error instead of API data | Verify that your research container has permitted access to the canonical API; do not copy credentials into notebooks |
| `KeyError: 'ph'` / `KeyError: 'organic_carbon_pct'` in **overview** notebook | Current API uses newer names | See the compatibility changes in the overview notebook guide |
| `Cannot save file into a non-existent directory: 'exports'` | Overview notebook does not create export folder | Execute `from pathlib import Path; Path('exports').mkdir(exist_ok=True)` before its export cell |
| `ValueError` about taxonomic rank or no retained reads | Requested rank not present, or filters too restrictive | Inspect printed available ranks; change `TAXON_RANK`, `MIN_TOTAL_READS`, or the unassigned-taxon option |
| A figure is empty or correlations show `NaN` | Too few complete observations, constant values, or no eligible rows | Inspect completeness and filtering tables; choose an appropriate subset |
| `.md` shows `#` and `**` instead of headings | Opened in text editor | Right-click file → **Open With → Markdown Preview** |

## 6. Suggested order for a new user

1. **Overview:** inspect what data are available and identify missing or inconsistent records.
2. **Soil properties:** learn how to interpret distributions, paired measurements, country comparisons, and correlation statistics.
3. **Microbial biodiversity:** explore the taxonomic summary dataset, sequencing-effort caveats, and marker-specific associations.

Use these documents and notebooks as teaching/research starters: record the analysis date and options used, keep output files with a methods description, and verify any interesting result with a research-grade protocol before presenting it as a scientific finding.
