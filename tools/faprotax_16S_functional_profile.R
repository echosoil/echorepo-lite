# ============================================================
# GUIDE VERSION — FAPROTAX functional profiling
# ============================================================
# WHAT THIS SCRIPT DOES
# ---------------------
# Starting from a cleaned bacterial count table and an aligned taxonomy table,
# this script runs FAPROTAX functional annotation using the `microeco` package.
#
# It produces:
#   1. a RAW FAPROTAX functional matrix
#      -> functional abundance relative to the whole bacterial community
#   2. an annotation-coverage table
#      -> fraction of each sample's community represented by FAPROTAX-annotated taxa
#
# ECHOREPO uses the RAW matrix for bacterial guild plots.
#
# WHY THERE ARE TWO OUTPUTS
# -------------------------
# FAPROTAX typically annotates only a subset of taxa. In soil datasets that
# subset can be far below 100%. Therefore:
# - RAW values answer: "what % of the whole community is assigned to function X?"
# - RENORMALISED values answer: "within the annotated fraction, how is function X distributed?"
#
# WHICH OUTPUT TO USE
# -------------------
# Use RAW if you want strict proportions relative to the full community.
# Use RENORM if you want easier comparison with published FAPROTAX summaries
# and with other samples that differ in annotation coverage.
# ============================================================

# ------------------------------------------------------------
# USER SETTINGS — command-line arguments
# ------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop(
    paste(
      "Usage:",
      "Rscript tools/faprotax_16S_functional_profile.R",
      "<otu_counts.csv>",
      "<taxonomy.csv>",
      "<output_dir>"
    )
  )
}

otu_file <- normalizePath(args[1], mustWork = TRUE)
tax_raw_file <- normalizePath(args[2], mustWork = TRUE)

if (!file.exists(otu_file)) {
  stop("OTU file does not exist: ", otu_file)
}

if (!file.exists(tax_raw_file)) {
  stop("Taxonomy file does not exist: ", tax_raw_file)
}

outdir <- args[3]
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
outdir <- normalizePath(outdir, mustWork = TRUE)

cat("--------------------------------------------------\n")
cat("FAPROTAX functional profiling\n")
cat("OTU file:      ", otu_file, "\n")
cat("Taxonomy file: ", tax_raw_file, "\n")
cat("Output dir:    ", outdir, "\n")
cat("--------------------------------------------------\n")

# ------------------------------------------------------------
# 0) PACKAGES
# ------------------------------------------------------------
# microeco provides the FAPROTAX wrapper and stores results in object slots.
# data.table is not essential in this exact script, but is often useful for
# large tables and is kept here for compatibility with related workflows.
library(microeco)
library(data.table)

# ------------------------------------------------------------
# 1) LOAD CLEAN OTU COUNTS
# ------------------------------------------------------------
# INPUT EXPECTATION:
#   rows = taxa/features
#   columns = samples
#
# OUTPUT:
#   `otu` = cleaned count matrix/data frame used for functional annotation
otu <- read.csv(otu_file, sep = ",", row.names = 1, check.names = FALSE)

# ------------------------------------------------------------
# 2) LOAD TAXONOMY AND ALIGN IT TO THE OTU TABLE
# ------------------------------------------------------------
# GOAL:
#   Ensure that the taxonomy table contains the same features and in the same
#   order as the OTU table.
#
# WHY THIS MATTERS:
#   Misalignment between abundance and taxonomy is one of the most common causes
#   of silent errors in downstream annotation.
tax_df  <- read.csv(tax_raw_file, sep = ";", row.names = 1, check.names = FALSE)

stopifnot(nrow(tax_df) == nrow(otu))
stopifnot(identical(rownames(tax_df), rownames(otu)))

# FAPROTAX through microeco expects standard ranked columns.
# Species is intentionally set to NA because 97% OTU-level species names are
# usually too uncertain to trust in a functional annotation framework.
tax2 <- data.frame(
  Kingdom = if ("Kingdom" %in% colnames(tax_df)) tax_df$Kingdom else NA,
  Phylum  = if ("Phylum" %in% colnames(tax_df)) tax_df$Phylum else NA,
  Class   = if ("Class"  %in% colnames(tax_df)) tax_df$Class  else NA,
  Order   = if ("Order"  %in% colnames(tax_df)) tax_df$Order  else NA,
  Family  = if ("Family" %in% colnames(tax_df)) tax_df$Family else NA,
  Genus   = if ("Genus"  %in% colnames(tax_df)) tax_df$Genus  else NA,
  Species = NA
)

# Clean common "unknown" labels so they become proper NA values.
# This prevents false pseudo-labels such as "uncultured_bacterium" from being
# treated as meaningful taxonomy.
clean_unknown <- function(x) {
  x <- as.character(x)
  x[x %in% c("", "NA", "Na", "N/A", "unclassified", "uncultured",
              "uncultured_bacterium", "uncultured_archaeon",
              "Incertae_Sedis", "Incertae Sedis")] <- NA
  x
}
tax2[] <- lapply(tax2, clean_unknown)
rownames(tax2) <- rownames(tax_df)

# Save the aligned and cleaned taxonomy used by this script.
# This is useful for reproducibility and for downstream plotting scripts.
write.table(tax2,
            file.path(outdir, "7_taxonomy_NA.csv"),
            quote = FALSE, sep = ";", row.names = TRUE, col.names = TRUE)

# ------------------------------------------------------------
# 3) BUILD THE MICROECO OBJECT
# ------------------------------------------------------------
# GOAL:
#   Combine abundance and taxonomy into the object structure expected by microeco.
#
# OUTPUT:
#   `micro` object that can be passed to trans_func()
micro <- microtable$new(
  otu_table = otu,
  tax_table = tax2
)
# Check rownames correspondance between otu and tax2
micro$tidy_dataset()

# ------------------------------------------------------------
# 4) RUN FAPROTAX ANNOTATION
# ------------------------------------------------------------
# GOAL:
#   Assign putative ecological functions to taxa based on taxonomy.
#
# IMPORTANT LIMITATION:
#   FAPROTAX is an inference tool. It does NOT measure genes or metabolism
#   directly; it infers functions from known taxonomy-function associations.
tf <- trans_func$new(dataset = micro)
tf$cal_func(prok_database = "FAPROTAX")

# This creates a taxon x function table using counts / abundance information.
tf$cal_spe_func()

# This converts the annotation into per-sample percentages.
# At this stage, percentages are relative to the WHOLE community.
tf$cal_spe_func_perc()

# Optional introspection block:
# use this when debugging slot names across different package versions.
res_slots <- names(tf)[grepl("^res_", names(tf))]
for (nm in res_slots) {
  obj <- tf[[nm]]
  if (is.data.frame(obj) || is.matrix(obj)) {
    cat("\n---", nm, "---\n")
    cat("dim:", paste(dim(obj), collapse = " x "), "\n")
    cat("first rownames:", paste(head(rownames(obj)), collapse = ", "), "\n")
    cat("first colnames:", paste(head(colnames(obj)), collapse = ", "), "\n")
  }
}

# ------------------------------------------------------------
# 4b) EXTRACT THE SAMPLE x FUNCTION MATRIX
# ------------------------------------------------------------
# microeco versions may differ in orientation, so the code checks whether the
# object needs to be transposed.
# ------------------------------------------------------------
# 4b) EXTRACT THE SAMPLE x FUNCTION MATRIX
# ------------------------------------------------------------

func_mat <- as.matrix(tf$res_spe_func_perc)

sample_ids <- colnames(otu)

rows_are_samples <- all(sample_ids %in% rownames(func_mat))
cols_are_samples <- all(sample_ids %in% colnames(func_mat))

if (rows_are_samples && !cols_are_samples) {

  func_sxf <- func_mat

} else if (cols_are_samples && !rows_are_samples) {

  func_sxf <- t(func_mat)

} else {
  stop(
    paste0(
      "Cannot determine orientation of tf$res_spe_func_perc. ",
      "Expected OTU-table sample IDs in exactly one matrix dimension. ",
      "Matrix dimensions: ",
      nrow(func_mat), " x ", ncol(func_mat)
    )
  )
}

# Put samples in exactly the same order as the OTU table.
func_sxf <- func_sxf[sample_ids, , drop = FALSE]

cat(
  "FAPROTAX output matrix:",
  nrow(func_sxf), "samples x",
  ncol(func_sxf), "functions\n"
)

# Save RAW output:
# percentages relative to the whole community.
write.csv(func_sxf, file.path(outdir, "8_faprotax_samples_x_functions.csv"))

# Sanity check:
# Ensure that the sample IDs in the output matrix match the OTU table.
expected_samples <- colnames(otu)
output_samples <- rownames(func_sxf)

if (!setequal(expected_samples, output_samples)) {
  missing_samples <- setdiff(expected_samples, output_samples)
  extra_samples <- setdiff(output_samples, expected_samples)

  stop(
    paste0(
      "FAPROTAX output sample mismatch. ",
      "Missing: ", length(missing_samples),
      "; extra: ", length(extra_samples)
    )
  )
}

cat(
  "Sample validation OK:",
  length(output_samples),
  "samples in FAPROTAX output\n"
)

# ------------------------------------------------------------
# 5) SAVE THE OUTPUT
# ------------------------------------------------------------

# Identify taxa that received at least one function.
annotated_features <- rownames(tf$res_spe_func)[rowSums(tf$res_spe_func > 0) > 0]

# Build a relative-abundance matrix from the cleaned OTU table.
otu_mat  <- as.matrix(otu)
otu_rel  <- sweep(otu_mat, 2, colSums(otu_mat), "/")
otu_rel[!is.finite(otu_rel)] <- 0

# For each sample, calculate how much of the community belongs to annotated taxa.
annot_in_otu <- intersect(annotated_features, rownames(otu_rel))
annot_frac   <- colSums(otu_rel[annot_in_otu, , drop = FALSE])

cat("Annotated fraction per sample (mean ± sd):",
    round(mean(annot_frac) * 100, 1), "% ±",
    round(sd(annot_frac) * 100, 1), "%\n")
cat("Annotated fraction range:",
    round(min(annot_frac) * 100, 1), "% –",
    round(max(annot_frac) * 100, 1), "%\n")

# Save annotation coverage per sample.
annot_df <- data.frame(
  Sample         = names(annot_frac),
  Annotated_frac = round(annot_frac, 4),
  Annotated_pct  = round(annot_frac * 100, 2)
)
write.csv(annot_df, file.path(outdir, "9_faprotax_annotated_fraction.csv"), row.names = FALSE)

cat("Done.\n")
