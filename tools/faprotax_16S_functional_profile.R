#!/usr/bin/env Rscript
# FAPROTAX functional profiling for ECHOREPO 16S biodiversity data.
#
# Files 6 (dense OTU counts) and 7 (taxonomy) are prepared by
# generate_biodiversity_piecharts.py. This script reads file 7 once, uses
# microeco/FAPROTAX to annotate the OTUs ONCE with a single dummy sample,
# then streams file 6 in bounded row blocks. No full OTU x sample matrix
# is materialized in R.
#
# The original script calls cal_spe_func_perc() with no arguments. That
# method is UNWEIGHTED: it calculates the fraction of PRESENT OTUs associated
# with each function, not the read-weighted fraction. Preserve that default
# (fraction in [0,1], rounded to 6 decimals on recent microeco releases).
#
# File 8: 8_faprotax_samples_x_functions.csv (samples x functions; fractions)
# File 9: 9_faprotax_annotated_fraction.csv (READ-weighted annotation coverage)
# File 7 intermediate: 7_taxonomy_NA.csv (cleaned taxonomy used by FAPROTAX)
#
# Usage:
# Rscript tools/faprotax_16S_functional_profile.R \
#   data/biodiversity/faprotax_work/6_otu_clean_counts_no_blanks.csv \
#   data/biodiversity/faprotax_work/7_taxonomy_clean.csv \
#   data/biodiversity
#
# Optional environment:
#   FAPROTAX_CHUNK_ROWS=128     number of OTU rows parsed per block
#   FAPROTAX_PROGRESS_ROWS=5000 progress messages (0 disables them)

clean_unknown_faprotax <- function(x) {
  x <- as.character(x)
  x[x %in% c("", "NA", "Na", "N/A", "unclassified", "uncultured",
             "uncultured_bacterium", "uncultured_archaeon",
             "Incertae_Sedis", "Incertae Sedis")] <- NA_character_
  x
}

faprotax_chunk_rows <- function() {
  raw <- Sys.getenv("FAPROTAX_CHUNK_ROWS", "128")
  n <- suppressWarnings(as.integer(raw))
  if (is.na(n) || n < 1L || n > 10000L) {
    stop("FAPROTAX_CHUNK_ROWS must be an integer in 1..10000")
  }
  n
}

# No 'microeco' import required for this function. Tests can inject a
# miniature known OTU-by-function annotation matrix.
aggregate_faprotax_stream <- function(otu_file, feature_ids, func_binary,
                                      chunk_rows = 128L, progress_rows = 5000L) {
  if (anyDuplicated(feature_ids) || anyNA(feature_ids) || !length(feature_ids)) {
    stop("Taxonomy OTU IDs must be unique and nonempty")
  }
  if (!is.matrix(func_binary)) {
    func_binary <- as.matrix(func_binary)
  }
  if (!identical(rownames(func_binary), feature_ids)) {
    stop("FAPROTAX annotations are not aligned with taxonomy OTU IDs")
  }
  if (!ncol(func_binary) || !nrow(func_binary) ||
      is.null(colnames(func_binary)) || anyDuplicated(colnames(func_binary))) {
    stop("FAPROTAX produced no valid function names")
  }
  if (anyNA(func_binary)) {
    stop("FAPROTAX returned NA function assignments")
  }

  con <- file(otu_file, open = "rt")
  on.exit(close(con), add = TRUE)

  header_line <- readLines(con, n = 1L, warn = FALSE)
  if (length(header_line) != 1L) stop("OTU count CSV is empty")
  header <- names(utils::read.csv(text = header_line, nrows = 0L,
                                  check.names = FALSE))
  if (length(header) < 2L ||
      !tolower(header[[1L]]) %in% c("otu_id", "otu id", "otu")) {
    stop("Expected OTU ID followed by sample columns in file 6")
  }
  sample_ids <- header[-1L]
  if (anyNA(sample_ids) || any(!nzchar(sample_ids)) || anyDuplicated(sample_ids)) {
    stop("File 6 has empty or duplicate sample column names")
  }

  n_samples <- length(sample_ids)
  n_functions <- ncol(func_binary)
  n_features <- length(feature_ids)
  annotation_present <- func_binary > 0
  annotated_features <- rowSums(annotation_present) > 0

  # The output matrices scale with sample_count x function_count rather than
  # OTU_count x sample_count. At ~2,736 samples, they are small.
  function_taxa <- matrix(
    0, nrow = n_samples, ncol = n_functions,
    dimnames = list(sample_ids, colnames(func_binary))
  )
  present_taxa <- numeric(n_samples)
  total_reads <- numeric(n_samples)
  annotated_reads <- numeric(n_samples)

  consumed <- 0L
  last_progress <- 0L
  start_time <- proc.time()[["elapsed"]]

  repeat {
    # read.csv on an open connection reads ONLY this block, not the whole file.
    # Explicit numeric classes prevent row-by-row type inference.
    block <- utils::read.csv(
      file = con, header = FALSE, col.names = header,
      nrows = chunk_rows, check.names = FALSE,
      colClasses = c("character", rep("numeric", n_samples))
    )
    if (nrow(block) == 0L) break

    n <- nrow(block)
    next_end <- consumed + n
    if (next_end > n_features ||
        !identical(as.character(block[[1L]]),
                   feature_ids[seq.int(consumed + 1L, next_end)])) {
      stop(sprintf(
        "OTU ID order differs between files 6 and 7 at row %d; refusing to publish",
        consumed + 1L
      ))
    }

    counts <- as.matrix(block[-1L])
    if (!is.numeric(counts) || anyNA(counts) ||
        any(!is.finite(counts)) || any(counts < 0)) {
      stop(sprintf("Invalid abundance values in file 6 near OTU row %d",
                   consumed + 1L))
    }
    present <- counts > 0
    present_taxa <- present_taxa + colSums(present)
    total_reads <- total_reads + colSums(counts)

    selected <- seq.int(consumed + 1L, next_end)
    annotated_in_block <- annotated_features[selected]
    if (any(annotated_in_block)) {
      annotated_reads <- annotated_reads +
        colSums(counts[annotated_in_block, , drop = FALSE])
    }

    # Sparse multiplication computes functional PRESENCE counts:
    # function_taxa[sample, func] += number of present OTUs matching func.
    # Matrix::Matrix only sees a bounded number of OTU rows at a time.
    ann <- Matrix::Matrix(annotation_present[selected, , drop = FALSE],
                          sparse = TRUE)
    pres <- Matrix::Matrix(present, sparse = TRUE)
    function_taxa <- function_taxa + t(as.matrix(crossprod(ann, pres)))

    consumed <- next_end
    if (progress_rows > 0L &&
        (consumed - last_progress >= progress_rows || consumed == n_features)) {
      message(sprintf(
        "[progress] FAPROTAX: %s/%s OTUs; %0.1f s elapsed",
        format(consumed, big.mark = ",", scientific = FALSE),
        format(n_features, big.mark = ",", scientific = FALSE),
        proc.time()[["elapsed"]] - start_time
      ))
      last_progress <- consumed
    }
  }

  if (consumed != n_features) {
    stop(sprintf(
      "OTU count CSV ended early: %d rows read; %d taxonomy rows expected",
      consumed, n_features
    ))
  }

  zero_samples <- sample_ids[present_taxa == 0]
  if (length(zero_samples)) {
    warning(sprintf(
      "%d sample(s) have no nonzero retained OTUs; writing zero functional fractions",
      length(zero_samples)
    ))
  }

  # Match the original microeco cal_spe_func_perc() defaults:
  # abundance_weighted = FALSE; fraction, not percentage; 6 decimals.
  divisor <- pmax(present_taxa, 1)
  fractions <- round(function_taxa / divisor, digits = 6L)
  fractions <- fractions[, colSums(fractions) != 0, drop = FALSE]

  # File 9 used a different quantity in the original script: the fraction of
  # READS associated with at least one annotated function.
  coverage <- ifelse(total_reads > 0, annotated_reads / pmax(total_reads, 1), 0)
  names(coverage) <- sample_ids

  list(
    fractions = fractions,
    coverage = coverage,
    sample_ids = sample_ids,
    source_otu_rows = consumed,
    zero_samples = zero_samples
  )
}

# Perform annotation without loading the actual OTU x sample count matrix.
# A single 1-read dummy sample keeps every OTU for taxonomy-based matching.
annotate_faprotax_once <- function(tax2) {
  if (!requireNamespace("microeco", quietly = TRUE)) {
    stop("Required R package 'microeco' is not installed")
  }
  # microeco::trans_func$cal_func() calls data("prok_func_FAPROTAX")
  # without a package= argument. With requireNamespace()/:: alone, its
  # bundled dataset may not be visible to data() on the user's R setup.
  # Attach microeco explicitly, matching the legacy script's library().
  if (!"package:microeco" %in% search()) {
    suppressPackageStartupMessages(library("microeco", character.only = TRUE))
  }
  otu_ids <- rownames(tax2)
  dummy <- data.frame(
    .FAPROTAX_reference = rep(1, nrow(tax2)),
    row.names = otu_ids, check.names = FALSE
  )
  micro <- microeco::microtable$new(
    otu_table = dummy,
    tax_table = tax2
  )
  micro$tidy_dataset()
  tf <- microeco::trans_func$new(dataset = micro)
  tf$cal_spe_func(prok_database = "FAPROTAX")
  annotation <- as.matrix(tf$res_spe_func)
  if (!setequal(rownames(annotation), otu_ids)) {
    stop("microeco's annotated OTU set differs from cleaned taxonomy")
  }
  annotation <- annotation[otu_ids, , drop = FALSE]
  storage.mode(annotation) <- "numeric"
  rm(tf, micro, dummy)
  gc(verbose = FALSE)
  annotation
}

write_faprotax_results <- function(profiles, outdir) {
  file8 <- file.path(outdir, "8_faprotax_samples_x_functions.csv")
  file9 <- file.path(outdir, "9_faprotax_annotated_fraction.csv")
  temporary8 <- paste0(file8, ".tmp")
  temporary9 <- paste0(file9, ".tmp")

  on.exit(unlink(c(temporary8, temporary9)), add = TRUE)

  if (!identical(rownames(profiles$fractions), profiles$sample_ids)) {
    stop("Internal error: FAPROTAX output sample IDs are not aligned")
  }
  write.csv(profiles$fractions, temporary8)
  annotated_frac <- profiles$coverage
  annotation_df <- data.frame(
    Sample = names(annotated_frac),
    Annotated_frac = round(annotated_frac, 4L),
    Annotated_pct = round(annotated_frac * 100, 2L)
  )
  write.csv(annotation_df, temporary9, row.names = FALSE)

  if (!file.exists(temporary8) || file.size(temporary8) == 0 ||
      !file.exists(temporary9) || file.size(temporary9) == 0) {
    stop("FAPROTAX output was not written completely")
  }

  # Outputs are swapped only after both temporary files have been written.
  if (!file.rename(temporary8, file8)) stop("Cannot publish file 8")
  if (!file.rename(temporary9, file9)) stop("Cannot publish file 9")

  message(sprintf("[OK] FAPROTAX output: %d samples x %d functions",
                  nrow(profiles$fractions), ncol(profiles$fractions)))
  message(sprintf("[OK] Wrote %s", file8))
  message(sprintf("[OK] Wrote %s", file9))
}

run_faprotax <- function(args) {
  if (length(args) != 3L) {
    stop(paste(
      "Usage: Rscript tools/faprotax_16S_functional_profile.R",
      "<otu_counts.csv> <taxonomy.csv> <output_dir>"
    ))
  }
  otu_file <- normalizePath(args[[1L]], mustWork = TRUE)
  tax_file <- normalizePath(args[[2L]], mustWork = TRUE)
  outdir <- args[[3L]]
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  outdir <- normalizePath(outdir, mustWork = TRUE)

  message("--------------------------------------------------")
  message("FAPROTAX functional profiling (memory-bounded)")
  message("OTU file:       ", otu_file)
  message("Taxonomy file:  ", tax_file)
  message("Output dir:     ", outdir)
  message("--------------------------------------------------")

  if (!requireNamespace("Matrix", quietly = TRUE)) {
    stop("The recommended R package 'Matrix' is required")
  }
  tax_df <- utils::read.csv(
    tax_file, sep = ";", row.names = 1L, check.names = FALSE
  )
  required <- c("Kingdom", "Phylum", "Class", "Order",
                "Family", "Genus")
  missing_cols <- setdiff(required, names(tax_df))
  if (length(missing_cols)) {
    stop("Missing taxonomy columns: ", paste(missing_cols, collapse = ", "))
  }
  if (anyDuplicated(rownames(tax_df))) stop("Duplicate taxonomy OTU IDs")

  # Match the original input normalization and 97% OTU species suppression.
  tax2 <- data.frame(
    Kingdom = tax_df$Kingdom,
    Phylum  = tax_df$Phylum,
    Class   = tax_df$Class,
    Order   = tax_df$Order,
    Family  = tax_df$Family,
    Genus   = tax_df$Genus,
    Species = rep(NA_character_, nrow(tax_df)),
    row.names = rownames(tax_df)
  )
  tax2[] <- lapply(tax2, clean_unknown_faprotax)

  tax_na_path <- file.path(outdir, "7_taxonomy_NA.csv")
  write.table(
    tax2, tax_na_path, quote = FALSE, sep = ";",
    row.names = TRUE, col.names = TRUE
  )
  message("[INFO] Cleaned taxonomy: ", nrow(tax2), " OTUs")
  rm(tax_df)

  message("[INFO] Annotating taxonomy once using microeco/FAPROTAX...")
  annotations <- annotate_faprotax_once(tax2)
  message(
    "[OK] Functional annotations: ", nrow(annotations),
    " OTUs x ", ncol(annotations), " functions"
  )
  rm(tax2)
  gc(verbose = FALSE)

  progress_rows <- suppressWarnings(as.integer(
    Sys.getenv("FAPROTAX_PROGRESS_ROWS", "5000")
  ))
  if (is.na(progress_rows) || progress_rows < 0L) {
    stop("FAPROTAX_PROGRESS_ROWS must be a nonnegative integer")
  }

  message("[INFO] Streaming OTU counts with chunks of ",
          faprotax_chunk_rows(), " rows...")
  profiles <- aggregate_faprotax_stream(
    otu_file, rownames(annotations), annotations,
    chunk_rows = faprotax_chunk_rows(), progress_rows = progress_rows
  )

  message("[INFO] Writing FAPROTAX functional profiles and coverage...")
  write_faprotax_results(profiles, outdir)
  frac <- profiles$coverage
  message(sprintf(
    "[OK] Annotated read fraction: mean %.1f%%; range %.1f%% - %.1f%%",
    100 * mean(frac), 100 * min(frac), 100 * max(frac)
  ))
}

# Do not run the pipeline when sourced by a standalone R unit test.
if (sys.nframe() == 0L) {
  run_faprotax(commandArgs(trailingOnly = TRUE))
}
