#!/usr/bin/env Rscript
# Memory-bounded FAPROTAX functional profiling for ECHOREPO.
#
# Uses microeco ONCE to annotate taxonomy (one artificial reference sample),
# then reads file 6 in bounded OTU-row chunks. The dense OTU x sample matrix
# is never created. Output files and the scientific calculation remain
# compatible with the original microeco cal_spe_func_perc() defaults:
#   unweighted OTU prevalence, 0..1 fractions, rounded to 6 decimal places.
#
# IMPORTANT SCIENTIFIC NOTE: unweighted prevalence is NOT read-weighted
# functional abundance. To request read-weighted fractions explicitly, set
# FAPROTAX_ABUNDANCE_WEIGHTED=1. Do not mix the two methods silently.
#
# Usage: Rscript tools/faprotax_16S_functional_profile.R \
#     6_otu_clean_counts_no_blanks.csv 7_taxonomy_clean.csv output_directory
#
# FAPROTAX_CHUNK_ROWS controls input rows per read (default 128).
# A .tmp file is used for each output, so failed jobs preserve old results.

read_integer_env <- function(name, default, min_value = 1L) {
  value <- Sys.getenv(name, unset = as.character(default))
  parsed <- suppressWarnings(as.integer(value))
  if (length(parsed) != 1L || is.na(parsed) || parsed < min_value) {
    stop(name, " must be an integer >= ", min_value, ", got ", shQuote(value))
  }
  parsed
}

# This function is deliberately independent of microeco, so it can be tested
# with an artificial OTU->function mapping and known expected results.
stream_faprotax_counts <- function(otu_file, otu_ids, function_map,
                                   chunk_rows = 128L, abundance_weighted = FALSE,
                                   fraction_scale = 1, decimals = 6L,
                                   progress_rows = 5000L) {
  if (!requireNamespace("Matrix", quietly = TRUE)) {
    stop("The 'Matrix' R package is required for sparse FAPROTAX aggregation")
  }
  if (!is.matrix(function_map) && !inherits(function_map, "Matrix")) {
    stop("function_map must be a matrix or sparse Matrix")
  }
  if (anyDuplicated(otu_ids) || anyNA(otu_ids)) {
    stop("OTU IDs must be unique and non-missing")
  }
  if (is.null(rownames(function_map)) ||
      !identical(rownames(function_map), otu_ids)) {
    stop("Function annotation IDs must match taxonomy OTU IDs in order")
  }
  if (is.null(colnames(function_map)) || anyDuplicated(colnames(function_map))) {
    stop("Function annotations must have unique named function columns")
  }
  if (chunk_rows < 1L || progress_rows < 1L) {
    stop("FAPROTAX chunk and progress row counts must be positive")
  }

  header <- names(utils::read.csv(
    otu_file, nrows = 0L, check.names = FALSE, stringsAsFactors = FALSE
  ))
  if (length(header) < 2L) {
    stop("File 6 must have an OTU ID column and at least one sample column")
  }
  sample_ids <- header[-1L]
  if (anyNA(sample_ids) || anyDuplicated(sample_ids) || any(!nzchar(sample_ids))) {
    stop("File 6 contains empty or duplicated sample names")
  }

  sample_n <- length(sample_ids)
  function_names <- colnames(function_map)
  n_functions <- length(function_names)
  sample_totals <- numeric(sample_n)
  sample_otu_totals <- numeric(sample_n)
  annotated_reads <- numeric(sample_n)
  function_numerators <- matrix(
    0, nrow = n_functions, ncol = sample_n,
    dimnames = list(function_names, sample_ids)
  )
  function_otu_counts <- Matrix::rowSums(function_map != 0)
  if (length(function_otu_counts) != length(otu_ids)) {
    stop("Invalid OTU->function annotation dimensions")
  }

  # Preserve source order and parse only one small block of wide CSV rows at
  # a time. Base read.csv() handles quoted OTU IDs and decimal abundances.
  source <- file(otu_file, open = "rt", encoding = "UTF-8")
  on.exit(close(source), add = TRUE)
  readLines(source, n = 1L, warn = FALSE)  # consume CSV header
  processed <- 0L
  next_progress <- as.integer(progress_rows)
  column_types <- c("character", rep("numeric", sample_n))

  repeat {
    block <- utils::read.csv(
      source, header = FALSE, col.names = header, nrows = as.integer(chunk_rows),
      check.names = FALSE, row.names = NULL, stringsAsFactors = FALSE,
      colClasses = column_types, comment.char = "", blank.lines.skip = FALSE,
      na.strings = c("NA")
    )
    rows <- nrow(block)
    if (rows == 0L) break

    positions <- seq.int(processed + 1L, processed + rows)
    if (max(positions) > length(otu_ids)) {
      stop("File 6 contains more OTU rows than taxonomy file 7")
    }
    if (!identical(as.character(block[[1L]]), otu_ids[positions])) {
      stop(
        "File 6 and file 7 have different OTU IDs or order at row ",
        processed + 1L,
        ". Regenerate both from the same PostgreSQL snapshot."
      )
    }

    # Only chunk_rows x sample_n doubles are materialized.
    counts <- as.matrix(block[-1L])
    storage.mode(counts) <- "double"
    if (anyNA(counts) || any(!is.finite(counts)) || any(counts < 0)) {
      stop("File 6 contains missing, infinite or negative read counts")
    }

    present <- counts > 0
    sample_totals <- sample_totals + colSums(counts)
    sample_otu_totals <- sample_otu_totals + colSums(present)
    annotated_rows <- function_otu_counts[positions] > 0
    if (any(annotated_rows)) {
      annotated_reads <- annotated_reads + colSums(
        counts[annotated_rows, , drop = FALSE]
      )
    }

    # The binary taxonomy->function table is sparse. This multiplication
    # accumulates per-function counts without creating an OTU x sample x
    # function array, or iterating over all functions in R for each OTU.
    feature_functions <- function_map[positions, , drop = FALSE]
    features <- if (abundance_weighted) counts else present * 1.0
    function_numerators <- function_numerators + as.matrix(
      Matrix::crossprod(feature_functions, features)
    )
    processed <- processed + rows

    if (processed >= next_progress) {
      cat(sprintf(
        "[progress] Processed %s/%s OTU rows (%.1f%%)\n",
        format(processed, big.mark = ","),
        format(length(otu_ids), big.mark = ","),
        100 * processed / length(otu_ids)
      ))
      flush.console()
      next_progress <- processed + as.integer(progress_rows)
    }
    rm(block, counts, present, feature_functions, features)
  }

  if (processed != length(otu_ids)) {
    stop(
      "File 6 ended after ", processed,
      " OTUs; taxonomy file 7 contains ", length(otu_ids)
    )
  }

  denominators <- if (abundance_weighted) sample_totals else sample_otu_totals
  zero_samples <- which(denominators == 0)
  if (length(zero_samples)) {
    warning(length(zero_samples), " samples have no retained OTU counts; ",
            "their functional fractions are set to zero")
    denominators[zero_samples] <- 1
  }
  fractions <- sweep(function_numerators, 2L, denominators, "/")
  fractions <- round(fractions * fraction_scale, digits = decimals)
  if (length(zero_samples)) fractions[, zero_samples] <- 0
  # microeco::cal_func_FR() defaults to remove_zero=TRUE, AFTER rounding.
  keep_functions <- rowSums(fractions) != 0
  fractions <- fractions[keep_functions, , drop = FALSE]

  # Store sample x function, matching the existing R output's orientation.
  output <- t(fractions)
  coverage <- ifelse(sample_totals > 0, annotated_reads / sample_totals, 0)
  names(coverage) <- sample_ids

  list(
    functions = output,
    annotated_fraction = coverage,
    samples = sample_ids,
    otu_count = processed,
    kept_function_count = ncol(output)
  )
}

clean_unknown_taxonomy <- function(x) {
  x <- as.character(x)
  x[x %in% c("", "NA", "Na", "N/A", "unclassified", "uncultured",
              "uncultured_bacterium", "uncultured_archaeon",
              "Incertae_Sedis", "Incertae Sedis")] <- NA_character_
  x
}

clean_faprotax_taxonomy <- function(tax_df) {
  ranks <- c("Kingdom", "Phylum", "Class", "Order", "Family", "Genus")
  tax2 <- data.frame(row.names = rownames(tax_df))
  for (rank in ranks) {
    tax2[[rank]] <- if (rank %in% names(tax_df)) {
      clean_unknown_taxonomy(tax_df[[rank]])
    } else {
      rep(NA_character_, nrow(tax_df))
    }
  }
  # 97% OTU taxonomy is not reliable enough for Species assignments.
  tax2[["Species"]] <- rep(NA_character_, nrow(tax_df))
  tax2
}

annotate_faprotax_once <- function(tax2) {
  # A single all-one reference column prevents microeco's tidy_dataset()
  # from dropping OTUs that are absent from any particular real sample.
  # This sample is NEVER used for the final per-sample fractions.
  dummy <- data.frame(
    .reference = rep(1, nrow(tax2)), row.names = rownames(tax2),
    check.names = FALSE
  )
  micro <- microeco::microtable$new(otu_table = dummy, tax_table = tax2)
  micro$tidy_dataset()
  tf <- microeco::trans_func$new(dataset = micro)
  if (is.function(tf$cal_func)) {
    tf$cal_func(prok_database = "FAPROTAX")
    mapping <- tf$res_func
  } else {
    tf$cal_spe_func()
    mapping <- tf$res_spe_func
  }
  if (is.null(mapping) || nrow(mapping) != nrow(tax2)) {
    stop("microeco FAPROTAX annotation failed or lost OTUs")
  }
  if (!identical(rownames(mapping), rownames(tax2))) {
    stop("microeco FAPROTAX annotation changed OTU order")
  }
  # A dense annotation frame would be a fraction of the size of file 6,
  # but keep it sparse for the chunked matrix multiplications.
  binary <- as.matrix(mapping) != 0
  dimnames(binary) <- dimnames(mapping)
  sparse <- Matrix::Matrix(binary * 1.0, sparse = TRUE)
  rm(micro, tf, dummy, mapping, binary)
  gc(verbose = FALSE)
  sparse
}

atomic_write_csv <- function(data, final_path, ...) {
  temporary <- paste0(final_path, ".tmp")
  on.exit(unlink(temporary), add = TRUE)
  utils::write.csv(data, file = temporary, ...)
  if (!file.rename(temporary, final_path)) {
    stop("Could not publish FAPROTAX output: ", final_path)
  }
}

main <- function(args = commandArgs(trailingOnly = TRUE)) {
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

  chunk_rows <- read_integer_env("FAPROTAX_CHUNK_ROWS", 128L)
  decimals <- read_integer_env("FAPROTAX_DECIMALS", 6L, min_value = 0L)
  weighted_flag <- Sys.getenv("FAPROTAX_ABUNDANCE_WEIGHTED", unset = "0")
  if (!weighted_flag %in% c("0", "1")) {
    stop("FAPROTAX_ABUNDANCE_WEIGHTED must be 0 or 1")
  }
  weighted <- weighted_flag == "1"

  cat(
    "--------------------------------------------------\n",
    "FAPROTAX memory-bounded functional profiling\n",
    "OTU file:      ", otu_file, "\n",
    "Taxonomy file: ", tax_file, "\n",
    "Output dir:    ", outdir, "\n",
    "Chunk rows:    ", chunk_rows, "\n",
    "Method:        ", if (weighted) "abundance-weighted" else "unweighted OTU prevalence", "\n",
    "Scale:         fractions (0..1)\n",
    "--------------------------------------------------\n",
    sep = ""
  )

  # The package now annotates the taxonomy ONCE instead of carrying the
  # complete giant OTU-by-sample count table inside an R6 object.
  suppressPackageStartupMessages(library(microeco))
  suppressPackageStartupMessages(library(Matrix))
  cat("[INFO] Loading taxonomy (file 7)...\n")
  tax_df <- utils::read.csv(
    tax_file, sep = ";", row.names = 1L, check.names = FALSE,
    stringsAsFactors = FALSE
  )
  if (!nrow(tax_df) || anyDuplicated(rownames(tax_df))) {
    stop("Taxonomy must have unique nonempty OTU IDs")
  }
  tax2 <- clean_faprotax_taxonomy(tax_df)
  rm(tax_df)

  cat("[INFO] Computing OTU->FAPROTAX assignments once...\n")
  function_map <- annotate_faprotax_once(tax2)
  cat("[OK] Annotated ", nrow(function_map), " OTUs across ",
      ncol(function_map), " possible functions\n", sep = "")

  cat("[INFO] Processing counts by OTU blocks...\n")
  result <- stream_faprotax_counts(
    otu_file, rownames(tax2), function_map,
    chunk_rows = chunk_rows, abundance_weighted = weighted,
    fraction_scale = 1, decimals = decimals
  )
  cat("[OK] Processed ", result$otu_count, " OTUs and ",
      length(result$samples), " samples; kept ",
      result$kept_function_count, " non-zero functions\n", sep = "")

  # Publish the same three outputs as the old script. All intermediate
  # computation has completed successfully before any final file is changed.
  taxonomy_path <- file.path(outdir, "7_taxonomy_NA.csv")
  taxonomy_tmp <- paste0(taxonomy_path, ".tmp")
  on.exit(unlink(taxonomy_tmp), add = TRUE)
  utils::write.table(
    tax2, taxonomy_tmp, quote = FALSE, sep = ";",
    row.names = TRUE, col.names = TRUE
  )

  function_path <- file.path(outdir, "8_faprotax_samples_x_functions.csv")
  coverage_path <- file.path(outdir, "9_faprotax_annotated_fraction.csv")
  function_tmp <- paste0(function_path, ".tmp")
  coverage_tmp <- paste0(coverage_path, ".tmp")
  on.exit(unlink(c(function_tmp, coverage_tmp)), add = TRUE)

  utils::write.csv(result$functions, function_tmp)
  fraction <- result$annotated_fraction
  annot_df <- data.frame(
    Sample = names(fraction),
    Annotated_frac = round(fraction, 4L),
    Annotated_pct = round(fraction * 100, 2L)
  )
  utils::write.csv(annot_df, coverage_tmp, row.names = FALSE)

  for (path in c(taxonomy_tmp, function_tmp, coverage_tmp)) {
    if (!file.exists(path) || file.info(path)$size == 0L) {
      stop("Missing or empty temporary FAPROTAX output: ", path)
    }
  }
  for (pair in list(
    c(taxonomy_tmp, taxonomy_path),
    c(function_tmp, function_path),
    c(coverage_tmp, coverage_path)
  )) {
    if (!file.rename(pair[[1L]], pair[[2L]])) {
      stop("Cannot publish output ", pair[[2L]])
    }
  }

  cat("FAPROTAX sample validation OK: ", nrow(result$functions), " samples\n", sep = "")
  cat("Annotated fraction per sample (mean +/- sd): ",
      round(mean(fraction) * 100, 1), "% +/- ",
      round(stats::sd(fraction) * 100, 1), "%\n", sep = "")
  cat("[OK] Wrote: ", function_path, "\n", sep = "")
  cat("[OK] Wrote: ", coverage_path, "\n", sep = "")
  cat("[OK] Wrote: ", taxonomy_path, "\n", sep = "")
  cat("Done.\n")
  invisible(result)
}

# Safe to source this file from regression tests without executing a run.
if (sys.nframe() == 0L) {
  main()
}
