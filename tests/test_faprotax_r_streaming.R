#!/usr/bin/env Rscript
# End-to-end R tests for the bounded-memory FAPROTAX reducer.
source("tools/faprotax_16S_functional_profile.R")

tmp <- tempfile("faprotax-r-test-")
dir.create(tmp)
on.exit(unlink(tmp, recursive = TRUE), add = TRUE)

samples <- c("ABCD-1111", "EFGH-2222", "IJKL-3333", "MNOP-4444")
otus <- c("otu_1", "otu_2", "otu_3", "otu_4", "otu_5")
counts <- matrix(c(
  10, 0, 3, 0,
   0, 2, 0, 0,
   5, 5, 0, 0,
   2, 0, 7, 0,
   0, 0, 0, 0
), nrow = 5, byrow = TRUE, dimnames = list(otus, samples))

# Two functions, including an OTU assigned to both; a non-annotated OTU;
# a never-observed OTU; and a zero-only sample column.
annotation <- matrix(
  c(1, 0,
    0, 1,
    1, 1,
    0, 0,
    0, 0),
  nrow = 5, byrow = TRUE,
  dimnames = list(otus, c("function_A", "function_B"))
)
otu_file <- file.path(tmp, "6_otu_clean_counts_no_blanks.csv")
write.csv(
  data.frame(OTU_ID = otus, counts, check.names = FALSE),
  otu_file, row.names = FALSE
)

# A simple reference implementation of microeco's unweighted FR formula.
reference <- matrix(
  0, nrow = length(samples), ncol = ncol(annotation),
  dimnames = list(samples, colnames(annotation))
)
for (j in seq_along(samples)) {
  active <- counts[, j] > 0
  if (any(active)) {
    for (f in seq_len(ncol(annotation))) {
      reference[j, f] <- round(
        sum(annotation[active, f] > 0) / sum(active), 6
      )
    }
  }
}

for (chunk_rows in c(1L, 2L, 3L, 100L)) {
  result <- aggregate_faprotax_stream(
    otu_file, otus, annotation,
    chunk_rows = chunk_rows, progress_rows = 0L
  )
  stopifnot(
    identical(rownames(result$fractions), samples),
    identical(colnames(result$fractions), colnames(annotation)),
    identical(unname(result$fractions), unname(reference)),
    identical(result$source_otu_rows, length(otus)),
    identical(result$zero_samples, samples[[4L]])
  )
  reads <- colSums(counts)
  annotated_reads <- colSums(counts[c(1L, 2L, 3L), , drop = FALSE])
  expected_coverage <- ifelse(reads > 0, annotated_reads / pmax(reads, 1), 0)
  stopifnot(isTRUE(all.equal(
    unname(result$coverage), unname(expected_coverage)
  )))
  write_faprotax_results(result, tmp)
  check8 <- read.csv(
    file.path(tmp, "8_faprotax_samples_x_functions.csv"),
    row.names = 1, check.names = FALSE
  )
  check9 <- read.csv(file.path(tmp, "9_faprotax_annotated_fraction.csv"))
  stopifnot(
    isTRUE(all.equal(as.matrix(check8), reference, check.attributes = FALSE)),
    identical(check9$Sample, samples),
    isTRUE(all.equal(check9$Annotated_frac, round(expected_coverage, 4)))
  )
}

# Validate the alignment guard: this must fail without publishing new results.
bad <- FALSE
tryCatch(
  aggregate_faprotax_stream(
    otu_file, rev(otus), annotation[rev(otus), , drop = FALSE],
    chunk_rows = 2, progress_rows = 0L
  ),
  error = function(e) {
    bad <<- grepl("OTU ID order differs", conditionMessage(e), fixed = TRUE)
  }
)
stopifnot(bad)

# If the real microeco package is present, compare against its legacy
# cal_spe_func_perc() output, using the SAME FAPROTAX annotations.
if (requireNamespace("microeco", quietly = TRUE)) {
  bacterial_tax <- data.frame(
    Kingdom = rep("Bacteria", 4),
    Phylum = c("Proteobacteria", "Nitrospirota", "Proteobacteria", "Firmicutes"),
    Class = c("Gammaproteobacteria", "Nitrospiria", "Gammaproteobacteria", "Bacilli"),
    Order = c("Nitrosomonadales", "Nitrospirales", "Nitrosomonadales", "Bacillales"),
    Family = c("Nitrosomonadaceae", "Nitrobacteraceae",
               "Nitrosomonadaceae", "Bacillaceae"),
    Genus = c("Nitrosomonas", "Nitrobacter", "Nitrosomonas", "Bacillus"),
    Species = rep(NA_character_, 4), row.names = otus[1:4]
  )
  reference_counts <- as.data.frame(counts[1:4, 1:3, drop = FALSE])
  full <- microeco::microtable$new(
    otu_table = reference_counts, tax_table = bacterial_tax
  )
  full$tidy_dataset()
  ref_tf <- microeco::trans_func$new(dataset = full)
  ref_tf$cal_spe_func(prok_database = "FAPROTAX")
  ref_tf$cal_spe_func_perc()

  ref_annotation <- as.matrix(ref_tf$res_spe_func)
  ref_annotation <- ref_annotation[otus[1:4], , drop = FALSE]
  reference_file <- file.path(tmp, "6_real_microeco_counts.csv")
  write.csv(
    data.frame(OTU_ID = otus[1:4], reference_counts,
               check.names = FALSE),
    reference_file, row.names = FALSE
  )
  result <- aggregate_faprotax_stream(
    reference_file, otus[1:4], ref_annotation,
    chunk_rows = 2L, progress_rows = 0L
  )
  legacy <- as.matrix(ref_tf$res_spe_func_perc)
  legacy <- legacy[samples[1:3], colnames(result$fractions), drop = FALSE]
  stopifnot(
    isTRUE(all.equal(
      unname(result$fractions), unname(legacy), tolerance = 1e-6
    ))
  )
  # The taxonomy-only dummy sample must generate the SAME annotation.
  dummy_map <- annotate_faprotax_once(bacterial_tax)
  stopifnot(identical(
    unname(dummy_map[otus[1:4], , drop = FALSE]),
    unname(ref_annotation[otus[1:4], , drop = FALSE])
  ))
  message("[OK] Full microeco compatibility test passed")
} else {
  message("[WARN] microeco is not installed: skipped optional real-library comparison")
}

message("[OK] Bounded-memory FAPROTAX R unit tests passed")
