#!/usr/bin/env Rscript
# Run with: Rscript tests/test_faprotax_r_streaming.R
source("tools/faprotax_16S_functional_profile.R", local = TRUE)

if (!requireNamespace("Matrix", quietly = TRUE)) stop("Matrix package required")

workdir <- tempfile("faprotax-r-test-")
dir.create(workdir)
on.exit(unlink(workdir, recursive = TRUE), add = TRUE)

otu_file <- file.path(workdir, "6_otu_clean_counts_no_blanks.csv")
sample_ids <- c("ABCD-1234", "EFGH-5678", "IJKL-9012")
otu_ids <- c("otu,one", 'otu"two', "otu3", "otu4", "otu5")
input <- data.frame(
  otu_id = otu_ids,
  "ABCD-1234" = c(5, 5, 0, 0, 7),
  "EFGH-5678" = c(0, 2, 3, 0, 1),
  "IJKL-9012" = c(0, 0, 0, 0, 0),
  check.names = FALSE
)
utils::write.csv(input, otu_file, row.names = FALSE, quote = TRUE)

# OTU 1 supports A, OTU 2 supports A+B, OTU 3 supports B,
# OTUs 4/5 have no assignment, and function C is never present.
function_map <- Matrix::Matrix(
  matrix(
    c(1, 0, 0,
      1, 1, 0,
      0, 1, 0,
      0, 0, 0,
      0, 0, 0),
    nrow = 5, ncol = 3, byrow = TRUE,
    dimnames = list(otu_ids, c("func_A", "func_B", "func_C"))
  ),
  sparse = TRUE
)

test_close <- function(actual, expected) {
  stopifnot(
    isTRUE(all.equal(
      unname(as.matrix(actual)), unname(as.matrix(expected)),
      tolerance = 1e-6, check.attributes = FALSE
    ))
  )
}

unweighted <- suppressWarnings(stream_faprotax_counts(
  otu_file, otu_ids, function_map, chunk_rows = 2L,
  abundance_weighted = FALSE
))
stopifnot(
  identical(rownames(unweighted$functions), sample_ids),
  identical(colnames(unweighted$functions), c("func_A", "func_B")),
  unweighted$otu_count == 5L
)
test_close(unweighted$functions, matrix(c(
  round(2 / 3, 6), round(1 / 3, 6),
  round(1 / 3, 6), round(2 / 3, 6),
  0, 0
), nrow = 3L, ncol = 2L, byrow = TRUE))
test_close(unweighted$annotated_fraction, c(10 / 17, 5 / 6, 0))

weighted <- suppressWarnings(stream_faprotax_counts(
  otu_file, otu_ids, function_map, chunk_rows = 3L,
  abundance_weighted = TRUE
))
test_close(weighted$functions, matrix(c(
  round(10 / 17, 6), round(5 / 17, 6),
  round(2 / 6, 6), round(5 / 6, 6),
  0, 0
), nrow = 3L, ncol = 2L, byrow = TRUE))
test_close(weighted$annotated_fraction, c(10 / 17, 5 / 6, 0))

for (bad_ids in list(
  c(otu_ids[-1], "WRONG"),
  otu_ids[-1]
)) {
  try_result <- try(
    stream_faprotax_counts(otu_file, bad_ids, function_map, chunk_rows = 2L),
    silent = TRUE
  )
  stopifnot(inherits(try_result, "try-error"))
}

# Optional method parity check: when microeco is installed, compare the new
# chunked unweighted/weighted fractions to microeco itself on the SAME
# numeric OTU matrix and the SAME binary taxonomy->function mapping.
# Use two nonzero samples because microeco tidy_dataset() drops zero-only ones.
if (requireNamespace("microeco", quietly = TRUE)) {
  small_counts <- as.matrix(input[c("ABCD-1234", "EFGH-5678")])
  rownames(small_counts) <- otu_ids
  tax2 <- data.frame(
    Kingdom = rep("Bacteria", length(otu_ids)),
    Phylum = rep("Proteobacteria", length(otu_ids)),
    Class = rep("Alphaproteobacteria", length(otu_ids)),
    Order = rep("Rhizobiales", length(otu_ids)),
    Family = rep("Rhizobiaceae", length(otu_ids)),
    Genus = rep("Rhizobium", length(otu_ids)),
    Species = rep(NA_character_, length(otu_ids)),
    row.names = otu_ids
  )
  dataset <- microeco::microtable$new(
    otu_table = as.data.frame(small_counts),
    tax_table = tax2
  )
  dataset$tidy_dataset()
  tf <- microeco::trans_func$new(dataset = dataset)
  # Directly assign a known binary mapping to exercise the exact version's
  # microeco functional-fraction implementation, independent of FAPROTAX DB.
  tf$res_func <- as.data.frame(as.matrix(function_map))
  for (is_weighted in c(FALSE, TRUE)) {
    if (is.function(tf$cal_func_FR)) {
      tf$cal_func_FR(
        abundance_weighted = is_weighted, perc = FALSE,
        dec = 6L, remove_zero = TRUE
      )
      reference <- tf$res_func_FR
    } else {
      tf$cal_spe_func_perc(abundance_weighted = is_weighted)
      reference <- tf$res_spe_func_perc
    }
    observed <- if (is_weighted) weighted$functions else unweighted$functions
    test_close(
      observed[rownames(reference), colnames(reference), drop = FALSE],
      reference
    )
  }
  cat("[OK] microeco functional-fraction parity tests\n")
} else {
  cat("[SKIP] microeco package is not installed; math/unit tests passed\n")
}

cat("[OK] Bounded-memory FAPROTAX streaming regression tests passed\n")
