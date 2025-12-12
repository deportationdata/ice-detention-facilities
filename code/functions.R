# cleaning helper functions
clean_text <- function(str) {
  str |>
    str_trim() |>
    str_to_lower() |>
    str_replace_all(
      "(?<=\\b)([A-Za-z]\\.)+(?=\\b)",
      ~ str_remove_all(.x, "\\.")
    ) |>
    str_replace_all("[[:punct:]]", " ") |>
    str_squish()
}

clean_facility_name <- function(str) {
  str |>
    str_trim() |>
    str_to_lower() |>
    # remove all content in parentheses or brackets
    str_replace_all("\\s*\\([^\\)]*\\)\\s*", " ") |>
    str_replace_all(c(
      "\\bcnt\\b" = "center",
      "\\bctr\\b" = "center",
      "\\bcor\\b" = "correctional",
      "\\bcorr\\b" = "correctional",
      "\\bcorrec\\b" = "correctional",
      "\\bcty\\b" = "county",
      "\\bco\\b" = "county",
      "\\bcplx\\b" = "complex",
      "\\bcdf\\b" = "contract detention facility",
      "\\bdept\\b" = "department",
      "\\bdept of corr\\b" = "department of corrections",
      "\\bdet\\b" = "detention",
      "\\bdf\\b" = "detention facility",
      "\\bfed\\b" = "federal",
      "\\bmet\\b" = "metropolitan",
      "\\bfac\\b" = "facility",
      "\\bfam\\b" = "family",
      "\\bfclty\\b" = "facility",
      "\\bfacilty\\b" = "facility",
      "\\bfo\\b" = "field office",
      "\\bhosp\\b" = "hospital",
      "\\bhsp\\b" = "hospital",
      "\\bhlth\\b" = "health",
      "\\bjuv\\b" = "juvenile",
      "\\bpen\\b" = "penitentiary",
      "\\bpub\\b" = "public",
      "\\bproc\\b" = "processing",
      "\\breg\\b" = "regional",
      "\\brm\\b" = "room",
      "\\brms\\b" = "rooms",
      "\\bsfty\\b" = "safety",
      "\\bspc\\b" = "service processing center",
      "\\busp\\b" = "us penitentiary"
    )) |>
    str_squish()
}

clean_city <- function(str) {
  str |>
    str_to_lower() |>
    str_replace_all(c(
      "\\bmt\\b" = "mount",
      "\\bft\\b" = "fort",
      "\\bste\\b" = "saint",
      "\\bst\\b" = "saint",
      "\\bspgs\\b" = "springs",
      "\\bspg\\b" = "springs",
      "\\bn\\b" = "north"
    ))
}

clean_street_address <- function(str) {
  str |>
    str_trim() |>
    str_to_lower() |>
    str_replace_all(c(
      "\\bstreet\\b" = "st",
      "\\bavenue\\b" = "ave",
      "\\bboulevard\\b" = "blvd",
      "\\bdrive\\b" = "dr",
      "\\broad\\b" = "rd",
      "\\bhighway\\b" = "hwy",
      "\\bplace\\b" = "pl",
      "\\blane\\b" = "ln",
      "\\bsquare\\b" = "sq",
      "\\bterrace\\b" = "ter",
      "\\bparkway\\b" = "pkwy",
      "\\bcourt\\b" = "ct",
      "\\bnorth\\b" = "n",
      "\\bsouth\\b" = "s",
      "\\beast\\b" = "e",
      "\\bwest\\b" = "w",
      "\\bfort\\b" = "ft"
    ))
}

link_facilities2 <- function(
  df_a,
  df_b,
  name_a = "name", # facility name in df_a
  name_b = "name", # facility name in df_b
  block_vars = c("state"), # columns present in both for blocking
  top_n = 1, # top matches per record in df_a
  min_score = 0.5, # drop matches below this composite score
  extra_stopwords = character()
) {
  stopifnot(all(block_vars %in% names(df_a)), all(block_vars %in% names(df_b)))

  # ---- packages ----
  library(dplyr)
  library(stringr)
  library(purrr)
  library(stringdist)

  # ---- helpers ----
  stop_terms <- c(
    # "county",
    # "cnty",
    # "co",
    # "jail",
    # "prison",
    # "detention",
    # "detentioncenter",
    # "detentionctr",
    # "facility",
    # "center",
    # "centre",
    # "correctional",
    # "corrections",
    # "sheriff",
    # "sheriffs",
    # "office",
    # "dept",
    # "department",
    # "ice",
    # "dhs",
    # "geo",
    # "corecivic",
    extra_stopwords
  ) |>
    unique()

  normalize_string <- function(x) {
    x |>
      str_to_lower() |>
      str_replace_all("[^a-z0-9 ]", " ") |>
      str_squish()
  }

  tokenize_vec <- function(x) {
    x_norm <- normalize_string(x)
    # split on whitespace
    str_split(x_norm, "\\s+") |>
      map(\(tokens) {
        tokens <- tokens[!(tokens %in% stop_terms) & tokens != ""]
        tokens
      })
  }

  # ---- add IDs + normalized columns ----
  df_a2 <-
    df_a |>
    mutate(
      .id_a = row_number(),
      .name_a = .data[[name_a]],
      .name_norm_a = normalize_string(.name_a),
      .tokens_a = tokenize_vec(.name_a)
    )

  df_b2 <-
    df_b |>
    mutate(
      .id_b = row_number(),
      .name_b = .data[[name_b]],
      .name_norm_b = normalize_string(.name_b),
      .tokens_b = tokenize_vec(.name_b)
    )

  # ---- blocking ----
  # inner_join on block_vars to create candidate pairs
  by_block <- setNames(block_vars, block_vars)

  candidates <-
    df_a2 |>
    inner_join(df_b2, by = by_block, suffix = c(".a", ".b"))

  if (nrow(candidates) == 0L) {
    message("No candidate pairs after blocking.")
    return(candidates)
  }

  # ---- similarity metrics ----
  candidates_scores <-
    candidates |>
    mutate(
      shared_tokens = map2(.tokens_a, .tokens_b, intersect),
      n_shared = map_int(shared_tokens, length),
      n_a = map_int(.tokens_a, length),
      n_b = map_int(.tokens_b, length),
      union_size = n_a + n_b - n_shared,
      jaccard = if_else(union_size > 0, n_shared / union_size, 0),
      overlap = if_else(pmin(n_a, n_b) > 0, n_shared / pmin(n_a, n_b), 0),
      # cosine/qgram-like distance on normalized strings (lower is better)
      dist_cosine = stringdist(.name_norm_a, .name_norm_b, method = "cosine"),
      sim_cosine = 1 - dist_cosine,
      # composite score (tune weights as you like)
      match_score = 0.6 * overlap + 0.3 * jaccard + 0.2 * sim_cosine
    )

  # ---- filter & pick top matches per df_a row ----
  result <-
    candidates_scores |>
    filter(match_score >= min_score) |>
    group_by(.id_a) |>
    slice_max(order_by = match_score, n = top_n, with_ties = FALSE) |>
    ungroup()

  # Return with original columns kept, plus scores
  result
}

# -----------------------Helpers
# -----------------------
abbr_dict <- c(
  "-" = " ",
  "&" = " AND ",

  "\\bST\\.?\\b" = " ST ",
  "\\bSAINT\\b" = " ST ",
  "\\bCTR\\b" = " CENTER ",
  "\\bCTRS\\b" = " CENTERS ",
  "\\bMED\\b" = " MEDICAL ",
  "\\bMED\\.\\b" = " MEDICAL ",
  "\\bMED CTR\\b" = " MEDICAL CENTER ",
  "\\bHOSP\\b" = " HOSPITAL ",
  "\\bMC\\b" = " MEDICAL CENTER ",
  "\\bUNIV\\b" = " UNIVERSITY ",
  "\\bUNIVERSITY HOSP\\b" = " UNIVERSITY HOSPITAL "
)

normalize_name <- function(x) {
  x |>
    str_to_upper() |>
    str_replace_all("[[:punct:]]", " ") |>
    str_replace_all(abbr_dict) |>
    str_squish()
}

generic_facility_words <- c(
  "HOSPITAL",
  "HOSPITALS",
  "HOSP",
  "MEDICAL",
  "CENTER",
  "CENTERS",
  "HEALTH",
  "REGIONAL",
  "MEMORIAL",
  "SYSTEM",
  "CLINIC"
)

tokenize_all <- function(x) {
  if (is.na(x) || x == "") {
    return(character())
  }
  x |>
    str_split("\\s+") |>
    pluck(1) |>
    unique()
}

tokenize_informative <- function(x) {
  toks <- tokenize_all(x)
  setdiff(toks, generic_facility_words)
}

jaccard_tokens <- function(a_tokens, b_tokens) {
  inter <- length(intersect(a_tokens, b_tokens))
  union <- length(union(a_tokens, b_tokens))
  if (union == 0) 0 else inter / union
}

overlap_tokens <- function(a_tokens, b_tokens) {
  inter <- length(intersect(a_tokens, b_tokens))
  denom <- min(length(a_tokens), length(b_tokens))
  if (denom == 0) 0 else inter / denom
}

fuzzy_overlap_tokens <- function(a_tokens, b_tokens, threshold = 0.9) {
  if (length(a_tokens) == 0 || length(b_tokens) == 0) {
    return(list(
      fuzzy_intersection = 0L,
      fuzzy_jaccard = 0,
      fuzzy_overlap = 0
    ))
  }

  sim_mat <- map(a_tokens, \(t) {
    1 - stringdist(t, b_tokens, method = "jw")
  }) |>
    do.call(what = rbind)

  best_per_a <- apply(sim_mat, 1, max)
  matched_a <- best_per_a >= threshold

  fuzzy_intersection <- sum(matched_a)
  union_size <- length(union(a_tokens, b_tokens))
  min_size <- min(length(a_tokens), length(b_tokens))

  fuzzy_jaccard <- if (union_size == 0) 0 else fuzzy_intersection / union_size
  fuzzy_overlap <- if (min_size == 0) 0 else fuzzy_intersection / min_size

  list(
    fuzzy_intersection = fuzzy_intersection,
    fuzzy_jaccard = fuzzy_jaccard,
    fuzzy_overlap = fuzzy_overlap
  )
}

# -----------------------
# Main linker
# -----------------------

link_facilities2 <- function(
  df_a,
  df_b,
  name_a = "name",
  name_b = "name",
  block_vars = c("state"),
  top_n = 1,
  min_score = 0.7,
  fuzzy_threshold = 0.9,
  greedy = TRUE # <-- new
) {
  # give each df a row id
  df_a2 <- df_a |>
    dplyr::mutate(.id_a = dplyr::row_number())

  df_b2 <- df_b |>
    dplyr::mutate(.id_b = dplyr::row_number())

  name_a_sym <- rlang::sym(name_a)
  name_b_sym <- rlang::sym(name_b)

  # candidate pairs via blocking on block_vars
  candidates <- df_a2 |>
    dplyr::select(.id_a, dplyr::all_of(block_vars), name.a = !!name_a_sym) |>
    dplyr::inner_join(
      df_b2 |>
        dplyr::select(
          .id_b,
          dplyr::all_of(block_vars),
          name.b = !!name_b_sym,
          dplyr::everything()
        ),
      by = block_vars
    )

  # score similarity for each candidate pair
  scored <- candidates |>
    dplyr::mutate(
      norm_a = normalize_name(name.a),
      norm_b = normalize_name(name.b)
    ) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      tokens_all_a = list(tokenize_all(norm_a)),
      tokens_all_b = list(tokenize_all(norm_b)),
      tokens_inf_a = list(tokenize_informative(norm_a)),
      tokens_inf_b = list(tokenize_informative(norm_b)),

      jaccard_all = jaccard_tokens(tokens_all_a, tokens_all_b),
      overlap_all = overlap_tokens(tokens_all_a, tokens_all_b),

      fuzzy_all = list(
        fuzzy_overlap_tokens(
          tokens_all_a,
          tokens_all_b,
          threshold = fuzzy_threshold
        )
      ),
      fuzzy_intersection_all = fuzzy_all$fuzzy_intersection,
      fuzzy_jaccard_all = fuzzy_all$fuzzy_jaccard,
      fuzzy_overlap_all = fuzzy_all$fuzzy_overlap,

      jaccard_inf = jaccard_tokens(tokens_inf_a, tokens_inf_b),
      overlap_inf = overlap_tokens(tokens_inf_a, tokens_inf_b),

      fuzzy_inf = list(
        fuzzy_overlap_tokens(
          tokens_inf_a,
          tokens_inf_b,
          threshold = fuzzy_threshold
        )
      ),
      fuzzy_intersection_inf = fuzzy_inf$fuzzy_intersection,
      fuzzy_jaccard_inf = fuzzy_inf$fuzzy_jaccard,
      fuzzy_overlap_inf = fuzzy_inf$fuzzy_overlap,

      jw_full = 1 - stringdist::stringdist(norm_a, norm_b, method = "jw"),
      lv_full = 1 - stringdist::stringdist(norm_a, norm_b, method = "lv"),

      match_score = 0.3 *
        jw_full +
        0.3 * fuzzy_overlap_all +
        0.2 * jaccard_all +
        0.2 * fuzzy_overlap_inf
    ) |>
    dplyr::ungroup() |>
    dplyr::select(
      -tokens_all_a,
      -tokens_all_b,
      -tokens_inf_a,
      -tokens_inf_b,
      -fuzzy_all,
      -fuzzy_inf
    )

  # -----------------------
  # Choose matches
  # -----------------------
  scored_filtered <- scored |>
    dplyr::filter(match_score >= min_score)

  if (greedy) {
    # Greedy global 1–1 matching: sort by score, then take first use of each .id_a and .id_b
    matches <- scored_filtered |>
      dplyr::arrange(dplyr::desc(match_score)) |>
      dplyr::mutate(
        used_a = !duplicated(.id_a),
        used_b = !duplicated(.id_b)
      ) |>
      dplyr::filter(used_a & used_b) |>
      dplyr::select(-used_a, -used_b) |>
      dplyr::arrange(.id_a, dplyr::desc(match_score))
  } else {
    # Original behavior: top_n per .id_a (ignores conflicts on .id_b)
    matches <- scored_filtered |>
      dplyr::group_by(.id_a) |>
      dplyr::slice_max(order_by = match_score, n = top_n, with_ties = FALSE) |>
      dplyr::ungroup() |>
      dplyr::arrange(.id_a, dplyr::desc(match_score))
  }

  matches
}
