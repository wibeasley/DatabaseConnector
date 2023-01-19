# Copied private dbplyr functions ----------------------------------------------
# Copied from 
# - https://github.com/tidyverse/dbplyr/blob/main/R/verb-copy-to.R
# - https://raw.githubusercontent.com/tidyverse/dbplyr/main/R/sql-clause.R
# - https://github.com/tidyverse/dbplyr/blob/main/R/utils-format.R
# - https://github.com/tidyverse/dbplyr/blob/main/R/translate-sql-quantile.R
# - https://github.com/tidyverse/dbplyr/blob/main/R/translate-sql-window.R
# - https://github.com/tidyverse/dbplyr/blob/main/R/utils.R
# - https://github.com/tidyverse/dbplyr/blob/main/R/translate-sql-conditional.R
# Commented-out calls to assert_that to avoid adding another dependency
# Removed all exported functions

sql_values_subquery_column_alias <- function(con, df, types, lvl) {
  df <- values_prepare(con, df)
  if (nrow(df) == 0L) {
    return(sql_values_zero_rows(con, df, types, lvl))
  }
  
  # The `SELECT` clause converts the values to the correct types. This needs
  # to use the translation of `as.<column type>(<column name>)` (e.g. `as.numeric(mpg)`)
  # because some backends need a special translation for some types e.g. casting
  # to logical/bool in MySQL
  #   `IF(<column name>, TRUE, FALSE)`
  # This is done with the help of `sql_cast_dispatch()` via dispatch on the
  # column type. The explicit cast is required so that joins work e.g. on date
  # columns in Postgres.
  # The `FROM` clause is simply the `VALUES` clause with table and column alias
  rows_clauses <- sql_values_clause(con, df, row = FALSE)
  rows_query <- sql_format_clauses(rows_clauses, lvl = lvl + 1, con = con)
  
  table_alias_sql <- sql(paste0("drvd(", escape(ident(colnames(df)), con = con), ")"))
  
  if (grepl("\\n", rows_query)) {
    rows_query <- sql(paste0("(\n", rows_query, "\n", indent_lvl(") AS ", lvl), table_alias_sql))
  } else {
    # indent is not perfect but okay
    rows_query <- sql(paste0("(", rows_query, ") AS ", table_alias_sql))
  }
  
  sql_query_select(
    con,
    select = sql_values_cast_clauses(con, df, types, na = FALSE),
    from = rows_query,
    lvl = lvl
  )
}

sql_values_subquery_union <- function(con, df, types, lvl, row, from = NULL) {
  df <- values_prepare(con, df)
  if (nrow(df) == 0L) {
    return(sql_values_zero_rows(con, df, types, lvl, from))
  }
  
  # The query consists of two parts:
  # 1) An outer select which converts the values to the correct types. This needs
  # to use the translation of `as.<column type>(<column name>)` (e.g. `as.numeric(mpg)`)
  # because some backends need a special translation for some types e.g. casting
  # to logical/bool in MySQL
  #   `IF(<column name>, TRUE, FALSE)`
  # This is done with the help of `sql_cast_dispatch()` via dispatch on the
  # column type. The explicit cast is required so that joins work e.g. on date
  # columns in Postgres.
  # 2) A subquery which is the union of:
  #   a) a zero row table which is just required to name the columns. This is
  #      necessary as e.g. SQLite cannot name `VALUES`.
  #   b) `UNION ALL` of one row `SELECT` statements
  sim_data <- rep_named(colnames(df), list(NULL))
  cols_clause <- escape(sim_data, con = con, parens = FALSE, collapse = NULL)
  
  null_row_query <- select_query(
    from = ident(from),
    select = sql(cols_clause),
    where = sql("0 = 1")
  )
  
  escaped_values <- purrr::map(df, escape, con = con, collapse = NULL, parens = FALSE)
  
  select_kw <- style_kw("SELECT ")
  union_kw <- style_kw("UNION ALL ")
  
  rows <- rlang::exec(paste, !!!escaped_values, sep = ", ")
  if (is_null(from)) {
    values_queries <- paste0(lvl_indent(lvl + 2), select_kw, rows, collapse = paste0(" ", union_kw, "\n"))
  } else {
    from_kw <- style_kw("FROM ")
    values_queries <- paste0(
      lvl_indent(lvl + 2), select_kw, rows, " ", from_kw, from,
      collapse = paste0(" ", union_kw, "\n")
    )
  }
  
  
  union_query <- set_op_query(null_row_query, sql(values_queries), type = "UNION", all = TRUE)
  subquery <- sql_render(union_query, con = con, lvl = lvl + 1)
  
  sql_query_select(
    con,
    select = sql_values_cast_clauses(con, df, types, na = FALSE),
    from = sql_subquery(con, subquery, name = "values_table", lvl = lvl),
    lvl = lvl
  )
}

sql_values_clause <- function(con, df, row = FALSE) {
  escaped_values <- purrr::map(df, escape, con = con, collapse = NULL, parens = FALSE)
  rows <- rlang::exec(paste, !!!escaped_values, sep = ", ")
  rows_sql <- sql(paste0(if (row) "ROW", "(", rows, ")"))
  
  list(sql_clause("VALUES", rows_sql))
}

sql_values_zero_rows <- function(con, df, types, lvl, from = NULL) {
  if (nrow(df) != 0L) {
    # TODO use `cli_abort()` after https://github.com/r-lib/rlang/issues/1386
    # is fixed
    abort("`df` does not have 0 rows", .internal = TRUE)
  }
  
  typed_cols <- sql_values_cast_clauses(con, df, types, na = TRUE)
  
  query <- select_query(
    from = ident(from),
    select = typed_cols,
    where = sql("0 = 1")
  )
  
  sql_render(query, con = con, lvl = lvl)
}

sql_values_cast_clauses <- function(con, df, types, na) {
  if (is_null(types)) {
    typed_cols <- purrr::map2_chr(
      df, colnames(df),
      ~ {
        val <- if (na) NA else ident(.y)
        cast_expr <- call2(sql_cast_dispatch(.x), val)
        translate_sql(!!cast_expr, con = con)
      }
    )
  } else {
    typed_cols <- purrr::imap_chr(
      types,
      ~ {
        val <- if (na) NA else ident(.y)
        sql_expr(cast(!!val %as% !!sql(.x)), con = con)
      }
    )
  }
  
  sql_vector(typed_cols, parens = FALSE, collapse = NULL, con = con)
}

values_prepare <- function(con, df) {
  UseMethod("values_prepare")
}


# This
sql_cast_dispatch <- function(x) {
  UseMethod("sql_cast_dispatch")
}


globalVariables(c("as.integer64"))

sql_select_clauses <- function(con,
                               select,
                               from,
                               where,
                               group_by,
                               having,
                               window,
                               order_by,
                               limit = NULL,
                               lvl) {
  out <- list(
    select = select,
    from = from,
    where = where,
    group_by = group_by,
    having = having,
    window = window,
    order_by = order_by,
    limit = limit
  )
  sql_format_clauses(out, lvl, con)
}

sql_clause <- function(kw, parts, sep = ",", parens = FALSE, lvl = 0) {
  clause <- list(
    kw = kw,
    parts = parts,
    sep = sep,
    parens = parens,
    lvl = lvl
  )
  
  class(clause) <- "sql_clause"
  clause
}

sql_clause_select <- function(con, select, distinct = FALSE, top = NULL, lvl = 0) {
  # assert_that(is.character(select))
  if (is_empty(select)) {
    cli_abort("Query contains no columns")
  }
  
  clause <- build_sql(
    "SELECT",
    if (distinct) sql(" DISTINCT"),
    if (!is.null(top)) build_sql(" TOP ", as.integer(top), con = con),
    con = con
  )
  
  sql_clause(clause, select)
}

sql_clause_from  <- function(from, lvl = 0) {
  sql_clause("FROM", from, lvl = lvl)
}

sql_clause_where <- function(where, lvl = 0) {
  if (length(where) == 0L) {
    return()
  }
  
  # assert_that(is.character(where))
  where_paren <- sql(paste0("(", where, ")"))
  sql_clause("WHERE", where_paren, sep = " AND", lvl = lvl)
}

sql_clause_group_by <- function(group_by, lvl = 0) {
  sql_clause("GROUP BY", group_by)
}

sql_clause_having <- function(having, lvl = 0) {
  if (length(having) == 0L) {
    return()
  }
  
  # assert_that(is.character(having))
  having_paren <- sql(paste0("(", having, ")"))
  sql_clause("HAVING", having_paren, sep = " AND")
}

sql_clause_window <- function(window) {
  sql_clause("WINDOW", window)
}

sql_clause_order_by <- function(order_by, subquery = FALSE, limit = NULL, lvl = 0) {
  if (subquery && length(order_by) > 0 && is.null(limit)) {
    warn_drop_order_by()
    NULL
  } else {
    sql_clause("ORDER BY", order_by)
  }
}

sql_clause_limit <- function(con, limit, lvl = 0){
  if (!is.null(limit) && !identical(limit, Inf)) {
    sql_clause("LIMIT", sql(format(limit, scientific = FALSE)))
  }
}

sql_clause_update <- function(table) {
  sql_clause("UPDATE", table)
}

sql_clause_set <- function(lhs, rhs) {
  update_clauses <- sql(paste0(lhs, " = ", rhs))
  
  sql_clause("SET", update_clauses)
}

sql_clause_insert <- function(con, cols, into = NULL, lvl = 0) {
  if (is.null(into)) {
    sql_clause("INSERT", cols, parens = TRUE, lvl = lvl)
  } else {
    kw <- paste0("INSERT INTO ", escape(into, con = con))
    sql_clause(kw, cols, lvl = lvl)
  }
}

sql_clause_on <- function(on, lvl = 0) {
  sql_clause("ON", on, sep = " AND", lvl = lvl)
}

sql_clause_where_exists <- function(table, where, not) {
  list(
    sql(paste0("WHERE ", if (not) "NOT ", "EXISTS (")),
    # lvl = 1 because they are basically in a subquery
    sql_clause("SELECT 1 FROM", table, lvl = 1),
    sql_clause_where(where, lvl = 1),
    sql(")")
  )
}

# helpers -----------------------------------------------------------------

sql_format_clauses <- function(clauses, lvl, con) {
  clauses <- unname(clauses)
  clauses <- purrr::discard(clauses, ~ !is.sql(.x) && is_empty(.x$parts))
  
  formatted_clauses <- purrr::map(clauses, sql_format_clause, lvl = lvl, con = con)
  clause_level <- purrr::map_dbl(clauses, "lvl", .default = 0)
  out <- indent_lvl(formatted_clauses, lvl + clause_level)
  
  sql_vector(out, collapse = "\n", parens = FALSE, con = con)
}

sql_format_clause <- function(x, lvl, con, nchar_max = 80) {
  if (is.sql(x)) {
    return(x)
  }
  
  lvl <- lvl + x$lvl
  
  # check length without starting a new line
  if (x$sep == " AND") {
    x$sep <- style_kw(x$sep)
  }
  
  fields_same_line <- escape(x$parts, collapse = paste0(x$sep, " "), con = con)
  if (x$parens) {
    fields_same_line <- paste0("(", fields_same_line, ")")
  }
  
  x$kw <- style_kw(x$kw)
  same_line_clause <- paste0(x$kw, " ", fields_same_line)
  nchar_same_line <- cli::ansi_nchar(lvl_indent(lvl)) + cli::ansi_nchar(same_line_clause)
  
  if (length(x$parts) == 1 || nchar_same_line <= nchar_max) {
    return(sql(same_line_clause))
  }
  
  indent <- lvl_indent(lvl + 1)
  collapse <- paste0(x$sep, "\n", indent)
  
  field_string <- paste0(
    x$kw, if (x$parens) " (", "\n",
    indent, escape(x$parts, collapse = collapse, con = con),
    if (x$parens) paste0("\n", indent_lvl(")", lvl))
  )
  
  sql(field_string)
}

lvl_indent <- function(times, char = "  ") {
  strrep(char, times)
}

indent_lvl <- function(x, lvl) {
  sql(paste0(lvl_indent(lvl), x))
}

# nocov start
wrap <- function(..., indent = 0) {
  x <- paste0(..., collapse = "")
  wrapped <- strwrap(
    x,
    indent = indent,
    exdent = indent + 2,
    width = getOption("width")
  )
  
  paste0(wrapped, collapse = "\n")
}
# nocov end

indent <- function(x) {
  x <- paste0(x, collapse = "\n")
  paste0("  ", gsub("\n", "\n  ", x))
}

indent_print <- function(x) {
  indent(utils::capture.output(print(x)))
}

style_kw <- function(x) {
  if (!dbplyr_use_colour()) {
    return(x)
  }
  
  highlight <- dbplyr_highlight()
  if (is_false(highlight)) {
    return(x)
  }
  
  highlight(x)
}

# function for the thousand separator,
# returns "," unless it's used for the decimal point, in which case returns "."
'big_mark' <- function(x, ...) {
  mark <- if (identical(getOption("OutDec"), ",")) "." else ","
  formatC(x, big.mark = mark, ...)
}

dbplyr_use_colour <- function() {
  getOption("dbplyr_use_colour", FALSE)
}

dbplyr_highlight <- function() {
  highlight <- getOption("dbplyr_highlight", cli::combine_ansi_styles("blue"))
  
  if (is_true(highlight)) {
    highlight <- cli::combine_ansi_styles("blue")
  }
  
  if (is_false(highlight)) {
    return(FALSE)
  }
  
  if (!inherits(highlight, "cli_ansi_style")) {
    msg <- "{.envvar dbplyr_highlight} must be `NULL`, `FALSE` or a {.cls cli_ansi_style}."
    cli::cli_abort(msg)
  }
  
  highlight
}
sql_quantile <- function(f,
                         style = c("infix", "ordered"),
                         window = FALSE) {
  force(f)
  style <- match.arg(style)
  force(window)
  
  function(x, probs, na.rm = FALSE) {
    check_probs(probs)
    check_na_rm(na.rm)
    
    sql <- switch(style,
                  infix = sql_call2(f, x, probs),
                  ordered = build_sql(
                    sql_call2(f, probs), " WITHIN GROUP (ORDER BY ", x, ")"
                  )
    )
    
    if (window) {
      sql <- win_over(sql,
                      partition = win_current_group(),
                      frame = win_current_frame()
      )
    }
    sql
  }
}

sql_median <- function(f,
                       style = c("infix", "ordered"),
                       window = FALSE) {
  quantile <- sql_quantile(f, style = style, window = window)
  function(x, na.rm = FALSE) {
    quantile(x, 0.5, na.rm = na.rm)
  }
}

check_probs <- function(probs) {
  if (!is.numeric(probs)) {
    cli_abort("{.arg probs} must be numeric")
  }
  
  if (length(probs) > 1) {
    cli_abort("SQL translation only supports single value for {.arg probs}.")
  }
}


win_register_activate <- function() {
  sql_context$register_windows <- TRUE
}

win_register_deactivate <- function() {
  sql_context$register_windows <- FALSE
}

win_register <- function(over) {
  sql_context$windows <- append(sql_context$windows, over)
}

win_register_names <- function() {
  windows <- sql_context$windows %||% character()
  
  window_count <- vctrs::vec_count(windows, sort = "location")
  window_count <- vctrs::vec_slice(window_count, window_count$count > 1)
  if (nrow(window_count) > 0) {
    window_count$name <- ident(paste0("win", seq_along(window_count$key)))
  } else {
    window_count$name <- ident()
  }
  window_count$key <- window_count$key
  
  sql_context$window_names <- window_count
  window_count
}

win_get <- function(over, con) {
  windows <- sql_context$window_names
  
  if (vctrs::vec_in(over, windows$key)) {
    id <- vctrs::vec_match(over, windows$key)
    ident(windows$name[[id]])
  } else {
    over
  }
}

win_reset <- function() {
  sql_context$window_names <- NULL
  sql_context$windows <- list()
}

rows <- function(from = -Inf, to = 0) {
  if (from >= to) cli_abort("{.arg from} ({from}) must be less than {.arg to} ({to})")
  
  dir <- function(x) if (x < 0) "PRECEDING" else "FOLLOWING"
  val <- function(x) if (is.finite(x)) as.integer(abs(x)) else "UNBOUNDED"
  bound <- function(x) {
    if (x == 0) return("CURRENT ROW")
    paste(val(x), dir(x))
  }
  
  if (to == 0) {
    sql(bound(from))
  } else {
    sql(paste0("BETWEEN ", bound(from), " AND ", bound(to)))
  }
}




# API to set default partitioning etc -------------------------------------

# Use a global variable to communicate state of partitioning between
# tbl and sql translator. This isn't the most amazing design, but it keeps
# things loosely coupled and is easy to understand.
sql_context <- new.env(parent = emptyenv())
sql_context$group_by <- NULL
sql_context$order_by <- NULL
sql_context$con <- NULL
# Used to carry additional information needed for special cases
sql_context$context <- list()

sql_context$register_windows <- FALSE
sql_context$windows <- NULL
sql_context$window_names <- NULL


set_current_con <- function(con) {
  old <- sql_context$con
  sql_context$con <- con
  invisible(old)
}

local_con <- function(con, env = parent.frame()) {
  old <- set_current_con(con)
  withr::defer(set_current_con(old), envir = env)
  invisible()
}

set_win_current_group <- function(vars) {
  stopifnot(is.null(vars) || is.character(vars))
  
  old <- sql_context$group_by
  sql_context$group_by <- vars
  invisible(old)
}

set_win_current_order <- function(vars) {
  stopifnot(is.null(vars) || is.character(vars))
  
  old <- sql_context$order_by
  sql_context$order_by <- vars
  invisible(old)
}

set_win_current_frame <- function(frame) {
  stopifnot(is.null(frame) || is.numeric(frame))
  
  old <- sql_context$frame
  sql_context$frame <- frame
  invisible(old)
}

# Not , because you shouldn't need it
sql_current_con <- function() {
  sql_context$con
}

# Functions to manage information for special cases
set_current_context <- function(context) {
  old <- sql_context$context
  sql_context$context <- context
  invisible(old)
}

sql_current_context <- function() sql_context$context

local_context <- function(x, env = parent.frame()) {
  old <- set_current_context(x)
  withr::defer(set_current_context(old), envir = env)
  invisible()
}

# Where translation -------------------------------------------------------

uses_window_fun <- function(x, con, lq) {
  stopifnot(is.list(x))
  
  calls <- unlist(lapply(x, all_calls))
  win_f <- ls(envir = dbplyr_sql_translation(con)$window)
  any(calls %in% win_f)
}

is_aggregating <- function(x, non_group_cols, agg_f) {
  if (is_quosure(x)) {
    x <- quo_get_expr(x)
  }
  
  if (is_symbol(x)) {
    xc <- as_name(x)
    return(!(xc %in% non_group_cols))
  }
  
  if (is_call(x)) {
    fname <- as.character(x[[1]])
    if (fname %in% agg_f) {
      return(TRUE)
    }
    
    return(all(purrr::map_lgl(x[-1], is_aggregating, non_group_cols, agg_f)))
  }
  
  return(TRUE)
}

common_window_funs <- function() {
  ls(dbplyr_sql_translation(NULL)$window) # nocov
}

#' @noRd
#' @examples
#' translate_window_where(quote(1))
#' translate_window_where(quote(x))
#' translate_window_where(quote(x == 1))
#' translate_window_where(quote(x == 1 && y == 2))
#' translate_window_where(quote(n() > 10))
#' translate_window_where(quote(rank() > cumsum(AB)))
translate_window_where <- function(expr, window_funs = common_window_funs()) {
  switch(typeof(expr),
         logical = ,
         integer = ,
         double = ,
         complex = ,
         character = ,
         string = ,
         symbol = window_where(expr, list()),
         language = {
           if (is_formula(expr)) {
             translate_window_where(f_rhs(expr), window_funs)
           } else if (is_call(expr, name = window_funs)) {
             name <- unique_subquery_name()
             window_where(sym(name), set_names(list(expr), name))
           } else {
             args <- lapply(expr[-1], translate_window_where, window_funs = window_funs)
             expr <- call2(node_car(expr), splice(lapply(args, "[[", "expr")))
             
             window_where(
               expr = expr,
               comp = unlist(lapply(args, "[[", "comp"), recursive = FALSE)
             )
             
           }
         },
         cli_abort("Unknown type: {typeof(expr)}") # nocov
  )
}


#' @noRd
#' @examples
#' translate_window_where_all(list(quote(x == 1), quote(n() > 2)))
#' translate_window_where_all(list(quote(cumsum(x) == 10), quote(n() > 2)))
translate_window_where_all <- function(x, window_funs = common_window_funs()) {
  out <- lapply(x, translate_window_where, window_funs = window_funs)
  
  list(
    expr = unlist(lapply(out, "[[", "expr"), recursive = FALSE),
    comp = unlist(lapply(out, "[[", "comp"), recursive = FALSE)
  )
}

window_where <- function(expr, comp) {
  stopifnot(is.call(expr) || is.name(expr) || is.atomic(expr))
  stopifnot(is.list(comp))
  
  list(
    expr = expr,
    comp = comp
  )
}

deparse_trunc <- function(x, width = getOption("width")) {
  text <- deparse(x, width.cutoff = width)
  if (length(text) == 1 && nchar(text) < width) return(text)
  
  paste0(substr(text[1], 1, width - 3), "...")
}

is.wholenumber <- function(x) {
  trunc(x) == x
}

deparse_all <- function(x) {
  x <- purrr::map_if(x, is_formula, f_rhs)
  purrr::map_chr(x, expr_text, width = 500L)
}

commas <- function(...) paste0(..., collapse = ", ")

in_travis <- function() identical(Sys.getenv("TRAVIS"), "true")

unique_table_name <- function() {
  # Needs to use option to unique names across reloads while testing
  i <- getOption("dbplyr_table_name", 0) + 1
  options(dbplyr_table_name = i)
  sprintf("dbplyr_%03i", i)
}
unique_subquery_name <- function() {
  # Needs to use option so can reset at the start of each query
  i <- getOption("dbplyr_subquery_name", 0) + 1
  options(dbplyr_subquery_name = i)
  sprintf("q%02i", i)
}
unique_subquery_name_reset <- function() {
  options(dbplyr_subquery_name = 0)
}

succeeds <- function(x, quiet = FALSE) {
  tryCatch(
    {
      x
      TRUE
    },
    error = function(e) {
      if (!quiet)
        message("Error: ", e$message) # nocov
      FALSE
    }
  )
}

c_character <- function(...) {
  x <- c(...)
  if (length(x) == 0) {
    return(character())
  }
  
  if (!is.character(x)) {
    cli_abort("Character input expected")
  }
  
  x
}

cat_line <- function(...) cat(paste0(..., "\n"), sep = "")

# nocov start
res_warn_incomplete <- function(res, hint = "n = -1") {
  if (dbHasCompleted(res)) return()
  
  rows <- big_mark(dbGetRowCount(res))
  cli::cli_warn("Only first {rows} results retrieved. Use {hint} to retrieve all.")
}

hash_temp <- function(name) {
  name <- ident(paste0("#", name))
  cli::cli_inform(
    paste0("Created a temporary table named ", name),
    class = c("dbplyr_message_temp_table", "dbplyr_message")
  )
  name
}
# nocov end

# Helper for testing
local_methods <- function(..., .frame = caller_env()) {
  local_bindings(..., .env = global_env(), .frame = .frame)
}

assert_flag <- function(x, arg, call = caller_env()) {
  vctrs::vec_assert(x, logical(), size = 1L)
  if (is.na(x)) {
    cli_abort("{.arg {arg}} must not be NA.", call = call)
  }
}

check_not_supplied <- function(arg, call = caller_env()) {
  if (!is_null(arg)) {
    arg <- caller_arg(arg)
    cli_abort("{.arg {arg}} is not supported in SQL translations.", call = call)
  }
}

sql_case_match <- function(.x, ..., .default = NULL, .ptype = NULL) {
  error_call <- current_call()
  check_not_supplied(.ptype, call = error_call)
  
  x_expr <- enexpr(.x)
  if (!is_symbol(x_expr) && !is_call(x_expr)) {
    msg <- "{.arg .x} must be a variable or function call, not {.obj_type_friendly {.x}}."
    cli_abort(msg, call = error_call)
  }
  
  formulas <- list2(...)
  formulas <- purrr::compact(formulas)
  
  n <- length(formulas)
  
  if (n == 0) {
    cli_abort("No cases provided", call = error_call)
  }
  
  con <- sql_current_con()
  query <- vector("list", n)
  value <- vector("list", n)
  
  for (i in seq_len(n)) {
    f <- formulas[[i]]
    env <- environment(f)
    
    query[[i]] <- sql_case_match_clause(f, .x, con)
    value[[i]] <- escape(enpar(quo(!!f[[3]]), tidy = FALSE, env = env), con = con)
  }
  
  clauses <- purrr::map2_chr(query, value, ~ paste0("WHEN (", .x, ") THEN ", .y))
  if (!is_null(.default)) {
    .default <- escape(enpar(quo(.default), tidy = FALSE, env = env), con = sql_current_con())
    clauses[[n + 1]] <- paste0("ELSE ", .default)
  }
  
  same_line_sql <- sql(paste0("CASE ", paste0(clauses, collapse = " "), " END"))
  if (nchar(same_line_sql) <= 80) {
    return(same_line_sql)
  }
  
  sql(paste0(
    "CASE\n",
    paste0(clauses, collapse = "\n"),
    "\nEND"
  ))
}

sql_case_match_clause <- function(f, x, con) {
  env <- environment(f)
  f_query <- f[[2]]
  if (is_call(f_query, "c")) {
    # need to handle `c(...)` specially because on `expr(c(1, y))` it returns
    # `sql('1', '`y`')`
    f_query <- translate_sql_(call_args(f_query), con)
  } else if (is_call(f_query) || is_symbol(f_query)) {
    f_query <- translate_sql(!!f_query, con = con)
  }
  
  # NA need to be translated to `IS NULL` instead of `IN (NULL)`
  # due to the preceeding translation it might be NA or the string NULL
  missing_loc <- is.na(f_query) | f_query == "NULL"
  f_query <- vctrs::vec_slice(f_query, !missing_loc)
  has_na <- any(missing_loc)
  
  query <- NULL
  if (!is_empty(f_query)) {
    values <- sql_vector(f_query, parens = TRUE, collapse = ", ", con = con)
    query <- translate_sql(!!x %in% !!values)
  }
  
  if (has_na) {
    query <- paste(c(query, build_sql(x, " IS NULL")), collapse = " OR ")
  }
  
  query
}


sql_if <- function(cond, if_true, if_false = quo(NULL), missing = quo(NULL)) {
  out <- build_sql("CASE WHEN ", enpar(cond), " THEN ", enpar(if_true))
  
  # `ifelse()` and `if_else()` have a three value logic: they return `NA` resp.
  # `missing` if `cond` is `NA`. To get the same in SQL it is necessary to
  # translate to
  # CASE
  #   WHEN <cond> THEN `if_true`
  #   WHEN NOT <cond> THEN `if_false`
  #   WHEN <cond> IS NULL THEN `missing`
  # END
  #
  # Together these cases cover every possible case. So, if `if_false` and
  # `missing` are identical they can be simplified to `ELSE <if_false>`
  if (!quo_is_null(if_false) && identical(if_false, missing)) {
    out <- paste0(out, " ELSE ", enpar(if_false), " END")
    return(sql(out))
  }
  
  if (!quo_is_null(if_false)) {
    false_sql <- build_sql(" WHEN NOT ", enpar(cond), " THEN ", enpar(if_false))
    out <- paste0(out, false_sql)
  }
  
  if (!quo_is_null(missing)) {
    missing_cond <- translate_sql(is.na(!!cond), con = sql_current_con())
    missing_sql <- build_sql(" WHEN ", missing_cond, " THEN ", enpar(missing))
    out <- paste0(out, missing_sql)
  }
  
  sql(paste0(out, " END"))
}

sql_case_when <- function(...,
                          .default = NULL,
                          .ptype = NULL,
                          .size = NULL,
                          error_call = caller_env()) {
  # TODO: switch to dplyr::case_when_prepare when available
  check_not_supplied(.ptype, call = error_call)
  check_not_supplied(.size, call = error_call)
  
  formulas <- list2(...)
  n <- length(formulas)
  
  if (n == 0) {
    cli_abort("No cases provided")
  }
  
  query <- vector("list", n)
  value <- vector("list", n)
  
  for (i in seq_len(n)) {
    f <- formulas[[i]]
    
    env <- environment(f)
    query[[i]] <- escape(enpar(quo(!!f[[2]]), tidy = FALSE, env = env), con = sql_current_con())
    value[[i]] <- escape(enpar(quo(!!f[[3]]), tidy = FALSE, env = env), con = sql_current_con())
  }
  
  clauses <- purrr::map2_chr(query, value, ~ paste0("WHEN ", .x, " THEN ", .y))
  # if a formula like TRUE ~ "other" is at the end of a sequence, use ELSE statement
  # TRUE has precedence over `.default`
  if (is_true(formulas[[n]][[2]])) {
    clauses[[n]] <- paste0("ELSE ", value[[n]])
  } else if (!is_null(.default)) {
    .default <- escape(enpar(quo(.default), tidy = FALSE, env = env), con = sql_current_con())
    clauses[[n + 1]] <- paste0("ELSE ", .default)
  }
  
  same_line_sql <- sql(paste0("CASE ", paste0(clauses, collapse = " "), " END"))
  if (nchar(same_line_sql) <= 80) {
    return(same_line_sql)
  }
  
  sql(paste0(
    "CASE\n",
    paste0(clauses, collapse = "\n"),
    "\nEND"
  ))
}

sql_switch <- function(x, ...) {
  input <- list2(...)
  
  named <- names2(input) != ""
  
  clauses <- purrr::map2_chr(names(input)[named], input[named], function(x, y) {
    build_sql("WHEN (", x , ") THEN (", y, ") ")
  })
  
  n_unnamed <- sum(!named)
  if (n_unnamed == 0) {
    # do nothing
  } else if (n_unnamed == 1) {
    clauses <- c(clauses, build_sql("ELSE ", input[!named], " "))
  } else {
    cli_abort("Can only have one unnamed (ELSE) input")
  }
  
  build_sql("CASE ", x, " ", !!!clauses, "END")
}

sql_is_null <- function(x) {
  x_sql <- enpar(enquo(x))
  sql_expr((!!x_sql %is% NULL))
}

enpar <- function(x, tidy = TRUE, env = NULL) {
  if (!is_quosure(x)) {
    cli_abort("Internal error: `x` must be a quosure.") # nocov
  }
  
  if (tidy) {
    x_sql <- eval_tidy(x, env = env)
  } else {
    x_sql <- eval_bare(x, env = env)
  }
  if (quo_is_call(x)) {
    build_sql("(", x_sql, ")")
  } else {
    x_sql
  }
}