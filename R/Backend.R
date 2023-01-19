# Modified from https://github.com/tidyverse/dbplyr/blob/main/R/backend-mssql.R, then made 
# these modifications:
# - Removed top docmentation, including simulation function
# - Find 'Microsoft SQL Server' and replace with 'DatabaseConnectorConnection'
# - Remove single quotes around function names
# - Copy required dbplyr private function at the bottom of this document
# - Commented-out calls to assert_that to avoid adding another dependency

#' @include DbplyrPrivateFunctions.R
NULL


#' @importFrom dbplyr dbplyr_edition
#' @export
dbplyr_edition.DatabaseConnectorConnection <- function(con) {
  2L
}

#' @importFrom dbplyr sql_query_select
#' @export
sql_query_select.DatabaseConnectorConnection <- function(con, select, from, where = NULL,
                                                    group_by = NULL, having = NULL,
                                                    window = NULL,
                                                    order_by = NULL,
                                                    limit = NULL,
                                                    distinct = FALSE,
                                                    ...,
                                                    subquery = FALSE,
                                                    lvl = 0) {
  sql_select_clauses(con,
                     select    = sql_clause_select(con, select, distinct, top = limit),
                     from      = sql_clause_from(from),
                     where     = sql_clause_where(where),
                     group_by  = sql_clause_group_by(group_by),
                     having    = sql_clause_having(having),
                     window    = sql_clause_window(window),
                     order_by  = sql_clause_order_by(order_by, subquery, limit),
                     lvl       = lvl
  )
}

#' @importFrom dbplyr sql_query_insert
#' @export
sql_query_insert.DatabaseConnectorConnection <- function(con, x_name, y, by, ...,
                                                    conflict = c("error", "ignore"),
                                                    returning_cols = NULL,
                                                    method = NULL) {
  method <- method %||% "where_not_exists"
  arg_match(method, "where_not_exists", error_arg = "method")
  # https://stackoverflow.com/questions/25969/insert-into-values-select-from
  conflict <- rows_check_conflict(conflict)
  
  parts <- rows_insert_prep(con, x_name, y, by, lvl = 0)
  
  clauses <- list2(
    parts$insert_clause,
    sql_returning_cols(con, returning_cols, "INSERTED"),
    sql_clause_select(con, sql("*")),
    sql_clause_from(parts$from),
    !!!parts$conflict_clauses
  )
  
  sql_format_clauses(clauses, lvl = 0, con)
}

#' @importFrom dbplyr sql_query_append
#' @export
sql_query_append.DatabaseConnectorConnection <- function(con, x_name, y, ...,
                                                    returning_cols = NULL) {
  parts <- rows_prep(con, x_name, y, by = list(), lvl = 0)
  insert_cols <- escape(ident(colnames(y)), collapse = ", ", parens = TRUE, con = con)
  
  clauses <- list2(
    sql_clause_insert(con, insert_cols, x_name),
    sql_returning_cols(con, returning_cols, "INSERTED"),
    sql_clause_select(con, sql("*")),
    sql_clause_from(parts$from)
  )
  
  sql_format_clauses(clauses, lvl = 0, con)
}

#' @importFrom dbplyr sql_query_update_from
#' @export
sql_query_update_from.DatabaseConnectorConnection <- function(con, x_name, y, by,
                                                         update_values, ...,
                                                         returning_cols = NULL) {
  # https://stackoverflow.com/a/2334741/946850
  parts <- rows_prep(con, x_name, y, by, lvl = 0)
  update_cols <- sql_escape_ident(con, names(update_values))
  
  clauses <- list(
    sql_clause_update(x_name),
    sql_clause_set(update_cols, update_values),
    sql_returning_cols(con, returning_cols, "INSERTED"),
    sql_clause_from(x_name),
    sql_clause("INNER JOIN", parts$from),
    sql_clause_on(parts$where, lvl = 1)
  )
  sql_format_clauses(clauses, lvl = 0, con)
}

#' @importFrom dbplyr sql_query_upsert
#' @export
sql_query_upsert.DatabaseConnectorConnection <- function(con,
                                                    x_name,
                                                    y,
                                                    by,
                                                    update_cols,
                                                    ...,
                                                    returning_cols = NULL,
                                                    method = NULL) {
  method <- method %||% "merge"
  arg_match(method, "merge", error_arg = "method")
  
  parts <- rows_prep(con, x_name, y, by, lvl = 0)
  
  update_cols_esc <- sql(sql_escape_ident(con, update_cols))
  update_values <- sql_table_prefix(con, update_cols, ident("...y"))
  update_clause <- sql(paste0(update_cols_esc, " = ", update_values))
  
  insert_cols <- c(by, update_cols)
  insert_cols_esc <- sql(sql_escape_ident(con, insert_cols))
  insert_cols_qual <- sql_table_prefix(con, insert_cols, ident("...y"))
  
  clauses <- list(
    sql_clause("MERGE INTO", x_name),
    sql_clause("USING", parts$from),
    sql_clause_on(parts$where, lvl = 1),
    sql("WHEN MATCHED THEN"),
    sql_clause("UPDATE SET", update_clause, lvl = 1),
    sql("WHEN NOT MATCHED THEN"),
    sql_clause_insert(con, insert_cols_esc, lvl = 1),
    sql_clause("VALUES", insert_cols_qual, parens = TRUE, lvl = 1),
    sql_returning_cols(con, returning_cols, "INSERTED"),
    sql(";")
  )
  sql_format_clauses(clauses, lvl = 0, con)
}

#' @importFrom dbplyr sql_query_delete
#' @export
sql_query_delete.DatabaseConnectorConnection <- function(con, x_name, y, by, ..., returning_cols = NULL) {
  parts <- rows_prep(con, x_name, y, by, lvl = 0)
  
  clauses <- list2(
    sql_clause("DELETE FROM", x_name),
    sql_returning_cols(con, returning_cols, table = "DELETED"),
    !!!sql_clause_where_exists(parts$from, parts$where, not = FALSE)
  )
  sql_format_clauses(clauses, lvl = 0, con)
}

#' @importFrom dbplyr sql_translation
#' @export
sql_translation.DatabaseConnectorConnection <- function(con) {
  mssql_scalar <-
    sql_translator(.parent = base_odbc_scalar,
                   
                   `!`           = function(x) {
                     if (mssql_needs_bit()) {
                       x <- with_mssql_bool(x)
                       mssql_as_bit(sql_expr(~ !!x))
                     } else {
                       sql_expr(NOT(!!x))
                     }
                   },
                   
                   `!=`           = mssql_infix_comparison("!="),
                   `==`           = mssql_infix_comparison("="),
                   `<`            = mssql_infix_comparison("<"),
                   `<=`           = mssql_infix_comparison("<="),
                   `>`            = mssql_infix_comparison(">"),
                   `>=`           = mssql_infix_comparison(">="),
                   
                   `&`            = mssql_infix_boolean("&", "%AND%"),
                   `&&`           = mssql_infix_boolean("&", "%AND%"),
                   `|`            = mssql_infix_boolean("|", "%OR%"),
                   `||`           = mssql_infix_boolean("|", "%OR%"),
                   
                   `[` = function(x, i) {
                     i <- with_mssql_bool(i)
                     build_sql("CASE WHEN (", i, ") THEN (", x, ") END")
                   },
                   
                   bitwShiftL     = sql_not_supported("bitwShiftL"),
                   bitwShiftR     = sql_not_supported("bitwShiftR"),
                   
                   `if`           = function(condition, true, false = NULL, missing = NULL) {
                     mssql_sql_if(enquo(condition), enquo(true), enquo(false), enquo(missing))
                   },
                   if_else        = function(condition, true, false, missing = NULL) {
                     mssql_sql_if(enquo(condition), enquo(true), enquo(false), enquo(missing))
                   },
                   ifelse         = function(test, yes, no) {
                     mssql_sql_if(enquo(test), enquo(yes), enquo(no))
                   },
                   case_when      = mssql_case_when,
                   
                   as.logical    = sql_cast("BIT"),
                   
                   as.Date       = sql_cast("DATE"),
                   as.numeric    = sql_cast("FLOAT"),
                   as.double     = sql_cast("FLOAT"),
                   as.character  = sql_cast("VARCHAR(MAX)"),
                   log           = sql_prefix("LOG"),
                   atan2         = sql_prefix("ATN2"),
                   ceil          = sql_prefix("CEILING"),
                   ceiling       = sql_prefix("CEILING"),
                   
                   # https://dba.stackexchange.com/questions/187090
                   pmin          = sql_not_supported("pmin()"),
                   pmax          = sql_not_supported("pmax()"),
                   
                   is.null       = mssql_is_null,
                   is.na         = mssql_is_null,
                   
                   # string functions ------------------------------------------------
                   nchar = sql_prefix("LEN"),
                   paste = sql_paste_infix(" ", "+", function(x) sql_expr(cast(!!x %as% text))),
                   paste0 = sql_paste_infix("", "+", function(x) sql_expr(cast(!!x %as% text))),
                   substr = sql_substr("SUBSTRING"),
                   substring = sql_substr("SUBSTRING"),
                   
                   # stringr functions
                   str_length = sql_prefix("LEN"),
                   str_c = sql_paste_infix("", "+", function(x) sql_expr(cast(!!x %as% text))),
                   # no built in function: https://stackoverflow.com/questions/230138
                   str_to_title = sql_not_supported("str_to_title()"),
                   # https://docs.microsoft.com/en-us/sql/t-sql/functions/substring-transact-sql?view=sql-server-ver15
                   str_sub = sql_str_sub("SUBSTRING", "LEN", optional_length = FALSE),
                   
                   # lubridate ---------------------------------------------------------------
                   # https://en.wikibooks.org/wiki/SQL_Dialects_Reference/Functions_and_expressions/Date_and_time_functions
                   as_date = sql_cast("DATE"),
                   
                   # Using DATETIME2 as it complies with ANSI and ISO.
                   # MS recommends DATETIME2 for new work:
                   # https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=sql-server-2017
                   as_datetime = sql_cast("DATETIME2"),
                   
                   today = function() sql_expr(CAST(SYSDATETIME() %AS% DATE)),
                   
                   # https://docs.microsoft.com/en-us/sql/t-sql/functions/datepart-transact-sql?view=sql-server-2017
                   year = function(x) sql_expr(DATEPART(YEAR, !!x)),
                   day = function(x) sql_expr(DATEPART(DAY, !!x)),
                   mday = function(x) sql_expr(DATEPART(DAY, !!x)),
                   yday = function(x) sql_expr(DATEPART(DAYOFYEAR, !!x)),
                   hour = function(x) sql_expr(DATEPART(HOUR, !!x)),
                   minute = function(x) sql_expr(DATEPART(MINUTE, !!x)),
                   second = function(x) sql_expr(DATEPART(SECOND, !!x)),
                   
                   month = function(x, label = FALSE, abbr = TRUE) {
                     if (!label) {
                       sql_expr(DATEPART(MONTH, !!x))
                     } else {
                       if (!abbr) {
                         sql_expr(DATENAME(MONTH, !!x))
                       } else {
                         cli_abort("{.arg abbr} is not supported in SQL Server translation")
                       }
                     }
                   },
                   
                   quarter = function(x, with_year = FALSE, fiscal_start = 1) {
                     if (fiscal_start != 1) {
                       cli_abort("{.arg fiscal_start} is not supported in SQL Server translation. Must be 1.")
                     }
                     
                     if (with_year) {
                       sql_expr((DATENAME(YEAR, !!x) + '.' + DATENAME(QUARTER, !!x)))
                     } else {
                       sql_expr(DATEPART(QUARTER, !!x))
                     }
                   },
    )
  
  if (mssql_version(con) >= "11.0") { # MSSQL 2012
    mssql_scalar <- sql_translator(
      .parent = mssql_scalar,
      as.logical = sql_try_cast("BIT"),
      as.Date = sql_try_cast("DATE"),
      as.POSIXct = sql_try_cast("DATETIME2"),
      as.numeric = sql_try_cast("FLOAT"),
      as.double = sql_try_cast("FLOAT"),
      
      # In SQL server, CAST (even with TRY) of INTEGER and BIGINT appears
      # fill entire columns with NULL if parsing single value fails:
      # https://gist.github.com/DavidPatShuiFong/7b47a9804a497b605e477f1bf6c38b37
      # So we parse to NUMERIC (which doesn't have this problem), then to the
      # target type
      as.integer = function(x) {
        sql_expr(try_cast(try_cast(!!x %as% NUMERIC) %as% INT))
      },
      as.integer64 = function(x) {
        sql_expr(try_cast(try_cast(!!x %as% NUMERIC(38L, 0L)) %as% BIGINT))
      },
      as.character = sql_try_cast("VARCHAR(MAX)"),
      as_date = sql_try_cast("DATE"),
      as_datetime = sql_try_cast("DATETIME2")
    )
  }
  
  sql_variant(
    mssql_scalar,
    sql_translator(.parent = base_odbc_agg,
                   sd            = sql_aggregate("STDEV", "sd"),
                   var           = sql_aggregate("VAR", "var"),
                   str_flatten = function(x, collapse = "") sql_expr(string_agg(!!x, !!collapse)),
                   
                   # percentile_cont needs `OVER()` in mssql
                   # https://docs.microsoft.com/en-us/sql/t-sql/functions/percentile-cont-transact-sql?view=sql-server-ver15
                   median = sql_median("PERCENTILE_CONT", "ordered", window = TRUE),
                   quantile = sql_quantile("PERCENTILE_CONT", "ordered", window = TRUE)
                   
    ),
    sql_translator(.parent = base_odbc_win,
                   sd            = win_aggregate("STDEV"),
                   var           = win_aggregate("VAR"),
                   str_flatten = function(x, collapse = "") {
                     win_over(
                       sql_expr(string_agg(!!x, !!collapse)),
                       partition = win_current_group(),
                       order = win_current_order()
                     )
                   }
    )
    
  )}

mssql_version <- function(con) {
  if (inherits(con, "TestConnection")) {
    attr(con, "version")
  } else {
    numeric_version(DBI::dbGetInfo(con)$db.version) # nocov
  }
}

#' @importFrom dbplyr sql_escape_raw
#' @export
sql_escape_raw.DatabaseConnectorConnection <- function(con, x) {
  
  if (is.null(x)) {
    "NULL"
  } else {
    # SQL Server binary constants should be prefixed with 0x
    # https://docs.microsoft.com/en-us/sql/t-sql/data-types/constants-transact-sql?view=sql-server-ver15#binary-constants
    paste0(c("0x", format(x)), collapse = "")
  }
}

#' @importFrom dbplyr sql_table_analyze
#' @export
sql_table_analyze.DatabaseConnectorConnection <- function(con, table, ...) {
  # https://docs.microsoft.com/en-us/sql/t-sql/statements/update-statistics-transact-sql
  build_sql("UPDATE STATISTICS ", as.sql(table, con = con), con = con)
}

#' @importFrom dbplyr db_table_temporary
#' @export
db_table_temporary.DatabaseConnectorConnection <- function(con, table, temporary) {
  if (temporary && substr(table, 1, 1) != "#") {
    table <- hash_temp(table)
  }
  
  list(
    table = table,
    temporary = FALSE
  )
}

#' @importFrom dbplyr sql_query_save
#' @export
sql_query_save.DatabaseConnectorConnection <- function(con, sql, name,
                                                  temporary = TRUE, ...){
  
  # https://stackoverflow.com/q/16683758/946850
  build_sql(
    "SELECT * INTO ", as.sql(name, con), " ",
    "FROM (\n  ", sql, "\n) AS temp",
    con = con
  )
}

#' @export
sql_values_subquery.DatabaseConnectorConnection <- sql_values_subquery_column_alias

#' @importFrom dbplyr sql_random
#' @export
sql_random.DatabaseConnectorConnection <- function(con) {
  sql_expr(RAND())
}

#' @importFrom dbplyr sql_returning_cols
#' @export
sql_returning_cols.DatabaseConnectorConnection <- function(con, cols, table, ...) {
  stopifnot(table %in% c("DELETED", "INSERTED"))
  returning_cols <- sql_named_cols(con, cols, table = ident(table))
  
  sql_clause("OUTPUT", returning_cols)
}

# Bit vs boolean ----------------------------------------------------------

mssql_needs_bit <- function() {
  context <- sql_current_context()
  identical(context$clause, "SELECT") || identical(context$clause, "ORDER")
}

with_mssql_bool <- function(code) {
  local_context(list(clause = ""))
  code
}

mssql_as_bit <- function(x) {
  if (mssql_needs_bit()) {
    sql_expr(cast(iif(!!x, 1L, 0L) %as% BIT))
  } else {
    x
  }
}

mssql_is_null <- function(x) {
  mssql_as_bit(sql_is_null({{x}}))
}

mssql_infix_comparison <- function(f) {
  # assert_that(is_string(f))
  f <- toupper(f)
  function(x, y) {
    mssql_as_bit(build_sql(x, " ", sql(f), " ", y))
  }
}

mssql_infix_boolean <- function(if_bit, if_bool) {
  force(if_bit)
  force(if_bool)
  
  function(x, y) {
    if (mssql_needs_bit()) {
      x <- with_mssql_bool(x)
      y <- with_mssql_bool(y)
      mssql_as_bit(sql_call2(if_bool, x, y))
    } else {
      sql_call2(if_bool, x, y)
    }
  }
}

mssql_sql_if <- function(cond, if_true, if_false = NULL, missing = NULL) {
  cond_sql <- with_mssql_bool(eval_tidy(cond))
  if (is_null(missing) || quo_is_null(missing)) {
    if_true_sql <- build_sql(eval_tidy(if_true))
    if (is_null(if_false) || quo_is_null(if_false)) {
      if_false_sql <- NULL
    } else {
      if_false_sql <- build_sql(eval_tidy(if_false))
    }
    sql_expr(IIF(!!cond_sql, !!if_true_sql, !!if_false_sql))
  } else {
    sql_if(cond, if_true, if_false, missing)
  }
}

mssql_case_when <- function(...) {
  with_mssql_bool(sql_case_when(...))
}

#' @importFrom dbplyr sql_escape_logical
#' @export
sql_escape_logical.DatabaseConnectorConnection <- function(con, x) {
  dplyr::if_else(x, "1", "0", "NULL")
}

globalVariables(c("BIT", "CAST", "%AS%", "%is%", "convert", "DATE", "DATENAME", "DATEPART", "IIF", "NOT", "SUBSTRING", "LTRIM", "RTRIM", "CHARINDEX", "SYSDATETIME", "SECOND", "MINUTE", "HOUR", "DAY", "DAYOFWEEK", "DAYOFYEAR", "MONTH", "QUARTER", "YEAR", "BIGINT", "INT"))

