# @file Dplyr.R
#
# Copyright 2017 Observational Health Data Sciences and Informatics
#
# This file is part of DatabaseConnector
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' @export
src_databaseConnector <- function(connectionDetails) {
  con <- DatabaseConnector::connect(connectionDetails)
  src_sql("databaseConnector",  con, disco = dplyr:::db_disconnector(con, "databaseConnector", TRUE))
}

#' @export
src_desc.src_databaseConnector <- function(con) {
  return(attr(con$con, "dbms"))
}

#' @export
tbl.src_databaseConnector <- function(src, from, ...) {
  from <- dbIdentifier(from)
  tbl_sql("databaseConnector", src = src, from = from, ...)
}

#' #' @export
#' sql_escape_ident.JDBCConnection <- function(con, x) {
#'   sql_quote(x, '"')
#' }

#' @export
sql_subquery.JDBCConnection <- function(con, from, name = unique_name(), ...) {
  if (is.ident(from)) {
    setNames(from, name)
  } else if (inherits(from, "dbIdentifier")) {
    # Workaround for table and field names with dots in them
    from
  } else {
    build_sql("(", from, ") ", ident(dplyr:::`%||%`(name, dplyr:::random_table_name())), con = con)
    # dplyr::sql_subquery(con, from, name, ...)
  }
}


#' @export
dbIdentifier <- function(...) {
  # Temporary solution until dplyr does this: https://github.com/rstats-db/DBI/issues/24
  identifier <- paste(as.character(list(...)), collapse = ".")
  class(identifier) <- c("sql", class(identifier), "dbIdentifier")
  return(identifier)
}

#' @export
sql_translate_env.JDBCConnection <- function(con) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
                   n = function() sql("count(*)"),
                   cor = sql_prefix("corr"),
                   cov = sql_prefix("covar"),
                   sd =  sql_prefix("stdev"),
                   var = sql_prefix("var"),
                   all = sql_prefix("bool_and"),
                   any = sql_prefix("bool_or"),
                   paste = function(x, collapse) build_sql("concat(", x, ", ", collapse, ")")
    ),
    base_win
  )
}

#' @export
db_insert_into.JDBCConnection <- function(con, table, values, ...) {
  if (nrow(values) == 0)
    return(NULL)
  insertTable(connection = con$con,
              tableName = table,
              data = values,
              createTable = FALSE,
              tempTable = FALSE)
}

#' @export
sql_select.JDBCConnection <- function(con, select, from, where = NULL,
                                      group_by = NULL, having = NULL,
                                      order_by = NULL,
                                      limit = NULL,
                                      distinct = FALSE,
                                      ...) {
  limitSyntax <- "none"
  if (!is.null(limit)) {
    assertthat::assert_that(is.numeric(limit), length(limit) == 1L)
    if (attr(con,"dbms") %in% c("pdw", "sql server")) {
      limitSyntax <- "top"
    } else if (attr(con,"dbms") == "oracle") {
      limitSyntax <- "rownum"
    } else {
      limitSyntax <- "limit"
    }
  }
  if (limitSyntax == "rownum") {
    where <- c(where, build_sql("ROWNUM <= ", sql(format(trunc(limit), scientific = FALSE))))
    if (length(class(where)) == 1) {
      class(where) <- append("sql", class(where))
    }
  }
  out <- vector("list", 10)
  names(out) <- c("select", "top", "distinct", "selectVars", "from", "where", "group_by", "having", "order_by", "limit")
  
  out$select <- sql("SELECT ")
  
  if (limitSyntax == "top") {
    out$top <- build_sql(
      "TOP ", sql(format(trunc(limit), scientific = FALSE)),
      con = con
    )
  }
  
  if (distinct) {
    out$distinct <- sql("DISTINCT ")
  }
  
  assertthat::assert_that(is.character(select), length(select) > 0L)
  out$selectVars <- build_sql(
    escape(select, collapse = ", ", con = con)
  )
  
  #assertthat::assert_that(is.character(from), length(from) == 1L)
  out$from <- build_sql("FROM ", from, con = con)
  
  if (length(where) > 0L) {
    assertthat::assert_that(is.character(where))
    
    where_paren <- escape(where, parens = TRUE, con = con)
    out$where <- build_sql("WHERE ", sql_vector(where_paren, collapse = " AND "))
  }
  
  if (length(group_by) > 0L) {
    assertthat::assert_that(is.character(group_by))
    out$group_by <- build_sql(
      "GROUP BY ",
      escape(group_by, collapse = ", ", con = con)
    )
  }
  
  if (length(having) > 0L) {
    assertthat::assert_that(is.character(having))
    out$having <- build_sql(
      "HAVING ",
      escape(having, collapse = ", ", con = con)
    )
  }
  
  if (length(order_by) > 0L) {
    assertthat::assert_that(is.character(order_by))
    out$order_by <- build_sql(
      "ORDER BY ",
      escape(order_by, collapse = ", ", con = con)
    )
  }
  
  if (limitSyntax == "limit") {
    out$limit <- build_sql(
      "LIMIT ", sql(format(trunc(limit), scientific = FALSE)),
      con = con
    )
  }
  
  escape(unname(dplyr:::compact(out)), collapse = "\n", parens = FALSE, con = con)
}

#' @export
collect.tbl_databaseConnector <- function(x, ..., n = Inf, warn_incomplete = TRUE) {
  assertthat::assert_that(length(n) == 1, n > 0L)
  if (n == Inf) {
    n <- -1
  }
  
  con <- con_acquire(x$src)
  on.exit(con_release(x$src, con), add = TRUE)
  x$ops <- do_collapse(x$ops)
  sql <- sql_render(x, con)
  sql <- fixSql(sql)
  sql <- SqlRender::translateSql(as.character(sql), targetDialect = attr(con, "dbms"))$sql
  out <- querySql(con, as.character(sql))
  colnames(out) <- tolower(colnames(out))
  grouped_df(out, groups(x))
}

#' @export
show_sql <- function(x) {
  con <- con_acquire(x$src)
  on.exit(con_release(x$src, con), add = TRUE) 
  x$ops <- do_collapse(x$ops, con)
  sql <- sql_render(x, con)
  sql <- fixSql(sql)
  sql <- SqlRender::translateSql(as.character(sql), targetDialect = attr(con, "dbms"))$sql
  return(sql)
}

# Collapse op_select, op_head, op_filter, op_arrange. Can have multiple ops of the same type (lowest overrides
# higher). Each op_head triggers a separate collapse.
do_collapse <- function(op, con) {
  if (!is.null(op$name)) {
    if (op$name == "head") {
      op <- do_head_collapse(op, con)
    } else {
      op$x <- do_collapse(op$x, con) 
      if (!is.null(op$y)) {
        op$y <- do_collapse(op$y, con) 
      }
    }
  }
  return(op)
}

do_head_collapse <- function(op, con) {
  op_collapsed <- list(name = "collapsed", args = list(head = list(n = op$args$n)))
  subOp <- op$x
  while(!is.null(subOp$name) && subOp$name %in% c("select", "arrange")) {
    if (subOp$name == "select" && is.null(op_collapsed$args$select)) {
      op_collapsed$args$select <- list(vars = select_vars_(op_vars(subOp$x), subOp$dots, include = op_grps(subOp$x)))
    } else if (subOp$name == "arrange" && is.null(op_collapsed$args$arrange)) {
      op_collapsed$args$arrange <- list(dots = subOp$dots)
    } 
    subOp <- subOp$x
  }
  op_collapsed$x <- do_collapse(subOp)
  class(op_collapsed) <- c("op_collapsed", "op_single", "op")
  return(op_collapsed)
}

#' @export
sql_build.op_collapsed <- function(op, con, ...) {
  from <- sql_build(op$x, con)
  if (!is.null(op$args$select)) {
    select = ident(op$args$select$vars)
  } else {
    select <- sql("*")
  }
  if (!is.null(op$args$arrange)) {
    order_by = translate_sql_(op$args$arrange$dots, con)
  } else {
    order_by <- character()
  }
  
  select_query(from = from, select = select, order_by = order_by, limit = op$args$head$n)
}

fixSql <- function(sql) {
  # Some side-effects of the eval function in the translate_sql_ function in translate-sql.r are
  # easier to fix with some gsubs:
  sql <- gsub("DATEADD\\(\"day\"", "DATEADD(DAY", sql)
  sql <- gsub("DATEADD\\(\"month\"", "DATEADD(MONTH", sql)
  sql <- gsub("DATEADD\\(\"year\"", "DATEADD(YEAR", sql)
  sql <- gsub("DATEDIFF\\(\"day\"", "DATEDIFF(DAY", sql)
  sql <- gsub("DATEDIFF\\(\"month\"", "DATEDIFF(MONTH", sql)
  sql <- gsub("DATEDIFF\\(\"year\"", "DATEDIFF(YEAR", sql)
  sql <- gsub("DATEADD\\(\"dd\"", "DATEADD(DD", sql)
  sql <- gsub("DATEADD\\(\"mm\"", "DATEADD(MM", sql)
  sql <- gsub("DATEADD\\(\"yyyy\"", "DATEADD(YYYY", sql)
  sql <- gsub("DATEDIFF\\(\"dd\"", "DATEDIFF(DD", sql)
  sql <- gsub("DATEDIFF\\(\"mm\"", "DATEDIFF(MM", sql)
  sql <- gsub("DATEDIFF\\(\"yyyy\"", "DATEDIFF(YYYY", sql)
  sql <- gsub("DATEADD\\(\"d\"", "DATEADD(D", sql)
  sql <- gsub("DATEADD\\(\"m\"", "DATEADD(M", sql)
  sql <- gsub("DATEADD\\(\"yy\"", "DATEADD(YY", sql)
  sql <- gsub("DATEDIFF\\(\"d\"", "DATEDIFF(D", sql)
  sql <- gsub("DATEDIFF\\(\"m\"", "DATEDIFF(M", sql)
  sql <- gsub("DATEDIFF\\(\"yy\"", "DATEDIFF(YY", sql)
  sql <- gsub("\\.0, ", ", ", sql)
  return(sql)
}

#
#' 
#' # Doesn't return TRUE for temporary tables
# #' @export
# db_has_table.JDBCConnection <- function(con, table, ...) {
#   table %in% db_list_tables(con$con)
# }

# #' @export
# db_begin.JDBCConnection <- function(con, ...) {
#   dbExecute(con, "BEGIN TRANSACTION")
# }
#' 
#' # http://www.postgresql.org/docs/9.3/static/sql-explain.html
#' #' @export
#' db_explain.PostgreSQLConnection <- function(con, sql, format = "text", ...) {
#'   format <- match.arg(format, c("text", "json", "yaml", "xml"))
#'   
#'   exsql <- build_sql(
#'     "EXPLAIN ",
#'     if (!is.null(format)) build_sql("(FORMAT ", sql(format), ") "),
#'     sql
#'   )
#'   expl <- dbGetQuery(con, exsql)
#'   
#'   paste(expl[[1]], collapse = "\n")
#' }
#' 
# #' @export
# db_query_fields.JDBCConnection <- function(con, sql, ...) {
#   fields <- build_sql(
#     "SELECT * FROM ", sql_subquery(con, sql), " WHERE 0=1",
#     con = con
#   )
#   
#   qry <- dbSendQuery(con, fields)
#   on.exit(dbClearResult(qry))
#   
#   dbGetInfo(qry)$fieldDescription[[1]]$name
# }
