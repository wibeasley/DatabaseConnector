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
src_databaseConnector <- function(connectionDetails, oracleTempTable = NULL) {
  con <- DatabaseConnector::connect(connectionDetails)
  attr(con, "oracleTempTable") <- oracleTempTable
  src_sql("databaseConnector",  con, disco = dplyr:::db_disconnector(con, "databaseConnector", TRUE))
}

#' @export
src_desc.src_databaseConnector <- function(con) {
  return(attr(con$obj, "dbms"))
}

#' @export
tbl.src_databaseConnector <- function(src, from, ...) {
  from <- dbIdentifier(from)
  tbl_sql("databaseConnector", src = src, from = from, ...)
}

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
  insertTable(connection = con,
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

sql_render.tbl_databaseConnector <- function(x, con) {
  x$ops <- do_collapse(x$ops)
  class(x) <- c("tbl_sql", "tbl_lazy", "tbl")
  sql <- sql_render(x, con)
  sql <- fixSql(sql)
  sql <- SqlRender::translateSql(as.character(sql), targetDialect = attr(con, "dbms"), oracleTempSchema = attr(con, "oracleTempTable"))$sql
  class(sql) <- c("sql", class(sql))
  return(sql)
}

#' @export
collect.tbl_databaseConnector <- function(x, ..., n = Inf, warn_incomplete = TRUE) {
  assertthat::assert_that(length(n) == 1, n > 0L)
  if (n == Inf) {
    n <- -1
  }
  
  con <- con_acquire(x$src)
  on.exit(con_release(x$src, con), add = TRUE)
  sql <- sql_render(x, con)
  out <- querySql(con, as.character(sql))
  colnames(out) <- tolower(colnames(out))
  # Grouping variables don't seem to update with joins, therefore not grouping df:
  grouped_df(out, groups(x))
  return(out)
}

#' @export
collectCamelCase <- function(x, ..., n = Inf, warn_incomplete = TRUE) {
  df <- collect(x, ..., n = n , warn_incomplete = warn_incomplete)
  colnames(df) <- SqlRender::snakeCaseToCamelCase(colnames(df))
  return(df)
}

#' @export
collapse.tbl_sql <- function(x, vars = NULL, ...) {
  con <- con_acquire(x$src)
  tryCatch({
    sql <- sql_render(x, con)
  }, finally = {
    con_release(x$src, con)
  })
  
  tbl(x$src, paste0("(", sql, ") ", dplyr:::random_table_name())) %>% group_by_(.dots = groups(x))
}

#' @export
show_sql <- function(x) {
  con <- con_acquire(x$src)
  on.exit(con_release(x$src, con), add = TRUE)
  sql <- sql_render(x, con)
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

#' @export
db_create_table.JDBCConnection <- function(con, table, types,
                                          temporary = FALSE, ...) {
  assertthat::assert_that(assertthat::is.string(table), is.character(types))
  
  field_names <- escape(ident(names(types)), collapse = NULL, con = con)
  fields <- sql_vector(
    paste0(field_names, " ", types),
    parens = TRUE,
    collapse = ", ",
    con = con
  )
  sql <- build_sql(
    "CREATE ", 
    "TABLE ", 
    if (temporary) sql("#"),
    ident(table), " ", fields,
    con = con
  )
  sql <- SqlRender::translateSql(as.character(sql), targetDialect = attr(con, "dbms"), oracleTempSchema = attr(con, "oracleTempTable"))$sql
  executeSql(con, sql, progressBar = FALSE, reportOverallTime = FALSE)
}

#' @export
copy_to.src_databaseConnector <- function(dest, df, name = deparse(substitute(df)),
                            types = NULL, temporary = TRUE,
                            unique_indexes = NULL, indexes = NULL,
                            analyze = TRUE, ...) {
  assertthat::assert_that(is.data.frame(df), assertthat::is.string(name), assertthat::is.flag(temporary))
  class(df) <- "data.frame" # avoid S4 dispatch problem in dbSendPreparedQuery
  if (!is.list(indexes)) {
    indexes <- as.list(indexes)
  }
  if (!is.list(unique_indexes)) {
    unique_indexes <- as.list(unique_indexes)
  }
  con <- con_acquire(dest)
  name <- paste0(if (temporary) "#",name)
  tryCatch({
    insertTable(connection = con,
                tableName = name,
                data = df,
                createTable = TRUE,
                tempTable = temporary,
                dropTableIfExists = FALSE)
    db_create_indexes(con, name, unique_indexes, unique = TRUE)
    db_create_indexes(con, name, indexes, unique = FALSE)
  }, finally = {
    con_release(dest, con)
  })
  
  tbl(dest, name)
}

#' @export
compute.tbl_databaseConnector <- function(x, name = dplyr:::random_table_name(), temporary = TRUE,
                            unique_indexes = list(), indexes = list(),
                            ...) {
  if (!is.list(indexes)) {
    indexes <- as.list(indexes)
  }
  if (!is.list(unique_indexes)) {
    unique_indexes <- as.list(unique_indexes)
  }
  
  con <- con_acquire(x$src)
  tryCatch({
    vars <- op_vars(x)
    assertthat::assert_that(all(unlist(indexes) %in% vars))
    assertthat::assert_that(all(unlist(unique_indexes) %in% vars))
    x_aliased <- select_(x, .dots = vars) # avoids problems with SQLite quoting (#1754)
    name <- db_save_query(con, sql_render(x_aliased, con), name = name, temporary = temporary)
    db_create_indexes(con, name, unique_indexes, unique = TRUE)
    db_create_indexes(con, name, indexes, unique = FALSE)
  }, finally = {
    con_release(x$src, con)
  })
  
  tbl(x$src, name) %>% group_by_(.dots = groups(x))
}


#' @export
db_save_query.JDBCConnection <- function(con, sql, name, temporary = TRUE, ...) {
  name <- paste0(if (temporary) "#",name)
  sql <- build_sql(
    "SELECT * INTO ", 
    sql(name), 
    " FROM (", 
    sql,
    ") temp123;",
    con = con
  )
  sql <- SqlRender::translateSql(as.character(sql), targetDialect = attr(con, "dbms"), oracleTempSchema = attr(con, "oracleTempTable"))$sql
  executeSql(con, sql, progressBar = FALSE, reportOverallTime = FALSE)
  name
}


#
#' 
#' # Doesn't return TRUE for temporary tables
# #' @export
# db_has_table.JDBCConnection <- function(con, table, ...) {
#   table %in% db_list_tables(con$con)
# }

#' @export
db_begin.JDBCConnection <- function(con, ...) {
  #dbExecute(con, "BEGIN TRANSACTION")
}

#' @export
db_query_fields.JDBCConnection <- function(con, sql, ...) {
  sql <- sql_select(con, sql("*"), sql_subquery(con, sql), where = sql("0 = 1"))
  sql <- SqlRender::translateSql(as.character(sql), targetDialect = attr(con, "dbms"))$sql
  res <- querySql(con, sql)
  tolower(names(res))
}
