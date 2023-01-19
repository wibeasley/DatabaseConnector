# Copyright 2023 Observational Health Data Sciences and Informatics
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

# Note: looks consistent with https://github.com/sparklyr/sparklyr/blob/main/R/dplyr_sql_translation.R
# but sql_translation.DatabaseConnectorConnection is never called by dplyr. It is called
# when using translate_sql


#' @importFrom dbplyr dbplyr_edition
#' @export
dbplyr_edition.DatabaseConnectorConnection <- function(con) 2L

#' @importFrom dbplyr sql_translation
#' @export
sql_translation.DatabaseConnectorConnection <- function(con) {
  message("Check")
  attr(con, "version") <- "15.0"
  return(dbplyr:::`sql_translation.Microsoft SQL Server`(con))
}
