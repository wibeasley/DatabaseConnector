library(DatabaseConnector)

connectionDetails <- createConnectionDetails(dbms = "postgresql", 
                                             server = "localhost/ohdsi", 
                                             schema = "cdm_synpuf",
                                             user = "postgres",
                                             password = Sys.getenv("pwPostgres"))

connectionDetails <- createConnectionDetails(dbms = "pdw", 
                                             server = "JRDUSAPSCTL01", 
                                             schema = "CDM_jmdc_V512",
                                             port = 17001)

con <- src_databaseConnector(connectionDetails)


# Link to CDM tables ------------------------------------------------------

person <- tbl(con, "person")

observation_period <- tbl(con, "observation_period")

concept <- tbl(con, "concept")

concept_ancestor <- tbl(con, "concept_ancestor")

condition_occurrence <- tbl(con, "condition_occurrence")


# Some example queries ----------------------------------------------------

dim(person)
colnames(person)
head(person)
males <- filter(person, race_concept_id != 8516)
malesCollapse <- collapse(males)
intersect(males, males)
# Not yet working. Seems to be problem with dplyr:
#intersect(malesCollapse, malesCollapse)
#semi_join(person, observation_period, by = "person_id")
#anti_join(person, observation_period, by = "person_id")

numberOfPersons <- collectCamelCase(person %>% count())$n

personsPerGender <- collectCamelCase(person %>% 
                                       group_by(gender_concept_id) %>% 
                                       count %>% 
                                       inner_join(concept, by = c("gender_concept_id" = "concept_id")) %>% 
                                       select(concept_name, n))

ageAtFirstObs <- collectCamelCase(inner_join(person, observation_period, by = "person_id") %>% 
                                    mutate(age = year(observation_period_start_date) - year_of_birth) %>% 
                                    group_by(age) %>% 
                                    count() %>% 
                                    arrange(age))

cumObs <- collectCamelCase(group_by(observation_period, person_id) %>% 
                             summarize(start_date = min(observation_period_start_date), end_date = min(observation_period_end_date)) %>%
                             mutate(obs_days = datediff(day, start_date, end_date)) %>% 
                             group_by(obs_days) %>%
                             count() %>%
                             arrange(obs_days))

months <- compute(observation_period %>%  
                    mutate(year = year(observation_period_start_date), month = month(observation_period_start_date)) %>%
                    distinct(year, month) %>%
                    select(year, month) %>%
                    mutate(start = datefromparts(year,month, 1))  %>%
                    mutate(end = dateadd(day, -1, dateadd(month, 1, start))))

# force cross join + filter until join on inequalities implemented in dplyr: https://github.com/hadley/dplyr/issues/2240
personsWithObsPerMonth <- collectCamelCase(inner_join(mutate(months, dummy = TRUE), mutate(observation_period, dummy = TRUE), by = "dummy") %>%
                                             filter(observation_period_start_date < start, observation_period_end_date > end) %>%
                                             group_by(year, month) %>% 
                                             count() %>%
                                             arrange(year, month))


personsPerCondition <- collectCamelCase(condition_occurrence %>% 
                                          group_by(condition_concept_id) %>%
                                          distinct(person_id, condition_concept_id) %>%
                                          count() %>%
                                          inner_join(concept, by = c("condition_concept_id" = "concept_id")) %>%
                                          select(concept_name, n) %>%
                                          arrange(-n))

conditionsByConcept <- condition_occurrence %>% 
  group_by(condition_concept_id)

personsPerCondition <- conditionsByConcept %>%
  distinct(person_id, condition_concept_id) %>%
  count() %>%
  rename(person_count = n)

codesPerCondition <- conditionsByConcept %>%
  count() %>%
  rename(record_count = n)

conditionToHlt <- concept_ancestor %>% 
  inner_join(concept, by = c("ancestor_concept_id" = "concept_id")) %>%
  filter(vocabulary_id == "meddra" & concept_class_id == "hlt") %>%
  group_by(descendant_concept_id) %>%
  summarise(ancestor_concept_id = max(ancestor_concept_id)) %>%
  ungroup %>%
  inner_join(concept, by = c("ancestor_concept_id" = "concept_id")) %>%
  select(condition_concept_id = descendant_concept_id, hlt_concept_id = ancestor_concept_id, hlt_name = concept_name)

htlToSoc <- concept_ancestor %>% 
  inner_join(concept, by = c("ancestor_concept_id" = "concept_id")) %>%
  filter(vocabulary_id == "meddra" & concept_class_id == "soc") %>%
  group_by(descendant_concept_id) %>%
  summarise(ancestor_concept_id = max(ancestor_concept_id)) %>%
  ungroup %>%
  inner_join(concept, by = c("ancestor_concept_id" = "concept_id")) %>%
  select(hlt_concept_id = descendant_concept_id, soc_concept_id = ancestor_concept_id, soc_name = concept_name)

conditionsTable <- collectCamelCase(inner_join(personsPerCondition, codesPerCondition, by = "condition_concept_id") %>%
                                      inner_join(concept %>% select(concept_id, concept_name), by = c("condition_concept_id" = "concept_id")) %>%
                                      left_join(conditionToHlt, by = "condition_concept_id") %>%
                                      left_join(htlToSoc, by = "hlt_concept_id") %>%
                                      arrange(-person_count))
popCount <- collectCamelCase(person %>% count())$n
conditionsTable$codePrevalence <- conditionsTable$personCount / popCount
conditionsTable$recordsPerPerson <- conditionsTable$recordCount / conditionsTable$personCount


anc <- collect(concept_ancestor %>%
                 filter(descendant_concept_id == 260139) %>%
                 inner_join(concept, by = c("ancestor_concept_id" = "concept_id")) %>%
                 filter(vocabulary_id == "meddra"))



# Handling schema names  ---------------------------------------------------------------
id <- dbIdentifier("CDM_hcup_V500", "dbo", "person")

hcup_person <- tbl(con, id)

hcup_person <- tbl(con, "CDM_hcup_V500.dbo.person")


# Writing to temp tables ---------------------------------------------------------------

df <- data.frame(id = 1:100, value = runif(100))
test <- copy_to(con, df, "test")
summarize(test, average =  mean(value))

temp_table <- compute(person %>% 
                        group_by(gender_concept_id) %>% 
                        count %>% 
                        inner_join(concept, by = c("gender_concept_id" = "concept_id")) %>% 
                        select(concept_name, n))

# Writing to permanent tables ----------------------------------------------------------

df <- data.frame(id = 1:100, value = runif(100))
test <- copy_to(con, df, dbIdentifier("scratch", "test"), temporary = FALSE)
summarize(test, average =  mean(value))

my_table <- compute(person %>% 
                      group_by(gender_concept_id) %>% 
                      count %>% 
                      inner_join(concept, by = c("gender_concept_id" = "concept_id")) %>% 
                      select(concept_name, n),
                    name = dbIdentifier("scratch", "test2") , 
                    temporary = FALSE)

dbDisconnect(con$obj)



src <- src_postgres(dbname = "ohdsi", 
                    host = "localhost",
                    user = "postgres",
                    password = Sys.getenv("pwPostgres"))

dbSendQuery(src$obj, "SET search_path TO cdm_synpuf")
items <- data.frame(id = c(1,2,3), name = c("a", "b", "c"))
map <- data.frame(id_1 = c(1,1,2), id_2 = c(1,2,3))

items <- copy_to(src, items)
map <- copy_to(src, map)

items %>%
  inner_join(map, by = c("id" = "id_1")) %>%
  group_by(id, name) %>%
  summarise(id_2 = max(id_2)) %>%
  rename(origin_id = id) %>%
  inner_join(items, by = c("id_2" = "id")) %>%
  select(name.y, origin_id)

items %>%
  inner_join(map, by = c("id" = "id_1")) %>%
  group_by(id, name) %>%
  summarise(id_2 = max(id_2)) %>%
  inner_join(items, by = c("id_2" = "id")) %>%
  select(hlt_name = concept_name.y, standard_name = concept_name.x, concept_id = concept_id.x)

dbDisconnect(src$obj)





items <- data.frame(id = c(1,2,3), name = c("a", "b", "c"))
map <- data.frame(id_1 = c(1,1,2), id_2 = c(1,2,3))

items <- copy_to(con, items)
map <- copy_to(con, map)

show_sql(items %>%
           inner_join(map, by = c("id" = "id_1")) %>%
           group_by(id, name) %>%
           summarise(id_2 = max(id_2)) %>% ungroup )

items %>%
  inner_join(map, by = c("id" = "id_1")) %>%
  group_by(id, name) %>%
  summarise(id_2 = max(id_2)) %>%
  rename(origin_id = id) %>%
  inner_join(items, by = c("id_2" = "id")) %>%
  select(name.y, origin_id)
