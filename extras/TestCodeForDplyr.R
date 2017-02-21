library(DatabaseConnector)

connectionDetails <- createConnectionDetails(dbms = "postgresql", 
                                             server = "localhost/ohdsi", 
                                             schema = "cdm_synpuf",
                                             user = "postgres",
                                             password = Sys.getenv("pwPostgres"))

connectionDetails <- createConnectionDetails(dbms = "pdw", 
                                             server = "JRDUSAPSCTL01", 
                                             schema = "CDM_Truven_MDCD_V432",
                                             port = 17001)

con <- src_databaseConnector(connectionDetails)


# Link to CDM tables ------------------------------------------------------

person <- tbl(con, "person")

observation_period <- tbl(con, "observation_period")

concept <- tbl(con, "concept")

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

numberOfPersons <- collect(person %>% count())$n

personsPerGender <- collect(person %>% 
                              group_by(gender_concept_id) %>% 
                              count %>% 
                              inner_join(concept, by = c("gender_concept_id" = "concept_id")) %>% 
                              select(concept_name, n))

ageAtFirstObs <- collect(inner_join(person, observation_period, by = "person_id") %>% 
                           mutate(age = year(observation_period_start_date) - year_of_birth) %>% 
                           group_by(age) %>% 
                           count() %>% 
                           arrange(age))

cumObs <- collect(group_by(observation_period, person_id) %>% 
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
system.time(
personsWithObsPerMonth <- collect(inner_join(mutate(months, dummy = TRUE), mutate(observation_period, dummy = TRUE), by = "dummy") %>%
                                    filter(observation_period_start_date < start, observation_period_end_date > end) %>%
                                    group_by(year, month) %>% 
                                    count() %>%
                                    arrange(year, month))
)

personsPerCondition <- collect(condition_occurrence %>% 
                                 group_by(condition_concept_id) %>%
                                 distinct(person_id, condition_concept_id) %>%
                                 count() %>%
                                 inner_join(concept, by = c("condition_concept_id" = "concept_id")) %>%
                                 select(concept_name, n) %>%
                                 arrange(-n))


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


