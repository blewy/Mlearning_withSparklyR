library(sparklyr)
library(dplyr)
library(nycflights13)
library(ggplot2)
sc <- spark_connect(master = "local")
flights <- copy_to(sc, flights, "flights")
airlines <- copy_to(sc, airlines, "airlines")
src_tbls(sc)


#### -----  dplyr Verbs  --------

# select ~ SELECT
# filter ~ WHERE
# arrange ~ ORDER
# summarise ~ aggregators: sum, min, sd, etc.
# mutate ~ operators: +, *, log, etc.

select(flights, year:day, arr_delay, dep_delay)

filter(flights, dep_delay > 1000)

arrange(flights, desc(dep_delay))

summarise(flights, mean_dep_delay = mean(dep_delay))

mutate(flights, speed = distance / air_time * 60)

### ......  Laziness
# When working with databases, dplyr tries to be as lazy as possible:
# It never pulls data into R unless you explicitly ask for it.

# It delays doing any work until the last possible moment: it collects together everything you want to do and then sends it to the database in one step.

c1 <- filter(flights, day == 17, month == 5, carrier %in% c('UA', 'WN', 'AA', 'DL'))
c2 <- select(c1, year, month, day, carrier, dep_delay, air_time, distance)
c3 <- arrange(c2, year, month, day, carrier)
c4 <- mutate(c3, air_time_hours = air_time / 60)

#This sequence of operations never actually touches the database. It’s not until you ask for the data (e.g. by printing c4) that dplyr requests the results from the database.

c4

# ------ Piping ------

library(magrittr)
c4 <- flights %>%
  filter(month == 5, day == 17, carrier %in% c('UA', 'WN', 'AA', 'DL')) %>%
  select(carrier, dep_delay, air_time, distance) %>%
  arrange(carrier) %>%
  mutate(air_time_hours = air_time / 60)

c4

## ----- Grouping ------

c4 %>%
  group_by(carrier) %>%
  summarize(count = n(), mean_dep_delay = mean(dep_delay))

#----  Collecting to R ----
#You can copy data from Spark into R’s memory by using collect().

carrierhours <- collect(c4)

# collect() executes the Spark query and returns the results to R for further analysis and visualization.

# Test the significance of pairwise differences and plot the results
with(carrierhours, pairwise.t.test(air_time, carrier))
ggplot(carrierhours, aes(carrier, air_time_hours)) + geom_boxplot()

# If you want to execute a query and store the results in a temporary table, use compute()

compute(c4, 'carrierhours')
src_tbls(sc)

# Window Functions

# Find the most and least delayed flight each day
bestworst <- flights %>%
  group_by(year, month, day) %>%
  select(dep_delay) %>% 
  filter(dep_delay == min(dep_delay) || dep_delay == max(dep_delay))
sql_render(bestworst)
bestworst

# Rank each flight within a daily
ranked <- flights %>%
  group_by(year, month, day) %>%
  select(dep_delay) %>% 
  mutate(rank = rank(desc(dep_delay)))
sql_render(ranked)
ranked

# Performing Joins

# Mutating joins, which add new variables to one table from matching rows in another.
# Filtering joins, which filter observations from one table based on whether or not they match an observation in the other table.
# Set operations, which combine the observations in the data sets as if they were set elements.

# The following statements are equivalent:
flights %>% left_join(airlines)
flights %>% left_join(airlines, by = "carrier")
flights %>% left_join(airlines, by = c("carrier", "carrier"))

#--------- Sampling ---------

sample_n(flights, 10)
sample_frac(flights, 0.01)

# ------- Writing Data --------

spark_write_parquet(tbl, "hdfs://hdfs.company.org:9000/hdfs-path/data")

# Ando to Read 
tbl <- spark_read_parquet(sc, "data", "hdfs://hdfs.company.org:9000/hdfs-path/data")
