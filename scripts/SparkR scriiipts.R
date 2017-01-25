
# http://spark.rstudio.com/index.html

# https://cran.rstudio.com/web/packages/dplyr/vignettes/introduction.html

#install.packages("sparklyr")
#library(sparklyr)
#spark_install(version = "1.6.2")

  
library(sparklyr)
sc <- spark_connect(master = "local")

library(dplyr)
iris_tbl <- copy_to(sc, iris)
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")

# You can list all of the available tables

src_tbls(sc)


# Using dplyr

# filter by departure delay
flights_tbl %>% filter(dep_delay == 2)

delay <- flights_tbl %>% 
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect()

delay

# plot delays
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)

#WINDOW FUNCTIONS
#dplyr window functions are also supported, for example:
  
batting_tbl %>%
select(playerID, yearID, teamID, G, AB:H) %>%
arrange(playerID, yearID, teamID) %>%
group_by(playerID) %>%
filter(min_rank(desc(H)) <= 2 & H > 0)
  
 
# Using SQL
# It’s also possible to execute SQL queries directly against tables within a Spark cluster. The spark_connection object implements a DBI interface for Spark, so you can use dbGetQuery to execute SQL and return the result as an R data frame
    
library(DBI)
iris_preview <- dbGetQuery(sc, "SELECT * FROM iris LIMIT 10")
iris_preview
#Machine Learning
#You can orchestrate machine learning algorithms in a Spark cluster via either Spark MLlib or via the H2O Sparkling Water extension package. Both provide a set of high-level APIs built on top of DataFrames that help you create and tune machine learning workflows  


# copy mtcars into spark
mtcars_tbl <- copy_to(sc, mtcars)

# transform our data set, and then partition into 'training', 'test'
partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

# fit a linear model to the training dataset
fit <- partitions$training %>%
  ml_linear_regression(response = "mpg", features = c("wt", "cyl"))

#For linear regression models produced by Spark, we can use summary() to learn a bit more about the quality of our fit, and the statistical significance of each of our predictors.

summary(fit)
  

# H2O SPARKLING WATER
#Let’s walk the same mtcars example, but in this case use H2O’s machine learning algorithms via the H2O Sparkling Water extension. The dplyr code used to prepare the data is the same, but after partitioning into test and training data we call h2o.glm rather than ml_linear_regression:

# convert to h20_frame (uses the same underlying rdd)


library(sparklyr)
library(rsparkling)
library(h2o)
library(dplyr)

# connect to spark
# connect to spark
sc <- spark_connect("local", version = "1.6.2")

# copy mtcars dataset into spark
mtcars_tbl <- copy_to(sc, mtcars, "mtcars", overwrite = TRUE)

# transform our data set, and then partition into 'training', 'test'
partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

# Now, we convert our training and test sets into H2O Frames using rsparkling’s data frame conversion functions:
training <- as_h2o_frame(sc, partitions$training)
test <- as_h2o_frame(sc, partitions$test)

# fit a linear model to the training dataset
fit <- h2o.glm(x = c("wt", "cyl"), 
               y = "mpg", 
               training_frame = training,
               lambda_search = TRUE)

print(fit)
summary(fit)

#Extensions

library(sparklyr)

# write a csv
tempfile <- tempfile(fileext = ".csv")
write.csv(nycflights13::flights, tempfile, row.names = FALSE, na = "")

# define an R interface to Spark line counting
count_lines <- function(sc, path) {
  spark_context(sc) %>% 
    invoke("textFile", path, 1L) %>% 
    invoke("count")
}

# call spark to count the lines in the csv
count_lines(sc, tempfile)

spark_disconnect(sc)
