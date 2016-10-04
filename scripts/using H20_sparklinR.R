library(sparklyr)
library(rsparkling)
library(dplyr)
library(h2o)
#spark_disconnect(sc)
sc <- spark_connect("local", version = "1.6.2")
mtcars_tbl <- copy_to(sc, mtcars, "mtcars", overwrite = TRUE)

#Now, let’s perform some simple transformations – we’ll
# Remove all cars with horsepower less than 100,
# Produce a column encoding whether a car has 8 cylinders or not,
# Partition the data into separate training and test data sets,
# Fit a model to our training data set,
# Evaluate our predictive performance on our test dataset.

# transform our data set, and then partition into 'training', 'test'
partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

training <- as_h2o_frame(sc, partitions$training)
test <- as_h2o_frame(sc, partitions$test)

#Alternatively, we can use the h2o.splitFrame() function instead of sdf_partition() to partition the data within H2O instead of Spark (e.g. partitions <- h2o.splitFrame(as_h2o_frame(mtcars_tbl), 0.5))


# fit a linear model to the training dataset
glm_model <- h2o.glm(x = c("wt", "cyl"), 
                     y = "mpg", 
                     training_frame = training,
                     lambda_search = TRUE)


print(glm_model)

library(ggplot2)

# compute predicted values on our test dataset
pred <- h2o.predict(glm_model, newdata = test)
# convert from H2O Frame to Spark DataFrame
predicted <- as_spark_dataframe(sc, pred)

# extract the true 'mpg' values from our test dataset
actual <- partitions$test %>%
  select(mpg) %>%
  collect() %>%
  `[[`("mpg")

# produce a data.frame housing our predicted + actual 'mpg' values
data <- data.frame(
  predicted = predicted,
  actual    = actual
)
# a bug in data.frame does not set colnames properly; reset here 
names(data) <- c("predicted", "actual")

# plot predicted vs. actual values
ggplot(data, aes(x = actual, y = predicted)) +
  geom_abline(lty = "dashed", col = "red") +
  geom_point() +
  theme(plot.title = element_text(hjust = 0.5)) +
  coord_fixed(ratio = 1) +
  labs(
    x = "Actual Fuel Consumption",
    y = "Predicted Fuel Consumption",
    title = "Predicted vs. Actual Fuel Consumption"
  )

#------------------- Models ---------------

iris_tbl <- copy_to(sc, iris, "iris", overwrite = TRUE)
iris_tbl

iris_hf <- as_h2o_frame(sc, iris_tbl)

#------------- K-Means Clustering ----------

kmeans_model <- h2o.kmeans(training_frame = iris_hf, 
                           x = 3:4,
                           k = 3,
                           seed = 1)

# print the cluster centers
h2o.centers(kmeans_model)

# print the centroid statistics
h2o.centroid_stats(kmeans_model)

#-------------- PCA -----------

pca_model <- h2o.prcomp(training_frame = iris_hf,
                        x = 1:4,
                        k = 4,
                        seed = 1)
print(pca_model)

#-------- Random Forest ------

y <- "Species"
x <- setdiff(names(iris_hf), y)
iris_hf[,y] <- as.factor(iris_hf[,y])

splits <- h2o.splitFrame(iris_hf, seed = 1)

rf_model <- h2o.randomForest(x = x, 
                             y = y,
                             training_frame = splits[[1]],
                             validation_frame = splits[[2]],
                             nbins = 32,
                             max_depth = 5,
                             ntrees = 400,
                             seed = 1)

h2o.confusionMatrix(rf_model, valid = TRUE)

h2o.varimp_plot(rf_model)

#---  Gradient Boosting Machine ---------

gbm_model <- h2o.gbm(x = x, 
                     y = y,
                     training_frame = splits[[1]],
                     validation_frame = splits[[2]],                     
                     ntrees = 20,
                     max_depth = 3,
                     learn_rate = 0.01,
                     col_sample_rate = 0.7,
                     seed = 1)

h2o.confusionMatrix(gbm_model, valid = TRUE)
h2o.varimp_plot(gbm_model)

# ----- Deep Learning ---------

path <- system.file("extdata", "prostate.csv", package = "h2o")
prostate_df <- spark_read_csv(sc, "prostate", path)
head(prostate_df)

prostate_hf <- as_h2o_frame(sc, prostate_df)
splits <- h2o.splitFrame(prostate_hf, seed = 1)

# Next we define the response and predictor columns.

y <- "VOL"
#remove response and ID cols
x <- setdiff(names(prostate_hf), c("ID", y))

# Now we can train a deep neural net.

dl_fit <- h2o.deeplearning(x = x, y = y,
                           training_frame = splits[[1]],
                           epochs = 15,
                           activation = "Rectifier",
                           hidden = c(10, 5, 10),
                           input_dropout_ratio = 0.7)

h2o.performance(dl_fit, newdata = splits[[2]])

#Note that the above metrics are not reproducible when H2O’s Deep Learning is run on multiple cores, however, the metrics should be fairly stable across repeat runs.

# ----------- Grid Search ---------

#H2O’s grid search capabilities currently supports traditional (Cartesian) grid search and random grid search. Grid search in R provides the following capabilities:
  
#  H2OGrid class: Represents the results of the grid search
#h2o.getGrid(<grid_id>, sort_by, decreasing): Display the specified grid
#h2o.grid: Start a new grid search parameterized by
    #model builder name (e.g., algorithm = "gbm")
    #model parameters (e.g., ntrees = 100)
    #hyper_parameters: attribute for passing a list of hyper parameters (e.g., list    (ntrees=c(1,100), learn_rate=c(0.1,0.001)))
    #search_criteria: optional attribute for specifying more a advanced search strategy

#-------------- Cartesian Grid Search ------------

splits <- h2o.splitFrame(prostate_hf, seed = 1)

y <- "VOL"
#remove response and ID cols
x <- setdiff(names(prostate_hf), c("ID", y))

# GBM hyperparamters
gbm_params1 <- list(learn_rate = c(0.01, 0.1),
                    max_depth = c(3, 5, 9),
                    sample_rate = c(0.8, 1.0),
                    col_sample_rate = c(0.2, 0.5, 1.0))

# Train and validate a grid of GBMs
gbm_grid1 <- h2o.grid("gbm", x = x, y = y,
                      grid_id = "gbm_grid1",
                      training_frame = splits[[1]],
                      validation_frame = splits[[1]],
                      ntrees = 100,
                      seed = 1,
                      hyper_params = gbm_params1)

# Get the grid results, sorted by validation MSE
gbm_gridperf1 <- h2o.getGrid(grid_id = "gbm_grid1", 
                             sort_by = "mse", 
                             decreasing = FALSE)
print(gbm_gridperf1)

#-------------  Random Grid Search --------------

#H2O’s Random Grid Search samples from the given parameter space until a set of constraints is met. The user can specify the total number of desired models using (e.g. max_models = 40), the amount of time (e.g. max_runtime_secs = 1000), or tell the grid to stop after performance stops improving by a specified amount. Random Grid Search is a practical way to arrive at a good model without too much effort.

# GBM hyperparamters
gbm_params2 <- list(learn_rate = seq(0.01, 0.1, 0.01),
                    max_depth = seq(2, 10, 1),
                    sample_rate = seq(0.5, 1.0, 0.1),
                    col_sample_rate = seq(0.1, 1.0, 0.1))
search_criteria2 <- list(strategy = "RandomDiscrete", 
                         max_models = 50)

# Train and validate a grid of GBMs
gbm_grid2 <- h2o.grid("gbm", x = x, y = y,
                      grid_id = "gbm_grid2",
                      training_frame = splits[[1]],
                      validation_frame = splits[[2]],
                      ntrees = 100,
                      seed = 1,
                      hyper_params = gbm_params2,
                      search_criteria = search_criteria2)

# Get the grid results, sorted by validation MSE
gbm_gridperf2 <- h2o.getGrid(grid_id = "gbm_grid2", 
                             sort_by = "mse", 
                             decreasing = FALSE)

#o get the best model, as measured by validation MSE, we simply grab the first row of the gbm_gridperf2@summary_table object, since this table is already sorted such that the lowest MSE model is on top.
gbm_gridperf2@summary_table[1,]

#------------------ Exporting Models ----------------

# There are two ways of exporting models from H2O – saving models as a binary file, or saving models as pure Java code.

#Binary Models

h2o.saveModel(gbm_grid2, path = "./models/h2omodels")


#Java (POJO) Models

h2o.download_pojo(my_model, path = "/models/h2omodels")

