library(sparklyr)
library(dplyr)
library(ggplot2)

sc <- spark_connect(master = "local", version = "1.6.2")
iris_tbl <- copy_to(sc, iris, "iris", overwrite = TRUE)
iris_tbl

#------------ K-Means Clustering --------------

kmeans_model <- iris_tbl %>%
  select(Petal_Width, Petal_Length) %>%
  ml_kmeans(centers = 3)

# print our model fit
print(kmeans_model)

# predict the associated class
predicted <- sdf_predict(kmeans_model, iris_tbl) %>%
  collect

table(predicted$Species, predicted$prediction)

# plot cluster membership
sdf_predict(kmeans_model) %>%
  collect() %>%
  ggplot(aes(Petal_Length, Petal_Width)) +
  geom_point(aes(Petal_Width, Petal_Length, col = factor(prediction + 1)),
             size = 2, alpha = 0.5) + 
  geom_point(data = kmeans_model$centers, aes(Petal_Width, Petal_Length),
             col = scales::muted(c("red", "green", "blue")),
             pch = 'x', size = 12) +
  scale_color_discrete(name = "Predicted Cluster",
                       labels = paste("Cluster", 1:3)) +
  labs(
    x = "Petal Length",
    y = "Petal Width",
    title = "K-Means Clustering",
    subtitle = "Use Spark.ML to predict cluster membership with the iris dataset."
  )

#--------- Linear Regression -----------

lm_model <- iris_tbl %>%
  select(Petal_Width, Petal_Length) %>%
  ml_linear_regression(Petal_Length ~ Petal_Width)

print(lm_model)

iris_tbl %>%
  select(Petal_Width, Petal_Length) %>%
  collect %>%
  ggplot(aes(Petal_Length, Petal_Width)) +
  geom_point(aes(Petal_Width, Petal_Length), size = 2, alpha = 0.5) +
  geom_abline(aes(slope = coef(lm_model)[["Petal_Width"]],
                  intercept = coef(lm_model)[["(Intercept)"]]),
              color = "red") +
  labs(
    x = "Petal Width",
    y = "Petal Length",
    title = "Linear Regression: Petal Length ~ Petal Width",
    subtitle = "Use Spark.ML linear regression to predict petal length as a function of petal width."
  )

# ------------ Logistic Regression -----------

# Prepare beaver dataset
beaver <- beaver2
beaver$activ <- factor(beaver$activ, labels = c("Non-Active", "Active"))
copy_to(sc, beaver, "beaver")
beaver_tbl <- tbl(sc, "beaver")

glm_model <- beaver_tbl %>%
  mutate(binary_response = as.numeric(activ == "Active")) %>%
  ml_logistic_regression(binary_response ~ temp)

print(glm_model)

#------------- PCA ---------------------

pca_model <- tbl(sc, "iris") %>%
  select(-Species) %>%
  ml_pca()
print(pca_model)


# ---------  Random Forest ----------

rf_model <- iris_tbl %>%
  ml_random_forest(Species ~ Petal_Length + Petal_Width, num.trees = 10L,type = "classification")

print(rf_model)

rf_predict <- sdf_predict(rf_model, iris_tbl) %>%
  ft_string_indexer("Species", "Species_idx") %>%
  collect

table(rf_predict$Species_idx, rf_predict$prediction)


# --------------- SDF Partitioning ------------
# Split a Spark DataFrame into training, test datasets.

partitions <- tbl(sc, "iris") %>%
  sdf_partition(training = 0.75, test = 0.25, seed = 1099)

fit <- partitions$training %>%
  ml_linear_regression(Petal_Length ~ Petal_Width)

estimate_mse <- function(df){
  sdf_predict(fit, df) %>%
    mutate(resid = Petal_Length - prediction) %>%
    summarize(mse = mean(resid ^ 2)) %>%
    collect
}

sapply(partitions, estimate_mse)

# ------------ FT String Indexing --------

#Use ft_string_indexer and ft_index_to_string to convert a character column into a numeric column and back again

ft_string2idx <- iris_tbl %>%
  ft_string_indexer("Species", "Species_idx") %>%
  ft_index_to_string("Species_idx", "Species_remap") %>%
  collect

table(ft_string2idx$Species, ft_string2idx$Species_remap)


# ------------ SDF Mutate -----------

ft_string2idx <- iris_tbl %>%
  sdf_mutate(Species_idx = ft_string_indexer(Species)) %>%
  sdf_mutate(Species_remap = ft_index_to_string(Species_idx)) %>%
  collect

ft_string2idx %>%
  select(Species, Species_idx, Species_remap) %>%
  distinct
