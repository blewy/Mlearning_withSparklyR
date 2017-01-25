
library(sparklyr)
library(rsparkling)
library(dplyr)
library(h2o)
library(ggplot2)
library(reshape2)
library(dplyr)
# to display the corners for large matrices
library(useful) 
library(data.table)
library(proxy)
library(Matrix)
#for svd
library(irlba)
library(plotly)
#spark_disconnect(sc)
#spark_install(version = "1.6.2")
sc <- spark_connect("local", version = "1.6.2")


#Download the movie lens datasets zip directly from the web site.
download.file("http://files.grouplens.org/datasets/movielens/ml-100k.zip", destfile = "./data/ml-100k.zip")

#Unzip the zip file into movies directly of the current working directory.
unzip("./data/ml-100k.zip", exdir = "./data/movies")
dir("./data/movies/ml-100k")

#User Dataset
user <- read.table("./data/movies/ml-100k/u.user", header=FALSE, sep="|", col.names = c("UserID", "age", "gender", "occupation","Zip"))
head(user)

user <- copy_to(sc, user, "user")


Analysis<-user %>%                    # take the data.frame "data"
  filter(!is.na(age)) %>%    # Using "data", filter out all rows with NAs in aa 
  #group_by(UserID) %>%          # Then, with the filtered data, group it by "bb"
  summarise(Unique_Elements = n_distinct(UserID),Genders = n_distinct(gender),Ocupations = n_distinct(occupation),Zip_codes = n_distinct(Zip)) %>%   # Now summarise with unique elements per group
collect()

Ages <- select(user, age) %>% collect()

ggplot(data = Ages, aes(x = age))+geom_histogram(binwidth = 5)+scale_color_gradient() +
  theme_bw()+scale_fill_brewer(palette = "Blues") +
  ylab("Count") + xlab("Users Age") +ggtitle("Age distribuition")


Ocupation_Analysis<-user %>%                    # take the data.frame "data"
  filter(!is.na(age)) %>%  # Using "data", filter out all rows with NAs in aa 
  group_by(occupation) %>%          # Then, with the filtered data, group it by "bb"
  summarise(Count=n()) %>%   # Now summarise with unique elements per group
  arrange(desc(Count)) %>% 
  filter(Count > 30)  %>% 
  collect()


Occupation <-user %>% select(occupation) %>% collect()




p <- ggplot(Occupation, aes(x=reorder(occupation, -table(occupation)[occupation]))) + geom_bar()
ggplotly(p)

## -------  Exploring the movie dataset  --------

#Movies Dataset
movies <- read.table("./data/movies/ml-100k/u.item", sep ="|", quote = "", header=FALSE, col.names = c("movie_id","movie_title","release_date","video_release date","IMDb_URL","unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"))
head(movies)

movies <- copy_to(sc, movies, "movies",  overwrite = TRUE )

substrRight <- function(x, n){
  ifelse(substr(x, nchar(x)-n+1, nchar(x)),"1900",substr(x, nchar(x)-n+1, nchar(x)))
}

release_date_teste<- movies %>% select(release_date) %>% collect()



movies<- mutate(movies, year = (substr(release_date, nchar(release_date)-4+1, nchar(release_date))))
movies<-mutate(movies, year = ifelse(year=="","1900", year))
movies<-mutate(movies, year = as.integer(year))

years <- movies %>% select(year) %>% collect()
table(years)

years_filtered <- movies %>% filter(year != 1900)

movie_ages <- mutate(years_filtered, ages =1998 - year)  %>% select(ages) %>% collect() 

table(movie_ages)

ggplot(movie_ages, aes(x=movie_ages)) + geom_bar()


## -------  Exploring the Rating dataset  --------

#Ratings Dataset
ratings <- read.table("./data/movies/ml-100k/u.data",header=FALSE,col.names = c("user","item", "rating","timestamp"))
head(ratings)

ratings <- copy_to(sc, ratings, "ratings")

n_ratings <- ratings %>% summarise(count = n()) %>% collect()

options("scipen"=100, "digits"=4)
cat("\nNumber of Ratings: ", n_ratings$count )


RatingAnalysis<- ratings %>%
  summarize(count = n(), mean_rating = mean(rating), max_rating=max(rating), min_rating=min(rating),users= n_distinct(user),items= n_distinct(item)) %>% collect()


cat("\nMinimum of Ratings: ", RatingAnalysis$min_rating )
cat("\nMaximun of Ratings: ", RatingAnalysis$max_rating )
cat("\nMean Rating: ", RatingAnalysis$mean_rating )
cat("\nAverage # of ratings per user:", RatingAnalysis$count/RatingAnalysis$users)
cat("\nAverage # of ratings per movie: ", RatingAnalysis$count/RatingAnalysis$items)



count_by_rating <- ratings %>%
  group_by(rating) %>%
  arrange(rating) %>%
  summarize(count = n()) %>% collect()
count_by_rating


user_rates <- ratings %>% select(rating) %>% arrange(rating) %>% collect()
p <- ggplot(user_rates, aes(x=rating)) + geom_bar()
p
ggplotly(p)



users_ratings <-  ratings %>%
  arrange(user) %>%
  group_by(user) %>%
  summarize(count=n(),sum = sum(rating), mean_rating = mean(rating)) %>% collect()
users_ratings

p1 <- ggplot(users_ratings, aes(x=count)) + geom_bar()
p1
ggplotly(p1)

###  ---- Processing and transforming your data   --------

mutate(teste, mean_year = mean(year)) 


teste<-movies %>% filter(year != 1900) %>% select(year) %>% collect()


cat("\n Mean year of release : ", mean(teste$year) )
cat("\n Median year of release : ", median(teste$year) )



stats<- teste %>% mutate( mean_year = mean(year), median_year = median(year)) %>% collect()

### ---- Extracting useful features from your data  ------------

# • Numerical features: These features are typically real or integer numbers, for example, the user age that we used in an example earlier.

# • Categorical features: These features refer to variables that can take one of a set of possible states at any given time. Examples from our dataset might include a user's gender or occupation or movie categories.

# • Text features: These are features derived from the text content in the data, for example, movie titles, descriptions, or reviews.

# • Other features: Most other types of features are ultimately represented numerically. For example, images, video, and audio can be represented as sets of numerical data. Geographical locations can be represented as latitude and longitude or geohash data.

### ------- Numerical features  ------------

### ------- Categoriacal features  ------------

Ocupation_unique<-user %>%                   
  filter(!is.na(age)) %>%  
  group_by(occupation) %>% 
  arrange(desc(occupation)) %>% 
  summarise(Count=n()) %>%
  collect()



####  -----------  Processing and transforming your data  -------------

# • Filter out or remove records with bad or missing values: This is sometimes unavoidable; however, this means losing the good part of a bad or missing record.

# • Fill in bad or missing data: We can try to assign a value to bad or missing data based on the rest of the data we have available. Approaches can include assigning a zero value, assigning the global mean or median, interpolating nearby or similar data points (usually, in a time-series dataset), and so on. Deciding on the correct approach is often a tricky task and depends on the data, situation, and one's own experience.

# • Apply robust techniques to outliers: The main issue with outliers is that they might be correct values, even though they are extreme. They might also be errors. It is often very difficult to know which case you are dealing with. Outliers can also be removed or filled in, although fortunately, there are statistical techniques (such as robust regression) to handle outliers and extreme values.

# • Apply transformations to potential outliers: Another approach for outliers or extreme values is to apply transformations, such as a logarithmic or Gaussian kernel transformation, to features that have potential outliers, or display large ranges of potential values. These types of transformations have the effect of dampening the impact of large changes in the scale of a variable and turning a nonlinear relationship into one that is linear.


#### Filling in bad or missing data

mutate(flights, speed = distance / air_time * 60)

#---------- Documentation -----------

# https://github.com/JustinChu/STAT545A_MovieStats/blob/master/cleanMovieLensData.R

#https://github.com/JustinChu/STAT545A_MovieStats/blob/master/cleanMovieLensData.R
