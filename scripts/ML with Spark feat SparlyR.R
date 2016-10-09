
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
#spark_disconnect(sc)
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

ggplot(Occupation, aes(x=reorder(occupation, -table(occupation)[occupation]))) + geom_bar()


## -------  Exploring the movie dataset  --------

#Movies Dataset
movies <- read.table("./data/movies/ml-100k/u.item", sep ="|", quote = "", header=FALSE, col.names = c("movie_id","movie_title","release_date","video_release date","IMDb_URL","unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"))
head(movies)

movies <- copy_to(sc, movies, "movies",  overwrite = TRUE )

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

release_date_teste<- movies %>% select(release_date) %>% collect()

mutate(movies, year = (substrRight(release_date,4) ))

#---------- Documentation -----------

# https://github.com/JustinChu/STAT545A_MovieStats/blob/master/cleanMovieLensData.R

