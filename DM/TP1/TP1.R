library(mongolite)
library(ggplot2)
library(tidyverse)
library(dplyr)
library(readxl)
library(purrr)
library(data.table)
library(jsonlite)



#Conexiones de mongo y carga de datasets estáticos #####
# Mongo
tweets <- mongo(collection = "tweets_mongo_covid19", db = "DMUBA")
users <- mongo(collection = "users_mongo_covid19", db = "DMUBA")
df_users <- users$find(limit = 100000, skip = 0, fields = '{  }')  # Como es pequeño y no tiene profundidad, importo todo
df_tweets <- tweets$find(limit = 100000, skip = 0, fields = '{  }')   # mas campos y con profundidad, vamos a necesitar mas detalle

#Csvs
Parent_folder <- (dirname(rstudioapi::getSourceEditorContext()$path))   # levanto la carpeta madre ( me independizo de la estructura de cada uno)
COVID_geographic <- read_excel(paste(Parent_folder,"/Datasets/COVID-19-geographic-disbtribution-worldwide-2020-05-25.xlsx",sep = ''))

#### Ejemplo de la clase #####
 
df_source_agreggate = tweets$aggregate('[{ "$group": {"_id": "$source", "total": { "$sum": 1} } },
                               { "$sort": { "total" : -1}}
                             ]')


names(df_source_agreggate) <- c("source", "count")

ggplot(data=head(df_source_agreggate, 10), aes(x=reorder(source, -count), y=count)) +
  geom_bar(stat="identity", fill="steelblue") +
  xlab("Source") + ylab("Cantidad de tweets") +
  labs(title = "Cantidad de tweets en los principales clientes")


users <- mongo(collection = "users_mongo_covid19", db = "DMUBA")
df_users = users$find(query = '{}', 
                      fields = '{"friends_count" : true, "listed_count" : true, "statuses_count": true, "favourites_count":true, "verified": true }')
hist(df_users$friends_count, main="cantidad de amigos por usuarios")
hist(log10(df_users$friends_count  + 1), main="Log10 - cantidad de amigos por usuarios")

boxplot(log10(df_users$friends_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account") 



#### Analisis exploratorio ####

# Cuantos usuarios son verificados?
table(df_users$verified)  # Muy pocos usuarios son verificados

top10_locations =sort(table(df_users$location),decreasing=TRUE)[1:10]   # Como podemos tocar las locations para sumarizar?
top10_locations =sort(table(df_users$location[df_users$verified]),decreasing=TRUE)   # Como podemos tocar las locations para sumarizar?


boxplot(log10(df_users$friends_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account") 
boxplot(log10(df_users$listed_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account")   # Esta es la mas sensible a si es verificada o no
boxplot(log10(df_users$statuses_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account") 
boxplot(log10(df_users$favourites_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account") 

  
# Me quedo con las variables numericas para detectar correlaciones
df_users_num <- df_users[colnames(df_users[unlist(lapply(df_users, is.numeric))])]
cor(df_users_num)  
cor(df_users_num[df_users$verified,])            # Hay correlación entre followers y listas publicas que sigue el usuario
cor(df_users_num[-df_users$verified,])


# Paso a explorar los tweets 

boxplot(df_tweets$retweet_count ~verified,data=df_tweets, main="Cantidad de retweets en cuentas verified vs no verified",
        ylab="Log de cantidad de retweets", xlab="Verified Account") 

# Filtro por tweets con las siguientes palabras
with_covid_words = dplyr::filter(df_tweets,grepl('COVID|Coronavirus|COVID-19|19|COVID_19', ignore.case = TRUE,text))
with_cuarentena_words = dplyr::filter(df_tweets,grepl('Cuarentena|Encierro', ignore.case = TRUE,text))


# cantidad de nulos por característica??
df_users.na <- sapply(df_users,function(x) sum(is.na(x)))

 #Integracion de datos######

# Puedo hacer un join entre tweets y users --> Ver si estan correspondidos, para eso hago un inner join y veo cuandos elementos tengo 
Integrated_df = inner_join(df_tweets,df_users,by=c("user_id" = "user_id"))   # el inner join me da completo, todos los tweets tienen datos
Integrated_df_num <- Integrated_df[colnames(Integrated_df[unlist(lapply(Integrated_df, is.numeric))])]
Features_repetidos <- Integrated_df[,sort(grep('.x|.y',colnames(Integrated_df), value= TRUE))]  # Vemos que las features estan repetidas, no hay nada nuevo entre los twitts y los usuarios

# Data Frame con estadísticas de Hashtags


# Here is the same thing in a single pipeline

dt2 <- readLines("https://files.pushshift.io/reddit/comments/sample_data.json") %>%
  map(fromJSON) %>%
  map(as.data.table) %>%
  rbindlist(fill = TRUE)


unchop(c(df_tweets$`_id`,df_tweets$hashtags))








# Eliminacion de columnas redundandtes



# Reduccion de datos y dimensiones
# Limpieza de Datos
# Analisis de outliers
# Transformaciones
# Generacion de nuevas variables
# Visualizaciones
#####
