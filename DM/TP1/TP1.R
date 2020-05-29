library(mongolite)
library(ggplot2)
library(tidyverse)
library(dplyr)
library(readxl)
library(sqldf)



#Conexiones de mongo y carga de datasets estáticos #####
# Mongo
tweets <- mongo(collection = "tweets_mongo_covid19", db = "DMUBA")
users <- mongo(collection = "users_mongo_covid19", db = "DMUBA")
df_users <- users$find(limit = 100000, skip = 0, fields = '{  }')  # Como es pequeño y no tiene profundidad, importo todo
df_tweets <- tweets$find(limit = 100000, skip = 0, fields = '{  }')  %>% 
             select(-c('lang','protected','country','country_code','place_url','place_name','place_full_name','place_type','lat','lng','symbols',)) %>% # Elimino columnas que no aportan por muchos NA
             mutate(created_at = as.Date(created_at))  # Casteo a

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


#Tratamiento de Hashtags       ##### 
unnested_hashtags <- unnest((select(df_tweets,created_at,user_id,hashtags)), cols = hashtags) %>% 
                     na.omit

Unique_users_byHashtags <- aggregate(data=unnested_hashtags, user_id ~ hashtags, function(x) length(unique(x)))%>%
                           arrange(desc(user_id)) %>%
                           setNames(c('hashtags','unique_user_count'))# Hashtags con mayores usuarios unicos
Users_byHashtags <- aggregate(data=unnested_hashtags, user_id ~ hashtags, function(x) length(x))%>%
                    arrange(desc(user_id)) %>% # Hashtags con mayores usuarios unicos
                    setNames(c('hashtags','user_count'))
Hashtags_count <- unnested_hashtags %>% 
                  select(hashtags) %>%
                  table %>%
                  data.frame %>%
                  setNames(c('hashtags','count'))
                  
Hashtags_time  <- aggregate(data=unnested_hashtags, user_id ~ created_at + hashtags, function(x) length(x))%>% 
                  arrange(desc(created_at),desc(user_id)) 
Hashtags_Mean_PerDay <- Hashtags_time %>%
                        group_by(hashtags)%>%
                        summarise_at(vars(user_id),list(Mean = mean))

Hashtags_min_date <- Hashtags_time %>% 
                     select(hashtags,created_at) %>%
                     group_by(hashtags) %>% 
                     filter(created_at == min(created_at)) %>% 
                     slice(1) %>%  # takes the first occurrence if there is a tie
                     setNames(c('hashtags','Min_date'))
  
Hashtags_info <- inner_join(Hashtags_count, Unique_users_byHashtags, by='hashtags') %>%
                 inner_join(., Users_byHashtags, by='hashtags') %>%
                 inner_join(.,Hashtags_Mean_PerDay, by ='hashtags') %>%
                 inner_join(.,Hashtags_min_date, by ='hashtags') 
                 
  
  













# Eliminacion de columnas redundandtes



# Reduccion de datos y dimensiones
# Limpieza de Datos
# Analisis de outliers
# Transformaciones
# Generacion de nuevas variables
# Visualizaciones
#####
