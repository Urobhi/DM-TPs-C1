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
# Como es pequeño y no tiene profundidad, importo todo


#Datos de Covid
Parent_folder <- (dirname(rstudioapi::getSourceEditorContext()$path))   # levanto la carpeta madre ( me independizo de la estructura de cada uno)

COVID_Data_aggregate <- read_excel(paste(Parent_folder,"/Datasets/COVID-19-geographic-disbtribution-worldwide-2020-05-25.xlsx",sep = ''))  %>%
                        select (dateRep,cases,deaths) %>%
                        arrange((dateRep)) %>%  # Ordeno por fecha
                        group_by(dateRep) %>%  
                        summarise(cases = sum(cases), deaths= sum(deaths)) %>%
                        write.csv(paste(Parent_folder,"/Datasets/COVID_World_aggregated.csv",sep = ''), row.names = FALSE)
      

# Puedo hacer un join entre tweets y users --> Ver si estan correspondidos, para eso hago un inner join y veo cuandos elementos tengo 
Integrated_df = inner_join(df_tweets,df_users,by=c("user_id" = "user_id"))   # el inner join me da completo, todos los tweets tienen datos
Integrated_df_num <- Integrated_df[colnames(Integrated_df[unlist(lapply(Integrated_df, is.numeric))])]
Features_repetidos <- Integrated_df[,sort(grep('.x|.y',colnames(Integrated_df), value= TRUE))]  # Vemos que las features estan repetidas, no hay nada nuevo entre los twitts y los usuarios



# Dataframe de usuarios #####
df_users <- users$find(limit = 100000, skip = 0, fields = '{  }')%>%  
            write.csv(paste(Parent_folder,"/Datasets/Users.csv",sep = ''),row.names = FALSE)




# Dataframe de Tweets#####

df_tweets <- tweets$find(limit = 100000, skip = 0, fields = '{  }')  %>% 
             select(-c('lang','protected','country','country_code','place_url','place_name','place_full_name','place_type','lat','lng','symbols',)) %>% # Elimino columnas que no aportan por muchos NA
             mutate(created_at = as.Date(created_at))  # Casteo a fecha

#Tratamiento de Hashtags       
unnested_hashtags <- unnest((select(df_tweets,created_at,user_id,hashtags)), cols = hashtags) %>% 
                     na.omit
Hashtags_time  <- aggregate(data=unnested_hashtags, user_id ~ created_at + hashtags, function(x) length(x))%>% 
                  arrange(desc(created_at),desc(user_id)) %>%
                  write.csv(paste(Parent_folder,"/Datasets/Hashtags_OverTime.csv",sep = ''),row.names = FALSE)


# Calculo de Métricas
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
                  
Hashtags_Mean_PerDay <- Hashtags_time %>%
                        group_by(hashtags)%>%
                        summarise_at(vars(user_id),list(Mean = mean))

Hashtags_min_date <- Hashtags_time %>% 
                     select(hashtags,created_at) %>%
                     group_by(hashtags) %>% 
                     filter(created_at == min(created_at)) %>% 
                     slice(1) %>%  # takes the first occurrence if there is a tie
                     setNames(c('hashtags','Min_date'))

len_time_window <- as.numeric(max(unique(Hashtags_time$created_at)) -min(unique(Hashtags_time$created_at)),units = "days")  
Hashtags_mean_alldays  <- Hashtags_time %>%
                         group_by(hashtags)%>%
                         summarise(mean_timeWindow = sum(user_id)/len_time_window)

# Join para integrar todos los datos
Hashtags_info <- inner_join(Hashtags_count, Unique_users_byHashtags, by='hashtags') %>%
                 inner_join(., Users_byHashtags, by='hashtags') %>%
                 inner_join(.,Hashtags_Mean_PerDay, by ='hashtags') %>%
                 inner_join(.,Hashtags_min_date, by ='hashtags') %>%
                 inner_join(.,Hashtags_mean_alldays,by = 'hashtags') %>%
                 write.csv(paste(Parent_folder,"/Datasets/Hashtags_info.csv",sep = ''),row.names = FALSE)  
                 
  
  













# Eliminacion de columnas redundandtes



# Reduccion de datos y dimensiones
# Limpieza de Datos
# Analisis de outliers
# Transformaciones
# Generacion de nuevas variables
# Visualizaciones
#####
