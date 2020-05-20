library(mongolite)
library(ggplot2)


#### Ejemplo de la clase #####
 
tweets <- mongo(collection = "tweets_mongo_covid19", db = "DMUBA")
df_source = tweets$aggregate('[{ "$group": {"_id": "$source", "total": { "$sum": 1} } },
                               { "$sort": { "total" : -1}}
                             ]')


names(df_source) <- c("source", "count")

ggplot(data=head(df_source, 10), aes(x=reorder(source, -count), y=count)) +
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


#### Listado de puntos para tener en cuenta####
#### Analisis exploratorio ####
users <- mongo(collection = "users_mongo_covid19", db = "DMUBA")
df_users = users$find(query = '{}', 
                      fields = '{"description" : true,"account_created_at" :true ,"location" : true,"friends_count" : true, "listed_count" : true, "statuses_count": true, "favourites_count":true, "verified": true }')

top10_locations =sort(table(df_users$location),decreasing=TRUE)[1:10]   # Como podemos tocar las locations para sumarizar?
top10_locations =sort(table(df_users$location[df_users$verified]),decreasing=TRUE)   # Como podemos tocar las locations para sumarizar?
table(df_users$verified)  # Muy pocos usuarios son verificados

boxplot(log10(df_users$friends_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account") 
boxplot(log10(df_users$listed_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account")   # Esta es la mas sensible a si es verificada o no
boxplot(log10(df_users$statuses_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account") 
boxplot(log10(df_users$favourites_count  + 1)~verified,data=df_users, main="Cantidad de amigos en cuentas verified vs no verified",
        ylab="Log de cantidad de amigos", xlab="Verified Account") 

  

df_users_num <- df_users[colnames(df_users[unlist(lapply(df_users, is.numeric))])]

cor(df_users_num[df_users$verified,])  # No parece haber grandes correlaciones entre los count. Tanto para verificados como para no 
cor(df_users_num[-df_users$verified,])
# Integracion de datos
# Reduccion de datos y dimensiones
# Limpieza de Datos
# Analisis de outliers
# Transformaciones
# Generacion de nuevas variables
# Visualizaciones
#####
