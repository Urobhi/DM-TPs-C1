library(mongolite)
library(ggplot2)
library(sqldf)

tweets <- mongo(collection = "tweets_mongo_covid19", db = "DMUBA")
users <- mongo(collection = "users_mongo_covid19", db = "DMUBA")

####------------------------------------RETWEETS
df_retweets <- as.data.frame(tweets$find(fields = '{"_id" : true, "user_id" : true, "status_id" : true, "created_at" : true, 
                                       "screen_name" : true, "text" : true, "is_retweet" : true, "favorite_count" : true, 
                                       "retweet_count" :  true, "statuses_count" : true,  "name" :  true, "quote_count" : true, "reply_count" : true, 
                                       "retweet_status_id" :  true, "retweet_text" :  true, "retweet_source" :  true, "retweet_favorite_count" :  true, 
                                       "retweet_retweet_count" :  true, "retweet_user_id" :  true, "retweet_screen_name" :  true, "retweet_name" :  true, 
                                       "retweet_followers_count" :  true, "retweet_friends_count" :  true, "retweet_statuses_count" :  true, 
                                       "retweet_location" :  true, "retweet_description" :  true, "retweet_verified" :  true
                                       }'))


#### Query para validar si el retweet_satus_id puede cruzar con el status_id

sqldf("select sum(case when r2.status_id is null then 1 end) as no_match,
      sum(case when r2.status_id is not null then 1 end) as match
      from df_retweets r1
      left join df_retweets r2 on r1.retweet_status_id = r2.status_id
      where r1.is_retweet = TRUE")

##output -> solo 375 retweets coinciden con un tweet original
#no_match: 20911 
#match: 375


###Query para generar artificalmente el count de retweets basado en los que tenemos en la base
#adicionalmente tambien agregue el max retweet_retweet_cout que viene como dato
#El output es el status_id del tweet y el nuevo campo así es fácil de anexar a la fuente original
#Donde no se trate de un retweet, deja el campo en NA

ret_data <- sqldf("select o.status_id, g.retweet_count_sample, rr.max_retweet_count_sample
      from df_retweets o
      left join (
            select r1.retweet_status_id, count(*) as retweet_count_sample
            from df_retweets r1
            where r1.is_retweet = TRUE
            group by r1.retweet_status_id
      ) g on o.retweet_status_id = g.retweet_status_id or o.status_id = g.retweet_status_id
      left join (
            select retweet_status_id, max(retweet_retweet_count) max_retweet_count_sample
            from df_retweets r1
            where r1.is_retweet = TRUE
            group by r1.retweet_status_id
      ) rr on o.retweet_status_id = rr.retweet_status_id or o.status_id = rr.retweet_status_id ")



####------------------------------------QUOTES
df_quotes <- as.data.frame(tweets$find(fields = '{"_id" : true, "user_id" : true, "status_id" : true, "created_at" : true, 
                                       "screen_name" : true, "text" : true, "is_quote" : true, "is_retweet" : true, "favorite_count" : true, 
                                       "quote_count" :  true, "statuses_count" : true,  "name" :  true, "quote_count" : true, "reply_count" : true, 
                                       "quoted_status_id" :  true, "quote_text" :  true, "quote_source" :  true, "quote_favorite_count" :  true, 
                                       "quote_quote_count" :  true, "quote_user_id" :  true, "quote_screen_name" :  true, "quote_name" :  true, 
                                       "quote_followers_count" :  true, "quote_friends_count" :  true, "quote_statuses_count" :  true, 
                                       "quote_location" :  true, "quote_description" :  true, "quote_verified" :  true,
                                       "retweet_count" :  true, "statuses_count" : true,  "name" :  true, "quote_count" : true, "reply_count" : true, 
                                       "retweet_status_id" :  true, "retweet_text" :  true, "retweet_source" :  true, "retweet_favorite_count" :  true, 
                                       "retweet_retweet_count" :  true, "retweet_user_id" :  true, "retweet_screen_name" :  true, "retweet_name" :  true, 
                                       "retweet_followers_count" :  true, "retweet_friends_count" :  true, "retweet_statuses_count" :  true, 
                                       "retweet_location" :  true, "retweet_description" :  true, "retweet_verified" :  true,
                                       "quoted_retweet_count": true
                                       }'))

#### Query para validar si el quote_satus_id puede cruzar con el status_id

sqldf("select sum(case when q2.status_id is null then 1 end) as no_match,
      sum(case when q2.status_id is not null then 1 end) as match
      from df_quotes q1
      left join df_quotes q2 on q1.quoted_status_id = q2.status_id
      where q1.is_quote = TRUE")
##output -> solo 33 quotes coinciden con un tweet original
#no_match: 5172 
#match: 33


###Query para confirmar si hay overlap entre quotes y retweets
sqldf("select is_retweet,
              count(*) as count_of_quotes
      from df_quotes
      where is_quote = true
      group by is_retweet
      ")

sqldf("select status_id from df_quotes where is_quote = true and is_retweet = false limit 5")

#output -> un quote puede no ser un retweet
# Quotes que no son retweets: 1789
# Quotes que son retweets: 3416



###Query para generar artificalmente el count de quotes basado en los que tenemos en la base
#adicionalmente tambien agregue el max quoted_retweet_count que viene como dato
#El output es el status_id del tweet y el nuevo campo así es fácil de anexar a la fuente original
#Donde no se trate de un retweet, deja el campo en NA

quote_data <- sqldf("select source.status_id, sample_quotes.quote_count_sample, max_quote.max_quote_sample
      from df_quotes source
      left join (
      select quoted_status_id, count(*) as quote_count_sample 
      from df_quotes 
      where is_quote = TRUE
      group by quoted_status_id
      ) sample_quotes on source.quoted_status_id = sample_quotes.quoted_status_id or source.status_id = sample_quotes.quoted_status_id
      left join (
      select quoted_status_id, max(quoted_retweet_count) max_quote_sample
      from df_quotes
      where is_quote = TRUE
      group by quoted_status_id
      ) max_quote on source.quoted_status_id = max_quote.quoted_status_id or source.status_id = max_quote.quoted_status_id ")
