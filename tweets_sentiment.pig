word_dict = load '/Sentiment_Analysis_Shanshan_Luo/AFINN.txt' using PigStorage() as (word:chararray, rating:int);

tweets = load '/Sentiment_Analysis_Shanshan_Luo/Tweets.csv' using PigStorage(',') as (id:bytearray, text:bytearray);
data_token = foreach tweets generate id, text, flatten(TOKENIZE(text, ' ')) as word;

sentiment_join = join data_token by word, word_dict by word using 'replicated';
sentiment_clean = foreach sentiment_join generate data_token::id as id, data_token::text as text, data_token::word as word, word_dict::rating as rating;

grp = group sentiment_clean by id;
tweets_sentiment = foreach grp generate group, AVG(sentiment_clean.rating) as rating;

final = join tweets_sentiment by group, tweets by id using 'replicated';
tweets_sentiment = foreach final generate tweets::id as id, tweets::text as text, tweets_sentiment::rating as rating;
store tweets_sentiment into 'hdfs://localhost:8020/Sentiment_Analysis_Shanshan_Luo/Tweets_sentiment.csv' using PigStorage(',');