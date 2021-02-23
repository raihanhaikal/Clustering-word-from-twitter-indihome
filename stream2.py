import tweepy
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

# twitter setup
api_key = "GHLQJpCSkBuykfLH22YgSkHDa"
api_secret = "uoKX0UnDOm4iuKmTOgv2cuXhMLK49cdReksgskSqhX4nl5CcDX"
access_token = "1341265173282111491-m92TFRVPn9wvVp98B48hceahopr5pi"
access_token_secret = "IvwuIxB4UTvn7oFlOVRMZ0n48n0NWqhGgOk19QDSMM0HW"
#Autentikasi
auth = tweepy.OAuthHandler(api_key, api_secret)
#Setup Access Token
auth.set_access_token(access_token, access_token_secret)
#Membuat objek API
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#Menyesuaikan waktu dengan waktu lokal (GMT +7 / UTC +7)
def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta(hours=7)   # the tweets are timestamped in GMT timezone, while I am in +1 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

#Inisialisasi Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0, 10, 1))
topic_name = 'topic3'

search_key = "Sony"
maxId = -1
maxTweets = 3000000
tweetCount = 0
tweetsPerQry = 1000000

while tweetCount < maxTweets:
    if maxId <= 0 :
        newTweets = api.search(q=search_key, count=tweetsPerQry, result_type="recent", tweet_mode = "extended")
    
    newTweets = api.search(q=search_key, lang="en", count=tweetsPerQry, since=2020-12-19, result_type="recent", tweet_mode = "extended", max_id=str(maxId-1))
    
    for i in newTweets:
        record = ''
        record = str(i.user.id_str)
        record += ';'
        record += str(i.full_text.encode('utf-8'))
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        print(str.encode(record))
        producer.send(topic_name, str.encode(record))

    tweetCount += len(newTweets)	
    maxId = newTweets[-1].id