import requests
import os
import ndjson
import json
import pymongo
from pymongo import MongoClient
from ratelimit import limits


client = MongoClient("localhost", 27017)
db = client["twitterDB"] #mongo database
collection = db["tweets"] #collection in twitterDB database
FIFTEEN_MINUTES = 900 #http requests per 15 minutes
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAANdSIgEAAAAAMf8ZyzEDuM5ZscrnVdXw4sX4log%3Dcnb6Vu2A2FyohoF1hgW7BI0CBWAiQfPjurKDVSlCGuROTUEz6u"
URL = "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=lang,geo,public_metrics,referenced_tweets,created_at,context_annotations&expansions=author_id,geo.place_id&user.fields=created_at&place.fields=full_name,name,geo,id,country,country_code,place_type"
#db.tweets.create_index([("data.id", pymongo.ASCENDING)], unique=True)


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


@limits(calls=49, period=FIFTEEN_MINUTES)  #limit http requests to 49 per 15 minutes
def get_data_from_url(url, headers):
    response = requests.request("GET", url, headers=headers, stream=True)  #make http request
    print(response.status_code)
        
    if response.status_code == 200:
        for response_line in response.iter_lines():
            if response_line:
                tweeter_json_data = json.loads(response_line)  #parse json response into a python dictionanry
                
                print(json.dumps(tweeter_json_data, indent=4, sort_keys=True))
                
                if tweeter_json_data["data"]["lang"] == "en": #check if tweets are in english
                    #t_list=[tweeter_json_data] 
                    tweet_id = tweeter_json_data["data"]["id"] #extract tweet_id from the tweet JSON
                    #collection.insert_many(t_list) #store tweets to the database
                    collection.replace_one({"data.id":tweet_id}, tweeter_json_data, upsert = True) #Insert tweets to database. Replace if contents are changed. 
                       
                
    else:      
        raise Exception("HTTP error: {} {}".format(response.status_code, response.text))

    
def main():
    headers = create_headers(BEARER_TOKEN)
    get_data_from_url(URL, headers)


if __name__ == "__main__":
    main()
