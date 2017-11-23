# -*- coding: utf-8 -*-
#Import the necessary methods from tweepy library
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys,os
import geocoder
import pysolr
from datetime import datetime

#Variables that contains the user credentials to access Twitter API 
data = json.load(open("/home/kentt/pems/twitter/twitterOAuth.json"))

access_token = data["access_token"]
access_token_secret = data["access_token_secret"]
consumer_key = data["consumer_key"]
consumer_secret = data["consumer_secret"]

#Google Maps APIKey
key=json.load(open("/home/kentt/pems/google/googleGeoAPIKey.json"))["key"]
os.environ["GOOGLE_API_KEY"] = key

#Init SOLR connection
zookeeper = pysolr.ZooKeeper("1.sherpa.client.sysedata.no:2181/solr")
solr = pysolr.SolrCloud(zookeeper, "dsbTwitter")

def main(argv):
    trackList=map(lambda x: x.decode("utf-8"),argv)
 #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=trackList)

class solrJson(object):
    def createSolrJson(self,twitterJson):
        #geocode locations
        location = twitterJson["user"]["location"]
        try:
            if location == None:
                lat=None
                lon=None
            else:
                geo = geocoder.google(location)
                latlon = geo.latlng
                lat=latlon[0]
                lon=latlon[1]
        except:
            lat=None
            lon=None

        #flatten hashtags
        tagList=twitterJson['entities']["hashtags"] #dict
        tagNewList=[]
        for tagDict in tagList:
            tag=tagDict['text']
            tagNewList.append("#"+tag)

        #flatten urls
        urlList=twitterJson['entities']["urls"] #dict
        urlNewList=[]
        for urlDict in urlList:
            url=urlDict['expanded_url']
            urlNewList.append(url)

        solrDict={
            "id": twitterJson["id_str"],
            "created_at": datetime.fromtimestamp(float(twitterJson["timestamp_ms"])/1000.),
            "retweet_count": twitterJson["retweet_count"],
            "text": twitterJson["text"],
            "lang": twitterJson["lang"],
            "user_image_url": twitterJson["user"]["profile_image_url"],
            "user_name": twitterJson["user"]["name"],
            "user_location": location,
            "hashtags": tagNewList,
            "urls": urlNewList,
            "lat": lat,
            "lon": lon,
            }
    
        return solrDict 


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    

    def on_data(self, data):
        json_data = json.loads(data)
        solrDict = solrJson().createSolrJson(json_data)
        solr.add([solrDict])
        if solrDict["user_location"]==None:
            location="N/A"
        else:
            location=solrDict["user_location"]

        print solrDict["created_at"].strftime("%Y-%m-%d %H:%M:%S")+" "+solrDict["user_name"]+" "+location+" "+",".join(solrDict["hashtags"])

        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    main(sys.argv[1:])
    sys.exit()

