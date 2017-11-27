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
import urllib2

#Variables that contains the user credentials to access Twitter API 
data = json.load(open("/home/kentt/pems/twitter/twitterOAuth.json"))

access_token = data["access_token"]
access_token_secret = data["access_token_secret"]
consumer_key = data["consumer_key"]
consumer_secret = data["consumer_secret"]

#Google Maps APIKey
key=json.load(open("/home/kentt/pems/google/googleGeoAPIKey.json"))["key"]
os.environ["GOOGLE_API_KEY"] = key

#OpenWeather APIKey
keyOW=json.load(open("/home/kentt/pems/openWeather/openWeatherKey.json"))["key"]


#Init SOLR connection
zookeeper = pysolr.ZooKeeper("1.sherpa.client.sysedata.no:2181/solr")
solr = pysolr.SolrCloud(zookeeper, "dsbTwitterWeather")

def main(argv):
    #test
#    twitterJson={u'quote_count': 0, u'contributors': None, u'truncated': False, u'text': u'\u30e2\u30a4\uff01iPhone\u304b\u3089\u30ad\u30e3\u30b9\u914d\u4fe1\u4e2d - / \u6687\u3059\u304e\u3066 https://t.co/C3bczciPxH', u'is_quote_status': False, u'in_reply_to_status_id': None, u'reply_count': 0, u'id': 933635134330363909, u'favorite_count': 0, u'source': u'<a href="http://twitcasting.tv/" rel="nofollow">TwitCasting</a>', u'retweeted': False, u'coordinates': None, u'timestamp_ms': u'1511430936783', u'entities': {u'user_mentions': [], u'symbols': [], u'hashtags': [], u'urls': [{u'url': u'https://t.co/C3bczciPxH', u'indices': [27, 50], u'expanded_url': u'http://cas.st/1918b946', u'display_url': u'cas.st/1918b946'}]}, u'in_reply_to_screen_name': None, u'id_str': u'933635134330363909', u'retweet_count': 0, u'in_reply_to_user_id': None, u'favorited': False, u'user': {u'follow_request_sent': None, u'profile_use_background_image': True, u'default_profile_image': False, u'id': 4746020838, u'default_profile': True, u'verified': False, u'profile_image_url_https': u'https://pbs.twimg.com/profile_images/932030222752301056/ha9E-hsx_normal.jpg', u'profile_sidebar_fill_color': u'DDEEF6', u'profile_text_color': u'333333', u'followers_count': 323, u'profile_sidebar_border_color': u'C0DEED', u'id_str': u'4746020838', u'profile_background_color': u'F5F8FA', u'listed_count': 2, u'profile_background_image_url_https': u'', u'utc_offset': None, u'statuses_count': 4851, u'description': u'\u3067\u3082\u5927\u4eba\u3063\u3066\u306a\u3093\u3060', u'friends_count': 292, u'location': "Oslo, Norge", u'profile_link_color': u'1DA1F2', u'profile_image_url': u'http://pbs.twimg.com/profile_images/932030222752301056/ha9E-hsx_normal.jpg', u'following': None, u'geo_enabled': True, u'profile_banner_url': u'https://pbs.twimg.com/profile_banners/4746020838/1510885177', u'profile_background_image_url': u'', u'name': u'\u512a\u304f\u3093\U0001f44d', u'lang': u'ja', u'profile_background_tile': False, u'favourites_count': 31177, u'screen_name': u'jsgnpdgdgkn', u'notifications': None, u'url': u'http://line.me/ti/p/zNCvr8H5Ee', u'created_at': u'Tue Jan 12 02:49:58 +0000 2016', u'contributors_enabled': False, u'time_zone': None, u'protected': False, u'translator_type': u'none', u'is_translator': False}, u'geo': None, u'in_reply_to_user_id_str': None, u'possibly_sensitive': False, u'lang': u'ja', u'created_at': u'Thu Nov 23 09:55:36 +0000 2017', u'filter_level': u'low', u'in_reply_to_status_id_str': None, u'place': None}
 #   solrDict = solrJson().createSolrJson(twitterJson)
 #   print solrDict
    ## Test done

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
            
        #Get weather
        if lat!=None and lon!=None:
            openWeatherURL="http://api.openweathermap.org/data/2.5/weather?lat="+str(lat)+"&lon="+str(lon)+"&APPID="+keyOW
            weatherDict=json.loads(urllib2.urlopen(openWeatherURL).read())
            owIconUrl="http://openweathermap.org/img/w/"+weatherDict["weather"][0]["icon"]+".png"
            owTemp=weatherDict["main"]["temp"] - 273.15
            owWindSpeed=weatherDict["wind"]["speed"]
            try:
                owWindDir=weatherDict["wind"]["deg"]
            except:
                owWindDir=None
            owWeatherDesc=weatherDict["weather"][0]["description"]
            owName=weatherDict["name"]
        else:
            owIconUrl=None
            owTemp=None
            owWindSpeed=None
            owWindDir=None
            owWeatherDesc=None
            owName=None

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
                "owIconUrl": owIconUrl,
                "owTemp": owTemp,
                "owWindSpeed": owWindSpeed,
                "owWindDir": owWindDir,
                "owWeatherDesc": owWeatherDesc,
                "owName": owName
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

        if solrDict["owName"]==None:
            solrDict["owName"]="N/A"
        if solrDict["owWeatherDesc"]==None:
            solrDict["owWeatherDesc"]="N/A"

        print solrDict["created_at"].strftime("%Y-%m-%d %H:%M:%S")+" "+solrDict["user_name"]+" "+location+" "+str(solrDict["lat"])+","+str(solrDict["lon"])+" "+",".join(solrDict["hashtags"])+" "+solrDict["owName"]+" "+solrDict["owWeatherDesc"]

        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    main(sys.argv[1:])
    sys.exit()

