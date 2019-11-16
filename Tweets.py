import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time

access_token = "1172181837491212300-cOesDy9SPfdoFn6MSzo8eE1J7oKy2g"
access_tokensecret = "cyjOFwPDkLTRfoXA8OiLGY6b9YPTRUHd7dbEIoJaY9cRP"
consumer_key= "WvysntlcsReTu8sD3SD31WoVk"
consumer_secret = "lbR96qqw9a3CeTJoNKUE2AK3uo85Jmw2w7uaYjFcliPHM9HBgt"

class analytics(StreamListener):
	def on_data(self,data):
		try:
			saveFile = open('teams.json','a')
			saveFile.write(data)
			saveFile.write(', \n')
			saveFile.close()
			return True

		except BaseException as except1:
			print ('data parsing error,',str(except1))
			time.sleep(5)

	def on_error(self,status):
		print (status)

verification = OAuthHandler(consumer_key,consumer_secret)
verification.set_access_token(access_token,access_tokensecret)
stream_twitter = Stream(verification,analytics())
stream_twitter.filter(track=['StrangerThings','netflixandchill','unbelievablenetflix','BreakingBad','ElCamino','TheCrown','Friends','netflixseries','netflixtime','netflixshows','netflix',])









