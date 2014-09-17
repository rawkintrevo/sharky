__author__= 'Trevor "Where is the ANY key?" Grant'
__date__= 'Aug. 18, 2014'
__license__= 'Apache'
__version__= '0.3'

from TwitterAPI import TwitterAPI
from threading import Thread
from time import clock
from numpy import mean
from pysci.stats import expon

class TwitterStreamer(Thread):
	def __init__(self, query, creds):
		Thread.__init__(self)
		self.creds= creds
		self.query= query
		self.resevior= []
		self.heartbeats= [0]

	def run(self):
		self.makeHandshake()
		self.sipFromStream()
		

	def kill(self):
		self.streamer.close()

	def makeHandshake(self):
		self.api = TwitterAPI(	self.creds['consumer_key'],
								self.creds['consumer_secret'], 
								self.creds['access_token_key'], 
								self.creds['access_token_secret'])

	def sipFromStream(self):
		self.streamer = self.api.request('statuses/filter', {'track': self.query_string, 'language': 'en'})
		for tweet in self.streamer.get_iterator():
			self.resevior.append(tweet)
			self.last_heartbeats.append(clock())
			if len(self.last_heartbeats) > 100: pop(self.last_heartbeats[0])

	def healthCheck(self):
		sums = 0 
		for i in range(2,len(self.last_heartbeat)):
			sums = sums + self.last_heartbeat[i] - self.last_heartbeat[i-1]
		avg_hb = sums / len(self.last_heartbeat)
		p_of_life = 1- expon.cdf(clock(),self.last_heartbeat[-1],scale= avg_hb)
		
