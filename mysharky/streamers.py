__author__= 'Trevor "Where is the ANY key?" Grant'
__date__= 'Aug. 18, 2014'
__license__= 'Apache'
__version__= '0.3'

from TwitterAPI import TwitterAPI
from multiprocessing import Process, Pipe
from time import clock,time



class TwitterStreamer(Process):
	def __init__(self, query, creds,tweet_pipe,hb_pipe):
		Process.__init__(self)
		self.creds= creds
		self.tweet_pipe = tweet_pipe
		self.hb_pipe= hb_pipe	
		self.query= ''
		self.query_string = ''
		self.p_of_life = 1		
		self.streamer= None
		self.updateQuery(query)
	
	def run(self):
		print 'Starting now.'
		self.makeHandshake()
		self.sipFromStream()
		print 'Exited Gracefully.'
		

	def makeHandshake(self):
		self.api = TwitterAPI(	self.creds['consumer_key'],
								self.creds['consumer_secret'], 
								self.creds['access_token_key'], 
								self.creds['access_token_secret'])
		
	def sipFromStream(self):
		print 'Entering Stream.'
		self.streamer = self.api.request('statuses/filter', {'track': self.query_string, 'language': 'en'})
		print 'Connection established.'
		for tweet in self.streamer.get_iterator():
			self.hb_pipe.send([time(),self.streamer.status_code])
			self.tweet_pipe.send(tweet)

	def updateQuery(self, new_query):
		self.query = new_query
		### Compile the query dict into a string. 
		### TODO: make this into a more robust fn for query processing
		### TODO: add boolean
		
		for sub_c in self.query:
			if len(sub_c) > 59:
				print "Query: '"+sub_c+"' is "+str(len(sub_c))+" chars long. Max limit is 60.  Please revise"
				self.kill()
				break
			self.query_string= self.query_string+ ',' + self.query[sub_c]
			#self.count_extended[sub_c] = {}   Old Tracking code
		self.query_string= self.query_string[1:] ## This clips the leading comma

