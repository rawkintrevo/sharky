__author__= 'Trevor "Where is the ANY key?" Grant'
__date__= 'Aug. 18, 2014'
__license__= 'Apache'
__version__= '0.3'

from TwitterAPI import TwitterAPI
from threading import Thread
from time import sleep,clock
from numpy import mean


class twitter_streamer(Thread):
	def __init__(self, query, creds, connection, tweet_processor, writer, commit):
		Thread.__init__(self)
		self.query = query
		self.creds = creds
		self.conn = connection
		self.tweet_processor = tweet_processor
		self.writer = writer
		self.commit = commit
		self.count ={'seen': 0, 'ignored': 0, 'uncommitted': 0, 'committed': 0, 'updated': 0}
		self.count_extended = {}
		self.errors = {'malformed tweet':0, 'write error':0, 'death count': 0}
		self.error_max = 100000		
		self.stop = False
		self.stayAlive = True
		self.query_string= ''
		self.streamer = None
		self.EnhanceYourCalm = False
		self.heartbeats= {'last': clock(), 'last_100':[]}
		

	def run(self):
		try:		
			self.restart()
		except Exception as e:
			print "Service unexpectedly stopped. " + str(e)
		print "Service has stopped as expected.\n"
	
	def handshakeTwitter(self):
		self.api = TwitterAPI(	self.creds['consumer_key'],
								self.creds['consumer_secret'], 
								self.creds['access_token_key'], 
								self.creds['access_token_secret'])

	def restart(self):
		#######################################
		### Call Twitter                    ###
		#######################################

		self.handshakeTwitter()
		self.updateQuery(self.query)
		while self.stayAlive:
			try:
				self.stream()
			except Exception as e:
				print e	
				self.errors['death count'] += 1

	def health(self):
		print "Alive: "+str(self.isAlive())
		print "Stream Status: "+str(self.streamer.status_code)
		print "Seconds since last heartbeat:" + str(clock() - self.heartbeats['last'])
		print "Average Heartbeat interval:" + str(mean(self.heartbeats['last_100']))

	
	def kill(self):
		print "Stopping Service..."
		self.stopStreamer()
		self.stayAlive = False		
		#raise NameError('OK')

	def stream(self):	
		#### Enter the Stream
		if self.EnhanceYourCalm == True:
			print 'Enhancing your calm...'
			while self.EnhanceYourCalm == True:
				sleep(5)
		self.streamer = self.api.request('statuses/filter', {'track': self.query_string, 'language': 'en'})
		if self.streamer.status_code == 200:
			print '\nConnection Established...\n'
		else:
			print 'Connection failed...' + str(self.streamer.status_code)
			self.EnhanceYourCalm == True
			return
		
		for tweet in self.streamer.get_iterator():
			#### Health Monitoring ######################
			self.heartbeats['last_100'].append(clock() - self.heartbeats['last'])
			if len(self.heartbeats['last_100']) > 100: dontcare = self.heartbeats['last_100'].pop(0)
			self.heartbeats['last'] = clock()
			
			
			self.count['seen'] +=1
			doc,count_keys = self.tweet_processor(tweet, self.query)
			if doc is None:
				self.count['ignored'] +=1
			else:
				temp_counter = self.writer(doc, self.conn)
				if temp_counter == 0: self.errors['write error'] +=1
				else: 
					self.count['uncommitted'] += 1
					self.count_extended[count_keys[0]][count_keys[1]] = self.count_extended[count_keys[0]].get(count_key[1],0) + 1
			if self.count['uncommitted'] > 100:
				self.commit(self.conn)
				self.count['committed'] += self.count['uncommitted']
				self.count['uncommitted'] = 0
			if self.stop == True:
				break
		print 'Exiting Stream'

	def stopStreamer(self):
		self.streamer.close()
		self.stop = True

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
			self.count_extended[sub_c] = {}
		self.query_string= self.query_string[1:] ## This clips the leading comma
		
