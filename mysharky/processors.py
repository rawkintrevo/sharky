from multiprocessing import Process,Pipe,Value, Queue
from datetime import datetime
from time import mktime, strptime
from pytz import utc


class TwitterProcessor(Process):
	def __init__(self, query,tweet_pipe,target_type, target_queue, beaver_shark_q):
		Process.__init__(self)
		self.tweet_pipe=tweet_pipe
		self.query = query
		self.target_type= target_type
		self.target_queue= target_queue
		self.beaver_shark_q= beaver_shark_q
	
	def run(self):
		while True:
			while self.tweet_pipe.poll():
				tweet= self.tweet_pipe.recv()
				if (self.target_type=='hbase' or self.target_type=='solr'):
					dt = datetime.fromtimestamp(mktime(strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")))
					solr_dt =  utc.localize(dt)
					message = tweet['text'].encode('ascii','ignore')
					for sub_c in self.query.keys():
						for keywd in self.query[sub_c].split(","):
							if keywd.lower() in message.lower():
								try:					
									impact = tweet['user']["followers_count"] + tweet['user']["friends_count"]
								except:
									impact = 0				
								if self.target_type== 'hbase':
									self.target_queue.put({	
											'rowname': str(tweet['id']), 
											'cols':{ 
												'c': {
													'id' : tweet['id'],
													'txt':	message, 
													'src': "twitter",
													"subc": sub_c,
													"imp": impact,
													'etim': dt,
													"author": tweet['user']['screen_name'],
													"kwd": keywd }	},
											 })
								if self.target_type== 'solr':
									self.target_queue.put({	
											"id": tweet['id'], 
											"message":	message, 
											'created_at': solr_dt,
											"source": "twitter",
											"sub_collection": sub_c,
											"impact": impact,
											"author": tweet['user']['screen_name']})
					continue
				if self.target_type=='mongodb':
					self.target_queue.put(tweet)

	


