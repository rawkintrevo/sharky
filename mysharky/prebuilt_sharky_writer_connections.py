from mysharky.writers import SharkyWriterConnection

def mongo_conn(self):
		from pymongo import MongoClient
		m= MongoClient()['shark']['test']
		return m

def mongo_processor(tweet, target_queue):
	## TODO: recast datetime
	target_queue.put(tweet)

def mongo_single_writer(self, doc):
	self.batch.append(doc)

def mongo_batch_insert(self):
	try:
		self.conn.insert(doc for doc in self.batch)
		self.beaver_shark_q.put(['info','%i documents inserted into MongoDB' % len(self.batch)])
		self.batch=[]
	except Exception, e:
		self.beaver_shark_q.put(['exception',e])


mongo_writer= SharkyWriterConnection('mongo_writer')

mongo_writer.setEstablishConnectionFn(mongo_conn)
mongo_writer.setProcessMessage(mongo_processor)
mongo_writer.setWriteOne(mongo_single_writer)
mongo_writer.setWriteBatch(mongo_batch_insert)
