from mysharky.writers import SharkyWriterConnection

#TODO should be able to pass parameters to conn fn

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


def csv_conn(self):
		import csv
		csvfile= open('tweets.csv', 'wb')
		csvwriter= csv.writer(csvfile)
		return csvwriter

def csv_processor(tweet, target_queue):
	target_queue.put([tweet['id'], tweet['text'], tweet['user']['name']])

def csv_single_writer(self, doc):
	self.batch.append(doc)

def csv_batch_insert(self):
	try:
		for row in self.batch:
			try:
				self.conn.writerow(row)	
			except Exception, e:
				self.beaver_shark_q.put(['exception',e])	
		self.beaver_shark_q.put(['info','%i documents inserted into csv' % len(self.batch)])
		self.batch=[]
	except Exception, e:
		self.beaver_shark_q.put(['exception',e])


csv_writer= SharkyWriterConnection('csv_writer')
csv_writer.setEstablishConnectionFn(csv_conn)
csv_writer.setProcessMessage(csv_processor)
csv_writer.setWriteOne(csv_single_writer)
csv_writer.setWriteBatch(csv_batch_insert)
