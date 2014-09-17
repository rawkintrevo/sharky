from multiprocessing import Process, Pipe,Value, Queue
from time import sleep, clock

from solr import Solr

####  EVERY connection must be a class with a .commit() method.
####  Starbase and solr already have these.  If you want to make 
####  a csv method, you need to define it as a custom class.  
####
####  commit() would either be to open the file and append everyone 20 lines or so
####  OR you would append every line as it comes in, and commit is a dummy funtion, but it 
####  needs to be there.

class SharkyWriter(Process):
	def __init__(self, queue,connection, health_pipe, writer_type):
		Process.__init__(self)
		self.queue= queue
		self.conn=  connection
		self.health_pipe= health_pipe
		self.writer_type = writer_type
#		if writer_type == 'solr':
#			self.conn = Solr('http://10.3.2.209:8983/solr/afi_social6_shard2_replica1')
#		
	def writer(self,doc, conn):
		try:
			if self.writer_type== 'hbase':	conn.insert(doc['rowname'],doc['cols'])	#HBase Method
			if self.writer_type== 'solr':	conn.add(doc)  ## Solr Method
		except Exception as e:
			print self.writer_type, e
			self.health_pipe.send(e)


	def run(self):
		batch_size= 0
		while True:
			while not self.queue.empty():
				doc = self.queue.get()
				self.writer(doc, self.conn)
				batch_size += 1
				try:
					self.health_pipe.send(batch_size)
					if batch_size > 20: 
						self.conn.commit()	# Either this succeed then in the next line 
											# batch_size is reset, or it fails, and batch_size
											# remains intact (until there finally is a successful commit)
						batch_size = 0
				except Exception as e:
					print e
					self.health_pipe.send('error in %s writer' % self.writer_type)					
			sleep(5)


