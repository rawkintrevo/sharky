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

class SharkyWriterConnection():
	def __init__(self, name):
		self.name= name
	
	def establishConnection(self):
		# Expected Parameters ()
		pass	

	def setEstablishConnectionFn(self, fn):
		self.establishConnection= fn

	def processMessage(self, message, target_queue):
		# Expected Parameters (message, target_queue)
		pass

	def setProcessMessage(self, fn):
		self.processMessage= fn

	def writeOne(self,message,batch):
		pass

	def setWriteOne(self, fn):
		self.writeOne= fn

	def writeBatch(self):
		pass

	def setWriteBatch(self,fn):
		self.writeBatch= fn


	

class SharkyWriter(Process):
	def __init__(self, queue, health_pipe, sharky_writer_conn, beaver_shark_q):
		Process.__init__(self)
		self.queue= queue					## type: Queue (multiprocessor)
		self.health_pipe= health_pipe		## type: Pipe  (multiprocessor)
		self.sharky_writer_conn= sharky_writer_conn
		self.beaver_shark_q= beaver_shark_q	## Info for the logger. 
		self.batch = []
		self.MaxBatchSize= 20
		
	def run(self):
		self.writeOne= self.sharky_writer_conn.writeOne
		self.writeBatch= self.sharky_writer_conn.writeBatch

		try:
			self.conn= self.sharky_writer_conn.establishConnection(self)
			self.beaver_shark_q.put(['info','Write connection %s established' % self.sharky_writer_conn.name])
		except Exception,e: 
			self.beaver_shark_q.put(['exception',e])
	
		
		while True:
			while not self.queue.empty():
				doc= self.queue.get()
				self.writeOne(self, doc)
				if len(self.batch) > self.MaxBatchSize:
					# try/except built into function
					self.writeBatch(self)			
			sleep(5)


