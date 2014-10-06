from multiprocessing import Process,Pipe,Value, Queue
from time import sleep

class TwitterProcessor(Process):
	def __init__(self, query,tweet_pipe, sharky_writer_conn, target_queue, beaver_shark_q):
		Process.__init__(self)
		self.tweet_pipe=tweet_pipe
		self.query = query			### Currently unused-- should be used for bucketing
		self.sharky_writer_conn_processor= sharky_writer_conn.processMessage
		self.target_queue= target_queue
		self.beaver_shark_q= beaver_shark_q
	
	def run(self):
		while True:
			while self.tweet_pipe.poll():
				message= self.tweet_pipe.recv()
				try:
					self.sharky_writer_conn_processor(message, self.target_queue)
				except Exception as e:
					self.beaver_shark_q.put(['exception',e])
			sleep(1)



