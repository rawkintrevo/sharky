import logging

#from starbase import Connection
#from solr import Solr
from multiprocessing import Pipe,Value, Queue, freeze_support
from time import clock, strftime, localtime,time, sleep

from mysharky.streamers import TwitterStreamer
from mysharky.processors import TwitterProcessor
from mysharky.writers import SharkyWriter
from mysharky.monitors import HealthMonitor, patientIsHealthy



class Sharky():
	def __init__(self, query, creds, writers):
		self.query= query
		self.creds= creds
		self.writers= writers


	def hungry(self):
		print 'nom, nom, nom'


	def eat(self):
		SLEEP_BETWEEN_REPORTS= 60*0.5

		try:
			tweet_pipe_streamer_end, tweet_pipe_processor_end = Pipe()
			signal_pipe_monitor_end, signal_pipe_sensor_end = Pipe()	
			hb_pipe_monitor_end, hb_pipe_streamer_end = Pipe()
			beaver_shark_q= Queue()
			logging.info('Pipes Established')
		except Exception as e:
			logging.exception(e)
		

		try:	
			monitor= HealthMonitor(hb_pipe_monitor_end, signal_pipe_monitor_end, beaver_shark_q)	
			ts = TwitterStreamer(self.query, self.creds, hb_pipe_streamer_end, tweet_pipe_streamer_end, beaver_shark_q)
		except Exception as e:
			logging.exception(e)

		reseviors= []
		processor_to_writer_pipes= []
		writer_to_final_pipes= []
		sharky_writers= []
		sharky_processors= []
		writer_health_pipe_recv_side= []
		writer_health_pipe_send_side= []
		logging.info('Connecting pipes, queues, writers, and processors...')
		for w in range(0,len(self.writers)):
			try:			
				sharky_writer_conn= self.writers[w]
				reseviors.append(Queue())
				male,female = Pipe()
				writer_health_pipe_recv_side.append(female)
				writer_health_pipe_send_side.append(male)
				logging.info('Creating %s', sharky_writer_conn.name)
				sharky_processors.append(TwitterProcessor(self.query, tweet_pipe_processor_end, sharky_writer_conn, reseviors[w], beaver_shark_q))
				logging.info('Processor successfully created')			
				sharky_writers.append(SharkyWriter(reseviors[w],writer_health_pipe_send_side[w], sharky_writer_conn, beaver_shark_q))
				logging.info('Writer successfully created')			
			except Exception as e:
				logging.exception(e)
							
		try:
			sharky_writers[w].start()
			sharky_processors[w].start()
			monitor.start()
			ts.start()
		except Exception as e:
			logging.exception(e)
	
		try:
			while True:
				if not patientIsHealthy(signal_pipe_sensor_end,logging):
					logging.warning("Recieved Streamer Restart Signal...")
					ts.terminate()
					ts = TwitterStreamer(self.query, self.creds, hb_pipe_streamer_end, tweet_pipe_streamer_end, beaver_shark_q)
					### Need to update TP to pull from new Pipes
					ts.start()
					logging.warning("Streamer restart successful.")
				### Write all logs to file		
				while not beaver_shark_q.empty():
					message= beaver_shark_q.get()
					# message[0] is the level of logging, message[1] is the arg for the log.
					getattr(logging,message[0])(message[1])
				### Poll each writer Health Pipe
				for w in range(0,len(sharky_writers)):
					while writer_health_pipe_recv_side[w].poll():
						batch_size = 0 
						batch_size = writer_health_pipe_recv_side[w].recv()
						if not type(batch_size)== int:
							print strftime('%H:%M:%S %m/%d/%y', localtime(new_data[0])), batch_size				
							sharky_writers[w].terminate()
							sharky_writers[w] = SharkyWriter(reseviors[w],writers[w][1],
													writer_health_pipe_send_side[w],target_type, beaver_shark_q)
				### Sleep the specified amount of time between reports
				sleep(SLEEP_BETWEEN_REPORTS)
		except KeyboardInterrupt:
			ts.terminate()
			monitor.terminate()
			processor.terminate()
			writer1.terminate()
			writer2.terminate()
			print 'Service offline'   #This allows you to shut it all down with Ctrl+C

		
