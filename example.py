from starbase import Connection
from solr import Solr
from multiprocessing import Pipe,Value, Queue
from time import clock, strftime, localtime,time, sleep

from mysharky.streamers import TwitterStreamer
from mysharky.processors import TwitterProcessor
from mysharky.writers import SharkyWriter
from mysharky.monitors import HealthMonitor


SLEEP_BETWEEN_REPORTS= 60*0.5
MAX_TIME_BEFORE_RESTART = 60*2

QUERY= {'food':				'apples,pizza,puppies',
		'cute_things':  	'puppies,kittens,me'}  


twitter_creds= {
	'consumer_key' : "xxx",
	'consumer_secret': 'yyy',
	'access_token_key': 'zzz',
	'access_token_secret': 'aaa'
}


#### Starbase ##################################################################
c = Connection(host='localhost',port='20550')
table_name = 'example1'
h = c.table(table_name).batch()

#### Solr #######################################################################
s = Solr('http://localhost:8983/solr/example1_shard1_replica1', timeout=5.0)




def stream_start(query, twitter_creds, tweet_pipe_streamer_end):
	signal_pipe_monitor_end, signal_pipe_sensor_end = Pipe()	
	hb_pipe_monitor_end, hb_pipe_streamer_end = Pipe()	
	stream = TwitterStreamer(query, twitter_creds,tweet_pipe_streamer_end, hb_pipe_streamer_end)
	monitor = HealthMonitor(hb_pipe_monitor_end, signal_pipe_monitor_end)	
	stream.start()
	monitor.start()
	return signal_pipe_sensor_end, stream,monitor


#if __name__=='__main__':

### Setup
tweet_pipe_streamer_end, tweet_pipe_processor_end = Pipe()
pipe_end, stream,monitor= stream_start(QUERY, twitter_creds, tweet_pipe_streamer_end)
w_hp_1_m,w_hp_1_f = Pipe()
w_hp_2_m,w_hp_2_f = Pipe()

solr_q = Queue()
hbase_q = Queue()
writer_reseviors = {'hbase': hbase_q, 'solr': solr_q}
processor = TwitterProcessor(QUERY, tweet_pipe_processor_end, writer_reseviors)	
writer1 = SharkyWriter(writer_reseviors['hbase'],h,w_hp_1_m,'hbase')
#writer2 = SharkyWriter(writer_reseviors['solr'],s,w_hp_2_m,'solr')
processor.start()
writer1.start()
#writer2.start()
	

try:
	while True:
		while pipe_end.poll():
			new_data = pipe_end.recv()	#### Burn through everyting waiting in the pipe and just get the last hb
		if time() - new_data[0] > MAX_TIME_BEFORE_RESTART:   ## The 'no pulse' case
			minutes_since_last_hb= str(int(round((time()-new_data[0])/60)))
			current_time = str(strftime('%H:%M:%S %m/%d/%y '))
			print current_time +'WARN: '+ minutes_since_last_hb +' minutes since last streamer heartbeat...restarting streamer and monitor'	
			stream.terminate()
			monitor.terminate()
			pipe_end, stream,monitor= stream_start(QUERY, twitter_creds, tweet_pipe_streamer_end)
		if new_data[3] != 200:    ## The streamer has failed case. 
			current_time = str(strftime('%H:%M:%S %m/%d/%y '))
			print current_time +'WARN: status code of streamer -- ' + str(new_data[3]) + ' -- restarting.'
			stream.terminate()
			monitor.terminate()
			pipe_end, stream,monitor= stream_start(QUERY, twitter_creds, tweet_pipe_streamer_end)
		print 'polling hbase health pipe' 
		while w_hp_1_f.poll():
			batch_size = 0 
			batch_size = w_hp_1_f.recv()
			if not type(batch_size)== int:
				print strftime('%H:%M:%S %m/%d/%y', localtime(new_data[0])), batch_size				
				writer1.terminate()
				writer1 = SharkyWriter(writer_reseviors['hbase'],h,w_hp_1_m,'hbase')
		print "%s hbase: %i" % (strftime('%H:%M:%S %m/%d/%y', localtime(new_data[0])), batch_size)
		print 'polling solr health pipe' 
		while w_hp_2_f.poll():
			batch_size = 0 
			batch_size = w_hp_2_f.recv()
			if not type(batch_size)== int:
				print strftime('%H:%M:%S %m/%d/%y', localtime(new_data[0])), batch_size
				writer2.terminate()
				writer2 = SharkyWriter(writer_reseviors['solr'],s,w_hp_2_m,'solr')
		print "%s solr: %i" % (strftime('%H:%M:%S %m/%d/%y', localtime(new_data[0])), batch_size)
		print strftime('%H:%M:%S %m/%d/%y', localtime(new_data[0]))+"  " +str(new_data[1]) +" "+ str(new_data[2]) +" "+str(new_data[3])
		sleep(SLEEP_BETWEEN_REPORTS)
except KeyboardInterrupt:
	stream.terminate()
	monitor.terminate()
	processor.terminate()
	writer1.terminate()
	writer2.terminate()
	print 'Service offline'   #This allows you to shut it all down with Ctrl+C


stream.terminate()
monitor.terminate()
processor.terminate()
writer1.terminate()
writer2.terminate()
quit()	
	

