from multiprocessing import Process, Pipe,Value, Queue
from numpy import mean
from time import sleep,clock,time
from scipy.stats import expon


def patientIsHealthy(signal_pipe_sensor_end, logging):
		MAX_TIME_BEFORE_RESTART = 60*2
		if signal_pipe_sensor_end.poll():
			while signal_pipe_sensor_end.poll():
				new_data = signal_pipe_sensor_end.recv()	#### Burn through everyting waiting in the pipe and just get the last hb
			### Check Pulse
			if time() - new_data[0] > MAX_TIME_BEFORE_RESTART:   ## The 'no pulse' case
				minutes_since_last_hb= str(int(round((time()-new_data[0])/60)))
				logging.warn("""%i minutes since last streamer heartbeat...
								restarting streamer and monitor""" %minutes_since_last_hb)	
				return False			
			### Check Stream for General Failure
			if new_data[3] != 200:    ## The streamer has failed case. 
				logging.warn('status code of streamer -- %s -- restarting.' %  str(new_data[3]) )
				return False
		else:
			return True

class HealthMonitor(Process):
	def __init__(self, monitor_pipe, signal_pipe_monitor_end, beaver_shark_q):
		Process.__init__(self)
		self.monitor_pipe= monitor_pipe
		self.signal_pipe= signal_pipe_monitor_end
		self.hb_intervals = [0]
		self.last_hb = time()
		self.seen = 0
		self.beaver_shark_q= beaver_shark_q
		

	def run(self):
		hb = self.last_hb  ## Otherwise you get an error for assignment before reference. 
		status_code = 0
		while True:
			p_of_life = self.healthMonitor()
			self.signal_pipe.send([self.last_hb, p_of_life, self.seen,status_code])
			sleep(2)
			while self.monitor_pipe.poll():
				self.seen +=1
				self.last_hb = hb
				new_data = self.monitor_pipe.recv()
				hb = new_data[0]
				status_code = new_data[1]
				self.hb_intervals.append(hb-self.last_hb)
				if len(self.hb_intervals) > 15: self.hb_intervals.pop(0)
				
	def healthMonitor(self):
		if len(self.hb_intervals) > 1:
			avg_hb_int = mean(self.hb_intervals)
			p_of_life= 1- expon.cdf(time(),self.last_hb,scale= avg_hb_int)
			return p_of_life
		else: 
			return 1


	
