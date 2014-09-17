from multiprocessing import Process, Pipe,Value, Queue
from numpy import mean
from time import sleep,clock,time
from scipy.stats import expon

class HealthMonitor(Process):
	def __init__(self, monitor_pipe, signal_pipe_monitor_end):
		Process.__init__(self)
		self.monitor_pipe= monitor_pipe
		self.signal_pipe= signal_pipe_monitor_end
		self.hb_intervals = [0]
		self.last_hb = time()
		self.seen = 0
		

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


