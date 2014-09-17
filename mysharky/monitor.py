from multiprocessing import Process, Pipe,Value, Queue
from numpy import mean

class HealthMonitor(Process):
	def __init__(self, monitor_pipe, call_for_restart):
		Process.__init__(self)
		self.queue= queue
		self.last_hbs = [clock()]
		
		

	def run(self):
		while True:
			while self.monitor_pipe.poll():
				hb = self.monitor_pipe.recv():
				self.last_hbs.append(hb)
				if len(self.last_hbs) > 100: self.last_hbs.pop(0)
				if self.healthMonitor() < .999999:
					self.call_for_restart()
	
	def call_for_restart(self):
		monitor_pipe.send([self.last_hbs, self.healthMonitor()])

	def healthMonitor():
		last_hb, avg_int= self.last_hbs[-1], mean(self.last_hbs)
		p_of_life= 1- expon.cdf(clock(),last_hb,scale= avg_int)
		return p_of_life

