from multiprocessing import freeze_support
import logging

from mysharky.sharky import Sharky
from mysharky.prebuilt_sharky_writer_connections import mongo_writer


#### Twitter Creds #####

API_KEY= 'xxx'
API_SECRET= 'yyy'
ACCESS_TOKEN= 'zzz'
ACCESS_TOKEN_SECRET= 'lol'

LOG_FILE= 'sharky.log'
LOG_LEVEL= logging.INFO
QUERY= {'food':				'apples,pizza,license plates',
		'cute_things':  	'puppies,kittens,me'}  


TWITTER_CREDS= {
	'consumer_key' : API_KEY,
	'consumer_secret': API_SECRET,
	'access_token_key': ACCESS_TOKEN,
	'access_token_secret': ACCESS_TOKEN_SECRET
}


WRITERS = [mongo_writer]

logging.basicConfig(filename=LOG_FILE, level= LOG_LEVEL, format='%(asctime)s:%(levelname)s: %(message)s')



if __name__ == '__main__':
	freeze_support()
	bruce= Sharky(QUERY, TWITTER_CREDS, WRITERS)
	bruce.eat()
		


	

