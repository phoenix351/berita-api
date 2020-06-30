from threading import Thread
import tweepy
import csv
import sys
from datetime import datetime,timedelta
from time import sleep
try:
  from twitter.Status import Status 
  from twitter.search import gettweets_bykeyword
  from twitter.search import get_api
  from twitter.Database import Database_connection as db_
except:
  from Status import Status 
  from search import gettweets_bykeyword
  from search import get_api
  from Database import Database_connection as db_



def stream_artif(key):
  
  api = get_api()
  tanggal = datetime.now()-timedelta(0)

  statuses = gettweets_bykeyword(key,2,False,0,key,tanggal)
  for status in statuses :
    
    Thread(target=status.insert_db).start()
  
if __name__ == '__main__':
  keyword_list = ['corona','covid','covid19','covid-19','korona','dampak corona','indonesia corona']
  #print(get_api().rate_limit_status())
  while True:
    stream_artif(' OR '.join(keyword_list))
    sleep(60*15)