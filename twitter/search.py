from threading import Thread

import tweepy
from datetime import datetime,timedelta
import sys

try:
	from twitter.Status import Status 
	from twitter.Database import Database_connection as dbx
except:
	from Status import Status 
	from Database import Database_connection as dbx


# Fill the API Key
def get_api():
	consumer_key = "3xiq8lS3b7xIMNhtXo1zGxqry"
	consumer_secret = "SFq7oeFsRa9NAP7rp5ETXAPrGZpdlP3R9owDLrUkKdoB2kHnD2"
	access_token = "1237633057084952576-gYchMdjf8OH7bPheYP8PIa8QEzC85T"
	access_token_secret = "tZTGG6F5VmKwRMexmGzuh0AYP1hzsI6TwYeol3uHEBQjs"

	# Auth.
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth)
	return api

def gettweets_bykeyword(keyword,tipe,save=False,rentang=0,id_indikator="",tanggal=datetime.now()):
	"""
	api = Tweepy. API instance
	keyword = katakunci
	tanggal = tanggal (max 7 hari kebelakang)
	tipe = result tipe (recent,popular,mixed)
	save = boolean disave apa tidak
	rentang = rentang waktu kebelakang
	id_indikator = indikator identifier
	"""
	t_ = ['popular','recent','mixed']
	tipe = t_[int(tipe)]
	api = get_api()
	pop =[]
	sejak_dt = tanggal-timedelta(rentang)
	sampai_dt = sejak_dt+timedelta(1)

	sejak = (sejak_dt).strftime('%Y-%m-%d')
	sampai = (sampai_dt).strftime('%Y-%m-%d')
	
	print("api calling...")
	
	try:
		api_call = tweepy.Cursor(api.search, q=keyword,since=sejak,
			until=sampai,lang='id',result_type=tipe,tweet_mode='extended' ).items(100)

	except:
		return "error"
	for tweet in api_call:
	    pop.append(tweet)
	#it option save = True
	
	

	
	for p in range(len(pop)):
			status = Status(pop[p],id_indikator)
			pop[p] = status
			if save:
				status.insert_db()
				
			
	return pop
def search_tweets_indikator():
	import time
	tanggal = datetime.now()
	
	#ambil indikator
	query_select = "select id_indikator, katakunci from katakunci_indikator"
	db = dbx()
	db.kursor.execute(query_select)
	results = db.kursor.fetchall()	
	#iterasi setiap keyword sbg k 
	t_ = []
	i=0
	for id_, keyword in results:
		k = keyword #ambil str dri tuple
		#membuah thread
		
		t = Thread(target=gettweets_bykeyword,args=(k,"recent",True,6,id_))
		t.start()
		t_.append(t)
		i +=1
		if i==60:
			sleep(15*60)
	for t in t_:t.join()

	db.tutup()
	return 0 
	

if __name__ == '__main__':
	api = get_api()
	#search_tweets_indikator()
	tanggal = datetime.now()-timedelta(3)
	keyword='jokowi'
	res = gettweets_bykeyword(keyword,0,False,0,keyword,tanggal)
	for r in res:
		print("+++++++++++++++++++++++++++++++")
		print(r.status_text)
	
	



