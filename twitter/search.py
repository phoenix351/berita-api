from threading import Thread
from Status import Status 
import tweepy
from datetime import datetime,timedelta
from Database import Database_connection as dbx
import sys
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

def gettweets_bykeyword(api,keyword,tanggal,tipe,save=False,rentang=1,id_indikator=""):
	"""
	api = Tweepy. API instance
	keyword = katakunci
	tanggal = tanggal (max 7 hari kebelakang)
	tipe = result tipe (recent,popular,mixed)
	save = boolean disave apa tidak
	rentang = rentang waktu kebelakang
	id_indikator = indikator identifier
	"""

	pop =[]
	sejak_dt = tanggal-timedelta(rentang)
	sampai_dt = sejak_dt+timedelta(1)

	sejak = (sejak_dt).strftime('%Y-%m-%d')
	sampai = (sampai_dt).strftime('%Y-%m-%d')
	
	print("api calling...")
	
	try:
		api_call = tweepy.Cursor(api.search, q=keyword,since=sejak,until=sampai,lang='id',result_type=tipe,tweet_mode='extended' ).items(100)

	except:
		return "error"
	for tweet in api_call:
	    pop.append(tweet)
	#it option save = True
	
	if save:
		print("saving...")
		status.insert_db()
		return 0;

	
	for p in pop:
			status = Status(p,id_indikator)
			
	return pop
def search_tweets_indikator():
	import time
	tanggal = datetime.now()
	api = get_api()
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
		
		t = Thread(target=gettweets_bykeyword,args=(api,k,tanggal,"recent",True,6,id_))
		t.start()
		t_.append(t)
		i +=1
		if (i % 4)==0:
			break
			time.sleep(3)
		
	for t in t_:t.join()

	db.tutup()
	return 0 
	

if __name__ == '__main__':
	api = get_api()
	#search_tweets_indikator()
	tanggal = datetime.now()-timedelta(3)
	res = gettweets_bykeyword(api,'corona',tanggal,'recent',False)
	for r in res:
		print(r)
	
	



