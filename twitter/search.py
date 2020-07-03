from threading import Thread

from concurrent.futures import ThreadPoolExecutor
import tweepy
from datetime import datetime,timedelta
import sys
from operator import itemgetter

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
def get_api2():
	consumer_key = "6lous84s4jlja5aO0asrnpaoX"
	consumer_secret = "0W4TNgNTkGtwmRHot7nWvXHxIhdSWBx5lspfN00C8Se1Rv8BpI"
	access_token = "1237633057084952576-vGSApHKVkVU9FGmS9ZXAaQTLqUadna"
	access_token_secret = "ldbvSxhjn6M1UayfHZjqx3PNzjTElRXJK3dgUkxhe8jtz"

	# Auth.
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth)
	return api

def gettweets_bykeyword(keyword,tipe,save=False,rentang=0,kategori="",tanggal=datetime.now(),api = get_api()):
	"""
	api = Tweepy. API instance
	keyword = katakunci
	tanggal = tanggal (max 7 hari kebelakang)
	tipe = result tipe (popular,recent,mixed)
	save = boolean disave apa tidak
	rentang = rentang waktu kebelakang
	kategori = kategori identifier
	"""
	t_ = ['popular','recent','mixed']
	tipe = t_[int(1)]
	
	pop =[]
	sejak_dt = tanggal-timedelta(6)
	sampai_dt = sejak_dt+timedelta(1)

	sejak = (sejak_dt).strftime('%Y-%m-%d')
	sampai = (sampai_dt).strftime('%Y-%m-%d')
	
	print("api calling...")
	
	try:
		api_call = tweepy.Cursor(api.search, q=keyword,since=sejak,
			until=sampai,lang='id',result_type=tipe,tweet_mode='extended' ).items(100)
		for tweet in api_call:
			pop.append(tweet)

	except:
		api = get_api2()
		api_call = tweepy.Cursor(api.search, q=keyword,since=sejak,
			until=sampai,lang='id',result_type=tipe,tweet_mode='extended' ).items(100)
		for tweet in api_call:
			pop.append(tweet)
	

	#it option save = True
	
	

	
	for p in range(len(pop)):
			status = Status(pop[p],kategori)
			pop[p] = status
			if save:
				status.insert_db()
				status.update_sum()
				
			
	return pop
def search_tweets_indikator():
	import time
	tanggal = datetime.now()
	#ambil indikator
	query_select = """
	select k.*, i.indikator 
	FROM katakunci_indikator k, indikator_ref i 
	WHERE i.id_indikator = k.id_indikator
	"""
	db = dbx()
	db.kursor.execute(query_select)
	results = db.kursor.fetchall()	
	#iterasi setiap keyword sbg k 
	
	i=0
	before = ""
	indikators = set([x[2] for x in results])
	tipe = 2
	
	for indikator in indikators:
		
		
		k = indikator
		#list semua yang bersesuaian dg indikator
		keyword_list = [key[1] for key in results if key[2] == indikator]
		keyword = ' OR '.join(keyword_list)
		hasil_api = []
		if len(keyword) >= 300:
			keyword = keyword.split(' OR ')
			p = len(keyword)
			keyword1 = ' OR '.join(keyword[0:(p//4)])
			keyword2 = ' OR '.join(keyword[(p//4):2*p//4])
			keyword3 = ' OR '.join(keyword[(2*p//4):3*p//4])
			keyword4 = ' OR '.join(keyword[(3*p//4):4*p//4])
			
			hasil_api.extend(gettweets_bykeyword(keyword1,tipe,save=False,rentang=6,kategori=k))
			hasil_api.extend(gettweets_bykeyword(keyword2,tipe,save=False,rentang=6,kategori=k))
			hasil_api.extend(gettweets_bykeyword(keyword3,tipe,save=False,rentang=6,kategori=k))
			hasil_api.extend(gettweets_bykeyword(keyword4,tipe,save=False,rentang=6,kategori=k))
		else:
			hasil_api = gettweets_bykeyword(keyword,tipe,save=False,rentang=0,kategori=k)
		
		#call api
		
		print("calling api for indikator ",indikator)
		with ThreadPoolExecutor(max_workers=7) as ex:
			hasil = []
			for h in hasil_api:
				#save with new thread
				f = ex.submit(h.insert_db)
				h.update_sum()
				hasil.append(f)
		
		[x.result() for x in hasil]
		print("sleeping")
		time.sleep(10)


	db.tutup()
	return 0
	
def analisis(topik):
	"""
	topik = topik yang akan diambil analisis nya 
	saat ini ada 2 topik yaitu 0 indikator BPS  dan 1 covid
	"""
	if topik==0:
		query_prov = '''select provinsi,sum(jumlah) as jumlah 
		from sum_tweets
		where jenis like '%indikator%' and length(provinsi) > 3
		group by provinsi
		order by jumlah desc 
		'''
		query_waktu = '''select tanggal, sum(jumlah) as jumlah 
		from sum_tweets
		where jenis like '%indikator%'
		group by tanggal
		'''
		query_ind =''' select kategori, sum(jumlah) as jumlah
		from sum_tweets
		where jenis like '%indikator%'
		group by kategori
		order by jumlah desc
		'''	
		db = dbx()
		db.kursor.execute(query_ind)
		result = db.kursor.fetchall()
		db.tutup()
		ind_sum = prov_sum = [{'indikator':r[0],'jumlah':int(r[1])} for r in result]
	else:
		query_prov = '''select provinsi,sum(jumlah) as jumlah 
		from sum_tweets
		where jenis like '%covid%' and length(provinsi) > 3
		group by provinsi
		order by jumlah desc 
		'''
		query_waktu = '''select tanggal, sum(jumlah) as jumlah 
		from sum_tweets
		where jenis like '%covid%'
		group by tanggal
		'''	
	db = dbx()
	db.kursor.execute(query_prov)
	result1 = db.kursor.fetchall()
	db.tutup()
	db = dbx()
	db.kursor.execute(query_waktu)
	result2 = db.kursor.fetchall()
	db.tutup()
	prov_sum = [{'provinsi':r[0],'jumlah':int(r[1])} for r in result1]
	waktu_sum = [{'tanggal':r[0].strftime('%Y-%m-%d'),'jumlah':int(r[1])} for r in result2]
	
	if topik ==0:
		hasil = {
	'provinsi_sum':prov_sum,
	'waktu_sum':waktu_sum,
	'indikator_sum': ind_sum
	}
	else:
		pop = getpopular_bykeyword('corona OR covid OR covid19 OR covid-19')
		hasil = {
	'provinsi_sum':prov_sum,
	'waktu_sum':waktu_sum,
	'popular':pop['popular'],
	'popular_today':pop['popular_today']
	}
			
	return hasil

	

def getpopular_bykeyword(keyword):

	with ThreadPoolExecutor(max_workers=7) as executor:
		future_pop = executor.submit(gettweets_bykeyword,keyword,1,False,6,keyword)
		future_popular_today = executor.submit(gettweets_bykeyword,keyword,1,False,0,keyword)

	popular = future_pop.result()
	popular = [p.__dict__ for p in popular]
	popular = list({p['status_text'] : p for p in popular}.values())
	popular = sorted(popular, key=itemgetter('status_retweets'), reverse=True)[0:10]
	
	popular_today = future_popular_today.result()
	popular_today = [p.__dict__ for p in popular_today]
	popular_today = list({p['status_text'] : p for p in popular_today}.values())
	popular_today = sorted(popular_today, key=itemgetter('status_retweets'), reverse=True)[0:10]

	hasil = {
	'popular':popular,
	'popular_today':popular_today
	}
	return hasil
if __name__ == '__main__':
	search_tweets_indikator();
	sys.exit('done')
	keyword = "corona OR covid"
	#search_tweets_indikator()
	tgl = [datetime.now()-timedelta(i) for i in range(6)]
	tipe=1
	with ThreadPoolExecutor(max_workers=7) as ex:
		i = 0
		api = get_api()
		fl = []
		for t in tgl:
			i += 1
			if i>=4:
				api = get_api2()
			f = ex.submit(gettweets_bykeyword,keyword,tipe,save=False,rentang=0,kategori="covid",tanggal=t,api = api)
			fl.append(f)
		fr = [f.result() for f in fl]
		for rx in fr:
			for r in rx:
				ex.submit(r.insert_db)
				ex.submit(r.update_sum,1)
	
	
	
	



