import flask 
#import simplejson as json
import json
from datetime import datetime,timedelta
"""import api.analisis_berita as analisis_berita
import api.ner_sentimen as ner_sentimen
from twitter.search import gettweets_bykeyword
"""
import api.analisis_berita as analisis_berita
import api.ner_sentimen as ner_sentimen
from twitter.search import gettweets_bykeyword
from twitter.search import analisis


from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from operator import itemgetter

app = flask.Flask(__name__)

@app.route('/get_beritadetail',methods=['GET'])
def get_beritadetail():
	args_ = flask.request.args
	err = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen ada yang terlewat atau tidak valid'}),
				        status=200,
				        mimetype='application/json')
	if 'katakunci' not in args_:
		return err

	if 'start' in args_:
		if 'end' in args_:
			start = args_['start']
			end = args_['end']
			katakunci = args_['katakunci']
			if 'batas' in args_:
				batas = args_['batas']
				try:
					batas = int(batas)
				except:
					batas=100
			else:
				batas = 100
			hasil = analisis_berita.getberitaby_keyword(katakunci,start,end,batas,0)
			response = app.response_class(
			        	response=json.dumps(hasil,default=str),
				        status=200,
				        mimetype='application/json')
			return response		
				
		else:
			
			return err
	else:
		return err


@app.route('/getsum_berita',methods=['GET'])
def getsum_berita():
	sum_berita = analisis_berita.gettotal()
	sum_sentimen = ner_sentimen.getsum_sentimen()
	list_hasil = {'sum_berita':sum_berita,'sum_sentimen':sum_sentimen}
	response = app.response_class(
		response=json.dumps(list_hasil),
		status=200,
		mimetype='application/json')
	return response

@app.route('/getsum_sumber',methods=['GET'])
def getsum_sumber():
	args_ = flask.request.args
	if 'start' in args_:
		if 'end' in args_:
			start = args_['start']
			end = args_['end']
			sum_ = analisis_berita.getsum_beritabysumber(start,end)
			response = app.response_class(
			        	response=json.dumps(sum_,default=str),
				        status=200,
				        mimetype='application/json')
			return response
		else:
			response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen tanggal berakhir tidak ada'}),
				        status=200,
				        mimetype='application/json')
			return response
	else:
		response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen tanggal mulai tidak ada'}),
				        status=200,
				        mimetype='application/json')
		return response

@app.route('/gettop_tags',methods=['GET'])
def gettop_tags():

	"""
	Definisi : Fungsi untuk mengambil 5 tag populer dalam suatu rentang waktu

	"""
	args_ = flask.request.args
	if 'start' in args_:
		if 'end' in args_:
			start = args_['start']
			end = args_['end']
			tags = analisis_berita.get_tag(start,end,5)
			
			response = app.response_class(
			        	response=json.dumps(tags,default=str),
				        status=200,
				        mimetype='application/json')
			return response
		else:
			response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen tanggal berakhir tidak ada'}),
				        status=200,
				        mimetype='application/json')
			return response
	else:
		response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen tanggal mulai tidak ada'}),
				        status=200,
				        mimetype='application/json')
		return response
@app.route('/gettop_indikator',methods=['GET'])
def gettop_indikator():

	"""
	Definisi : Fungsi untuk mengambil 5 indikator popular dalam suatu rentang waktu

	"""
	#code goes here
@app.route('/getdaily_sumber',methods=['GET'])
def getdaily_sumber():

	"""
	Definisi : fungsi untuk mengambil data summary harian jumlah berita berdasarkan sumber
	"""
	args_ = flask.request.args
	if 'start' in args_:
		if 'end' in args_:
			start = args_['start']
			end = args_['end']
			daily_sumber = analisis_berita.getdaily_beritabysumber(start,end)
			
			response = app.response_class(
			        	response=json.dumps(daily_sumber,default=str),
				        status=200,
				        mimetype='application/json')
			return response
		else:
			response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen tanggal berakhir tidak ada'}),
				        status=200,
				        mimetype='application/json')
			return response
	else:
		response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen tanggal mulai tidak ada'}),
				        status=200,
				        mimetype='application/json')
		return response

@app.route('/getdaily_indikator',methods=['GET'])
def getdaily_indikator():

	"""
	Definisi : fungsi untuk mengambil data summary harian jumlah berita berdasarkan kategori
	"""
	args_ = flask.request.args
	if 'start' in args_:
		if 'end' in args_:
			start = args_['start']
			end = args_['end']
			ind_ = args_['ind']
			dai_ind = analisis_berita.getdaily_beritabyindikator(start,end,ind_)
			response = app.response_class(
				response=json.dumps(dai_ind,default=str),
				status=200,
				mimetype='application/json')
			return response
			
			response = app.response_class(
			        	response=json.dumps(daily_sumber),
				        status=200,
				        mimetype='application/json')
			return response
		else:
			response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen tanggal berakhir tidak ada'}),
				        status=200,
				        mimetype='application/json')
			return response
	else:
		response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen tanggal mulai tidak ada'}),
				        status=200,
				        mimetype='application/json')
		return response
	


@app.route('/getsum_indikator',methods=['GET'])
def getsum_indikator():

	"""
	Definisi : fungsi untuk mengambil summary jumlah berita pada kategori indikator 114
	"""
	sum_ind = analisis_berita.getsum_indikator()
	response = app.response_class(
		response=json.dumps(sum_ind),
		status=200,
		mimetype='application/json')
	return response

@app.route('/getsum_ner',methods=['GET'])
def getsum_ner():
	"""
	Definisi : fungsi untuk mengambil Top berdasarkan kategori / indikator terpilih:
					1. Top Tokoh
					2. Top Jabatan / Posisi
					3. Top Organisasi
					4. Top Lokasi
					5. hasil sentimen kutipan
	"""
	args_ = flask.request.args
	if 'kategori' in args_:
		kategori = args_['kategori']
		top_tokoh = ner_sentimen.getsumner_byentitas('tokoh',kategori)
		top_posisi = ner_sentimen.getsumner_byentitas('posisi',kategori)
		top_organisasi = ner_sentimen.getsumner_byentitas('organisasi',kategori)
		top_lokasi = ner_sentimen.getsumner_byentitas('lokasi',kategori)
		sentimen = ner_sentimen.getsentimenkutipan_byind(kategori)
		hasil = {
		'tokoh':top_tokoh,
		'posisi':top_posisi,
		'organisasi':top_organisasi,
		'lokasi':top_lokasi,
		'sentimen':sentimen
		}
		response = app.response_class(
			response=json.dumps(hasil,default=str),
			status=200,
			mimetype='application/json')
		return response
	else:
		response = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen kategori / indikator tidak ada'}),
				        status=200,
				        mimetype='application/json')
		return response
	
@app.route('/get_words',methods=['GET'])
def get_words():
	"""
	Definisi : fungsi untuk mengambil data untuk visualisasi wordcloud kutipan berdasarkan kategori indikator tertentu

	"""
	#code goes here

@app.route('/getsna_tokoh',methods=['GET'])
def getsna_tokoh():
	"""
	Definisi : fungsi untuk mengambil data SNA antar tokoh
	"""
	#code goes here

@app.route('/getsna_indikator',methods=['GET'])
def getsna_indikator():
	"""
	Definisi : fungsi untuk mengambil data SNA antar indikator dan tokoh
	"""
	#code goes here

@app.route('/getsum_sentimen',methods=['GET'])
def getsum_sentimen():
	"""
	Definisi : fungsi untuk mengambil summary sentimen pada setiap kategori 
	"""
	err = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen kategori / indikator tidak ada'}),
				        status=200,
				        mimetype='application/json')
	args_ = flask.request.args
	if 'kategori' in args_:
		kategori = args_['kategori']
		if len(kategori) <=3 :
			return err
		sentimen = ner_sentimen.getsentimen_byind(kategori)
		kutipan = ner_sentimen.getsentimenkutipan_byind(kategori)
		hasil = {
		'sentimen':sentimen,
		'sentimen_kutipan':kutipan
		}
		
		response = app.response_class(
			response=json.dumps(hasil,default=str),
			status=200,
			mimetype='application/json')
		return response
	else:
		
		return err

@app.route('/twitter/gettweets_bykeywords',methods=['GET'])
def gettweets():
	
	args_ = flask.request.args
	if ('keyword' not in args_):
		err = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen kategori / indikator tidak ada'}),
				        status=200,
				        mimetype='application/json')
		return err
	tanggal_list = [(datetime.now()-timedelta(i))for i in range(7)]
	keyword = args_['keyword']
	try:
		tipe = int(args_['tipe'])
	except:
		tipe = 1
	i = 0
	#hasil_list = []
	jumlah_harian = []
	lokasi = []
	list_hasil = []
	with ThreadPoolExecutor(max_workers=7) as executor:
		for tanggal in tanggal_list:
			
			future = executor.submit(gettweets_bykeyword,keyword,1,False,0,keyword,tanggal)
			list_hasil.append(future)
		future_pop = executor.submit(gettweets_bykeyword,keyword,1,False,6,keyword)
		future_popular_today = executor.submit(gettweets_bykeyword,keyword,1,False,0,keyword)
	for i in range(len(list_hasil)):
		
		
		hasil = list_hasil[i].result()
		for h in hasil:
			if (str(h.provinsi) != 'None') and (len(str(h.provinsi))>2):
				lokasi.append(h.provinsi)
		harian = {
		'tanggal':tanggal_list[-i],
		'jumlah':len(hasil)
		}
		jumlah_harian.append(harian)
	jumlah_harian = sorted(jumlah_harian, key=itemgetter('tanggal'), reverse=False)
	lokasi = Counter(lokasi)
	popular = future_pop.result()
	popular = [p.__dict__ for p in popular]
	popular = list({p['status_text'] : p for p in popular}.values())
	popular = sorted(popular, key=itemgetter('status_retweets'), reverse=True)[0:10]
	
	popular_today = future_popular_today.result()
	popular_today = [p.__dict__ for p in popular_today]
	popular_today = list({p['status_text'] : p for p in popular_today}.values())
	popular_today = sorted(popular_today, key=itemgetter('status_retweets'), reverse=True)[0:10]
	
	hasil = {
	'jumlah_harian':jumlah_harian,
	'popular':popular,
	'popular_today':popular_today,
	'lokasi':lokasi
	}

	response = app.response_class(
			response=json.dumps(hasil,default=str),
			status=200,
			mimetype='application/json')
	return response


@app.route('/twitter/monitor',methods=['GET'])
def getmonitor():
	err = app.response_class(
			        	response=json.dumps({'pesan' : 'maaf argumen format request tidak sesuai'}),
				        status=200,
				        mimetype='application/json')
	args_ = flask.request.args
	if 'monitor' in args_:
		#do
		request = {'indikator':0,'covid':1}
		client_req = args_['monitor']
		if client_req in request:
			hasil = analisis(request[client_req])
			response = app.response_class(
				response=json.dumps(hasil),
				status=200,
				mimetype='application/json')
			return response
		else :
			return err
	else:
		
		return err

if __name__ == '__main__':
	#serve(app,port=5000)
	app.run()