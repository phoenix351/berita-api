from Database_connection import Database_connection as db_
from re import sub as ganti
from collections import Counter
import operator
db = db_()
def kata2list(kat):
	kata = kat
	kata = ganti("[\[\]\']","",kata)
	kata = ganti('"',"",kata)
	kata_v = []
	kata_s = kata.strip().split(",")
	for x in range(len(kata_s)):
		kbx = kata_s[x].strip()
		kata_v.append(kbx)
  
	return kata_v

def getberitaby_keyword(keyword,start,end,limit,offset=0):
	db = db_()
	param = (('%'+keyword+'%'),start,end,limit,offset)
	query = """select * from berita_detail 
	where isi like %s and waktu >= %s and waktu <= %s
	limit %s offset %s"""
	try:
		db.kursor.execute(query,param)
	except :
		raise
	hasil = db.kursor.fetchall()
	k = []
	for baris in hasil:
			objek ={
				'id_berita':baris[0],
				'tag':baris[1],
				'judul':baris[2],
				'penulis':baris[3],
				'sumber':baris[4],
				'waktu':baris[5],
				'isi':baris[6]

			}
			k.append(objek)
	db.koneksi.close()
	return k
def getsum_beritabysumber(start,end):
	db = db_()
	param = (start,end)
	query = """
	select sumber,sum(jumlah ) as jumlah
	from beritasum_sumber 
	where waktu >= %s and waktu <= %s
	group by sumber
	"""
	try:
		db.kursor.execute(query,param)
		#db.kursor.execute('select distinct(sumber) from berita_detail')
	except :
		
		raise
	hasil = db.kursor.fetchall()
	nilai = []
	for baris in hasil:
		objek = {
		'sumber':str.capitalize(baris[0]),
		'jumlah':baris[1]
		}
		nilai.append(objek)
	db.koneksi.close()
	return nilai
def getdaily_beritabysumber(start,end):
	db = db_()
	param = (start,end)
	query = """
	select sumber,waktu,jumlah
	from beritasum_sumber
	where waktu >= %s and waktu <= %s
	"""
	db.kursor.execute(query,param)
	hasil = db.kursor.fetchall()
	k = []
	for baris in hasil:
		objek = {
		'sumber':baris[0],
		'waktu':baris[1],
		'jumlah':baris[2]
		}
		k.append(objek)
	db.tutup()
	return k

def getdaily_beritabyindikator(start,end,indikator):
	db = db_()
	param = ('%'+indikator+'%',start,end)
	query = """
	select waktu,jumlah
	from beritasum_indikator
	where indikator like %s and waktu >= %s and waktu <= %s
	"""
	db.kursor.execute(query,param)
	hasil = db.kursor.fetchall()
	k = []
	for baris in hasil:
		objek = {
		'waktu':baris[0],
		'jumlah':baris[1]
		}
		k.append(objek)
	db.tutup()
	return k
def get_tag(start,end,limit=0):
	db = db_()
	param = (start,end,limit)
	query = """
	select tag, sum(jumlah) as jumlah
	from sum_tag 
	where waktu >= %s and waktu <= %s
	group by tag
	order by jumlah desc
	limit %s
	"""
	
	db.kursor.execute(query,param)
	ar=[]
	hasil = db.kursor.fetchall()
	for baris in hasil:
		obj = {
		'nama_tag':baris[0],
		'jumlah':baris[1]
		}
		ar.append(obj)

	
	
	db.koneksi.close()
	
	return ar			
def get_indikator(start,end,limit):
	db = db_()
	param = (start,end)
	query = """
	select indikator,jumlah
	from beritasum_indikator
	where waktu >= %s and waktu <= %s 
	group by indikator
	order by jumlah desc
	"""
	try:
		db.kursor.execute(query,param)
		#db.kursor.execute('select distinct(sumber) from berita_detail')
	except :
		raise
	hasil = db.kursor.fetchall()
	k = []
	for baris in hasil:
		objek = {
		'sub_indikator':baris[0],
		'jumlah_berita':baris[1]
		}
		k.append(objek)
	db.koneksi.close()
	if limit == 0:
		limit = len(hasil)
	return k[0:limit]

def getsum_indikator():
	db = db_()
	query = """
	select id_indikator,jumlah
	from indikator_sum
	"""
	try:
		db.kursor.execute(query)
		#db.kursor.execute('select distinct(sumber) from berita_detail')
	except :
		raise
	hasil = db.kursor.fetchall()
	k = []
	for baris in hasil:
		objek = {
		'id_indikator':baris[0],
		'jumlah_berita':baris[1]
		}
		k.append(objek)
	db.koneksi.close()
	
	return k

def gettotal():
	db = db_()
	query = """
	select count(distinct(b.id_berita)) as totalberita, count(distinct(n.id_berita)) as totalberita_bps, count(distinct(b.sumber)) as totalsumber
	from berita_detail b, ner_output n
	"""
	
	
		
	objek = {
		'totalberita':348850,
		'totalberita_bps':12952,
		'totalsumber':6
		}
	db.koneksi.close()
	
	return objek

if __name__ == '__main__':
	
	print(getberitaby_keyword('inflasi','2020-01-01','2020-02-02',10,0))
