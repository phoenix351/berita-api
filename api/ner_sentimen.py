from Database_connection import Database_connection as db_
db = db_()

def getsum_sentimen():
	db = db_()
	query = """
	select distinct(sentimen),count(*) as jumlah
	from sentimen
	where sentimen = 'isi'
	group by sentimen
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
		'sentimen':baris[0],
		'jumlah':baris[1]
		}
		k.append(objek)
	db.koneksi.close()
	
	return k
def getsentimen_byind(indikator):
	db = db_()
	param = (('%'+indikator+'%'),)
	query = """
	select distinct(sentimen),count(*) as jumlah
	from sentimen
	where sentimen ='isi' and indikator like ? 
	group by sentimen
	order by sentimen desc
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
		'sentimen':baris[0],
		'jumlah':baris[1]
		}
		k.append(objek)
	db.koneksi.close()
	
	return k
def getsentimenkutipan_byind(indikator):
	db = db_()
	param = (('%'+indikator+'%'),)
	query = """
	select distinct(sentimen),count(*) as jumlah
	from sentimen
	where sentimen='kutipan' and indikator like ?
	group by sentimen
	order by sentimen desc
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
		'sentimen':baris[0],
		'jumlah':baris[1]
		}
		k.append(objek)
	db.koneksi.close()
	return k
def getsumner_byentitas(jenis,indikator):
	param = ('%'+jenis+'%','%'+indikator+'%')
	query = '''
	select entitas, sum(jumlah) as jumlah 
	from sum_ner
	where jenis_entitas like ? and  indikator like ?
	group by entitas
	order by jumlah desc
	limit 10
	'''
	db = db_()
	db.kursor.execute(query,param)
	hasil = db.kursor.fetchall()
	k = []
	for baris in hasil:
		objek ={
		'entitas':baris[0],
		'jumlah':baris[1]
		}
		k.append(objek)
	db.tutup()
	return k


if __name__ == '__main__':
	print(getsentimen_byind('ekspor'))