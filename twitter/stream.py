from threading import Thread
import tweepy
import csv
import sys
from datetime import datetime,timedelta
from time import sleep
try:
  from twitter.Status import Status as s
  from twitter.search import gettweets_bykeyword
  from twitter.search import get_api
  from twitter.Database import Database_connection as db_
except:
  import Status.Status as s
  import search.gettweets_bykeyword as gettweets_bykeyword
  import search.get_api as get_api
  import Database.Database_connection as db_


class Status(s):
  def insert_db(self):
    
    db = db_()
    query = '''
    INSERT INTO `corona_twit` 
    (`id_indikator`,`status_posted`, `user_desc`, `user_status_count`, `user_name`, 
    `user_target_reply`, `user_verified`, `status_text`, `status_hashtags`, 
    `user_location`, `user_following`, `user_followers`, `retweets`, 
    `coordinate`, `country_code`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''
    param = (self.id_indikator,self.status_posted,self.user_desc,self.user_status_count,self.user_name,
             self.user_target_reply,self.user_verified,self.status_text,self.status_hashtags,
             self.user_location,self.user_following,self.user_followers,self.status_retweets,
             self.status_coordinate,self.country_code)
    try:
      db.kursor.execute(query,param)
      db.koneksi.commit()
    except Exception as ex:
      db.koneksi.rollback()
      print(ex)
    db.tutup()




def cek_wil(tup):
  list_wilayah = ['aceh',
 'simeulue',
 'aceh singkil',
 'aceh selatan',
 'aceh tenggara',
 'aceh timur',
 'aceh tengah',
 'aceh barat',
 'aceh besar',
 'pidie',
 'bireuen',
 'aceh utara',
 'aceh barat daya',
 'gayo lues',
 'aceh tamiang',
 'nagan raya',
 'aceh jaya',
 'bener meriah',
 'pidie jaya',
 'kota banda aceh',
 'kota sabang',
 'kota langsa',
 'kota lhokseumawe',
 'kota subulussalam',
 'sumatera utara',
 'nias',
 'mandailing natal',
 'tapanuli selatan',
 'tapanuli tengah',
 'tapanuli utara',
 'toba samosir',
 'labuhan batu',
 'asahan',
 'simalungun',
 'dairi',
 'karo',
 'deli serdang',
 'langkat',
 'nias selatan',
 'humbang hasundutan',
 'pakpak bharat',
 'samosir',
 'serdang bedagai',
 'batu bara',
 'padang lawas utara',
 'padang lawas',
 'labuhan batu selatan',
 'labuhan batu utara',
 'nias utara',
 'nias barat',
 'kota sibolga',
 'kota tanjung balai',
 'kota pematang siantar',
 'kota tebing tinggi',
 'kota medan',
 'kota binjai',
 'kota padangsidimpuan',
 'kota gunungsitoli',
 'sumatera barat',
 'kepulauan mentawai',
 'pesisir selatan',
 'solok',
 'sijunjung',
 'tanah datar',
 'padang pariaman',
 'agam',
 'lima puluh kota',
 'pasaman',
 'solok selatan',
 'dharmasraya',
 'pasaman barat',
 'kota padang',
 'kota solok',
 'kota sawah lunto',
 'kota padang panjang',
 'kota bukittinggi',
 'kota payakumbuh',
 'kota pariaman',
 'riau',
 'kuantan singingi',
 'indragiri hulu',
 'indragiri hilir',
 'pelalawan',
 'siak',
 'kampar',
 'rokan hulu',
 'bengkalis',
 'rokan hilir',
 'kepulauan meranti',
 'kota pekanbaru',
 'kota dumai',
 'jambi',
 'kerinci',
 'merangin',
 'sarolangun',
 'batang hari',
 'muaro jambi',
 'tanjung jabung timur',
 'tanjung jabung barat',
 'tebo',
 'bungo',
 'kota jambi',
 'kota sungai penuh',
 'sumatera selatan',
 'ogan komering ulu',
 'ogan komering ilir',
 'muara enim',
 'lahat',
 'musi rawas',
 'musi banyuasin',
 'banyu asin',
 'ogan komering ulu selatan',
 'ogan komering ulu timur',
 'ogan ilir',
 'empat lawang',
 'penukal abab lematang ilir',
 'musi rawas utara',
 'kota palembang',
 'kota prabumulih',
 'kota pagar alam',
 'kota lubuklinggau',
 'bengkulu',
 'bengkulu selatan',
 'rejang lebong',
 'bengkulu utara',
 'kaur',
 'seluma',
 'mukomuko',
 'lebong',
 'kepahiang',
 'bengkulu tengah',
 'kota bengkulu',
 'lampung',
 'lampung barat',
 'tanggamus',
 'lampung selatan',
 'lampung timur',
 'lampung tengah',
 'lampung utara',
 'way kanan',
 'tulangbawang',
 'pesawaran',
 'pringsewu',
 'mesuji',
 'tulang bawang barat',
 'pesisir barat',
 'kota bandar lampung',
 'kota metro',
 'kep. bangka belitung',
 'bangka',
 'belitung',
 'bangka barat',
 'bangka tengah',
 'bangka selatan',
 'belitung timur',
 'kota pangkal pinang',
 'kepulauan riau',
 'karimun',
 'bintan',
 'natuna',
 'lingga',
 'kepulauan anambas',
 'kota batam',
 'kota tanjung pinang',
 'dki jakarta',
 'kepulauan seribu',
 'kota jakarta selatan',
 'kota jakarta timur',
 'kota jakarta pusat',
 'kota jakarta barat',
 'kota jakarta utara',
 'jawa barat',
 'bogor',
 'sukabumi',
 'cianjur',
 'bandung',
 'garut',
 'tasikmalaya',
 'ciamis',
 'kuningan',
 'cirebon',
 'majalengka',
 'sumedang',
 'indramayu',
 'subang',
 'purwakarta',
 'karawang',
 'bekasi',
 'bandung barat',
 'pangandaran',
 'kota bogor',
 'kota sukabumi',
 'kota bandung',
 'kota cirebon',
 'kota bekasi',
 'kota depok',
 'kota cimahi',
 'kota tasikmalaya',
 'kota banjar',
 'jawa tengah',
 'cilacap',
 'banyumas',
 'purbalingga',
 'banjarnegara',
 'kebumen',
 'purworejo',
 'wonosobo',
 'magelang',
 'boyolali',
 'klaten',
 'sukoharjo',
 'wonogiri',
 'karanganyar',
 'sragen',
 'grobogan',
 'blora',
 'rembang',
 'pati',
 'kudus',
 'jepara',
 'demak',
 'semarang',
 'temanggung',
 'kendal',
 'batang',
 'pekalongan',
 'pemalang',
 'tegal',
 'brebes',
 'kota magelang',
 'kota surakarta',
 'kota salatiga',
 'kota semarang',
 'kota pekalongan',
 'kota tegal',
 'd i yogyakarta',
 'kulon progo',
 'bantul',
 'gunung kidul',
 'sleman',
 'kota yogyakarta',
 'jawa timur',
 'pacitan',
 'ponorogo',
 'trenggalek',
 'tulungagung',
 'blitar',
 'kediri',
 'malang',
 'lumajang',
 'jember',
 'banyuwangi',
 'bondowoso',
 'situbondo',
 'probolinggo',
 'pasuruan',
 'sidoarjo',
 'mojokerto',
 'jombang',
 'nganjuk',
 'madiun',
 'magetan',
 'ngawi',
 'bojonegoro',
 'tuban',
 'lamongan',
 'gresik',
 'bangkalan',
 'sampang',
 'pamekasan',
 'sumenep',
 'kota kediri',
 'kota blitar',
 'kota malang',
 'kota probolinggo',
 'kota pasuruan',
 'kota mojokerto',
 'kota madiun',
 'kota surabaya',
 'kota batu',
 'banten',
 'pandeglang',
 'lebak',
 'tangerang',
 'serang',
 'kota tangerang',
 'kota cilegon',
 'kota serang',
 'kota tangerang selatan',
 'bali',
 'jembrana',
 'tabanan',
 'badung',
 'gianyar',
 'klungkung',
 'bangli',
 'karang asem',
 'buleleng',
 'kota denpasar',
 'nusa tenggara barat',
 'lombok barat',
 'lombok tengah',
 'lombok timur',
 'sumbawa',
 'dompu',
 'bima',
 'sumbawa barat',
 'lombok utara',
 'kota mataram',
 'kota bima',
 'nusa tenggara timur',
 'sumba barat',
 'sumba timur',
 'kupang',
 'timor tengah selatan',
 'timor tengah utara',
 'belu',
 'alor',
 'lembata',
 'flores timur',
 'sikka',
 'ende',
 'ngada',
 'manggarai',
 'rote ndao',
 'manggarai barat',
 'sumba tengah',
 'sumba barat daya',
 'nagekeo',
 'manggarai timur',
 'sabu raijua',
 'malaka',
 'kota kupang',
 'kalimantan barat',
 'sambas',
 'bengkayang',
 'landak',
 'pontianak',
 'sanggau',
 'ketapang',
 'sintang',
 'kapuas hulu',
 'sekadau',
 'melawi',
 'kayong utara',
 'kubu raya',
 'kota pontianak',
 'kota singkawang',
 'kalimantan tengah',
 'kotawaringin barat',
 'kotawaringin timur',
 'kapuas',
 'barito selatan',
 'barito utara',
 'sukamara',
 'lamandau',
 'seruyan',
 'katingan',
 'pulang pisau',
 'gunung mas',
 'barito timur',
 'murung raya',
 'kota palangka raya',
 'kalimantan selatan',
 'tanah laut',
 'kota baru',
 'banjar',
 'barito kuala',
 'tapin',
 'hulu sungai selatan',
 'hulu sungai tengah',
 'hulu sungai utara',
 'tabalong',
 'tanah bumbu',
 'balangan',
 'kota banjarmasin',
 'kota banjar baru',
 'kalimantan timur',
 'paser',
 'kutai barat',
 'kutai kartanegara',
 'kutai timur',
 'berau',
 'penajam paser utara',
 'mahakam hulu',
 'kota balikpapan',
 'kota samarinda',
 'kota bontang',
 'kalimantan utara',
 'malinau',
 'bulungan',
 'tana tidung',
 'nunukan',
 'kota tarakan',
 'sulawesi utara',
 'bolaang mongondow',
 'minahasa',
 'kepulauan sangihe',
 'kepulauan talaud',
 'minahasa selatan',
 'minahasa utara',
 'bolaang mongondow utara',
 'siau tagulandang biaro',
 'minahasa tenggara',
 'bolaang mongondow selatan',
 'bolaang mongondow timur',
 'kota manado',
 'kota bitung',
 'kota tomohon',
 'kota kotamobagu',
 'sulawesi tengah',
 'banggai kepulauan',
 'banggai',
 'morowali',
 'poso',
 'donggala',
 'toli-toli',
 'buol',
 'parigi moutong',
 'tojo una-una',
 'sigi',
 'banggai laut',
 'morowali utara',
 'kota palu',
 'sulawesi selatan',
 'kepulauan selayar',
 'bulukumba',
 'bantaeng',
 'jeneponto',
 'takalar',
 'gowa',
 'sinjai',
 'maros',
 'pangkajene dan kepulauan',
 'barru',
 'bone',
 'soppeng',
 'wajo',
 'sidenreng rappang',
 'pinrang',
 'enrekang',
 'luwu',
 'tana toraja',
 'luwu utara',
 'luwu timur',
 'toraja utara',
 'kota makassar',
 'kota parepare',
 'kota palopo',
 'sulawesi tenggara',
 'buton',
 'muna',
 'konawe',
 'kolaka',
 'konawe selatan',
 'bombana',
 'wakatobi',
 'kolaka utara',
 'buton utara',
 'konawe utara',
 'kolaka timur',
 'konawe kepulauan',
 'muna barat',
 'buton tengah',
 'buton selatan',
 'kota kendari',
 'kota baubau',
 'gorontalo',
 'boalemo',
 'gorontalo',
 'pohuwato',
 'bone bolango',
 'gorontalo utara',
 'kota gorontalo',
 'sulawesi barat',
 'majene',
 'polewali mandar',
 'mamasa',
 'mamuju',
 'mamuju utara',
 'mamuju tengah',
 'maluku',
 'maluku tenggara barat',
 'maluku tenggara',
 'maluku tengah',
 'buru',
 'kepulauan aru',
 'seram bagian barat',
 'seram bagian timur',
 'maluku barat daya',
 'buru selatan',
 'kota ambon',
 'kota tual',
 'maluku utara',
 'halmahera barat',
 'halmahera tengah',
 'kepulauan sula',
 'halmahera selatan',
 'halmahera utara',
 'halmahera timur',
 'pulau morotai',
 'pulau taliabu',
 'kota ternate',
 'kota tidore kepulauan',
 'papua barat',
 'fakfak',
 'kaimana',
 'teluk wondama',
 'teluk bintuni',
 'manokwari',
 'sorong selatan',
 'sorong',
 'raja ampat',
 'tambrauw',
 'maybrat',
 'manokwari selatan',
 'pegunungan arfak',
 'kota sorong',
 'papua',
 'merauke',
 'jayawijaya',
 'jayapura',
 'nabire',
 'kepulauan yapen',
 'biak numfor',
 'paniai',
 'puncak jaya',
 'mimika',
 'boven digoel',
 'mappi',
 'asmat',
 'yahukimo',
 'pegunungan bintang',
 'tolikara',
 'sarmi',
 'keerom',
 'waropen',
 'supiori',
 'mamberamo raya',
 'nduga *',
 'lanny jaya',
 'mamberamo tengah',
 'yalimo',
 'puncak',
 'dogiyai',
 'intan jaya',
 'deiyai',
 'kota jayapura']
  tf = tup.lower() in ''.join(list_wilayah)
  return tf

def process_(status,key):

  if status.user_location:
      if 'bharat' in status.user_location.lower():
        return 0 
      if (len(status.user_location)>3) & cek_wil(status.user_location):
        status.insert_db()
        print("saved")
        return 0
  else:
        if status.country_code:
          if (status.country_code=='ID'):
            status.insert_db()
            print("saved")
            return 0
        
  


def stream_artif(key):
  
  api = get_api()
  tanggal = datetime.now()-timedelta(0)

  statuses = gettweets_bykeyword(key,2,False,0,key,tanggal)
  for status in statuses :
    
    Thread(target=status.insert_db,args=(1,)).start()
  
if __name__ == '__main__':
  keyword_list = ['corona','covid','covid19','covid-19','korona','dampak corona','indonesia corona']
  #print(get_api().rate_limit_status())
  while True:
    stream_artif(' OR '.join(keyword_list))
    sleep(5)


