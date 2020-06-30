from datetime import datetime
import re
try:
  from twitter.Database import Database_connection as db_
except:
  from Database import Database_connection as db_
import pandas as pd
import numpy as np
import os

path = os.path.abspath('twitter/')
kabkot = pd.read_csv(path+"as.csv",engine='python',header=None)
kabkot.columns = ['kode_kab','kode_prov','nama_kab']
prov = pd.read_csv(path+"prov.csv",engine='python',header=None)
prov.columns = ['kode_prov','nama_prov']
prov_dict = prov.set_index('kode_prov').to_dict()['nama_prov']
kabkot.nama_kab = kabkot.nama_kab.apply(lambda x : x.replace('KABUPATEN ','').replace('KOTA ',''))
kabkot.kode_prov = kabkot.kode_prov.apply(lambda x : prov_dict[x])
kabkot_dict = kabkot.set_index('nama_kab').to_dict()['kode_prov']


def koreksi(kota):
  kota = kota.lower()
  kota = kota.replace('kota','').replace('kabupaten','').replace('provinsi','').replace('indonesia','')
  kota = ' '.join(kota.split())
  regex = re.compile('[^a-zA-Z\s]')
  #First parameter is the replacement, second parameter is your input string
  kota = regex.sub('', kota)
  
  if 'jakarta' in kota:
    return 'jakarta pusat'
  if 'papua' in kota:
    return 'jayapura'
  if  ('yogya' in kota) or ('jogja' in kota) or ('djogja' in kota):
    return 'sleman'
  if 'bandung' in kota:
    return 'bandung'
  if 'dumai' in kota:
    return 'riau'
  if kota in ['bangka','belitung']:
    return 'kepulauan bangka belitung'
  
  
  return kota


def kota2prov(kota):
  kota = koreksi(kota)
  try:
    kota = kota.upper()
  except:
    return None
  
  logic1 = prov.nama_prov.str.contains(kota,case=False,regex=False)

  logic1 = logic1.any()

  for provx in prov.nama_prov:
    if provx in kota:
      return provx 

  if logic1:
    return kota
  else:
    logic2 = kota in kabkot_dict.keys()
    if logic2:
      kota = kabkot_dict[kota]
      return kota
  return None

def bersih(teks):
  
  #remove url 
  teks = re.sub(r'&\S*;', ' ', teks, flags=re.MULTILINE)
  teks = re.sub(r'\n', ' ', teks, flags=re.MULTILINE)
  teks = re.sub(r'https*:\/\/[\S]*', ' ', teks, flags=re.MULTILINE)
  #teks = re.sub(r'RT\s@\w+:', ' ', teks, flags=re.MULTILINE)
  return teks

class Status:
  '''
  status_posted = datetime.now()
  user_desc = ""
  user_status_count=0
  user_name = ""
  user_target_reply = None
  user_verified = False
  status_text = ""
  status_hashtags = ""
  user_location=None
  user_following = 0
  user_followers = 0
  status_retweet_count = 0
  status_coordinate = None
  country_code = None
  '''
  def __init__(self,status,kategori=""):

    #definis variable yang gamungkin null
    self.kategori = kategori
    self.status_posted = str(status.created_at)
    self.user_desc = bersih(str(status.user.description)).__repr__()
    self.user_status_count = status.user.statuses_count
    self.user_name = status.user.screen_name
    try:
      self.user_target_reply = status.in_reply_to_screen_name
    except:
      self.user_target_reply = None
    self.user_verified = status.user.verified
    try:
      self.status_text = bersih(status.full_text).__repr__()
    except:
      self.status_text =  bersih(status.text).__repr__()
    self.status_hashtags = str(status.entities['hashtags'])
    self.user_following = status.user.friends_count
    self.user_followers = status.user.followers_count
    self.status_retweets = status.retweet_count
    try:
      self.user_location =  status.user.location
    except:
      self.user_location = None
    try:
      self.status_coordinate =  str(status.coordinates)
    except:
      self.status_coordinate =  None
    try:
      self.country_code = status.place.country_code
    except:
      self.country_code = None
    try:
      self.provinsi = kota2prov(self.user_location)
    except:
      self.provinsi = "NA"

  def insert_db(self):
    db = db_()
    query = '''
    INSERT INTO `tweets`
    (`kategori`,`status_posted`, `user_desc`, `user_status_count`, `user_name`, 
    `user_target_reply`, `user_verified`, `status_text`, `status_hashtags`, 
    `user_location`,`provinsi` , `user_following`, `user_followers`, `retweets`, 
    `coordinate`, `country_code`) 
    VALUES (%s,%s,%s,%s,
    %s,%s,%s,%s,
    %s,%s,%s,%s,
    %s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE retweets = %s

    '''
    param = (self.kategori,self.status_posted,self.user_desc,self.user_status_count,self.user_name,
             self.user_target_reply,self.user_verified,self.status_text,self.status_hashtags,
             self.user_location,self.provinsi,self.user_following,self.user_followers,self.status_retweets,
             self.status_coordinate,self.country_code,self.status_retweets)
    
    try:
      db.kursor.execute(query,param)
      db.koneksi.commit()
    except Exception as ex:
      db.koneksi.rollback()
      print(ex)
    db.tutup()
  def update_sum(self,tipe=0):
    """
    tipe = 0 apabila monitoring indikator 
            1 apabila monitoring covid
    """
    print('saving...')
    if len(str(self.provinsi))<=3:
      provx = "NA"
    else:
      provx = self.provinsi
    jenis = ['indikator','covid']
    tipe = jenis[int(tipe)]
    param = (self.status_posted,self.kategori,provx,tipe)
    query = """
    INSERT INTO sum_tweets
    VALUES(%s,%s,%s,1,%s)
    ON DUPLICATE KEY
    UPDATE jumlah = jumlah + 1
    """
   
    db = db_()
    try:
      db.kursor.execute(query,param)
      db.koneksi.commit()
    except Exception as ex:
      db.koneksi.rollback()
      print(ex)
    db.tutup()
if __name__ == '__main__':
    print(kota2prov("kalimantan "))
