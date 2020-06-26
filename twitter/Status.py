from datetime import datetime
import re
try:
  from twitter.Database import Database_connection as db_
except:
  from Database import Database_connection as db_
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
  def __init__(self,status,id_indikator=""):

    #definis variable yang gamungkin null
    self.id_indikator = id_indikator
    self.status_posted = str(status.created_at)
    self.user_desc = bersih(str(status.user.description))
    self.user_status_count = status.user.statuses_count
    self.user_name = status.user.screen_name
    try:
      self.user_target_reply = status.in_reply_to_screen_name
    except:
      self.user_target_reply = None
    self.user_verified = status.user.verified
    try:
      self.status_text = bersih(status.full_text)
    except:
      self.status_text =  bersih(status.text)
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
  def insert_db(self,table=0):
    db = db_()
    query = '''
    INSERT INTO `tweets`
    (`id_indikator`,`status_posted`, `user_desc`, `user_status_count`, `user_name`, 
    `user_target_reply`, `user_verified`, `status_text`, `status_hashtags`, 
    `user_location`, `user_following`, `user_followers`, `retweets`, 
    `coordinate`, `country_code`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''
    param = (self.id_indikator,self.status_posted,self.user_desc,self.user_status_count,self.user_name,
             self.user_target_reply,self.user_verified,self.status_text,self.status_hashtags,
             self.user_location,self.user_following,self.user_followers,self.status_retweets,
             self.status_coordinate,self.country_code)
    if table == 1:
      query = '''
      INSERT INTO `corona_twit`
      (`id_indikator`,`status_posted`, `user_desc`, `user_status_count`, `user_name`, 
      `user_target_reply`, `user_verified`, `status_text`, `status_hashtags`, 
      `user_location`, `user_following`, `user_followers`, `retweets`, 
      `coordinate`, `country_code`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      '''

    try:
      db.kursor.execute(query,param)
      db.koneksi.commit()
    except Exception as ex:
      db.koneksi.rollback()
      print(ex)
      raise "error"
    db.tutup()
    

    