from sqlite3 import Error
import sqlite3
import os.path
class Database_sqlite:
	def __init__(self):
		BASE_DIR = os.path.dirname(os.path.abspath(__file__))
		db_path = os.path.join(BASE_DIR, r"db-lite.db")
		self.koneksi = sqlite3.connect(db_path)
		self.kursor = self.koneksi.cursor()
	def tutup(self):
		self.koneksi.close()
