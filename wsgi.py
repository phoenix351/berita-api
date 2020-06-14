from api.main import app 
from flask_cors import CORS
import os
#from waitress import serve
#1234

app.config["DEBUG"] = False
CORS(app)

if __name__ == "__main__":
	port = int(os.environ.get('PORT',5000))
	app.run(host='127.0.0.1',port=port)