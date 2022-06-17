import requests, sqlite3
from datetime import datetime,timedelta
now = datetime.now()-timedelta(hours=2)
dt_string = now.strftime("%y-%m-%d %H:%M:%S")
print(dt_string)
conn_sql = sqlite3.connect("/home/beeline/binotell.db") # или :memory: чтобы сiохранить в RAM
cursor_sql = conn_sql.cursor()
cursor_sql.execute("SELECT SUM(num_rows) FROM binotell_log WHERE id=1 AND date_>='"+dt_string+"';")
incomming = cursor_sql.fetchone()[0]
cursor_sql.execute("SELECT SUM(num_rows) FROM binotell_log WHERE id=0 AND date_>='"+dt_string+"';")
outgoing=cursor_sql.fetchone()[0]

requests.get('https://api.telegram.org/bot5240373199:AAGGczxN1guUWVVAZi5psAuPSdsyKWM24P0/sendMessage?chat_id=@binotell&text=incomming:'+str(incomming)+' outgoing:'+str(outgoing))
