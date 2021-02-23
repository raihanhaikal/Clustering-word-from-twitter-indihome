import mysql.connector as mysql
import pandas as pd

db = mysql.connect(
    host = 'localhost',
    user = 'root',
    passwd = '',
    database = "twittersony"
)

cursor = db.cursor()

#query select dan insert
select = "select id_user ,cuitan ,tanggal_cuit from tweet"

#mengambil text
dataBase = cursor.execute(select)
records = cursor.fetchall()
dfData = pd.DataFrame(records)

dfData.to_csv("dump_twitter.csv", header=["User_ID", "Tweet", "Timestamp"], index=False)