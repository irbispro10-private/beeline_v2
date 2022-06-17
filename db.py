# -- coding: utf-8 --
import pypyodbc
from accessify import protected
class DB:
    def __init__(self, server, db, login, passwd):
        self.server =server
        self.db = db
        self.login = login
        self.passwd = passwd
        # self.cnxn = pymssql.connect(host=self.server, user=self.login, password=self.passwd, database=self.db, charset='utf8',autocommit=True)
        self.cnxn = pypyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.db + ';UID=' + self.login + ';PWD=' + self.passwd)
        self.cursor = self.cnxn.cursor()

    def insert(self, table, values):
        fields = self.get_columns(table)
        for i in range(len(fields)):
            if fields[i] == 'date':
                fields[i] = '[date]'
        sql ="INSERT INTO ["+self.db+"].dbo."+table+"("+', '.join(fields)+") VALUES("+', '.join(['\''+str(val)+'\'' for val in values])+");"
        # print(sql)
        self.cursor.execute(sql)

    def insert_if_not_exist(self, table, values):
        fields = self.get_columns(table)
        tmp_fields=[]
        tmp_fields=fields.copy()
        for i in range(len(fields)):
            if len(fields[i].split(' '))>1:
                fields[i] = '['+fields[i]+']'

        sql = "IF NOT EXISTS (SELECT * FROM [" + self.db + "].dbo." + table + " WHERE "+fields[0]+"='"+str(values[0])+"' AND "+fields[2]+"='"+str(values[2])+"' AND "+fields[3]+"='"+str(values[3])+"') " \
                "BEGIN " \
                "INSERT INTO [" + self.db + "].dbo." + table + "(" + ', '.join(fields) + ") VALUES(" + ', '.join(
                ['\'' + str(val) + '\'' for val in values]) + ") END;"
        # print(sql)
        self.cursor.execute(sql)
        if int(self.cursor.rowcount) == 1:
            return 1
        else:
            return 0

    def select(self, tab_name,column):
        self.cursor.execute("SELECT \""+column+"\" FROM ms_1.dbo."+tab_name)
        rows = self.cursor.fetchall()
        return rows

    def del_period(self, time_start, time_end):
        self.cursor.execute("DELETE FROM Skorozvon WHERE [date] > '"+time_start+"' AND [date] < '"+time_end+"';")
        # self.commit()

    @protected
    def get_columns(self, table):
        self.cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'"+table+"';")
        rows = self.cursor.fetchall()
        return [row[0] for row in rows]

    def clean_tab(self, table):
        self.cursor.execute("DELETE FROM ["+self.db+"].dbo."+table)

    def commit(self):
        self.cnxn.commit()

    def close(self):
        self.cnxn.close()