from beeline_api import beeline
from db import DB
import sys, datetime,time, sqlite3, logging

def main():
    _beeline = beeline('686f9b8b-f02d-4131-9175-49e9abf3da20')
    db = DB(server='fromnmpz61.database.windows.net', login='owner-jdoomfm', db='mybi-jdoomfm', passwd='bhX1ayj2WN0X')
    if sys.argv[1] == '-d':
        days = int(sys.argv[2])
        # date_to = datetime.datetime.now()
        date_to = datetime.datetime(year=2022, month=6, day=8, hour=0,minute=0,second=1)
        dt_stop = date_to - datetime.timedelta(days=days)
        date_from = date_to - datetime.timedelta(hours=6)
        counter_incomming = 0
        counter_outgoing = 0
        counter = 0
        while (dt_stop <= date_to):
            calls = _beeline.get_statistics(date_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                            date_to.strftime("%Y-%m-%dT%H:%M:%SZ"))
            print(date_from.strftime("%Y-%m-%dT%H:%M:%SZ"), date_to.strftime("%Y-%m-%d %H:%M:%S"))
            date_to = date_from
            date_from = date_to - datetime.timedelta(hours=6)
            counter += 1
            if counter%10==0:
                time.sleep(60)

            for call in calls:
                mp3 = None
                if call['direction'] == 'INBOUND':
                    if (call['duration']>0):
                        mp3 = _beeline.get_record_id_by_user(date_from=(datetime.datetime.utcfromtimestamp(int(call['startDate']) / 1000) - datetime.timedelta(minutes=2)).strftime(
                        "%Y-%m-%dT%H:%M:%S.000Z"), date_to=(datetime.datetime.utcfromtimestamp(int(call['startDate']) / 1000) + datetime.timedelta(minutes=2)).strftime(
                        "%Y-%m-%dT%H:%M:%S.000Z"), userid=call['abonent']['userId'], phone=call['phone_from'], direction='INBOUND', duration=call['duration'])

                    values = [(datetime.datetime.utcfromtimestamp(int(call['startDate']) / 1000) + datetime.timedelta(hours=3)).strftime(
                        "%Y-%m-%d %H:%M:%S"), call['status'], call['phone_from'], call['abonent']['phone'],
                              call['duration'], call['abonent']['lastName'], call['abonent']['email'], mp3]


                    counter_incomming += db.insert_if_not_exist('binotell_incomming', values)


                else:
                    if (call['duration'] > 0):
                        mp3 = _beeline.get_record_id_by_user(date_from=(datetime.datetime.utcfromtimestamp(
                            int(call['startDate']) / 1000) - datetime.timedelta(minutes=2)).strftime(
                            "%Y-%m-%dT%H:%M:%S.000Z"), date_to=(datetime.datetime.utcfromtimestamp(
                            int(call['startDate']) / 1000) + datetime.timedelta(minutes=2)).strftime(
                            "%Y-%m-%dT%H:%M:%S.000Z"), userid=call['abonent']['userId'], phone=call['phone_to'],
                            direction='OUTBOUND', duration=call['duration'])

                    values = [(datetime.datetime.utcfromtimestamp(int(call['startDate']) / 1000) + datetime.timedelta(hours=3)).strftime(
                        "%Y-%m-%d %H:%M:%S"), call['status'], call['abonent']['phone'], call['phone_to'],
                        call['duration'], call['abonent']['lastName'], call['abonent']['email'], mp3]

                    counter_outgoing += db.insert_if_not_exist('binotell_outgoing', values)



            db.commit()

            conn_sql = sqlite3.connect("/home/beeline/binotell.db")  # или :memory: чтобы сiохранить в RAM
            cursor_sql = conn_sql.cursor()
            now = datetime.now()
            dt_string = now.strftime("%y-%m-%d %H:%M:%S")
            sql = "INSERT INTO binotell_log VALUES('" + dt_string + "'," + str(counter_incomming) + ",1);"
            cursor_sql.execute(sql)
            sql = "INSERT INTO binotell_log VALUES('" + dt_string + "'," + str(counter_outgoing) + ",0);"
            cursor_sql.execute(sql)
            conn_sql.commit()

if __name__=='__main__':
    main()