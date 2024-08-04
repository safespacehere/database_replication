import psycopg2
from psycopg2 import Error
from time import sleep
import random
from threading import Thread

status = 1
bd_list = ['pmib9505.cbd', 'pmib9505.pbd1', 'pmib9505.pbd2']
bd_short_name_list = ['cbd', 'pbd1', 'pbd2']


def simulation_prorgamm(time_interval):
    try:
        # Подключение к базе данных
        connection = psycopg2.connect(user="user",
                                  password="password",
                                  host="127.0.0.1",
                                  database="students")
    except (Exception, Error) as error:
        print("Ошибка при подключении к базе данных: ", error)
        exit()


    while status:
        rand = random.randint(0, 2)
        current_bd = bd_list[rand]
        bd_short_name = bd_short_name_list[rand]
        operation = random.randint(1, 3)
        if operation == 1:  # Вставка
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute("select max(id) from " + current_bd)
                max_id = cursor.fetchone()
                if max_id[0] is not None:
                    max_id = str(max_id[0] + 1)
                else:
                    max_id = str(1)
                insert = "insert into " + current_bd + " values(" + max_id + ", \'Новая страна " +\
                    max_id + "\', \'Глава страны " + max_id + "\', current_timestamp," \
                                     " \'Вставка в " + bd_short_name + "\');\n"
                record_id = max_id
                cursor.execute("select max(change_id) from pmib9505.change_journal")
                max_id = cursor.fetchone()
                if max_id[0] is not None:
                    max_id = str(max_id[0] + 1)
                else:
                    max_id = str(1)
                insert += ("insert into "\
                    "pmib9505.change_journal(change_id, change_time, changed_bd, record_id, new_string, operation_id)"\
                    "select" + max_id + ", " + "change_time, " + "\'" + bd_short_name + "\', "
                    "id, country || \'; \' || head_of_state, operation_id from " + current_bd + " where id = " + record_id + ";)")
                cursor.execute(insert)
                connection.commit()
            except(Exception, Error) as error:
                print("Ошибка при выполнении операции вставки: ", error)
                if cursor is not None:
                    connection.rollback()
            finally:
                if cursor is not None:
                    cursor.close()
        elif operation == 2:  # Обновление
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute("select min(id) from " + current_bd)
                min_id = cursor.fetchone()
                min_id = str(min_id[0])
                update = "update " + current_bd + " set head_of_state = \'Обновление главы \' || current_time, " \"change_time = current_timestamp, "\
                                                    "operation_id = \'Обновление в " + bd_short_name + "\' where id = " + min_id +";\n"
                record_id = min_id
                cursor.execute("select max(change_id) from pmib9505.change_journal")
                max_id = cursor.fetchone()
                if max_id[0] is not None:
                    max_id = str(max_id[0] + 1)
                else:
                    max_id = str(1)
                cursor.execute("select country || \'; \' || head_of_state from " + current_bd + " where id = " + min_id)
                old_string = cursor.fetchone()
                update += "insert into " \
                  "pmib9505.change_journal(change_id, change_time, changed_bd, record_id, new_string, old_string, operation_id)"\
                          "select" + max_id + ", " + "change_time, " + "\'" + bd_short_name + "\', " \
                                                                                         "id, country || \'; \' || head_of_state, " + "\'" + old_string[0] + "\'" +\
                    ", operation_id from " + current_bd + " where id = " + record_id + ";"
                cursor.execute(update)
                connection.commit()
            except(Exception, Error) as error:
                print("Ошибка при выполнении операции обновления: ", error)
                if cursor is not None:
                    connection.rollback()
            finally:
                if cursor is not None:
                    cursor.close()
        else:  # Удаление
            cursor = None
            try:
            cursor = connection.cursor()
            cursor.execute("select max(id) from " + current_bd)
            max_id = cursor.fetchone()
            max_id = str(max_id[0])
            delete = "delete from " + current_bd + " where id = " + max_id + ";\n"
            record_id = max_id
            cursor.execute("select max(change_id) from pmib9505.change_journal")
            max_id = cursor.fetchone()
            if max_id[0] is not None:
                max_id = str(max_id[0] + 1)
            else:
                max_id = str(1)
            cursor.execute("select country || \'; \' || head_of_state from " + current_bd + " where id = " + record_id)
            old_string = cursor.fetchone()
            delete += "insert into pmib9505.change_journal values(" + max_id + ",current_timestamp, " + "\'" + bd_short_name +"\'"\
             ", " + record_id + ", null, " + "\'" + old_string[0] + "\'" + ", \'Удаление из " + bd_short_name + "\');"
            cursor.execute(delete)
            connection.commit()
            except(Exception, Error) as error:
                print("Ошибка при выполнении операции удаления: ", error)
                if cursor is not None:
                    connection.rollback()
            finally:
                if cursor is not None:
                    cursor.close()
        sleep(time_interval)
    connection.close()
    exit()

def replicator(number_of_transactions):
    try:
        # Подключение к базе данных
        connection = psycopg2.connect(user="pmi-b9505",
                                  password="Pyenbym8",
                                  host="students.ami.nstu.ru",
                                  database="students")
    except (Exception, Error) as error:
        print("Ошибка при подключении к базе данных: ", error)
        exit()
    time = 0
    while status:
        if time == 0:
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute("select current_timestamp")
                time = cursor.fetchone()
                time = str(time[0])
                connection.commit()
            except(Exception, Error) as error:
                print("Ошибка при получении текущего времени на сервере: ", error)
                if cursor is not None:
                    connection.rollback()
            finally:
                if cursor is not None:
                    cursor.close()
            current_transactions = 0
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute("select count(*) from pmib9505.change_journal where change_time >
                timestamp \'" + time + "\'")
                current_transactions = cursor.fetchone()
                current_transactions = int(current_transactions[0])
                connection.commit()
            except(Exception, Error) as error:
                print("Ошибка при выполнении операции вставки: ", error)
                if cursor is not None:
                    connection.rollback()
            finally:
                if cursor is not None:
                    cursor.close()
            if current_transactions == number_of_transactions:
                replication = "lock table pmib9505.cbd, pmib9505.pbd1, pmib9505.pbd2, pmib9505.change_journal in exclusivemode;\n"
                # Формирование запроса для репликации из пбд1 в цбд (с приоритетом пбд1 над цбд)
                bd_recieve = bd_list[0]  # pmib9505.cbd
                bd_send = bd_list[1]  # pmib9505.pbd1
                bd_send_short = bd_short_name_list[1]  # pbd1
                replication += "insert into " + bd_recieve + " select * from " + bd_send + " where id not in " \
                                                                                           "(select id from " + bd_recieve + ");\n"
                replication += ("delete from " + bd_recieve + " where id not in (select id from " + bd_send + ");\n")
                replication += "update " + bd_recieve + " set country = " + bd_send + ".country, " \
                                                                                    "head_of_state = " + bd_send + ".head_of_state, " \
                                                                                    "change_time = " + bd_send + ".change_time, " \
                                                                                    "operation_id = " + bd_send + ".operation_id from " + bd_send + \
                                                                                    " where " + bd_send + ".id = " + bd_recieve + ".id;\n"
                # Формирование запроса для репликации из пбд2 в цбд (с приоритетом пбд2 над цбд)
                bd_recieve = bd_list[0]  # pmib9505.cbd
                bd_send = bd_list[2]  # pmib9505.pbd2
                bd_send_short = bd_short_name_list[2]  # pbd2
                replication += "insert into " + bd_recieve + " select * from " + bd_send + " where id not in " \
                                "(select id from" + bd_recieve + ") and \'Удаление из pbd1\' <> (select operation_id from" \
                                "pmib9505.change_journal where" + bd_send + ".id = record_id" \
                                "order by change_time desc limit1);\n"
                replication += "delete from " + bd_recieve + " where id not in (select id from " + bd_send + ") " \
                                "and substring(operation_id from \'pbd1\') <> " \
                                "\'pbd1\';\n"
                replication += "update " + bd_recieve + " set country = " + bd_send + ".country, " \
                                                                      "head_of_state = " + bd_send + ".head_of_state, " \
                                                                        "change_time = " + bd_send + ".change_time, " \
                                                                         "operation_id = " + bd_send + ".operation_id from " + bd_send + \
                                                                        " where " + bd_send + ".id = " + bd_recieve + ".id " \
                                                                        "and substring(" + bd_send + ".operation_id from \'pbd1\') <> " \
                                                                                          "\'pbd1\';\n"
                # Формирование запроса для репликации из пбд2 в цбд с устранением коллизий между пбд1 и пбд2
                # в пользу более позднего обновления
                replication += "delete from " + bd_recieve + " where id not in (select id from " + bd_send + ") " \
                                "and \'Удаление из pbd2\' = (select operation_id from " \
                                "pmib9505.change_journal where " + bd_recieve + ".id = record_id " \
                                  "order by change_time desc limit 1);\n"
                replication += "update " + bd_recieve + " set country = " + bd_send + ".country, " \
                                "head_of_state = " + bd_send + ".head_of_state, " \
                                "change_time = " + bd_send + ".change_time, " \
                                "operation_id = " + bd_send + ".operation_id from " + bd_send + \
                               " where " + bd_send + ".id = " + bd_recieve + ".id " \
                                "and \'Обновление в pbd2\' = (select operation_id from " \
                                 "pmib9505.change_journal where " + bd_recieve + ".id = record_id " \
                                 "order by change_time desc limit 1);\n"
                # Формирование запроса для репликации из цбд в пбд1 (с приоритетом пбд1 над цбд)
                bd_recieve = bd_list[1]  # pmib9505.pbd1
                bd_send = bd_list[0]  # pmib9505.cbd
                bd_send_short = bd_short_name_list[0]  # cbd
                replication += "insert into " + bd_recieve + " select * from " + bd_send + " where id not in " \ "(select id from" + bd_recieve + ");\n"
                replication += "delete from " + bd_recieve + " where id not in (select id from " + bd_send + ");\n"
                replication += "update " + bd_recieve + " set country = " + bd_send + ".country, " \
                                "head_of_state = " + bd_send + ".head_of_state, " \
                                "change_time = " + bd_send + ".change_time, " \
                                "operation_id = " + bd_send + ".operation_id from " + bd_send + \
                               " where " + bd_send + ".id = " + bd_recieve + ".id;\n"
                # Формирование запроса для репликации из цбд в пбд2 (с приоритетом пбд2 над цбд)
                bd_recieve = bd_list[2]  # pmib9505.pbd2
                bd_send = bd_list[0]  # pmib9505.cbd
                bd_send_short = bd_short_name_list[0]  # cbd
                replication += "insert into " + bd_recieve + " select * from " + bd_send + " where id not in " \
                                                                                           "(select id from " + bd_recieve + ");\n"
                replication += "delete from " + bd_recieve + " where id not in (select id from " + bd_send + ");\n"
                replication += "update " + bd_recieve + " set country = " + bd_send + ".country, " \
                                "head_of_state = " + bd_send + ".head_of_state, " \
                                "change_time = " + bd_send + ".change_time, " \
                                "operation_id = " + bd_send + ".operation_id from " + bd_send + \
                               " where " + bd_send + ".id = " + bd_recieve + ".id;\n"
                cursor = None
                try:
                    cursor = connection.cursor()
                    cursor.execute(replication)
                    connection.commit()
                    cursor2 = None
                    try:
                        cursor2 = connection.cursor()
                        cursor2.execute("delete from pmib9505.cbd_journal;"
                                        "insert into pmib9505.cbd_journal "
                                        "select id, country || '', head_of_state || '', change_time, operation_id, current_timestamp from pmib9505.cbd"
                                        " order by id;"
                                        "delete from pmib9505.pbd1_journal;"
                                        "insert into pmib9505.pbd1_journal "
                                        "select id, country || '', head_of_state || '', change_time, operation_id, current_timestamp from pmib9505.pbd1"
                                        " order by id;"
                                        "delete from pmib9505.pbd2_journal;"
                                        "insert into pmib9505.pbd2_journal "
                                        "select id, country || '', head_of_state || '', change_time, operation_id, current_timestamp from pmib9505.pbd2"
                                        " order by id;")
                        connection.commit()
                    except(Exception, Error) as error:
                        print("Ошибка при сохранении журналов данных: ", error)
                        if cursor2 is not None:
                            connection.rollback()
                    finally:
                        if cursor2 is not None:
                            cursor2.close()
                    time = 0
                except(Exception, Error) as error:
                    print("Ошибка при выполнении репликации: ", error)
                    if cursor is not None:
                        connection.rollback()
                finally:
                    if cursor is not None:
                        cursor.close()
    connection.close()

print("Введите интервал работы программы имитации:")
time = int(input())
print("Введите число транзакций для определения частоты репликации:")
count = int(input())
sim_tread = Thread(target=simulation_prorgamm, args=(time,))
rep_tread = Thread(target=replicator, args=(count,))
sim_tread.start()
rep_tread.start()
print("Программа выполняется\n")
print("Введите любой символ для завершения работы:")
check = input()
if check:
    status = 0
    sim_tread.join()
    rep_tread.join()
print("Работа программы завершена."
