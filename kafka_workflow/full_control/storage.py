import sqlite3
from aiokafka import ConsumerRecord

db = r"C:\Users\user1451\PycharmProjects\kafka_playground\identifier.sqlite"


def create_connection(db_file):
    try:
        return sqlite3.connect(db_file)
    except Exception as e:
        print(e)


def get_offset(partition):
    with create_connection(db) as conn:
        query = f"select partition from tss_offsets where partition = {partition.partition};"
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        partition = rows[0][0]
    return partition


def save_and_commit(consumer, record: ConsumerRecord):
    print(f"Topic: {record.topic}, Partition: {record.partition}, Offset: {record.offset}, "
          f"Key: {record.key}, Value: {record.value}")
    with create_connection(db) as conn:
        insert_query = f"insert into tss_data (skey, svalue) values(\"{record.key}\", \"{record.value}\");"
        update_query = f"update tss_offsets set offset = {record.offset} where " \
                       f"topic_name = \"{record.topic}\" and partition = {record.partition}"
        cur = conn.cursor()
        cur.execute(insert_query)
        cur.execute(update_query)
        conn.commit()
