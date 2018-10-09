import bs4
import json
import requests
import os
import psycopg2

cores = 4


def connect():
    db_user = 'admin'
    db_pass = 'admin'
    db_host = '192.168.1.63'
    db_port = '5432'
    db_db = 'youtube'

    return psycopg2.connect(user=db_user,
                            password=db_pass,
                            host=db_host,
                            port=db_port,
                            database=db_db)


def channels():
    conn = connect()
    sql = 'SELECT Chan.id, Chan.chan_serial FROM youtube.entities.chans as Chan ORDER BY subs'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


def videos():
    conn = connect()
    sql = 'SELECT Vid.video_serial FROM youtube.entities.videos as Vid'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = set(x for x in cursor.fetchall())

    cursor.close()
    conn.close()

    return records


def main():
    chans = channels()
    vids = videos()

    print(len(chans))
    print(len(vids))


if __name__ == '__main__':
    main()
