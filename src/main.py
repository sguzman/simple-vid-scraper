import asyncio
import bs4
import datetime
import json
import os
import psycopg2
import queue
import requests
import threading

cores = 4
print_queue = queue.Queue()


def json_pretty(js):
    print(json.dumps(js, indent=' ', separators={', ', ': '}))


def process_script(script):
    raw_split = script.text.split('\n')
    raw_json = raw_split[1].lstrip('    window["ytInitialData"] = ').rstrip(';')
    return json.loads(raw_json)


def select_script_tag(soup):
    for script in soup.findAll('script'):
        if script.text.startswith('\n    window["ytInitialData"]'):
            return script


def souped(url, params, headers):
    if headers is None:
        headers = {}

    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/69.0.3497.100 Safari/537.36 '
    headers['User-Agent'] = user_agent

    req = requests.get(url, params=params, headers=headers)
    soup = bs4.BeautifulSoup(req.text, 'html.parser')

    return soup


def soup_channel(chan_serial):
    url = f'https://www.youtube.com/channel/{chan_serial}/videos'
    return souped(url, None, None)


def index_nest(obj, keys):
    try:
        tmp = obj
        for i in keys:
            tmp = tmp[i]

        return tmp
    except:
        return None


def get_video_items_cont(obj):
    items_index = [1,
                   'response',
                   'continuationContents',
                   'gridContinuation',
                   'items'
                   ]

    json_data = index_nest(obj, items_index)
    if json_data is None:
        return None

    return video_ids(json_data)


def get_cont_token_cont(obj):
    items_index = [1,
                   'response',
                   'continuationContents',
                   'gridContinuation',
                   'continuations',
                   0,
                   'nextContinuationData',
                   'continuation'
                   ]

    return index_nest(obj, items_index)


def get_video_items(obj):
    items_index = ['contents',
                   'twoColumnBrowseResultsRenderer',
                   'tabs',
                   1,
                   'tabRenderer',
                   'content',
                   'sectionListRenderer',
                   'contents',
                   0,
                   'itemSectionRenderer',
                   'contents',
                   0,
                   'gridRenderer',
                   'items'
                   ]

    json_data = index_nest(obj, items_index)
    return video_ids(json_data)


def get_cont_token(obj):
    items_index = ['contents',
                   'twoColumnBrowseResultsRenderer',
                   'tabs',
                   1,
                   'tabRenderer',
                   'content',
                   'sectionListRenderer',
                   'contents',
                   0,
                   'itemSectionRenderer',
                   'contents',
                   0,
                   'gridRenderer',
                   'continuations',
                   0,
                   'nextContinuationData',
                   'continuation',
                   ]

    return index_nest(obj, items_index)


def soup_next_page(token):
    url = f'https://www.youtube.com/browse_ajax'
    params = {
        'ctoken': token,
        'continuation': token
    }

    headers = {
        'x-spf-previous': 'https://www.youtube.com/channel/UC0rZoXAD5lxgBHMsjrGwWWQ/videos',
        'x-spf-referer': 'https://www.youtube.com/channel/UC0rZoXAD5lxgBHMsjrGwWWQ/videos',
        'x-youtube-client-name': '1',
        'x-youtube-client-version': '2.20180921',
        'x-youtube-page-cl': '214220627',
        'x-youtube-page-label': 'youtube.ytfe.desktop_20180921_0_RC2',
        'x-youtube-utc-offset': '-420',
        'x-youtube-variants-checksum': '00589810531d478dd01596fd6f1241e0'
    }
    return souped(url, params, headers)


def video_ids(items):
    vids = []
    for item in items:
        grid = item['gridVideoRenderer']['videoId']
        vids.append(grid)

    return vids


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
    sql = 'SELECT Chan.id, Chan.chan_serial FROM youtube.entities.chans as Chan ORDER BY subs DESC'
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


async def print_async(*args):
    print(datetime.datetime.now(), *args)


def p(*args):
    asyncio.run(print_async(*args))


def scrape_videos(chan_serial):
    soup = soup_channel(chan_serial)
    script = select_script_tag(soup)
    json_data = process_script(script)

    vids = get_video_items(json_data)
    cont = get_cont_token(json_data)

    idx = 0
    while True:
        resp = soup_next_page(cont)
        json_data = json.loads(resp.text)

        items = get_video_items_cont(json_data)
        if items is None:
            return vids

        vids.extend(items)

        cont = get_cont_token_cont(json_data)
        p(len(vids))
        idx = idx + 1


def main():
    chans = channels()
    vids = videos()

    p('Received', len(chans), 'channels')
    p('Received', len(vids), 'videos')

    for blank, c in chans:
        p(scrape_videos(c))


if __name__ == '__main__':
    main()
