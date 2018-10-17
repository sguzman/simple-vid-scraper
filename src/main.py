import atexit
import bs4
import json
import psycopg2
import random
import requests
import ssl


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
    if req.status_code != 200:
        return None

    soup = bs4.BeautifulSoup(req.text, 'html.parser')

    return soup


def soup_channel(chan_serial):
    url = f'https://www.youtube.com/channel/{chan_serial}/videos?flow=grid&view=0'
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
    if json_data is None:
        return None

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
    db_host = 'localhost'
    db_port = '5432'
    db_db = 'youtube'

    return psycopg2.connect(user=db_user,
                            password=db_pass,
                            host=db_host,
                            port=db_port,
                            database=db_db)


def channels():
    conn = connect()
    sql = 'SELECT id, serial FROM youtube.entities.channels'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


def insert_vids(conn, chan_id, vids):
    sql = 'INSERT INTO youtube.entities.videos (id, serial) VALUES (%s, %s) ON CONFLICT DO NOTHING'
    cursor = conn.cursor()
    for v in vids:
        data = [chan_id, v]
        cursor.execute(sql, data)

    conn.commit()
    cursor.close()


def scrape_videos(conn, chan):
    try:
        chan_id, chan_serial = chan
        print('Processing channel', chan_serial)

        soup = soup_channel(chan_serial)

        script = select_script_tag(soup)
        if script is None:
            return

        json_data = process_script(script)

        vids = get_video_items(json_data)

        count = len(vids)
        insert_vids(conn, chan_id, vids)

        cont = get_cont_token(json_data)

        while True:
            resp = soup_next_page(cont)
            if resp is None:
                break

            json_data = json.loads(resp.text)

            vids = get_video_items_cont(json_data)
            if vids is None:
                break

            count += len(vids)
            insert_vids(conn, chan_id, vids)

            cont = get_cont_token_cont(json_data)

        print('Found', count, 'videos for channel', chan_serial)
    except ssl.SSLEOFError:
        scrape_videos(conn, chan)


def main():
    conn = None

    def close_conn():
        conn.close()

    atexit.register(close_conn)

    conn = connect()
    chans = channels()
    random.shuffle(chans)

    print('Received', len(chans), 'channels')

    for c in chans:
        scrape_videos(conn, c)
    conn.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Detected Ctrl-C - Quitting...')
