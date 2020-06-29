import datetime
import sqlite3
import time

import pandas as pd
import requests


def period(begin, end):
    begin_timestamp = time.mktime(datetime.datetime.strptime(begin, "%d/%m/%y").timetuple())
    date_1 = datetime.datetime.strptime(end, "%d/%m/%y")
    end_date = date_1 + datetime.timedelta(days=1)
    result = f"{end_date.day}/{end_date.month}/{end_date.year}"
    end_timestamp = time.mktime(datetime.datetime.strptime(result, "%d/%m/%Y").timetuple())
    return [begin_timestamp, end_timestamp]


def scrape(period, link):
    endpoint = "api.vk.com/method"
    method = "wall.get"
    access_token = "d0629f88d0629f88d0629f88bad0088a04dd062d0629f888cdb5b9bfd4d895fcdad86bd"
    params = "owner_id=" + link + "&count=100"
    offset = 0
    posts = []
    begin = period[0]
    end = period[1]
    while True and offset < 200:
        url = "https://{endpoint}/{method}?{params}&v=5.52&access_token={token}&offset={offset}".format(
            endpoint=endpoint, method=method, token=access_token, params=params, offset=offset)
        result = requests.get(url).json()
        count = result['response']['count']
        for post in result['response']['items']:
            if post['date'] >= begin and post['date'] <= end:
                posts.append(post)

        offset += 100
        if count <= offset:
            break
    return posts


def get_comments(link, posts, period, request):
    begin = period[0]
    end = period[1]
    endpoint = "api.vk.com/method"
    method = "wall.getComments"
    access_token = "d0629f88d0629f88d0629f88bad0088a04dd062d0629f888cdb5b9bfd4d895fcdad86bd"
    params = "owner_id=" + link + "&"
    param2 = 'count=100&'

    rows_list = []

    conn = sqlite3.connect('test2.db', check_same_thread=False)
    c = conn.cursor()

    for post in posts:
        post_id = post['id']
        comments = []
        offset = 0
        while True and offset < 1000:
            url = "https://{endpoint}/{method}?{params}post_id={post_id}&{count}v=5.52&access_token={token}&offset={offset}&preview_length=0&need_likes=1".format(
                endpoint=endpoint, method=method, token=access_token, params=params, post_id=post_id, offset=offset,
                count=param2)
            result = requests.get(url).json()
            count = result['response']['count']
            comments += result['response']['items']
            offset += 100
            if count <= offset:
                break

        for comment in comments:
            if comment['date'] >= begin and comment['date'] <= end:
                post_id = post['id']
                text = comment['text']
                likes = comment['likes']['count']
                if text != '':
                    dict1 = {}
                    dict1.update({"post_id": post_id, "text": text, "likes": likes})
                    rows_list.append(dict1)

    all_comments = pd.DataFrame(rows_list)
    c.execute("UPDATE test SET comments = ? where request_id = ?", (all_comments.to_string(), request))
    conn.commit()
    c.close()
    return all_comments
