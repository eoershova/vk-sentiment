import pickle
import random
import sqlite3
import time

import numpy as np

from scraper import period, scrape, get_comments

# импортирует натренированный векторайзер и модель линейной регрессии
tfidf_vectorizer_char = pickle.load(open('tfidf_vectorizer_char.pickle', 'rb'))
filename = 'model.sav'
model = pickle.load(open(filename, 'rb'))

# ищет позитивные и негативные комментарии с наибольшим числом лайков
def find_pos_neg(comments):
    pos = comments.loc[comments['prediction'] == 1]
    neg = comments.loc[comments['prediction'] == -1]
    pos_text, neg_text = '', ''

    if not (pos.empty):
        pos_text = pos.text.values[random.randint(0, pos.shape[0]-1)]
        if pos_text == "Comment deleted by user or page manager":
            pos_text = pos.text.values[random.randint(0, pos.shape[0]-1)]

        most_liked_pos = pos.sort_values(by=['likes'])
        most_liked_pos_comment = most_liked_pos.text.head().values[0]
    else:
        [pos_text, most_liked_pos_comment] = 'Нет позитивных комментариев', 'Нет позитивных комментариев'
    if not (neg.empty):
        neg_text = neg.text.values[random.randint(0, neg.shape[0]-1)]
        if neg_text == "Comment deleted by user or page manager":
            neg_text = neg.text.values[random.randint(0, neg.shape[0]-1)]

        most_liked_neg = neg.sort_values(by=['likes'])
        most_liked_neg_comment = most_liked_neg.text.head().values[0]
    else:
        [neg_text, most_liked_neg_comment] = 'Нет негативных комментариев', 'Нет негативных комментариев'

    pos["impact"] = pos['likes'] * pos['prediction']
    neg["impact"] = neg['likes'] * neg['prediction'] * -1
    for i, row in pos.iterrows():
        raw = (row['impact'])
        if raw == 0:
            pos.at[i, 'impact'] = 1

    for i, row in neg.iterrows():
        raw = (row['impact'])
        if raw == 0:
            neg.at[i, 'impact'] = 1
    all_impact = pos['impact'].sum() + neg['impact'].sum()
    positive_index = pos['impact'].sum() / all_impact * 100

    return [pos_text, neg_text, most_liked_pos_comment, most_liked_neg_comment, positive_index]


def predict(comments):
    texts = comments.text
    X = tfidf_vectorizer_char.transform(texts)
    y_pred = model.predict(X)
    comments['prediction'] = y_pred
    n_positives = np.sum(y_pred == 1)
    n_negatives = np.sum(y_pred == -1)
    examples = find_pos_neg(comments)
    ex_positive = examples[0]
    ex_negative = examples[1]
    most_liked_pos = examples[2]
    most_liked_neg = examples[3]
    positive_index = examples[4]

    return [f"{n_positives}@@{n_negatives}@@{ex_positive}@@{ex_negative}@@{most_liked_pos}@@{most_liked_neg}@@{positive_index}", comments.to_string()]

# асинхронность пришлось убрать, потому что pythonanywhere не поддержирвает треды :(
def async_handler():
    conn = sqlite3.connect('test2.db', check_same_thread=False)
    c = conn.cursor()
    while True:
        c.execute("SELECT token, period, request_id FROM test WHERE score = 'unready'")
        request = c.fetchone()

        if request is not None:
            print('ПРИНИМАЮСЬ')
            request_id = request[2]
            print(type(request_id))
            date = request[1]
            token = request[0]
            dates = period(date.split(" ")[0], date.split(" ")[1])
            posts = scrape(dates, token)
            comments = get_comments(token, posts, dates, request_id)
            result = predict(comments)
            predictions_df = result[1]
            c.execute("UPDATE test SET score = ? where request_id = ?", (result[0], request_id))
            c.execute("UPDATE test SET comments = ? where request_id = ?", (predictions_df, request_id))
            conn.commit()
            print('УПРАВИЛСЯ')
            continue
        time.sleep(0.5)
