import sqlite3
import threading

from flask import Flask, render_template, request

from ml import async_handler

th = threading.Thread(
    target=async_handler,
    args=(),
    daemon=True,
)
th.start()

app = Flask(__name__)

global n_request
n_request = 0


@app.route('/', methods=['GET', 'POST'])
def index():
    conn = sqlite3.connect('test2.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS test(request_id, comments, score, period, token)")
    if request.method == "POST":
        global n_request
        n_request += 1
        token = request.form['token']
        begin = request.form['begin']
        end = request.form['end']
        period = begin + ' ' + end
        print(period)
        c.execute("INSERT INTO test VALUES (?, ?, ?, ?, ?)", (n_request, '', 'unready', period, token))
        conn.commit()
        c.close()
    return render_template('index.html', htmlText=(f"request/{n_request}"))


@app.route('/request/<int:request_id>', methods=['GET'])
def show_results(request_id):
    conn = sqlite3.connect('test2.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT score FROM test WHERE request_id = ?", (request_id,))
    request = c.fetchone()

    if request is not None and request[0] != 'unready':
        score = request[0]
        score = score.split("@@")
        positive_n = score[0]
        negative_n = score[1]
        positive_ex = score[2]
        negative_ex = score[3]
        positive_most_liked = score[4]
        negative_most_liked = score[5]
        positive_index = score[6]
        return render_template('result.html', positive=positive_n, negative=negative_n, positive_ex=positive_ex,
                               negative_ex=negative_ex,positive_most_liked=positive_most_liked, negative_most_liked=negative_most_liked, positive_index=positive_index)
    elif request is None:
        return render_template('nonexistent.html')
    else:
        return render_template('unready.html')


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
