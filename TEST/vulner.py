import os
import sqlite3
import subprocess
import pickle
from flask import Flask, request

app = Flask(__name__)
app.secret_key = "supersecret123"
DEBUG = True

DB_PATH = "users.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT, password TEXT)")
    cur.execute("INSERT INTO users(username, password) VALUES ('admin', 'admin123')")
    conn.commit()
    conn.close()


@app.route('/login', methods=['POST'])
def login():
    username = request.form.get('username')
    password = request.form.get('password')
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    query = f"SELECT * FROM users WHERE username = '{username}' AND password = '{password}'"
    cur.execute(query)
    user = cur.fetchone()
    conn.close()
    if user:
        return "Login success"
    return "Login failed", 401


@app.route('/ping')
def ping():
    host = request.args.get('host', '127.0.0.1')
    output = os.popen(f"ping -c 1 {host}").read()
    return f"<pre>{output}</pre>"


@app.route('/run')
def run_cmd():
    cmd = request.args.get('cmd')
    result = subprocess.check_output(cmd, shell=True)
    return result


@app.route('/load', methods=['POST'])
def load_data():
    data = request.files['file'].read()
    obj = pickle.loads(data)
    return str(obj)


@app.route('/read')
def read_file():
    name = request.args.get('name')
    with open(name, 'r') as f:
        return f.read()


@app.route('/calc')
def calc():
    expr = request.args.get('expr')
    return str(eval(expr))


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
##
