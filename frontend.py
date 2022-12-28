import time
import os
import pymysql
import hashlib
import json
import uuid
import redis
from flask import Flask, request, session, redirect, abort, make_response, jsonify, g
from datetime import datetime
from config import config

class NotificatorManager:
    def __init__(self, config):
        self.config = config
        self.redis = redis.Redis(config['redis-host'])
        self.redis.ping()

        thispath = os.path.dirname(os.path.realpath(__file__))
        BASEPATH = os.path.join(thispath)

        self.app = Flask(__name__, static_url_path='/static')
        self.app.url_map.strict_slashes = False
        self.app.secret_key = config['http-secret']

    def success(self, response=None):
        if response:
            return {"status": "success", "response": response}

        return {"status": "success"}

    def error(self, message):
        return {"status": "error", "message": message}

    def channel(self, key):
        cursor = g.db.cursor()
        query = "SELECT id, name, secret FROM channels WHERE id = %s"

        cursor.execute(query, (key))
        value = cursor.fetchone()

        if value == None:
            return None

        return {"key": key, "name": value[1], "secret": value[2]}

    #
    # http routes
    #
    def routes(self):
        @self.app.before_request
        def before_request_handler():
            g.db = pymysql.connect(
                host=config['db-server'],
                user=config['db-user'],
                password=config['db-password'],
                database=config['db-dbname'],
                autocommit=True
            )

        @self.app.route("/", methods=['GET'])
        def index():
            return jsonify(self.success())

        @self.app.route("/channel", methods=['POST'])
        def channel():
            content = request.json

            if "name" not in content:
                abort(400)

            ukey = str(uuid.uuid4()).encode('utf-8')
            key = hashlib.md5(ukey).hexdigest()
            secret = uuid.uuid4()

            cursor = g.db.cursor()
            query = "INSERT INTO channels (id, name, secret) VALUES (%s, %s, %s)"
            cursor.execute(query, (key, content['name'], secret))

            return jsonify(self.success({"secret": secret, "key": key}))

        @self.app.route("/subscribe/<channel>", methods=['POST'])
        def subscribe(channel):
            content = request.json

            if "device" not in content:
                abort(400)

            if self.channel(channel) == None:
                abort(404)

            cursor = g.db.cursor()
            query = "INSERT INTO devices (channel, token) VALUES (%s, %s)"
            cursor.execute(query, (channel, content['device']))

            return jsonify(self.success())

        @self.app.route("/notify/<channel>", methods=['POST'])
        def notify(channel):
            content = request.json

            for check in ["title", "message", "secret"]:
                if check not in content:
                    abort(400)

            category = content["category"] if "category" in content else "null"

            chaninfo = self.channel(channel)
            if chaninfo == None:
                abort(404)

            if chaninfo["secret"] != content["secret"]:
                abort(401)

            cursor = g.db.cursor()
            query = "SELECT token FROM devices WHERE channel = %s"
            cursor.execute(query, (channel,))

            for device in cursor.fetchall():
                print(device)

                notification = {
                    "token": device[0],
                    "title": content["title"],
                    "message": content["message"],
                    "category": category,
                }

                self.redis.lpush(config["redis-queue"], json.dumps(notification))

            return jsonify(self.success())

    def listen(self):
        self.app.run(
            host="0.0.0.0",
            port=self.config['http-port'],
            debug=self.config['debug'],
            threaded=True
        )

if __name__ == '__main__':
    root = NotificatorManager(config)
    root.routes()
    root.listen()

