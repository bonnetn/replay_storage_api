import gzip
import json
import logging
import os
import sqlite3
import uuid
from json import JSONDecodeError

import tornado.httpserver
import tornado.httputil
import tornado.ioloop
import tornado.web
from tornado import options

SIZE_LIMIT = 1 * 1e6
PATH = os.getenv('STORAGE_PATH', default='/tmp/')
UPLOAD = 'upload/'
DB = 'upload.db'

UPLOAD_DIR = os.path.join(PATH, UPLOAD)
DB_PATH = os.path.join(PATH, DB)

con = sqlite3.connect(DB_PATH)
con.execute("create table if not exists uploads (id integer primary key, uuid varchar unique)")

if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)


# Successful

def get_file_path(replay_uuid):
    return os.path.join(PATH, UPLOAD, str(replay_uuid) + ".json.gz")


class GetReplayHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        # Enable CORS.
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET, OPTIONS')

    def get(self, user_input):
        # Make sure the UUID provided is a valid UUID.
        try:
            replay_uuid = uuid.UUID(user_input)
        except ValueError:
            raise tornado.web.HTTPError(status_code=400, log_message='not a valid ID')

        try:
            with gzip.open(get_file_path(replay_uuid), 'rb') as f:
                self.write(json.loads(f.read()))
        except FileNotFoundError:
            raise tornado.web.HTTPError(status_code=404, log_message='not found')

    def options(self, _):
        self.set_status(204)
        self.finish()


class RootReplayHandler(tornado.web.RequestHandler):
    def get(self):
        with con:
            result = con.execute("SELECT uuid FROM uploads").fetchall()

        self.write({
            'uploads': [row[0] for row in result],
        })

    def post(self):
        if len(self.request.files) != 1:
            raise tornado.web.HTTPError(status_code=400,
                                        log_message='exactly 1 file must be provided')

        for field_name, files in self.request.files.items():
            # Only 1 file should be sent at a time.
            if len(self.request.files) != 1:
                raise tornado.web.HTTPError(status_code=400,
                                            log_message='exactly 1 file must be provided')

            # The file must be valid JSON.
            info = files[0]
            try:
                body = json.loads(info["body"])
            except JSONDecodeError:
                raise tornado.web.HTTPError(status_code=400, log_message='data is not valid JSON')

            # The file is valid, creating a new unique ID.
            replay_uuid = uuid.uuid4()
            logging.info(f"Receiving replay {replay_uuid}")

            with con:
                con.execute("INSERT INTO uploads (uuid) VALUES (?)", (str(replay_uuid),))

            with gzip.open(get_file_path(replay_uuid), "wb") as f:
                f.write(json.dumps(body).encode())

            self.write({'id': str(replay_uuid)})


def make_app():
    return tornado.web.Application([
        (r"/replay/", RootReplayHandler),
        (r"/replay/(.+)", GetReplayHandler),
    ])


if __name__ == "__main__":
    options.parse_command_line()
    app = make_app()

    server = tornado.httpserver.HTTPServer(app, max_buffer_size=SIZE_LIMIT)
    server.listen(8888)

    tornado.ioloop.IOLoop.current().start()
