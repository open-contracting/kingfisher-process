import json
import logging
import logging.config
import os
from flask import Flask, render_template, current_app

import ocdskingfisherprocess.signals.signals
import ocdskingfisherprocess.web.views_api_v1 as views_api_v1
from ocdskingfisherprocess.config import Config
from ocdskingfisherprocess.database import DataBase


def create_app(config=None):
    if not config:
        config = Config()
        config.load_user_config()

    logging_config_file_full_path = os.path.expanduser('~/.config/ocdskingfisher-process/logging.json')
    if os.path.isfile(logging_config_file_full_path):
        with open(logging_config_file_full_path) as f:
            logging.config.dictConfig(json.load(f))

    database = DataBase(config=config)

    ocdskingfisherprocess.signals.signals.setup_signals(config, database)

    app = Flask(__name__)
    app.kingfisher_config = config
    app.kingfisher_database = database
    app.kingfisher_web_logger = logging.getLogger('ocdskingfisher.web')

    app.add_url_rule('/', view_func=hello)
    app.add_url_rule('/robots.txt', view_func=robots_txt)
    app.add_url_rule('/api', view_func=api)

    app.add_url_rule('/app', view_func=app_index)
    app.add_url_rule('/app/collection', view_func=app_collections)
    app.add_url_rule('/app/collection/<collection_id>', view_func=app_collection_index)
    app.add_url_rule('/app/collection/<collection_id>/file', view_func=app_collection_files)

    app.add_url_rule('/api/v1/', view_func=views_api_v1.RootV1View.as_view('api_v1_root'))
    app.add_url_rule('/api/v1/submit/end_collection_store/',
                     view_func=views_api_v1.SubmitEndCollectionStoreView.as_view('api_v1_submit_end_collection_store'))
    app.add_url_rule('/api/v1/submit/file/',
                     view_func=views_api_v1.SubmitFileView.as_view('api_v1_submit_file'))
    app.add_url_rule('/api/v1/submit/item/',
                     view_func=views_api_v1.SubmitItemView.as_view('api_v1_submit_item'))
    app.add_url_rule('/api/v1/submit/file_errors/',
                     view_func=views_api_v1.SubmitFileErrorsView.as_view('api_v1_submit_file_errors'))

    return app


def hello():
    return "OCDS Kingfisher"


def robots_txt():
    return "User-agent: *\nDisallow: /"


def app_index():
    return render_template("app/index.html")


def app_collections():
    return render_template("app/collections.html", collections=current_app.kingfisher_database.get_all_collections())


def app_collection_index(collection_id):
    collection = current_app.kingfisher_database.get_collection(collection_id)
    notes = current_app.kingfisher_database.get_all_notes_in_collection(collection_id)
    return render_template("app/collection/index.html", collection=collection, notes=notes)


def app_collection_files(collection_id):
    collection = current_app.kingfisher_database.get_collection(collection_id)
    files = current_app.kingfisher_database.get_all_files_in_collection(collection_id)
    return render_template("app/collection/files.html", collection=collection, files=files)


def api():
    return "OCDS Kingfisher APIs"
