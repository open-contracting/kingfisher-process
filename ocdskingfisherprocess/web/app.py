from flask import Flask, request
from ocdskingfisherprocess.config import Config
from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.database import DataBase
from ocdskingfisherprocess.util import parse_string_to_date_time

config = Config()
config.load_user_config()

app = Flask(__name__)


@app.route("/")
def hello():
    return "OCDS Kingfisher"


@app.route("/robots.txt")
def robots_txt():
    return "User-agent: *\nDisallow: /"


@app.route("/api/")
def api():
    return "OCDS Kingfisher APIs"


@app.route("/api/v1/")
def api_v1():
    return "OCDS Kingfisher APIs V1"


@app.route("/api/v1/submit/file/", methods=['POST'])
def api_v1_submit_file():
    api_key = request.headers.get('Authorization')[len('ApiKey '):]
    if not api_key or api_key not in config.web_api_keys:
        return "ACCESS DENIED"  # TODO proper error

    # TODO check all required fields are there!

    database = DataBase(config=config)
    store = Store(config=config, database=database)

    data = request.get_json()

    collection_source = data.get('collection_source')
    collection_data_version = parse_string_to_date_time(data.get('collection_data_version'))
    collection_sample = data.get('collection_sample', False)

    store.load_collection(
        collection_source,
        collection_data_version,
        collection_sample,
    )

    file_filename = data.get('file_name')
    file_url = data.get('url')
    file_data_type = data.get('data_type')

    store.store_file_from_data(file_filename, file_url, file_data_type, data.get('data'))

    return "OCDS Kingfisher APIs V1 Submit"


@app.route("/api/v1/submit/item/", methods=['POST'])
def api_v1_submit_item():
    api_key = request.headers.get('Authorization')[len('ApiKey '):]
    if not api_key or api_key not in config.web_api_keys:
        return "ACCESS DENIED"  # TODO proper error

    # TODO check all required fields are there!

    database = DataBase(config=config)
    store = Store(config=config, database=database)

    data = request.get_json()

    collection_source = data.get('collection_source')
    collection_data_version = parse_string_to_date_time(data.get('collection_data_version'))
    collection_sample = data.get('collection_sample', False)

    store.load_collection(
        collection_source,
        collection_data_version,
        collection_sample,
    )

    file_filename = data.get('file_name')
    file_url = data.get('url')
    file_data_type = data.get('data_type')
    item_number = int(data.get('number'))

    store.store_file_item(
        file_filename,
        file_url,
        file_data_type,
        data.get('data'),
        item_number,
    )

    return "OCDS Kingfisher APIs V1 Submit"
