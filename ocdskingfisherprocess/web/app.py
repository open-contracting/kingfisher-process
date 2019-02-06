from flask import Flask, render_template
from ocdskingfisherprocess.config import Config
from ocdskingfisherprocess.database import DataBase
import ocdskingfisherprocess.web.views_api_v1 as views_api_v1

config = Config()
config.load_user_config()

app = Flask(__name__)


@app.route("/")
def hello():
    return "OCDS Kingfisher"


@app.route("/robots.txt")
def robots_txt():
    return "User-agent: *\nDisallow: /"


@app.route("/app")
def app_index():
    return render_template("app/index.html")


@app.route("/app/collection")
def app_collections():
    database = DataBase(config=config)
    return render_template("app/collections.html", collections=database.get_all_collections())


@app.route("/app/collection/<collection_id>")
def app_collection_index(collection_id):
    database = DataBase(config=config)
    collection = database.get_collection(collection_id)
    return render_template("app/collection/index.html", collection=collection)


@app.route("/app/collection/<collection_id>/file")
def app_collection_files(collection_id):
    database = DataBase(config=config)
    collection = database.get_collection(collection_id)
    files = database.get_all_files_in_collection(collection_id)
    return render_template("app/collection/files.html", collection=collection, files=files)


@app.route("/api/")
def api():
    return "OCDS Kingfisher APIs"


app.add_url_rule('/api/v1/', view_func=views_api_v1.RootV1View.as_view('api_v1_root'))
app.add_url_rule('/api/v1/submit/end_collection_store/',
                 view_func=views_api_v1.SubmitEndCollectionStoreView.as_view('api_v1_submit_end_collection_store'))
app.add_url_rule('/api/v1/submit/file/',
                 view_func=views_api_v1.SubmitFileView.as_view('api_v1_submit_file'))
app.add_url_rule('/api/v1/submit/item/',
                 view_func=views_api_v1.SubmitItemView.as_view('api_v1_submit_item'))
app.add_url_rule('/api/v1/submit/file_errors/',
                 view_func=views_api_v1.SubmitFileErrorsView.as_view('api_v1_submit_file_errors'))
