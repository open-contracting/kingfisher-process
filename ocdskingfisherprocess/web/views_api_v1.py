import json
import os
import tempfile
from flask import current_app
from flask import request, views

from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.util import parse_string_to_date_time, parse_string_to_boolean


class RootV1View(views.View):

    def dispatch_request(self):
        return "OCDS Kingfisher APIs V1"


class BaseAPIViewAuthAndCollectionNeeded(views.View):

    def _check_authorization(self, request):
        api_key = request.headers.get('Authorization', '')[len('ApiKey '):]
        return api_key and api_key in current_app.kingfisher_config.web_api_keys

    def _load_collection_variables(self, request):

        # get source, test
        self.collection_source = request.form.get('collection_source')

        if not self.collection_source:
            return False

        # get data_version, test
        self.collection_data_version = parse_string_to_date_time(request.form.get('collection_data_version'))

        if not self.collection_data_version:
            return False

        # get sample (No test because if it's not there it is read as False and that's fine)
        self.collection_sample = parse_string_to_boolean(request.form.get('collection_sample', False))

        # all passed so ...
        return True


class SubmitEndCollectionStoreView(BaseAPIViewAuthAndCollectionNeeded):
    methods = ['POST']

    def dispatch_request(self):
        if not self._check_authorization(request):
            return "ACCESS DENIED", 401

        if not self._load_collection_variables(request):
            return "COLLECTION FIELDS NOT SPECIFIED", 400

        # TODO check all required fields are there!

        store = Store(config=current_app.kingfisher_config, database=current_app.kingfisher_database)

        store.load_collection(
            self.collection_source,
            self.collection_data_version,
            self.collection_sample,
        )

        current_app.kingfisher_web_logger.info("End Collection API V1 Store called for collection " + str(store.collection_id))

        if store.is_collection_store_ended():
            return "OCDS Kingfisher APIs V1 Submit - Already Done!"
        else:
            store.end_collection_store()
            return "OCDS Kingfisher APIs V1 Submit"


class SubmitFileView(BaseAPIViewAuthAndCollectionNeeded):
    methods = ['POST']

    def dispatch_request(self):
        if not self._check_authorization(request):
            return "ACCESS DENIED", 401

        if not self._load_collection_variables(request):
            return "COLLECTION FIELDS NOT SPECIFIED", 400

        # TODO check all required fields are there!

        store = Store(config=current_app.kingfisher_config, database=current_app.kingfisher_database)

        store.load_collection(
            self.collection_source,
            self.collection_data_version,
            self.collection_sample,
        )

        current_app.kingfisher_web_logger.info("Submit File API V1 called for collection " + str(store.collection_id))

        store.add_collection_note(request.form.get('collection_note'))

        file_filename = request.form.get('file_name')
        file_url = request.form.get('url')
        file_data_type = request.form.get('data_type')
        file_encoding = request.form.get('encoding', 'utf-8')

        if 'file' in request.files:

            (tmp_file, tmp_filename) = tempfile.mkstemp(prefix="ocdskf-")
            os.close(tmp_file)

            request.files['file'].save(tmp_filename)

            store.store_file_from_local(file_filename, file_url, file_data_type, file_encoding, tmp_filename)

            os.remove(tmp_filename)

        elif 'local_file_name' in request.form:

            store.store_file_from_local(file_filename, file_url, file_data_type, file_encoding, request.form.get('local_file_name'))

        else:

            raise Exception('Did not send file data')

        return "OCDS Kingfisher APIs V1 Submit"


class SubmitItemView(BaseAPIViewAuthAndCollectionNeeded):
    methods = ['POST']

    def dispatch_request(self):
        if not self._check_authorization(request):
            return "ACCESS DENIED", 401

        if not self._load_collection_variables(request):
            return "COLLECTION FIELDS NOT SPECIFIED", 400

        # TODO check all required fields are there!

        store = Store(config=current_app.kingfisher_config, database=current_app.kingfisher_database)

        store.load_collection(
            self.collection_source,
            self.collection_data_version,
            self.collection_sample,
        )

        current_app.kingfisher_web_logger.info("Submit Item API V1 called for collection " + str(store.collection_id))

        store.add_collection_note(request.form.get('collection_note'))

        file_filename = request.form.get('file_name')
        file_url = request.form.get('url')
        file_data_type = request.form.get('data_type')
        item_number = int(request.form.get('number'))

        data = json.loads(request.form.get('data'))

        try:
            store.store_file_item(
                file_filename,
                file_url,
                file_data_type,
                data,
                item_number,
            )
        except Exception as e:
            store.store_file_item_errors(file_filename, item_number, file_url, [str(e)])

        return "OCDS Kingfisher APIs V1 Submit"


class SubmitFileErrorsView(BaseAPIViewAuthAndCollectionNeeded):
    methods = ['POST']

    def dispatch_request(self):
        if not self._check_authorization(request):
            return "ACCESS DENIED", 401

        if not self._load_collection_variables(request):
            return "COLLECTION FIELDS NOT SPECIFIED", 400

        # TODO check all required fields are there!

        store = Store(config=current_app.kingfisher_config, database=current_app.kingfisher_database)

        store.load_collection(
            self.collection_source,
            self.collection_data_version,
            self.collection_sample,
        )

        current_app.kingfisher_web_logger.info("Submit File Error API V1 called for collection " + str(store.collection_id))

        file_filename = request.form.get('file_name')
        file_errors_raw = request.form.get('errors')
        file_errors = json.loads(file_errors_raw)
        file_url = request.form.get('url')

        store.store_file_errors(file_filename, file_url, file_errors)

        return "OCDS Kingfisher APIs V1 Submit"
