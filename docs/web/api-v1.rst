Web API Version 1
=================

Authentication
--------------

All calls to this must pass an API_KEY as an Authorization HTTP header of type ApiKey

eg `Authorization: ApiKey API_KEY_GOES_HERE`

To set the key, see :doc:`config`.


Creating a collection
---------------------

There is no special API call for this. Just call one of the store methods. If the collection does not exist, it will be created automatically.

Store File
----------

The end point is /api/v1/submit/file/

Pass data as POST variables.

Firstly, you must pass details of the collection.

*  `collection_source` - String.
*  `collection_data_version` - String. In format YYYY-MM-DD HH:MM:SS
*  `collection_sample` - String. Pass "true" for True.

Secondly, you must pass details of the file.

* `file_name` - String.
* `url` - String.
* `data_type` -  String. See section on file data types in :doc:`data-model`.
* `encoding` - String. Defaults to `utf-8`.

Finally, the actual file data must be passed. This can be done in one of several ways:

* Pass the actual file as an attached file in the `file` key.
* If the file is available in the local file system and the user running the API server can read it, you can pass the full filename as a POST variable with the key `local_file_name`. For large files, this saves transferring the contents over HTTP.

Optionally, you can also pass:

* `collection_note` - A note to save alongside the collection.

Store Item
----------

The end point is /api/v1/submit/item/

Pass data as POST variables.

Firstly, you must pass details of the collection.

*  `collection_source` - String.
*  `collection_data_version` - String. In format YYYY-MM-DD HH:MM:SS
*  `collection_sample` - String. Pass "true" for True.

Secondly, you must pass details of the file.

* `file_name` - String.
* `url` - String.
* `data_type` -  String. See section on file data types in :doc:`data-model`.

Thirdly, you must pass details of the item in the file.

* `number` - Integer.

Finally, pass the data as a string in the `data` key.

Optionally, you can also pass:

* `collection_note` - A note to save alongside the collection.

End Collection Store
--------------------

You can call this to say that a collection's store stage is done, and no more files or items will be sent.
This is important because some transforms can only run on completely stored collections.

The end point is /api/v1/submit/end_collection_store/

Pass data as POST variables.

You must pass details of the collection.

*  `collection_source` - String.
*  `collection_data_version` - String. In format YYYY-MM-DD HH:MM:SS
*  `collection_sample` - String. Pass "true" for True.

File Errors
-----------

You can call this if errors prevented a file from being got from a remote API at all. It will simply store the errors in the database for later analysis.

The end point is /api/v1/submit/file_errors/

Pass data as POST variables.

Firstly, you must pass details of the collection.

*  `collection_source` - String.
*  `collection_data_version` - String. In format YYYY-MM-DD HH:MM:SS
*  `collection_sample` - String. Pass "true" for True.

Secondly, you must pass details of the file.

* `file_name` - String.
* `url` - String.

Finally, pass details of the errors in the `errors` key. The data should be a JSON List of strings.
