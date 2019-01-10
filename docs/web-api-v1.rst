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

You must pass all the data as one JSON object in the body of a post request.

The JSON object needs to contain a number of keys.

Firstly, you must pass details of the collection.

*  `collection_source` - String.
*  `collection_data_version` - String. In format YYYY-MM-DD HH:MM:SS
*  `collection_sample` - Boolean.

Secondly, you must pass details of the file.

* `file_name` - String.
* `url` - String.
* `data_type` -  String. See section on file data types in :doc:`data-model`.

Finally, pass the actual file loaded as JSON data in the `data` key.

Store Item
----------

The end point is /api/v1/submit/item/

You must pass all the data as one JSON object in the body of a post request.

Firstly, you must pass details of the collection.

*  `collection_source` - String.
*  `collection_data_version` - String. In format YYYY-MM-DD HH:MM:SS
*  `collection_sample` - Boolean.

Secondly, you must pass details of the file.

* `file_name` - String.
* `url` - String.
* `data_type` -  String. See section on file data types in :doc:`data-model`.

Thirdly, you must pass details of the item in the file.

* `number` - Integer.

Finally, pass the actual file loaded as JSON data in the `data` key.

