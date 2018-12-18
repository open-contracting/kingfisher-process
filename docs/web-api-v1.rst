Web API Version 1
=================

Authentication
--------------

All calls to this must pass as an API_KEY as a GET parameter only.

eg  http://....../?API_KEY=KEY_GOES_HERE

To set the key, see :doc:`config`.


Creating a collection
---------------------

There is no special API call for this. Just call one of the store methods. If the collection does not exist, it will be created automatically.

Store File
----------

The end point is /api/v1/submit/file/

You must pass details as POST parameters.

Firstly, you must pass details of the collection.

*  collection_source - String.
*  collection_data_version - String. In format YYYY-MM-DD HH:MM:SS
*  collection_sample - Boolean. Pass 1 for true and 0 for false.

Secondly, you must pass details of the file.

* file_name - String.
* file_url - String.
* file_data_type -  String. See section on file data types in :doc:`data-model`.
* file_encoding - String.

Finally, pass the actual file as a file upload named "file".

Store Item
----------

The end point is /api/v1/submit/item/

You must pass details as POST parameters.

Firstly, you must pass details of the collection.

*  collection_source - String.
*  collection_data_version - String. In format YYYY-MM-DD HH:MM:SS
*  collection_sample - Boolean. Pass 1 for true and 0 for false.

Secondly, you must pass details of the file.

* file_name - String.
* file_url - String.
* file_data_type -  String. See section on file data types in :doc:`data-model`. But when passing an item, only some data types can be used.
* file_encoding - String.
* number - Integer.

Finally, pass the actual item as a file upload named "file".
