.. highlight:: rst

.. index::
   single: Range defined PUT requests


Range defined PUT requests
===============================

Allows to update a file range using a PUT request.

REST syntax
+++++++++++

.. code-block:: text

   PUT http://<host>:8000/eos/file 


REST headers

.. code-block:: text

   x-upload-totalsize: <total file size>               - specifies total file size
   x-upload-mtime: <mtime unixtimestamp>               - specifies mtime to store in the namespace
   x-upload-range: bytes=<first-offset>-<last-offset>  - <first> specifies the start offset to place the content. <last> has to match the content-length of the uploaded body
   x-upload-checksum: <type>:<value>                   - <type> is usually adler32 or md5, value is the corresponding hexadeciaml value
   x-upload-done: true|false                           - if true the checksum will be calculated after this upload. The flag is intrinsic if the range <last-offset> value equals content-length e.g. the last piece always triggers a checksum calculation unless this header is explicitly set to false. 

   #CURL Example uploading 3 pieces of a file with x-upload-done implicit
   curl --header "x-upload-checksum: adler32:abcdabcd" --header "x-upload-totalsize: 2285" --header "x-upload-mtime: 1533100000" --header "x-upload-range: bytes=0-1023" -L -X PUT -T "file.0" http://localhost:8000/eos/http/file
   curl --header "x-upload-checksum: adler32:abcdabcd" --header "x-upload-totalsize: 2285" --header "x-upload-mtime: 1533100000" --header "x-upload-range: bytes=1024-2047" -L -X PUT -T "file.1" http://localhost:8000/eos/http/file
   curl --header "x-upload-checksum: adler32:abcdabcd" --header "x-upload-totalsize: 2285" --header "x-upload-mtime: 1533100000" --header "x-upload-range: bytes=2048-2284" -L -X PUT -T "file.2" http://localhost:8000/eos/http/file

   #CURL Example uploading 3 pieces out of order with x-upload-done explicit
   curl --header "x-upload-done: false" --header "x-upload-checksum: adler32:abcdabcd" --header "x-upload-totalsize: 2285" --header "x-upload-mtime: 1533100000" --header "x-upload-range: bytes=2048-2284" -L -X PUT -T "file.2" http://localhost:8000/eos/http/file
   curl --header "x-upload-done: false" --header "x-upload-checksum: adler32:abcdabcd" --header "x-upload-totalsize: 2285" --header "x-upload-mtime: 1533100000" --header "x-upload-range: bytes=0-1023" -L -X PUT -T "file.0" http://localhost:8000/eos/http/file
   curl --header "x-upload-done: true" --header "x-upload-checksum: adler32:abcdabcd" --header "x-upload-totalsize: 2285" --header "x-upload-mtime: 1533100000" --header "x-upload-range: bytes=1024-2047" -L -X PUT -T "file.1" http://localhost:8000/eos/http/file


