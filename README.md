# DuckDB Google Cloud Storage Extension


DuckDB community extension to seamlessly read and write Google Cloud Storage files without using S3 protocol interoperability.

This extension use the official Google [Cloud Storage C++ client library](https://cloud.google.com/cpp/docs/reference/storage/latest).

This allows using Google authentication to access files without using S3 interoperability Access and Secret keys.

To avoid conflict with the official [HTTPFS / S3](https://duckdb.org/docs/stable/extensions/httpfs/overview.html) extension, it uses a dedicated file system prefix : 
`gsfs://`

On a non-managed GCP environment you must set your [Active Default Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default) :
```shell
# use your login as default credentials
gcloud auth application-default login
# OR use a service account
export GOOGLE_APPLICATION_CREDENTIALS=<service_account.json> 
```


Reading file : 
```sql
select * from read_csv('gsfs://<GCS_BUCKET/my_file.csv');
```

Writing file: 
```sql
COPY (select 'value' as key) to 'gsfs://<GCS_BUCKET>/write_example.csv' ;
```



## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/gcs/gcs.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `gcs.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `gcs()` that takes a string arguments and returns a string:
```
D select gcs('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Gcs Jane 🐥 │
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL gcs
LOAD gcs
```
