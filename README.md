# tap-mongodb

![validate](https://github.com/MeltanoLabs/tap-mongodb/actions/workflows/validate.yaml/badge.svg)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json)](https://github.com/charliermarsh/ruff)

`tap-mongodb` is a Singer tap for extracting data from a [MongoDB](https://www.mongodb.com/)
or [AWS DocumentDB](https://aws.amazon.com/documentdb/) database. The tap supports extracting records from the database
directly (incremental replication mode, the default) and also supports extracting change events from the database's
Change Stream API (in log-based replication mode).

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/MeltanoLabs/tap-mongodb.git@main
```

## Configuration

### Accepted Config Options

| Setting                                  | Type               | Required |               Default               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|:-----------------------------------------|--------------------|:--------:|:-----------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| database                                 | string             |   True   |                  -                  | Database from which records will be extracted.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| mongodb_connection_string                | password           |  False   |                  -                  | MongoDB connection string. See [the MongoDB documentation](https://www.mongodb.com/docs/manual/reference/connection-string/#connection-string-uri-format) for specification. The username and password included in this string must be url-encoded - the tap will not url-encode it.                                                                                                                                                                                                                                                                                                         |
| documentdb_credential_json_string        | password           |  False   |                  -                  | JSON string with keys 'username', 'password', 'engine', 'host', 'port', 'dbClusterIdentifier' or 'dbName', 'ssl'. See example and strucure [in the AWS documentation here](https://docs.aws.amazon.com/secretsmanager/latest/userguide/reference_secret_json_structure.html#reference_secret_json_structure_docdb). The password from this JSON object will be url-encoded by the tap before opening the database connection. The intent of this setting is to enable management of an AWS DocumentDB database credential via AWS SecretsManager                                             |
| documentdb_credential_json_extra_options | string             |  False   |                  -                  | JSON string containing key-value pairs which will be added to the connection string options when using documentdb_credential_json_string. For example, when set to the string `{"tls":"true","tlsCAFile":"my-ca-bundle.pem"}`, the options `tls=true&tlsCAFile=my-ca-bundle.pem` will be passed to the MongoClient.                                                                                                                                                                                                                                                                          |
| datetime_conversion                      | string             |  False   |              datetime               | Parameter passed to MongoClient 'datetime_conversion' parameter. See documentation at https://pymongo.readthedocs.io/en/stable/examples/datetimes.html#handling-out-of-range-datetimes for details. The default value is 'datetime', which will throw a bson.errors.InvalidBson error if a document contains a date outside the range of datetime.MINYEAR (year 1) to datetime.MAXYEAR (9999).                                                                                                                                                                                               |
| prefix                                   | string             |  False   |                 ''                  | An optional prefix which will be added to the name of each stream.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| filter_collections                       | string \| string[] |  False   |                 []                  | Collections to discover (default: all). Useful for improving catalog discovery performance.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| start_date                               | date_iso8601       |  False   |             1970-01-01              | Start date - used for incremental replication only. In log-based replication mode, this setting is ignored.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| add_record_metadata                      | boolean            |  False   |                False                | When true, _sdc metadata fields will be added to records produced by the tap.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| allow_modify_change_streams              | boolean            |  False   |                False                | In AWS DocumentDB (unlike MongoDB), change streams must be enabled specifically (see the [documentation here](https://docs.aws.amazon.com/documentdb/latest/developerguide/change_streams.html#change_streams-enabling) ). If attempting to open a change stream against a collection on which change streams have not been enabled, an OperationFailure error will be raised. If this property is set to True, when this error is seen, the tap will execute an admin command to enable change streams and then retry the read operation. Note: this may incur new costs in AWS DocumentDB. |
| operation_types                          | list(string)       |  False   | create,delete,insert,replace,update | List of MongoDB change stream operation types to include in tap output. The default behavior is to limit to document-level operation types. See full list of operation types in [the MongoDB documentation](https://www.mongodb.com/docs/manual/reference/change-events/#operation-types). Note that the list of allowed_values for this property includes some values not available to all MongoDB versions.                                                                                                                                                                                |                                                                                                                                                                               |

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Configuration

#### MongoDB

#### AWS DocumentDB

### Source Authentication and Authorization

#### Incremental replication mode

In order to run `tap-mongodb` in incremental replication mode, the credential used must have read privileges to the
collections from which you wish to extract records. If your credential has the `readAnyDatabase@admin` permission, for
example, or `read@test_database` (where `test_database` is the `database` setting in the tap's configuration), that
should be sufficient.

Collection-level read permissions are untested but are expected to work as well:

```javascript
privileges: [
    {resource: {db: "test_database", collection: "TestOrders"}, actions: ["find"]}
]
```

The above collection-level read permission should allow the tap to extract from the `test_database.TestOrders`
collection in incremental replication mode.

#### Log-based replication

In order to run `tap-mongodb` in log-based replication mode, which extracts records via the database's Change Streams
API, MongoDB and AWS DocumentDB have different requirements around permissions.

In MongoDB, the credential must have both `find` and `changeStreams` permissions on a database collection in order to
use `tap-mongodb` in log-based replication mode. The `readAnyDatabase@admin` built-in role provides this for all
databases, while `read@test_database` will provide the necessary access for all collections in the `test_database`
database.

## Usage

You can easily run `tap-mongodb` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-mongodb --version
tap-mongodb --help
tap-mongodb --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_mongodb/tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `tap-mongodb` CLI interface directly using `poetry run`:

```bash
poetry run tap-mongodb --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-mongodb
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-mongodb --version
# OR run a test `elt` pipeline:
meltano run tap-mongodb target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
