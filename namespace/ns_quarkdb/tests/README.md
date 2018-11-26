## Running EOS QuarkDB Namespace tests

In order to run the `eos-ns-quarkdb-tests` executable,
you must have a running QuarkDB instance available.

The executable will connect to that instance using parameters provided
in the following environment variables:

```shell
EOS_QUARKDB_HOSTPORT (defaults to localhost:9999)
EOS_QUARKDB_PASSWD
```

You must make sure that these variables contain the same values declared
in the QuarkDB config file.


### Installing and running QuarkDB

The QuarkDB library can be installed from:
http://storage-ci.web.cern.ch/storage-ci/quarkdb/tag/el7/x86_64/

Some setup is necessary before running for the first time.
A complete documentation can be found [here][1].

QuarkDB runs in a similar fashion to other XRootD plugins: `xroot -c config.file`

### The QuarkDB config file

Packaged with EOS, a simple QuarkDB configuration file is provided :
`xrd.cf.quarkdb`.

Ensure the same configuration values are used here
as in the environment variables.

## Build, setup and run demo

A simple setup from build to running the tests is provided as guidance.

```shell
# EOS_QUARKDB_HOSTPORT=localhost:9999
# EOS_QUARKDB_PASSWD=password_must_be_atleast_32_characters

# Build executable
mkdir build
cd build/
cmake3 ../
make eos-ns-quarkdb-tests -j4

# Install, setup and run QuarkDB
yum install -y quarkdb
quarkdb-create --path /var/lib/quarkdb/node-1 --clusterID ns-test --nodes $EOS_QUARKDB_HOSTPORT
chown -R daemon:daemon /var/lib/quarkdb
xrootd -n quarkdb -c xrd.cf.quarkdb -l /var/log/quarkdb/xrdlog.quarkdb -Rdaemon &

# Run tests
./namespace/ns_quarkdb/tests/eos-ns-quarkdb-tests
```

[1]: http://quarkdb.web.cern.ch/quarkdb/docs/master/CONFIGURATION.html