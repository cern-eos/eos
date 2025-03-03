# How to run the ns_quarkdb microbenchmark

## CMake flag + EOS Compilation

It's important to build EOS as `RelWithDebInfo` otherwise Google Benchmark will tell you that the benchmark may not be accurate.
In order to use the provided Google Benchmark, add the flag `-DUSE_SYSTEM_GBENCH="OFF"`.

E.g:
```bash
cmake ../ -G Ninja -DUSE_SYSTEM_GBENCH="OFF" -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DCMAKE_INSTALL_PREFIX=/usr/ -Wno-dev
```

## Create a new quarkdb instance

Create a new quarkdb instance under `/var/lib/quarkdb/quarkdb-unit-tests`.


```bash
# Create the following configuration file:
 $ cat /etc/xrd.cf.quarkdb-unit-tests 
xrd.port 7778
xrd.protocol redis:7778 libXrdQuarkDB.so

redis.mode standalone
redis.database /var/lib/quarkdb/quarkdb-unit-tests

#----------------------------------------------------------
# $EOS_QUARKDB_HOSTPORT environment variable must be set
# with the same value used for redis.myself
#----------------------------------------------------------
# redis.myself localhost:9999

#----------------------------------------------------------
# $EOS_QUARKDB_PASSWD environment variable must be set
# with the same value used for redis.password
#----------------------------------------------------------
redis.password_file /etc/eos.keytab

```

```bash
UUID=eostest-$(uuidgen); echo $UUID; sudo runuser daemon -s /bin/bash -c "quarkdb-create --path /var/lib/quarkdb/quarkdb-unit-tests --clusterID $UUID --nodes localhost:7778"
```

```bash
# Create this configuration file too:
$ cat /etc/systemd/system/eos@quarkdb-unit-tests.service.d/custom.conf
[Service]
User=daemon
Group=daemon
```

```bash
systemctl start eos@quarkdb-unit-tests
```

## Run the benchmark

```bash
export EOS_QUARKDB_HOSTPORT='localhost:7778'
cd /eos/build/dir/
/test/microbenchmarks/eos-nslocking-microbenchmark
```

