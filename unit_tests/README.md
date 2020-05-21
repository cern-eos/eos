## EOS Unit Tests

In development, unit tests are run upon every commit in the CI pipeline.  
Once per day, in the nightly build, a version compiled 
with AddressSanitizer is being executed as well.

### Building and running unit tests

Unit tests are built by default, in the `all` target.  
To build them individually, the following targets are available:  

```bash
make eos-unit-tests 
make eos-unit-tests-fst
```
They are provided as executables, with the EOS dependencies linked statically.  

To run the unit tests, simply start the executable.

### Unit tests with Address Sanitizer 

To compile unit tests using Address Sanitizer, 
enable the CMAKE `ASAN` flag:

```bash
cmake3 ../ -DASAN=1
make eos-unit-tests eos-unit-tests-fst
```
To run unit tests with Address Sanitizer, we recommend using 
the following suppression file `LeakSanitizer.supp`,
in order to ignore known memory leaks.

Upon installation, the file is placed in `/var/eos/test/`.

```bash
LSAN_OPTIONS=suppressions=/var/eos/test/LeakSanitizer.supp eos-unit-tests-fst
```

#### Known memory leaks

Known memory leaks are marked in the `LeakSanitizer.supp` suppression file.  
The file is located within the repo in `misc/var/eos/test`.  

Any _accepted_ memory leak should be registered.

More information on suppressions can be found [here][1].

### Installing from RPMs

Unit tests are provided in the `eos-test` RPM.  

If the Address Sanitizer test package is installed, 
the sanitizer suppression file will also be placed under `/var/eos/test/`. 


[1]: https://github.com/google/sanitizers/wiki/AddressSanitizerLeakSanitizer#suppressions
