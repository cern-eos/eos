Generating man pages
====================

Man pages are generated from the `eos command --help` output
using the `help2man` utility.  

They are generated upon every commit and added to the eos-client RPM package during the CI pipeline.
This process is controlled at build time using the `BUILD_MANPAGES` flag.


Generating manually
-------------------

To generate man pages manually, run the following:  
`./create_man.sh`

This will create a new folder _man1/_ containing all the man archives.

Note: since they are generated automatically, spacing might not be properly aligned.
