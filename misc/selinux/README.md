eosfuse SELinux
===============

The eosfuse SELinux policy is built via the following steps:
1. Establish the platform distribution `dist` (e.g.: CC7)
2. Write the new `eosfuse.te` file -- Remember to increase the version!
2. Run `checkmodule -M -m -o eosfuse.mod eosfuse.te`
3. Run `semodule_package -o eosfuse-${dist}.pp -m eosfuse.mod`
4. Test the policy: `semodule -i eosfuse-${dist}.pp`

eosfuse and the build process
-----------------------------

The eosfuse policy files for the supported platforms are version controlled.

In the build process, the `eosfuse-selinux` target runs the `choose_selinux.sh`
to decide which policy to pick depending on the platform (defaults to CC7). 
In the end, the chosen file will be installed as `eosfuse.pp`.

#### Example

```bash
# Choosing the version
CC7  --> eosfuse-7.pp
rest --> eosfuse-7.pp 

# Actual installation
/usr/share/selinux/targeted/eosfuse.pp
/usr/share/selinux/mls/eosfuse.pp
/usr/share/selinux/strict/eosfuse.pp
```
