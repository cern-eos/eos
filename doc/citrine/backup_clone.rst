Clones for Backups
==================

Summary
-------

The EOS backup system backs up EOS data from short-lived, copy-on-write backup clones
into "blobs" onto a local file system or another EOS instance such as CTA.

A backup clone is created at the very beginning of the process, typically within only seconds. From there
copy-on-write techniques ensure that data remain unchanged as of the clone creation time while they are 
copied to backup media, typically much slower. The system operates on
a directory hierarchy and supports full and differential/incremental backups. Backups can be nested, i.e a higher level
backup will skip directories handled by lower-level backups.

A file is included in an incremental backup based on
file modification time tracked independently of mtime: the set of files
modified after a certain timestamp are identified in a json-formatted backup catalogue and marked for copy-on-write 
in case of modifications. At the end of the backup, the backup clone is automatically deleted.

Two python
programs, eos-backup and eos-backup-browser, are the high level entry into the system. 

- eos-backup scans the catalogue and stores data in blobs. It also performs the inverse operation, reconstructing files from data in backup blobs into an EOS hierachy, based on a series of full/incremental backup catalogues.

- eos-backup-browser "mounts" a volatile file system constructed from a sequence of fulle/incremental backup catalogues, which can then be used to quickly view a file (reconstruct it from backup blobs). This can be useful e.g. when searching for clues as to from when a directory should be restored. It is also used to determine which blob files should be present (e.g. recalled from tape) in order to restore a particular sub-hierarchy.

Each backup results in a backup catalogue to be created describing the directoriey hierarchy. This catalogue is vital as the backup blobs are useless without it, it shouled be stored alongside on backup media alongside the blobs. In addition, a consistent history of at least one full and related incremental backups should be kept: in order to reconstruct a directory hierarchy in a consistent manner, potentially all of them are needed upom restore.
  
Clone internal logic
--------------------

1. syncTime

a "server-store-timestamp" is maintained for all directories and files. This is
conceptually different from mtime (which can be set by the user), however: for
containers the TMTime field is used; for files 'syncTime' has been added - but
it is only different from the default (hence mostly not stored) if mtime is set
explicitly, which should be rare. Thus, in most practical cases syncTime will be
mtime, minimising the increase in quarkdb footprint. While a clone exists,
transient "cloneId" and possibly "cloneFST" attributes are maintained in the
mgm.

2. the clone

the clone is a "sparse" or "lazy" clone, starting empty. Upon cloning, all
files are tagged with the cloneId field. When files are modified, their original
contents are "reflinked" on the FSTs (see "cp --reflink", which implements a
copy-on-write on the original FST) into a new file, which is placed in a
container modelled after their parent containers under
/eos/<instance>/proc/clone/cloneId. For each file the clone process returns the
foreseen clone path, for use in backup scripts. 

3. cloning in hierarchies

Cloning starts at directory level. All files at directory level are tagged with
the cloneId. The cloning process then recursively descends into subdirectories.
To prevent this, a directory can be flagged with a different cloneId (i.e. cloning
a parent directory will not re-clone a child directory previously cloned), or a
special "dummy" cloneId used as a marker on a directory preventing descent.  

4. clone process details 

A cloneId is a [unique] second-resolution timestamp generated automatically at
the beginning of the cloning process. The clone process is triggered by a "find"
command with the '-x sys.clone=+...' option (requires
privileges):

 "-x sys.clone=? <dir>"
     displays all files and clone attributes under <dir>,
 "-x sys.clone=-1556195651 <dir>"
     deletes all traces of the clone 1556195651,
 "-x sys.clone=+10 <dir>"
     makes a full backup clone (numbers < 10 are markers, from 10 onwards second-precision timestamps),
 "-x sys.clone=+1 <dir>"
     sets a marker not to descend into <dir>  while cloning,
 "-x sys.clone=+1556195651 <dir>"
     makes a clone of all files altered after 1556195651.0

Cloning tags every file and directory in the hierarchy with the (generated)
cloneId.

5. copy-on-write

Whenever a file is deleted, or entirely rewritten using OTRUNCATE, its
data are first transferred (using a low-cost "rename") to a clone file under a
predefined name in the clone hierarchy.
When a file is
updated (modified) without OTRUNCATE, a copy-on-write clone is created (using
reflink, which is low-cost in recent file systems). 

6. the backup process

"find -j -x sys.clone=+nnnnnnnn /path" clones a directory tree and creates a
json output for reference, which must be kept safe indicating which files should
be backed up and where their copy-on-write clones. As soon as the clone has been
created, write operations to files may result in copy-on-write clones be created,
hence during back-up a decision has to be made whether to back up a possible clone
file or, for a full backup, the live file. For a live file, the decision may even
have to be revised after the copy if a copy-on-write clone has been created meanwhile.

7. cleanup

After data have been backed up, 'find -x sys.clone=-nnnnnnnn' deletes all the cloneId and
clone-tag attributes and cleans the clone directory under /proc/clone/cloneId.
The temporary clones disappear from the FSTs by-and-by. The back-up now 
resides on backup media only. 

A typical backup-restore cycle
------------------------------

Here's a complete backup and restore recipe, including incremental (as
implemented by test/eos-backup-test):

a. backup triggers a full backup issuing 'find -j -x sys.clone=+10 <path>' and
stores the returned json output for reference in a safe place. For each output line, 
the json dictionary's key 'n' contains the original path name, 't' the server-store-time,
'c' the cloneId (if this is different from the backup's cloneId the file belongs to another
backup!), 'p' the clonePath suffix. The eos-backup-test program illustrates how
those can be used for managing backups and restores.
The backupId is the cloneId returned for the top-level
directory (the first line). For each file, backup copies the version under the
clone path to backup media if it exist, or the live file otherwise. If the live
file has been copied, a check is performed again after the copy to assess no
clone file has been created meanwhile, or the clone file replaces the live file
on backup media. <path> is backed up recursively, descent into
subdirectories can be stopped with a directory marker (see above). A filename
ending in a '/' signals a directory. The clonePath is relative to
/eos/<instance>/proc/clone and structured backupId/cloneDir/clonePath for files,
or simply backupId for directories in the report;

b. backup then cleans the backup clone: find -f -x sys.clone=-nnnnnnnn <path>.
The directory returns to its normal EOS state (without copy-on-write overhead);

c. incremental backups may be triggered using
'find -j -x sys.clone=+<previousBackupId> <path>'. The previousBackupId would either
be the backupId of a previous backup or the backupId of the full backup, depending on
the type of increment desired. Again, the backupId of the incremental backup is
the cloneId of the top-level (first) directory. An incremental backup reports
*all* current files below <path>. Those not modified since the previous backup
can be distinguished in the output by a backupId (often '0') that differs from
the top-level directory - no need to back them up again;

d. restore collects all stored backup reports (full, incremental 1,
incremental2, ... up-to-desired-restore date) for <path> into one list sorted by
filename and backupId. The last (!) incremental report contains the *final* list
of files to be restored.  For every file in the sorted list, only the most
recent (highest backupId) is restored from backup media (and only if it appears
in the final list. At this point a filter could be applied to the list to only
restore selected files.




Timing
------

- metadata operations under (ideally) the biggest lock possible for consistency
- the standard write may be delayed for copy-on-write - slow with ancient kernels
- copy-on-write clones are short lived, and only needed for partially modified files
- the backup will be "largely" consistent, even without a BIG lock

Timing in current implementation (in seconds), 100 files in 100 directories
(=10000 files), 30 bytes each (`date`), dockertest-instance, bash script, using
fusex:

creation:           105         /eos/rtb/tobbicke/backuptest/t??/tt??

append `date`:      115

clone creation:     1

append `date`:      443

append `date`:      147

"wc -c" on clones:  45          /eos/dockertest/proc/clone/1558357820/Dxxx/Fxxx         

"wc -c" on live files:  49

remove all clones:  2

The cloning time alone is not visible here, since it includes formatting and
displaying the result. Tests would be needed on bigger instances to evaluate the
impact of mgm locking.

The "append date" processes naturally increase in duration - there are more data
to ship to the FSTs. The first run after the clone actually includes creating
the clones - hence it is more "expensive".



Backup / Restore Utilities
==========================

These utilities wrap the low-end find commands into higher level operational tools:

eos-backup can be used to clone, back up and restore EOS directories. Data are stored in
"blobs" in a filesystem, or in EOS or CTA.

eos-backup-browser can "mount" an existing backup hierarchy (as defined by a series of full and incremental/differential backup catalogues) using fuse for easy consultation or recovery.

eos-backup
----------

 eos-backup clone -B *BackupPrefix* [-P <parentId>] /eos/pathName
   clones a directory tree, fully or based on parentId, and creates a cloneFile; prints "cloneId <cloneId> catalog <cloneFile>" on stdout

 eos-backup backup -B *BackupPrefix* [-F catalogFile] [-P <parentId>] /eos/pathName
   clones a directory tree unless a cloneFile is passed with -F, and then backs up files into *BackupPrefix*.cloneId and deletes the clone; prints "cloneId <cloneId> backup media <backupDir>" followed by whatever the deletion of the clone says.

 eos-backup restore -F /inputCatalog1[,/inputCatalog2[,...]] /outputDirectory
   performs a restore into outputDirectory based on 1 or more catalogFiles

*BackupPrefix* would typically be a root://instance//path into a CTA or EOS store, or be a path on a mounted file system

/eos pathnames are as seen by the MGM, not fuse-mounted in the local file system!

 environment (can also be specified through -U root://eos-instance): 
   EOS_MGM_URL=root://eos-instance *must* point to the MGM serving /eos/pathName

*Example*: a simple full_backup/make_changes/incremental_backup/restore example in bash::

 # full clone and backup in one go, for simplicity into the local file system
 read xx cloneId1 xxx media1 <<<$(eos-backup backup -B /tmp/Backup /eos/dockertest/backuptest)

 # make some changes, delete/add/modify files
 date > /eos/dockertest/backuptest/now1

 # an incremental backup based on full backup
 read xx cloneId2 xxx media2 <<<$(eos-backup backup -B /tmp/Backup -P $cloneId1 /eos/dockertest/backuptest)

 # make more changes, delete/add/modify files
 date >> /eos/dockertest/backuptest/now1
 date > /eos/dockertest/backuptest/now2

 # another incremental backup based on previous backup, illustrating clone/backup in two steps
 read xx cloneId3 xxx cloneFile3 <<<$(eos-backup clone -B /tmp/Backup -P $cloneId2 /eos/dockertest/backuptest)

 # back those files up
 read xx cloneId3 xxx media3 <<<$(eos-backup backup -B /tmp/Backup -F $cloneFile3 /eos/dockertest/backuptest)

 # restore the lot
 eos-backup restore -F $media1/catalog,$media2/catalog,$media3/catalog -B /tmp/Backup /tmp/Restore

   
eos-backup-browser
------------------

 eos-backup-browser -F $media1/catalog[,$media2/catalog[,...]] [-L regexp1[,regexp2[,...]] /mountPoint

   mounts the eos-backup tree designated by the catalog Files under /mountPoint, read-only. Files can
   be copied somewhere else from there, provided the backup media-files are accessible. Can be used to easily recover
   a small set of files, without the need to restore a whole tree!

   if '-L' is specified, it designates a list of regular expressions. All media-files containing data
   for files matching any of the regular expressions are listed. This can be used to establish which files
   would have to be restored from e.g. tape.
   files can be copied 

