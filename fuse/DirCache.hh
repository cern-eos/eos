#ifndef __CACHED_DIR__HH__
#define __CACHED_DIR__HH__

/*--------------------------------------------------------------------------------------------------------*/
#include "xrdposix.hh"
#include <fuse/fuse_lowlevel.h>
/*--------------------------------------------------------------------------------------------------------*/

#ifdef __cplusplus
  extern "C" {
#endif

  void cache_init();
  int get_dir_from_cache(fuse_ino_t inode, time_t mtv_sec, char *fullpath, struct dirbuf **b);
  void sync_dir_in_cache(fuse_ino_t dir_inode, char *name, int nentries, time_t mtv_sec, struct dirbuf *b);

  int get_entry_from_dir(fuse_req_t req, fuse_ino_t dir_inode, const char* entry_name, const char* ifullpath);
  void add_entry_to_dir(fuse_ino_t dir_inode, fuse_ino_t entry_inode, const char *entry_name, struct fuse_entry_param *e);

#ifdef __cplusplus
  }
#endif

#endif
