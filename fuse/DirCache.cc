/*----------------------------------------------------------------------------*/
#include <cstring>
#include <google/dense_hash_map>
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "DirCache.hh"
/*----------------------------------------------------------------------------*/

#define NAME_SIZE 16348
#define MAX_CACHE_SIZE 10000

using std::string;
using std::tr1::hash;
using google::dense_hash_map;


struct eqstr {
  bool operator()(string s1, string s2) const {
    return (s1.compare(s2) == 0);
  }
};


struct eqllu {
  bool operator()(long long s1, long long s2) const {
    return (s1 == s2);
  }
};


class DirCache;
class DirEntry;
class SubDirEntry;

typedef dense_hash_map<long long, DirEntry*, hash<long long>, eqllu>::iterator dir_iterator;
typedef dense_hash_map<string, SubDirEntry*, hash<string>, eqstr>::iterator subdir_iterator;

DirCache* cache = NULL;


/*----------------------------------------------------------------------------*/
class SubDirEntry 
{
public:
  string name_;
  fuse_ino_t inode_;
  struct fuse_entry_param e_;
  
  SubDirEntry(const char* name, fuse_ino_t inode, struct fuse_entry_param *e)
    : inode_(inode) 
  {
    name_ = name;
    e_.attr_timeout = e->attr_timeout;
    e_.entry_timeout = e->entry_timeout;
    e_.ino = e->attr.st_ino;
    e_.attr = e->attr;
    e_.generation = 0;
  }; 
  
  ~SubDirEntry(){};
};


/*----------------------------------------------------------------------------*/
class DirEntry
{
public:
  time_t mtv_sec_;  
  struct dirbuf b_;
  char* name_;

  DirEntry (const char          *name, 
            unsigned long long  inode, 
            int                 no_entries, 
            time_t              mtv_sec, 
            struct dirbuf      *b)
    : filled_(false), no_entries_(no_entries), inode_(inode)
  {
    mtv_sec_ = mtv_sec;
    name_ = new char[NAME_SIZE];
    name_ = strcpy(name_, name);

    b_.size = b->size;
    b_.p = new char[b_.size];
    b_.p = (char*) memcpy(b_.p, b->p, b_.size *  sizeof(char));

    subdir_hash_.set_empty_key("\0");
    subdir_hash_.set_deleted_key(" ");
  };


  ~DirEntry() 
  {
    delete[] name_;
    delete[] b_.p;
    subdir_hash_.clear();
  }

  int GetNoEntries() { return no_entries_; };

  bool Filled () { return filled_; };

  void Update(const char *name, int no_entries, time_t mtv_sec, struct dirbuf *b) 
  {
    name_ = (char*) memset((void *)name_, 0, NAME_SIZE);
    name_ = strcpy(name_, name);
    mtv_sec_ = mtv_sec;
    no_entries_ = no_entries;
    subdir_hash_.clear();

    if (b_.size != b->size){
      b_.size = b->size;
      b_.p = (char*) realloc(b_.p, b_.size * sizeof(char));
    }
    b_.p = (char*) memset(b_.p, 0, b_.size * sizeof(char));
    b_.p = (char*) memcpy(b_.p, b->p, b_.size * sizeof(char));
    filled_ = false;
  };


  SubDirEntry* GetEntry(const char *name) 
  {
    string entry_name = name;
    subdir_iterator iter = subdir_hash_.find(entry_name);
    if (iter != subdir_hash_.end())
      return (SubDirEntry*) iter->second;
    else
      return NULL;
  };

  
  void AddEntry(SubDirEntry* entry)
  {
    subdir_hash_[entry->name_] = entry;
    if (subdir_hash_.size() >= (unsigned int)(no_entries_ - 2))
      filled_ = true;
  };

private:
  bool filled_;
  int no_entries_;
  unsigned long long inode_;
  dense_hash_map<string, SubDirEntry*, hash<string>, eqstr> subdir_hash_; 
};


/*----------------------------------------------------------------------------*/
class DirCache 
{
public:
  dense_hash_map<long long, DirEntry*, hash<long long> , eqllu> dir_hash_;

  DirCache() 
  {
    total_dirs_ = 0;
    dir_hash_.set_empty_key(0);  
    dir_hash_.set_deleted_key(-1);  
    pthread_mutex_init(&mutex_, NULL);
  };

  void SizeControl() {
    if (total_dirs_ >= MAX_CACHE_SIZE) {
      DirEntry* dir = NULL;
      unsigned long long i = 0;
      unsigned long long entries_del = (unsigned long long) (0.25 * MAX_CACHE_SIZE);
      dir_iterator iter = dir_hash_.begin();
      
      while ((i <= entries_del) && (iter != dir_hash_.end())){
        dir = (DirEntry*) iter->second;
        i += dir->GetNoEntries();
        total_dirs_ -= dir->GetNoEntries();
        dir_hash_.erase(iter++);
      }
    }   
  };

  void AddNoEntries(int count) { total_dirs_ += count; };

  void Lock() { pthread_mutex_lock(&mutex_); };

  void Unlock() { pthread_mutex_unlock(&mutex_); };


  ~DirCache() {
    pthread_mutex_destroy(&mutex_);
  };

private:
  pthread_mutex_t mutex_;
  unsigned long long total_dirs_;  //total number of entries stored in cache (subdirs)
};


/*----------------------------------------------------------------------------*/
//initialise the cache data structure
void cache_init() 
{
  cache = new DirCache();
}


//@return -1 - error
//         0 - not in cache
//         1 - dir in cache and valid
//         2 - in cache but not valid, to be updated
/*----------------------------------------------------------------------------*/
//fill the dirbuf structure with info from cache and return it
int get_dir_from_cache(fuse_ino_t inode, time_t mtv_sec, char *fullpath, 
                       struct dirbuf **b) 
{
  cache->Lock();
  int retc;
  char* namep;
  unsigned long long in;
  dir_iterator iter = cache->dir_hash_.find((long long) inode);
  
  if (iter != cache->dir_hash_.end()) {
    DirEntry *dir = (DirEntry*)iter->second;
    if (dir->mtv_sec_ == mtv_sec){
      if (xrd_inodirlist_entry(inode, 0, &namep, &in)) {
        xrd_inodirlist(inode, fullpath);
        *b = xrd_inodirlist_getbuffer(inode);
        if (!b) 
          retc = -1 ; //error
        else {
          (*b)->size = dir->b_.size;
          (*b)->p = (char*) realloc((*b)->p, (*b)->size * sizeof(char));
          (*b)->p = (char*) memcpy((*b)->p, dir->b_.p, (*b)->size * sizeof(char));
          retc = 1; 
        }
      }
      else {
        *b = xrd_inodirlist_getbuffer(inode);
        retc = 1; //in cache and valid
      }
    }
    else 
      retc = 2; //in cache but not valid
  }
  else
    retc = 0;   //not in cache
  
  cache->Unlock();
  return retc;
}


/*----------------------------------------------------------------------------*/
//add or update dir in cache
void sync_dir_in_cache(fuse_ino_t inode, char *name, 
                       int nentries, time_t mtv_sec, struct dirbuf *b) 
{
  cache->Lock();
  dir_iterator iter = cache->dir_hash_.find((unsigned long long) inode);
  if (iter != cache->dir_hash_.end()){ //update
    DirEntry* dir = (DirEntry*) iter->second;
    dir->Update(name, nentries, mtv_sec, b);  
  }
  else {                              //add
    cache->SizeControl();  //delete some entries if cache full
    DirEntry *dir  = new DirEntry(name, inode, nentries, mtv_sec, b);
    cache->dir_hash_[(unsigned long long)inode] = dir;
    cache->AddNoEntries(nentries);
  }
  cache->Unlock();
}


//@return -2 - dir filled but no entry found
//        -1 - dir in cache but not filled
//         0 - dir not in cache
//         1 - found entry
/*----------------------------------------------------------------------------*/
int get_entry_from_dir(fuse_req_t req, fuse_ino_t dir_inode, 
                       const char* entry_name, const char* ifullpath) 
{ 
  cache->Lock();
  int retc;
  dir_iterator iter = cache->dir_hash_.find((unsigned long long)dir_inode);
  if (iter != cache->dir_hash_.end()) {
    DirEntry* dir = (DirEntry*) iter->second;
    if (dir->Filled()){
      SubDirEntry* entry = (SubDirEntry*)dir->GetEntry(entry_name);
      if (entry) {
        xrd_store_inode(entry->e_.attr.st_ino, ifullpath);
        fuse_reply_entry(req, &(entry->e_));
        retc = 1; //success
      }
      else 
        retc = -2;//dir filled but entry not found
    }
    else 
      retc = -1;  //dir in cache but not filled
  }
  else
    retc = 0;     //dir not in cache
  
  cache->Unlock();
  return retc;
}


/*----------------------------------------------------------------------------*/
void add_entry_to_dir(fuse_ino_t dir_inode, fuse_ino_t entry_inode, 
                      const char *entry_name, struct fuse_entry_param *e)
{
  cache->Lock();
  dir_iterator iter = cache->dir_hash_.find((unsigned long long) dir_inode);
  if (iter != cache->dir_hash_.end()){
    DirEntry* dir = (DirEntry*) iter->second;
    SubDirEntry * entry = new SubDirEntry(entry_name, entry_inode, e);
    dir->AddEntry(entry);
  }
  cache->Unlock();
}



