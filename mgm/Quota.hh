#ifndef __EOSMGM_QUOTA__HH__
#define __EOSMGM_QUOTA__HH__

/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
/*----------------------------------------------------------------------------*/
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/GlobalConfig.hh"
#include "common/RWMutex.hh"
#include "mgm/FstFileSystem.hh"
#include "mq/XrdMqMessage.hh"
#include "namespace/accounting/QuotaStats.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <set>
#include <stdint.h>
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

#define EOSMGMQUOTA_DISKHEADROOM 1024ll*1024ll*1024l*25

class SpaceQuota {
private:
  XrdSysMutex Mutex;
  XrdOucString SpaceName;
  time_t LastCalculationTime;
  time_t LastEnableCheck;
  bool On;
  eos::QuotaNode* QuotaNode;

  bool Enabled() {
    time_t now = time(NULL);
    if ( now > (LastEnableCheck+5) ) {
      std::string spacename = SpaceName.c_str();
      std::string key="quota";
      if (FsView::gFsView.mSpaceView.count(spacename)) {
	std::string ison = FsView::gFsView.mSpaceView[spacename]->GetConfigMember(key);
	if (ison == "on") {
	  On = true;
	} else {
	  On = false;
	}
      } else {
	On = false;
      }
      LastEnableCheck = now;
    }
    return On;
  }

  // one hash map for user view! depending on eQuota Tag id is either uid or gid!

  google::dense_hash_map<long long, unsigned long long> Quota; // the key is (eQuotaTag<<32) | id

  unsigned long long PhysicalFreeBytes; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalFreeFiles; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalMaxBytes;  // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalMaxFiles;  // this is coming from the statfs calls on all file systems

  // this is used to recalculate the values without invalidating the old one
  unsigned long long PhysicalTmpFreeBytes; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalTmpFreeFiles; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalTmpMaxBytes;  // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalTmpMaxFiles;  // this is coming from the statfs calls on all file systems

public:
  XrdSysMutex OpMutex;

  enum eQuotaTag {kUserBytesIs=1, kUserBytesTarget=2, kUserFilesIs=3, kUserFilesTarget=4, kGroupBytesIs=5, kGroupBytesTarget=6, kGroupFilesIs=7, kGroupFilesTarget=8, kAllUserBytesIs=9, kAllUserBytesTarget=10, kAllGroupBytesIs=11, kAllGroupBytesTarget=12, kAllUserFilesIs=13, kAllUserFilesTarget=14, kAllGroupFilesIs=15, kAllGroupFilesTarget=16};


  static const char* GetTagAsString(int tag) {
    if (tag == kUserBytesTarget) { return "userbytes";} 
    if (tag == kUserFilesTarget) { return "userfiles";}
    if (tag == kGroupBytesTarget){ return "groupbytes";}
    if (tag == kGroupFilesTarget){ return "groupfiles";}
    if (tag == kAllUserBytesTarget) { return "alluserbytes";}
    if (tag == kAllUserFilesTarget) { return "alluserfiles";}
    if (tag == kAllGroupBytesTarget){ return "allgroupbytes";}
    if (tag == kAllGroupFilesTarget){ return "allgroupfiles";}    
    return 0;
  }    

  static unsigned long GetTagFromString(XrdOucString &tag) {
    if (tag == "userbytes"    ) return kUserBytesTarget;
    if (tag == "userfiles"    ) return kUserFilesTarget;
    if (tag == "groupbytes"   ) return kGroupBytesTarget;
    if (tag == "groupfiles"   ) return kGroupFilesTarget;
    if (tag == "alluserbytes" ) return kAllUserBytesTarget;
    if (tag == "alluserfiles" ) return kAllUserFilesTarget;
    if (tag == "allgroupbytes") return kAllGroupBytesTarget;
    if (tag == "allgroupfiles") return kAllGroupFilesTarget;
    return 0;
  }    

  const char* GetTagCategory(int tag) {

    if ( ( tag == kUserBytesIs) || ( tag == kUserBytesTarget) ||
	 ( tag == kUserFilesIs) || ( tag == kUserFilesTarget) ) return "USER ";
    if ( ( tag == kGroupBytesIs)|| ( tag == kGroupBytesTarget) ||
	 ( tag == kGroupFilesIs)|| ( tag == kGroupFilesTarget) ) return "GROUP";
    if ( ( tag == kAllUserBytesIs) || ( tag == kAllUserBytesTarget) ||
	 ( tag == kAllUserFilesIs) || ( tag == kAllUserFilesTarget) ) return "USER ";
    if ( ( tag == kAllGroupBytesIs)|| ( tag == kAllGroupBytesTarget) ||
	 ( tag == kAllGroupFilesIs)|| ( tag == kAllGroupFilesTarget) ) return "GROUP";
    return "-----";
  }
  const char* GetTagName(int tag) {
    if (tag == kUserBytesIs)     { return "USED-BYTES";}
    if (tag == kUserBytesTarget) { return "AVAL-BYTES";} 
    if (tag == kUserFilesIs)     { return "USED-FILES";}
    if (tag == kUserFilesTarget) { return "AVAL-FILES";}
    if (tag == kGroupBytesIs)    { return "USED-BYTES";}
    if (tag == kGroupBytesTarget){ return "AVAL-BYTES";}
    if (tag == kGroupFilesIs)    { return "USED-FILES";}
    if (tag == kGroupFilesTarget){ return "AVAL-FILES";}
    if (tag == kAllUserBytesIs)     { return "USED-BYTES";}
    if (tag == kAllUserBytesTarget) { return "AVAL-BYTES";}
    if (tag == kAllUserFilesIs)     { return "USED-FILES";}
    if (tag == kAllUserFilesTarget) { return "AVAL-FILES";}
    if (tag == kAllGroupBytesIs)    { return "USED-BYTES";}
    if (tag == kAllGroupBytesTarget){ return "AVAL-BYTES";}
    if (tag == kAllGroupFilesIs)    { return "USED-FILES";}
    if (tag == kAllGroupFilesTarget){ return "AVAL-FILES";}
    return "---- -----";
  }

  eos::QuotaNode* GetQuotaNode() { return QuotaNode; }
  
  const char* GetQuotaStatus(unsigned long long is, unsigned long long avail) {
    double p = (avail)?(100.0 * is /avail): 100.0;
    if (p < 90) {
      return "OK";
    }
    if (p < 99) {
      return  "WARNING";
    }
    return "EXCEEDED";
  }
  
  const char* GetQuotaPercentage(unsigned long long is, unsigned long long avail, XrdOucString &spercentage) {
    char percentage[1024];
    sprintf(percentage, "%.02f", avail?(100.0 * is / avail):100.0);
    spercentage = percentage;
    return spercentage.c_str();
  }

  bool NeedsRecalculation() { if ( (time(NULL) - LastCalculationTime) > 10 ) return true; else return false;}

  void UpdateTargetSums();
  void UpdateIsSums();


  SpaceQuota(const char* name);
  ~SpaceQuota();

  google::dense_hash_map<long long, unsigned long long>::const_iterator Begin() { return Quota.begin();}
  google::dense_hash_map<long long, unsigned long long>::const_iterator End()   { return Quota.end();}

  ///////////////////////////////////////////////////////////////////////////////////////////////////////
  // Scheduling Filesystem Vector/Hashs


  // std::string = <grouptag>|<uid>:<gid> 
  std::map<std::string, FsGroup*> schedulingGroup;
  // std::string = <schedulinggroupindex>:<grouptag>|<uid>:<gid>
  std::map<std::string, eos::common::FileSystem::fsid_t > schedulingFileSystem;

  
  // First index is the scheduling group index f.e. if we have default.0,default.1,default.2 .... SchedulingView[0] points to the array with all file system IDs in scheduling group default.0 ... if we have only default all filesystem are in index 0

  const char* GetSpaceName() { return SpaceName.c_str();}

  long long GetQuota(unsigned long tag, unsigned long id, bool lock=true);

  void SetQuota(unsigned long tag, unsigned long id, unsigned long long value, bool lock=true); 
  void AddQuota(unsigned long tag, unsigned long id, long long value, bool lock=true); 

  void SetPhysicalTmpFreeBytes(unsigned long long bytes) {PhysicalTmpFreeBytes = bytes;}
  void SetPhysicalTmpFreeFiles(unsigned long long files) {PhysicalTmpFreeFiles = files; }

  void SetPhysicalFreeBytes(unsigned long long bytes) {PhysicalFreeBytes = bytes;}
  void SetPhysicalFreeFiles(unsigned long long files) {PhysicalFreeFiles = files; }

  void SetPhysicalTmpMaxBytes(unsigned long long bytes)  {PhysicalTmpMaxBytes = bytes;}
  void SetPhysicalTmpMaxFiles(unsigned long long files)  {PhysicalTmpMaxFiles = files;}

  void SetPhysicalMaxBytes(unsigned long long bytes)  {PhysicalMaxBytes = bytes;}
  void SetPhysicalMaxFiles(unsigned long long files)  {PhysicalMaxFiles = files;}

  void ResetPhysicalFreeBytes() {SetPhysicalFreeBytes(0);}
  void ResetPhysicalFreeFiles() {SetPhysicalFreeFiles(0);}

  void ResetPhysicalMaxBytes()  {SetPhysicalMaxBytes(0);}
  void ResetPhysicalMaxFiles()  {SetPhysicalMaxFiles(0);}

  void ResetPhysicalTmpFreeBytes() {SetPhysicalTmpFreeBytes(0);}
  void ResetPhysicalTmpFreeFiles() {SetPhysicalTmpFreeFiles(0);}

  void ResetPhysicalTmpMaxBytes()  {SetPhysicalTmpMaxBytes(0);}
  void ResetPhysicalTmpMaxFiles()  {SetPhysicalTmpMaxFiles(0);}

  void AddPhysicalFreeBytes(unsigned long long bytes) {PhysicalFreeBytes += bytes;}
  void AddPhysicalFreeFiles(unsigned long long files) {PhysicalFreeFiles += files;}
  void AddPhysicalMaxBytes(unsigned long long bytes)  {PhysicalMaxBytes += bytes;}
  void AddPhysicalMaxFiles(unsigned long long files)  {PhysicalMaxFiles += files;}

  void AddPhysicalTmpFreeBytes(unsigned long long bytes) {PhysicalTmpFreeBytes += bytes;}
  void AddPhysicalTmpFreeFiles(unsigned long long files) {PhysicalTmpFreeFiles += files;}
  void AddPhysicalTmpMaxBytes(unsigned long long bytes)  {PhysicalTmpMaxBytes += bytes;}
  void AddPhysicalTmpMaxFiles(unsigned long long files)  {PhysicalTmpMaxFiles += files;}

  void PhysicalTmpToFreeBytes() {PhysicalFreeBytes=PhysicalTmpFreeBytes;}
  void PhysicalTmpToFreeFiles() {PhysicalFreeFiles=PhysicalTmpFreeFiles;}
  void PhysicalTmpToMaxBytes()  {PhysicalMaxBytes=PhysicalTmpMaxBytes;}
  void PhysicalTmpToMaxFiles()  {PhysicalMaxFiles=PhysicalTmpMaxFiles;}

  unsigned long long GetPhysicalFreeBytes() { return PhysicalFreeBytes;}
  unsigned long long GetPhysicalFreeFiles() { return PhysicalFreeFiles;}
  unsigned long long GetPhysicalMaxBytes()  { return PhysicalMaxBytes;}
  unsigned long long GetPhysicalMaxFiles()  { return PhysicalMaxFiles;}

  void RmQuota(unsigned long tag, unsigned long id, bool lock=true);

  void ResetQuota(unsigned long tag, unsigned long id, bool lock=true) {
    return SetQuota(tag, id, 0, lock);
  }
  void PrintOut(XrdOucString& output, long uid_sel=-1, long gid_sel=-1, bool monitoring=false);


  unsigned long long Index(unsigned long tag, unsigned long id) { unsigned long long fulltag = tag; fulltag <<=32; fulltag |= id; return fulltag;}
  unsigned long UnIndex(unsigned long long reindex) {return (reindex>>32) & 0xffff;}

  // the write placement routine
  int FilePlacement(uid_t uid, gid_t gid, const char* grouptag, unsigned long lid, std::vector<unsigned int> &selectedfs, bool truncate=false, int forcedindex=-1, unsigned long long bookingsize=1024*1024*1024ll);

  // the access routine
  int FileAccess(uid_t uid, gid_t gid, unsigned long forcedfsid, const char* forcedspace, unsigned long lid, std::vector<unsigned int> &locationsfs, unsigned long &fsindex, bool isRW);

};

class Quota : eos::common::LogId {
private:
  
public:
  static XrdOucHash<SpaceQuota> gQuota;
  static eos::common::RWMutex gQuotaMutex;

  static SpaceQuota* GetSpaceQuota(const char* name, bool nocreate=false);
  
  Quota() {}
  ~Quota() {};

  void   Recalculate();

  // builds a list with the names of all spaces
  static int GetSpaceNameList(const char* key, SpaceQuota* spacequota, void *Arg);

  static void PrintOut(const char* space, XrdOucString &output, long uid_sel=-1, long gid_sel=-1, bool monitoring = false);
  
  static bool SetQuota(XrdOucString space, long uid_sel, long gid_sel, long long bytes, long long files, XrdOucString &msg, int &retc); // -1 means it is not set for all long/long long values

  static bool RmQuota(XrdOucString space, long uid_sel, long gid_sel, XrdOucString &msg, int &retc); // -1 means it is not set for all long/long long values

  // callback function for the namespace implementation to calculate the size a file occupies
  static uint64_t MapSizeCB(const eos::FileMD *file);

  // load function to initialize all SpaceQuota's with the quota node definition from the namespace
  static void LoadNodes();

  // inserts the current state of the quota nodes into SpaceQuota's
  static void NodesToSpaceQuota();

  // insert current state of a single quota node into a SpaceQuota
  static void NodeToSpaceQuota(const char* name, bool lock=true);

};

EOSMGMNAMESPACE_END

#endif
