#ifndef __EOSMGM_FSVIEW__HH__
#define __EOSMGM_FSVIEW__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <sys/vfs.h>
#include <map>
#include <set>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------
//! Classes providing views on filesystems by space,group,node
//------------------------------------------------------------------------

class BaseView : public std::set<eos::common::FileSystem::fsid_t> {
public:
  std::string mName;
  std::string mType;
  BaseView(){};
  ~BaseView(){};

  void Print(std::string &out, std::string headerformat, std::string listformat);

  virtual std::string GetMember(std::string member) {
    if (member == "name")
      return mName;
    if (member == "type")
      return mType;

    return "";
  }

  long long SumLongLong(const char* param); // calculates the sum of <param> as long long
  double SumDouble(const char* param);      // calculates the sum of <param> as double
  double AverageDouble(const char* param);  // calculates the average of <param> as double
  double SigmaDouble(const char* param);    // calculates the standard deviation of <param> as double
};

class FsSpace : public BaseView {
public:

  FsSpace(const char* name) {mName = name; mType = "spaceview";}
  ~FsSpace() {};
};

//------------------------------------------------------------------------
class FsGroup : public BaseView {
public:

  FsGroup(const char* name) {mName = name; mType="groupview";}
  ~FsGroup(){};
};

//------------------------------------------------------------------------
class FsNode : public BaseView {
public:

  FsNode(const char* name) {mName = name; mType="nodesview";}
  ~FsNode(){};
};

//------------------------------------------------------------------------
class FsView : public eos::common::LogId {
private:
  
  eos::common::FileSystem::fsid_t NextFsId;
  std::map<eos::common::FileSystem::fsid_t , std::string> Fs2UuidMap;
  std::map<std::string, eos::common::FileSystem::fsid_t>  Uuid2FsMap;

public:

  bool Register   (eos::common::FileSystem* fs); // this adds or modifies a filesystem
  bool UnRegister (eos::common::FileSystem* fs); // this removes a filesystem

  bool RegisterNode   (const char* nodequeue);            // this adds or modifies an fst node
  bool UnRegisterNode (const char* nodequeue);            // this removes an fst node

  eos::common::RWMutex ViewMutex;  // protecting all xxxView variables
  eos::common::RWMutex MapMutex;   // protecting all xxxMap varables

  std::map<std::string , FsSpace* > mSpaceView;
  std::map<std::string , FsGroup* > mGroupView;
  std::map<std::string , FsNode* >  mNodeView;

  std::map<eos::common::FileSystem::fsid_t, eos::common::FileSystem*> mIdView;
  std::map<eos::common::FileSystem*, eos::common::FileSystem::fsid_t> mFileSystemView;

  // filesystem mapping functions
  eos::common::FileSystem::fsid_t CreateMapping(std::string fsuuid);
  bool                        ProvideMapping(std::string fsuuid, eos::common::FileSystem::fsid_t fsid);
  eos::common::FileSystem::fsid_t GetMapping(std::string fsuuid);
  std::string GetMapping(fsid_t fsuuid);

  void PrintSpaces(std::string &out, std::string headerformat, std::string listformat);
  void PrintGroups(std::string &out, std::string headerformat, std::string listformat);
  void PrintNodes (std::string &out, std::string headerformat, std::string listformat);

  FsView() {};
  ~FsView() {};

  static FsView gFsView; // singleton
};

EOSMGMNAMESPACE_END

#endif
