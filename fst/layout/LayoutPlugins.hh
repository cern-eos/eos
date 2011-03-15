#ifndef __EOSFST_LAYOUTPLUGIN_HH__
#define __EOSFST_LAYOUTPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/layout/Layout.hh"
#include "fst/layout/PlainLayout.hh"
#include "fst/layout/ReplicaLayout.hh"
#include "fst/layout/ReplicaParLayout.hh"
#include "fst/layout/Raid5Layout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class LayoutPlugins {
public:
  LayoutPlugins() {};
  ~LayoutPlugins() {};

  static Layout* GetLayoutObject(XrdFstOfsFile* thisFile, unsigned int layoutid, XrdOucErrInfo *error) {
    if (eos::common::LayoutId::GetLayoutType(layoutid) == eos::common::LayoutId::kPlain) {
      return (Layout*)new PlainLayout(thisFile, layoutid, error);
    }
    if (eos::common::LayoutId::GetLayoutType(layoutid) == eos::common::LayoutId::kReplica) {
      return (Layout*)new ReplicaParLayout(thisFile,layoutid, error);
    }
    if (eos::common::LayoutId::GetLayoutType(layoutid) == eos::common::LayoutId::kRaid5) {
      return (Layout*)new Raid5Layout(thisFile,layoutid, error);
    }
    return 0;
  }
};

EOSFSTNAMESPACE_END

#endif
