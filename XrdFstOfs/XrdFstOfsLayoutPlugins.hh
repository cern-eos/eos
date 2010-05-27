#ifndef __XRDFSTOFS_LAYOUTPLUGIN_HH__
#define __XRDFSTOFS_LAYOUTPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdFstOfs/XrdFstOfsFile.hh"
#include "XrdFstOfs/XrdFstOfsLayout.hh"
#include "XrdFstOfs/XrdFstOfsPlainLayout.hh"
#include "XrdFstOfs/XrdFstOfsReplicaLayout.hh"
#include "XrdFstOfs/XrdFstOfsRaid5Layout.hh"
/*----------------------------------------------------------------------------*/


class XrdFstOfsLayoutPlugins {
public:
  XrdFstOfsLayoutPlugins() {};
  ~XrdFstOfsLayoutPlugins() {};

  static XrdFstOfsLayout* GetLayoutObject(XrdFstOfsFile* thisFile, unsigned int layoutid, XrdOucErrInfo *error) {
    if (XrdCommonLayoutId::GetLayoutType(layoutid) == XrdCommonLayoutId::kPlain) {
      return (XrdFstOfsLayout*)new XrdFstOfsPlainLayout(thisFile, layoutid, error);
    }
    if (XrdCommonLayoutId::GetLayoutType(layoutid) == XrdCommonLayoutId::kReplica) {
      return (XrdFstOfsLayout*)new XrdFstOfsReplicaLayout(thisFile,layoutid, error);
    }
    if (XrdCommonLayoutId::GetLayoutType(layoutid) == XrdCommonLayoutId::kRaid5) {
      return (XrdFstOfsLayout*)new XrdFstOfsRaid5Layout(thisFile,layoutid, error);
    }
    return 0;
  }
};

#endif
