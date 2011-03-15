#ifndef __XRDFSTOFS_CONFIG_HH__
#define __XRDFSTOFS_CONFIG_HH__
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
class XrdFstOfsConfig {
public:
  bool             autoBoot;             // -> indicates if the node tries to boot automatically or waits for a boot message from a master
  XrdOucString     FstMetaLogDir;        //  Directory containing the meta data log files
  int              FstQuotaReportInterval; // Interval after which Quota has to be published even if it didn't change
  XrdOucString     FstOfsBrokerUrl;      // Url of the message broker
  XrdOucString     FstDefaultReceiverQueue; // Queue where we are sending to by default
  static XrdFstOfsConfig gConfig;
  XrdFstOfsConfig() {}
  ~XrdFstOfsConfig() {}
};

#endif
