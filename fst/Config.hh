#ifndef __EOSFST_CONFIG_HH__
#define __EOSFST_CONFIG_HH__
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class Config {
public:
  bool             autoBoot;             // -> indicates if the node tries to boot automatically or waits for a boot message from a master
  XrdOucString     FstMetaLogDir;        //  Directory containing the meta data log files
  int              FstQuotaReportInterval; // Interval after which Quota has to be published even if it didn't change
  XrdOucString     FstOfsBrokerUrl;      // Url of the message broker
  XrdOucString     FstDefaultReceiverQueue; // Queue where we are sending to by default
  XrdOucString     FstQueue;             // our queue name
  XrdOucString     FstQueueWildcard;     // our queue match name

  static Config gConfig;
  Config() {FstQuotaReportInterval=0;autoBoot=false;}
  ~Config() {}
};

EOSFSTNAMESPACE_END

#endif
