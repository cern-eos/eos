/*----------------------------------------------------------------------------*/
#include "mq/XrdMqMessaging.hh"
#include "mgm/FstNode.hh"
#include "mgm/FsView.hh"
#include "mgm/Quota.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucTokenizer.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
FstNode::Update(XrdAdvisoryMqMessage* advmsg) 
{
  if (!advmsg)
    return false;

  // register the node to the global view and config
  std::string nodequeue = advmsg->kQueue.c_str();

  if (FsView::gFsView.RegisterNode(advmsg->kQueue.c_str())) {
    std::string nodeconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(gOFS->NodeConfigQueuePrefix.c_str(), advmsg->kQueue.c_str());
    
    if (!eos::common::GlobalConfig::gConfig.Get(nodeconfigname.c_str())) {
      if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(nodeconfigname.c_str(), advmsg->kQueue.c_str())) {
	eos_static_crit("cannot add node config queue %s", nodeconfigname.c_str());
      }
    }
  }

  { // lock for write
    eos::common::RWMutexWriteLock(FsView::gFsView.ViewMutex);
    if (FsView::gFsView.mNodeView[nodequeue]) {
      if (advmsg->kOnline) {
	FsView::gFsView.mNodeView[nodequeue]->SetStatus("online");
      } else {
	FsView::gFsView.mNodeView[nodequeue]->SetStatus("offline");
      }
      eos_static_info("Setting heart beat to %llu\n", (unsigned long long) advmsg->kMessageHeader.kSenderTime_sec);
      FsView::gFsView.mNodeView[nodequeue]->SetHeartBeat(advmsg->kMessageHeader.kSenderTime_sec);
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_END
