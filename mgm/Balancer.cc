/* ------------------------------------------------------------------------- */
#include "mgm/Balancer.hh"
#include "mgm/FsView.hh"
#include "common/StringConversion.hh"
/* ------------------------------------------------------------------------- */
/* ------------------------------------------------------------------------- */
EOSMGMNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
Balancer::Balancer(const char* spacename) 
{
  //----------------------------------------------------------------
  //! constructor of the space balancer
  //----------------------------------------------------------------
  mSpaceName = spacename;
  XrdSysThread::Run(&thread, Balancer::StaticBalance, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "Balancer Thread");
}

/* ------------------------------------------------------------------------- */
Balancer::~Balancer()
{
  //----------------------------------------------------------------
  //! destructor stops the balancer thread
  //----------------------------------------------------------------

  XrdSysThread::Cancel(thread);
  XrdSysThread::Join(thread,NULL);
}

/* ------------------------------------------------------------------------- */
void*
Balancer::StaticBalance(void* arg)
{
  //----------------------------------------------------------------
  //! static thread startup function calling Run
  //----------------------------------------------------------------
  return reinterpret_cast<Balancer*>(arg)->Balance();
}

/* ------------------------------------------------------------------------- */
void*
Balancer::Balance(void)
{
  //----------------------------------------------------------------
  //! balancing file distribution on a space
  //----------------------------------------------------------------

  XrdSysThread::SetCancelOn();

  // loop forever until cancelled
  while (1) {
    bool IsSpaceBalancing=true;
    double SpaceDifferenceThreshold=0;
    
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      std::set<FsGroup*>::const_iterator git;
      if (!FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str()))
        break;

      if (FsView::gFsView.mSpaceView[mSpaceName.c_str()]->GetConfigMember("balancer") == "on")
        IsSpaceBalancing=true;
      else 
        IsSpaceBalancing=false;

      SpaceDifferenceThreshold = strtod(FsView::gFsView.mSpaceView[mSpaceName.c_str()]->GetConfigMember("balancer.threshold").c_str(),0);

      if (IsSpaceBalancing) {
        // loop over all groups
        for (git = FsView::gFsView.mSpaceGroupView[mSpaceName.c_str()].begin(); git != FsView::gFsView.mSpaceGroupView[mSpaceName.c_str()].end(); git++) {
          double dev=0;
          if ( (dev=(*git)->MaxDeviation("stat.statfs.usedbytes")) > SpaceDifferenceThreshold) {
            // we should run a balancing job
            (*git)->StartBalancerJob();
          }
          XrdOucString sizestring1;
          XrdOucString sizestring2;
          eos_static_debug("space=%-10s group=%-20s deviation=%-10s threshold=%-10s", mSpaceName.c_str(), (*git)->GetMember("name").c_str(), eos::common::StringConversion::GetReadableSizeString(sizestring1,(unsigned long long)dev,"B"), eos::common::StringConversion::GetReadableSizeString(sizestring2, (unsigned long long)SpaceDifferenceThreshold,"B"));
        }
      }
    }

    // hang a little bit around ...
    for (size_t sleeper = 0; sleeper < 100; sleeper++) {
      usleep(100000);
      XrdSysThread::CancelPoint();
    }
  }
  return 0;
}

EOSMGMNAMESPACE_END
