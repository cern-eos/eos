/* ------------------------------------------------------------------------- */
#include "mgm/Balancer.hh"


EOSMGMNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
Balancer::Balancer(const char* spacename) 
{
  //----------------------------------------------------------------
  //! constructor of the space balancer
  //----------------------------------------------------------------
  mSpaceName = spacename;
  XrdSysThread::Run(&thread, Balancer::StaticBalance, static_cast<void *>(this),0, "Balancer Thread");
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
  return 0;
}

EOSMGMNAMESPACE_END
