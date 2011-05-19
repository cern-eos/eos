#ifndef __EOSFST_TRANSFER_JOB__
#define __EOSFST_TRANSFER_JOB__

/* ------------------------------------------------------------------------- */
#include "fst/Namespace.hh"
#include "fst/txqueue/TransferQueue.hh"
#include "common/TransferJob.hh"
/* ------------------------------------------------------------------------ */
#include "Xrd/XrdJob.hh"
/* ------------------------------------------------------------------------- */
#include <string>

/* ------------------------------------------------------------------------- */


EOSFSTNAMESPACE_BEGIN

class TransferJob: public XrdJob {

private:
  TransferQueue* mQueue;
  eos::common::TransferJob* mJob;
  int mBandWidth; // band width in Mb/s
  int mTimeOut;   // max duration for a transfer in seconds
  XrdOucString mSourceUrl;
  XrdOucString mTargetUrl;

public:

  TransferJob(TransferQueue* queue, eos::common::TransferJob* cjob,  int bw, int timeout=3600);
  ~TransferJob();

  void DoIt();
  std::string NewUuid();

  const char* GetSourceUrl();
  const char* GetTargetUrl();
};

EOSFSTNAMESPACE_END

#endif
