#include "mgm/groupdrainer/DrainProgressTracker.hh"

namespace eos::mgm {

void
DrainProgressTracker::setTotalFiles(fsid_t fsid,
                                    uint64_t total_files)
{
  std::scoped_lock lg(mFsTotalFilesMtx);
  auto [it, inserted] = mFsTotalfiles.try_emplace(fsid, total_files);

  // NOTE: this may not be necessary if we guarantee that
  // there will be no files inserted in the FS after we start draining
  // but anyway this is almost 0 cost now as we know the iterator position
  // of insertion, so we don't do any more traversal.
  if (!inserted && it->second < total_files) {
    mFsTotalfiles.insert_or_assign(it, fsid, total_files);
  }

}

void
DrainProgressTracker::increment(fsid_t fsid)
{
  std::scoped_lock lg(mFsScheduledCtrMtx);
  mFsScheduledCounter[fsid]++;
}

float
DrainProgressTracker::getDrainStatus(DrainProgressTracker::fsid_t fsid) const
{
  std::scoped_lock lg(mFsScheduledCtrMtx, mFsTotalFilesMtx);
  auto total_it = mFsTotalfiles.find(fsid);
  auto counter_it = mFsScheduledCounter.find(fsid);

  if (total_it == mFsTotalfiles.end() ||
      counter_it == mFsScheduledCounter.end()) {
    return 0;
  }

  return (static_cast<float>(counter_it->second)/total_it->second)*100;

}

void
DrainProgressTracker::dropFsid(DrainProgressTracker::fsid_t fsid)
{
  std::scoped_lock lg(mFsScheduledCtrMtx, mFsTotalFilesMtx);
  mFsTotalfiles.erase(fsid);
  mFsScheduledCounter.erase(fsid);
}

void
DrainProgressTracker::clear()
{
  std::scoped_lock lg(mFsScheduledCtrMtx, mFsTotalFilesMtx);
  mFsTotalfiles.clear();
  mFsScheduledCounter.clear();
}

uint64_t
DrainProgressTracker::getTotalFiles(DrainProgressTracker::fsid_t fsid) const
{
  std::scoped_lock lg(mFsTotalFilesMtx);
  auto it = mFsTotalfiles.find(fsid);
  return it == mFsTotalfiles.end() ? 0 : it->second;
}

uint64_t
DrainProgressTracker::getFileCounter(DrainProgressTracker::fsid_t fsid) const
{
  std::scoped_lock lg(mFsScheduledCtrMtx);
  auto it = mFsScheduledCounter.find(fsid);
  return it == mFsScheduledCounter.end() ? 0 : it->second;
}

} // eos::mgm