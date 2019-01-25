#include "mgm/TapeAwareGcLru.hh"

#include <stdexcept>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
TapeAwareGcLru::TapeAwareGcLru(const FidQueue::size_type maxQueueSize):
  mMaxQueueSize(maxQueueSize), mMaxQueueSizeExceeded(false)
{
  if(0 == maxQueueSize) {
    throw MaxQueueSizeIsZero(std::string(__FUNCTION__) +
      " failed: maxQueueSize must be greater than 0");
  }
}

//------------------------------------------------------------------------------
//! Notify the queue a file has been accessed
//------------------------------------------------------------------------------
void TapeAwareGcLru::fileAccessed(const IFileMD::id_t fid)
{
  const auto mapEntry = mFidToQueueEntry.find(fid);

  // If a new file has been accessed
  if(mFidToQueueEntry.end() == mapEntry) {
    newFileHasBeenAccessed(fid);
  } else {
    queuedFileHasBeenAccessed(fid, mapEntry.value());
  }
}

//------------------------------------------------------------------------------
// Handle the fact a new file has been accessed
//------------------------------------------------------------------------------
void TapeAwareGcLru::newFileHasBeenAccessed(const IFileMD::id_t fid) {
  // Ignore the new file if the maximum queue size has been reached
  // IMPORTANT: This should be a rare situation
  if(mFidToQueueEntry.size() == mMaxQueueSize) {
    mMaxQueueSizeExceeded = true;
  } else {
    // Add file to the front of the LRU queue
    mQueue.push_front(fid);
    mFidToQueueEntry[fid] = mQueue.begin();
  }
}

//------------------------------------------------------------------------------
// Handle the fact that a file already in the queue has been accessed
//------------------------------------------------------------------------------
void TapeAwareGcLru::queuedFileHasBeenAccessed(const IFileMD::id_t fid,
  FidQueue::iterator &queueItor) {
  // Erase the existing file from the LRU queue
  mQueue.erase(queueItor);

  // Push the identifier of the file onto the front of the LRU queue
  mQueue.push_front(fid);
  mFidToQueueEntry[fid] = mQueue.begin();
}

//------------------------------------------------------------------------------
//! Return true if the queue is empty
//------------------------------------------------------------------------------
bool TapeAwareGcLru::empty() const
{
  return mQueue.empty();
}

//------------------------------------------------------------------------------
//! Return queue size
//------------------------------------------------------------------------------
TapeAwareGcLru::FidQueue::size_type TapeAwareGcLru::size() const
{
  return mFidToQueueEntry.size();
}

//------------------------------------------------------------------------------
//! Pop and return the identifier of the least used file
//------------------------------------------------------------------------------
IFileMD::id_t TapeAwareGcLru::getAndPopFidOfLeastUsedFile()
{
  if(mQueue.empty()) {
    throw QueueIsEmpty(std::string(__FUNCTION__) +
      " failed: The queue is empty");
  } else {
    mMaxQueueSizeExceeded = false;

    const auto lruFid = mQueue.back();
    mQueue.pop_back();
    mFidToQueueEntry.erase(lruFid);
    return lruFid;
  }
}

//------------------------------------------------------------------------------
//! Return true if the maximum queue size has been exceeded
//------------------------------------------------------------------------------
bool TapeAwareGcLru::maxQueueSizeExceeded() const noexcept {
  return mMaxQueueSizeExceeded;
}

EOSMGMNAMESPACE_END
