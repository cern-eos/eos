//------------------------------------------------------------------------------
//! @file ResponseCollector.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Object used for handling async responses
//------------------------------------------------------------------------------

#pragma one
#include "fst/Namespace.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include <future>
#include <list>
#include <mutex>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class ResponseCollector
//------------------------------------------------------------------------------
class ResponseCollector
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ResponseCollector() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ResponseCollector()
  {
    // Make sure all futures are handled
    (void) CheckResponses(true);
  };

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  ResponseCollector& operator =(ResponseCollector&& other) noexcept
  {
    if (this != &other) {
      mResponses.swap(other.mResponses);
    }

    return *this;
  }

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  ResponseCollector(ResponseCollector&& other) noexcept
  {
    *this = std::move(other);
  }

  //----------------------------------------------------------------------------
  //! Collect future object
  //!
  //@ @param fut future object
  //----------------------------------------------------------------------------
  void CollectFuture(std::future<XrdCl::XRootDStatus> fut);

  //----------------------------------------------------------------------------
  //! Check the status of the responses
  //!
  //! @param wait_all if true then block waiting for replies, otherwise only
  //!        check replies that are ready
  //!
  //! @return true if all responses successful, otherwise false
  //----------------------------------------------------------------------------
  bool CheckResponses(bool wait_all);

private:
  mutable std::mutex mMutex;
  std::list<std::future<XrdCl::XRootDStatus>> mResponses;
};

EOSFSTNAMESPACE_END
