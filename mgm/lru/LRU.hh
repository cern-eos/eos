//------------------------------------------------------------------------------
//! @file LRU.hh
//! @author Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include "common/AssistedThread.hh"
#include "common/Mapping.hh"
#include "common/RegexWrapper.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"
#include <XrdOuc/XrdOucErrInfo.hh>
#include <memory>
#include <sys/types.h>

namespace qclient
{
class QClient;
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// @brief  This class implements an LRU engine
//------------------------------------------------------------------------------
class LRU
{
public:
  static const char* gLRUPolicyPrefix;

  //----------------------------------------------------------------------------
  //! Simple struct describing LRU options
  //----------------------------------------------------------------------------
  struct Options {
    bool enabled;                  //< Is LRU even enabled?
    std::chrono::seconds interval; //< Run LRU every this many seconds.
  };

  struct PolicyRule {
    std::string m_regMatch;
    time_t m_age;
    /**
     * <0 means file should be less than m_size, >0 means file should be equal
     * or more than m_size. Should not be equal to 0
     */
    std::optional<ssize_t> m_size;
    time_t m_now;

    PolicyRule(const std::string& regMatch, time_t age, ssize_t size, time_t now)
        : m_regMatch(regMatch)
        , m_age(age)
        , m_size(size != 0 ? std::optional<ssize_t>(size) : std::nullopt)
        , m_now(now)
    {
    }

    bool
    operator==(const PolicyRule& other) const
    {
      return m_regMatch == other.m_regMatch && m_age == other.m_age &&
             m_size == other.m_size;
    }

    bool
    nameMatches(const std::string& fileName) const
    {
      return common::eos_regex_match(fileName, m_regMatch);
    }

    bool
    ageMatches(time_t fileCreationTime) const
    {
      return (fileCreationTime + m_age < m_now);
    }

    bool
    sizeMatches(const size_t& fileSize) const
    {
      if (m_size) {
        return (*m_size > 0 ? fileSize > static_cast<size_t>(*m_size)
                            : fileSize < static_cast<size_t>(-*m_size));
      }
      // if no size, then return true as we should not consider size to be part of the
      // matching process
      return true;
    }

    std::string
    getSizeCriteria() const
    {
      if (!m_size) {
        return "none";
      }

      if (*m_size > 0) {
        return std::to_string(*m_size);
      }

      return std::string("-") + std::to_string(static_cast<size_t>(-*m_size));
    }

    bool
    matches(const std::string& name, const time_t fileCreationTime,
            const size_t size) const
    {
      return nameMatches(name) && ageMatches(fileCreationTime) && sizeMatches(size);
    }
  };

  friend std::ostream&
  operator<<(std::ostream& os, const PolicyRule& rule)
  {
    return os << "PolicyRule{m_regMatch=" << rule.m_regMatch << ", m_age=" << rule.m_age
              << ", m_size="
              << (rule.m_size ? std::to_string(*rule.m_size) : std::string("N/A"))
              << " }";
  }

  typedef std::vector<PolicyRule> PolicyRules;

  //----------------------------------------------------------------------------
  //! Extracts the age and the time criteria from a string containing time:size
  //! E.g: 1mo:>1G
  //!
  //! @return true if parsing succeeded, false otherwise
  //----------------------------------------------------------------------------
  static bool extractTimeSizeCriterias(const std::string& input, time_t& age,
                                       ssize_t& size, std::ostringstream& errMsg);

  //----------------------------------------------------------------------------
  //! Parse an "sys.lru.expire.match" policy
  //!
  //! @return true if parsing succeeded, false otherwise
  //----------------------------------------------------------------------------
  static bool parseExpireMatchPolicy(const std::string& policy,
                                     std::map<std::string, time_t>& matchAgeMap);

  static bool parseExpireSizeMatchPolicy(const std::string& policy,
                                         PolicyRules& matchAgeSizeMap,
                                         std::ostringstream& errMsg);

  //----------------------------------------------------------------------------
  //! Retrieve current LRU configuration options
  //----------------------------------------------------------------------------
  Options getOptions();

  //----------------------------------------------------------------------------
  //! Retrieve "lru.interval" configuration option as string, or empty if
  //! cannot be found. Assumes gFsView.ViewMutex is at-least readlocked.
  //----------------------------------------------------------------------------
  std::string getLRUIntervalConfig() const;

  //----------------------------------------------------------------------------
  //! Constructor. To run the LRU thread, call Start
  //----------------------------------------------------------------------------
  LRU();

  //----------------------------------------------------------------------------
  //! Destructor - stop the background thread, if running
  //----------------------------------------------------------------------------
  ~LRU();

  //----------------------------------------------------------------------------
  //! Start the LRU thread
  //----------------------------------------------------------------------------
  void Start();

  //----------------------------------------------------------------------------
  //! Stop the LRU thread
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //!  @brief Remove empty directories if they are older than age given in
  //!         policy
  //! @param dir directory to proces
  //! @param policy minimum age to expire
  //----------------------------------------------------------------------------
  void AgeExpireEmpty(const char* dir, const std::string& policy);

  //----------------------------------------------------------------------------
  //! @brief Remove all files older than the policy defines
  //! @param dir directory to process
  //! @param policy minimum age to expire
  //----------------------------------------------------------------------------
  void SizeAgeExpire(const char* dir, const std::string& policy);

  //----------------------------------------------------------------------------
  //! Expire the oldest files to go under the low watermark
  //!
  //! @param dir directory to process
  //! @param policy high water mark when to start expiration
  //----------------------------------------------------------------------------
  void CacheExpire(const char* dir, std::string& low, std::string& high);

  //----------------------------------------------------------------------------
  //! Convert all files matching
  //!
  //! @param dir directory to process
  //! @param map storing all the 'sys.conversion.<match>' policies
  //----------------------------------------------------------------------------
  void ConvertMatch(const char* dir, eos::IContainerMD::XAttrMap& map);

  //----------------------------------------------------------------------------
  //! Signal the LRU stat it shoudl refresh it's options
  //----------------------------------------------------------------------------
  inline void RefreshOptions()
  {
    mRefresh = true;
  }

  //----------------------------------------------------------------------------
  //! Struct representing LRU entry
  //----------------------------------------------------------------------------
  struct lru_entry {
    //! Compare operator to use struct in a map
    bool operator< (lru_entry const& lhs) const
    {
      if (lhs.ctime == ctime) {
        return (path < lhs.path);
      }

      return (ctime < lhs.ctime);
    }

    std::string path;
    time_t ctime;
    unsigned long long size;
  };

  //! Entry in an lru queue having path name, mtime, size
  typedef struct lru_entry lru_entry_t;

private:
  //----------------------------------------------------------------------------
  // LRU method doing the actual policy scrubbing
  //
  // This thread loops in regular intervals over all directories which have
  // a LRU policy attribute set (sys.lru.*) and applies the defined policy.
  //----------------------------------------------------------------------------
  void backgroundThread(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  // Process the given directory, apply all policies
  //----------------------------------------------------------------------------
  void processDirectory(const std::string& dir,
                        eos::IContainerMD::XAttrMap& map);

  //----------------------------------------------------------------------------
  // Perform a single LRU cycle, QDB namespace
  //----------------------------------------------------------------------------
  void performCycleQDB(ThreadAssistant& assistant) noexcept;

  std::unique_ptr<qclient::QClient> mQcl; ///< Internal QCl object
  AssistedThread mThread; ///< thread id of the LRU thread
  eos::common::VirtualIdentity mRootVid; ///< Uses the root vid
  XrdOucErrInfo mError; ///< XRootD error object
  std::atomic<bool> mRefresh; ///< Flag to mark option refresh
};

EOSMGMNAMESPACE_END
