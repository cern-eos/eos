//------------------------------------------------------------------------------
//! @file NegStatCache.hh
//! @author Geoffray Adde <geoffray.adde@cern.ch>
//! @brief negative stat cache to hold the non existing stat request
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

#ifndef __EOSFUSE_NEGSTATCACHE_HH__
#define __EOSFUSE_NEGSTATCACHE_HH__

/*----------------------------------------------------------------------------*/
#include <map>
#include <vector>
#include <set>
#include <sys/stat.h>
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "common/Timing.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
//! Size Cache Entry
//------------------------------------------------------------------------------
class NegStatCache
{
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param size - max number of entries
  //! @param lifetime - lifetime in nanoseconds of a cash entry
  //!
  //--------------------------------------------------------------------------
  NegStatCache(size_t size = 4096, size_t lifetime_ns = 15000000000);

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~NegStatCache();

  //--------------------------------------------------------------------------
  //! Get an entry in the negstatcache
  //!
  //! @return the errno associated with the stat of the path if any, 0 otherwise
  //!
  //--------------------------------------------------------------------------
  int GetNoExist (const std::string &path);

  //--------------------------------------------------------------------------
  //! Update(create if not there yet) negstatcache entry
  //!
  //! @param path - path of the entry
  //!
  //--------------------------------------------------------------------------
  void UpdateNoExist( const std::string & path, const int & erno);

  //--------------------------------------------------------------------------
  //! Forget any information about en entry
  //!
  //! @param path - path of the entry
  //!
  //--------------------------------------------------------------------------
  void Forget (const std::string & path);

  //--------------------------------------------------------------------------
  //! Node of the negative cache tree
  //!
  //--------------------------------------------------------------------------
  typedef struct stat_negcache_node
  {
    std::string mName; ///< name of that directory/file
    stat_negcache_node *mParent; ///< parent directory/file
    std::map<std::string, stat_negcache_node*> mChildren; ///< children directories/files
    int mProbedErrno; ///< errno coming from the stat of the file

    stat_negcache_node() :
    mName(""),mParent(NULL),mChildren(),mProbedErrno(0)
    {}
  } stat_negcache_node;

  //--------------------------------------------------------------------------
  //! Dump the content of cache in a stream
  //!
  //! @param os - stream to dump the information to
  //! @param noMtime - inhibate dumping the cache mtime information per entry
  //! @param node - node where to start the dump
  //! @param fullname - reserved for recursive call
  //!
  //--------------------------------------------------------------------------
  void Dump (std::ostream &os, bool noMtime = true, stat_negcache_node *node = 0, std::string *fullname = NULL);

private:
  //--------------------------------------------------------------------------
  //! remove a node from the negcache path tree
  //!
  //! @param node - node to remove/forget
  //! @param uproot - go uproot as long as the node are meaningless and have on child
  //! @param entirebranch - remove the whole branch attahced to that node
  //!
  //! if the node is in the middle of the branch the information is just updated
  //! if the node is a leaf, it's removed and the tree is pruned
  //!
  //--------------------------------------------------------------------------
  void rmNode (stat_negcache_node *node, bool uproot = true, bool entirebranch = false);

  //--------------------------------------------------------------------------
  //! look for a match in the negcache path tree
  //!
  //! @param path - path to match
  //! @param exact - find an exact match
  //!
  //! if an exact match is not required, return the first probed node (if any)
  //! matching a directory being a prefix to the searched path
  //!
  //! @return the matched node if any, null otherwise
  //!
  //--------------------------------------------------------------------------
  stat_negcache_node * findMatch (const std::string &path,bool exact=false, bool* exactout=NULL);

  //--------------------------------------------------------------------------
  //! get the node for a path in the negcache path tree
  //!
  //! @param path - path to match
  //! @param create - create the node in the tree if does not exist
  //!
  //! @return the node to get if any, null otherwise
  //!
  //--------------------------------------------------------------------------
  stat_negcache_node * getNode (const std::string &path,bool create=true);

  //--------------------------------------------------------------------------
  //! update the mtime maps
  //!
  //! @param node - node to remove from the maps
  //! @param timeNs - mtime in nsec.
  //!
  //--------------------------------------------------------------------------
  void updateMtime(stat_negcache_node * node, long long timeNs);

  //--------------------------------------------------------------------------
  //! remove any reference to a node in the mtime maps
  //!
  //! @param node - node to remove from the maps
  //!
  //--------------------------------------------------------------------------
  void eraseMtime(stat_negcache_node * node);

  //--------------------------------------------------------------------------
  //! Cleanup expired entires
  //!
  //--------------------------------------------------------------------------
  void expire(const struct timespec *now=NULL);

  /////////////////////
  // MEMBERS
  /////////////////////
  eos::common::RWMutex mMutex;       ///< mutex protecting the cache map
  stat_negcache_node pTreeRoot;      ///< root of the negstatcache tree

  std::map<long long, std::set<stat_negcache_node *> > mMtime2Nodes; ///< mtime in ns => tree nodes
  std::map<stat_negcache_node *, long long> mNode2Mtime;             ///< tree node => mtime in ns

  size_t mCacheSize; ///< threshold to trigger a clean-up
  ssize_t mLifeTimeInNs; ///< nano-seconds of validity lifetime
};


#endif
