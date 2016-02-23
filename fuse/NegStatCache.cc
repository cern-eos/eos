//------------------------------------------------------------------------------
//! @file NegStatCache.cc
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

/*----------------------------------------------------------------------------*/
#include "NegStatCache.hh"
/*----------------------------------------------------------------------------*/
//--------------------------------------------------------------------------
//! Constructor
//!
//! @param
//! @param
//! @param
//!
//--------------------------------------------------------------------------
NegStatCache::NegStatCache (size_t size, size_t lifetime_ns)
{
  mCacheSize = size;
  mLifeTimeInNs = lifetime_ns;
  pTreeRoot.mParent = NULL;
  mMutex.SetBlocking (true);
}

//--------------------------------------------------------------------------
//! Destructor
//--------------------------------------------------------------------------
NegStatCache::~NegStatCache ()
{
}

//--------------------------------------------------------------------------
//! Get entry
//!
//! @return true if a non-exist entry is found
//!
//--------------------------------------------------------------------------
int NegStatCache::GetNoExist (const std::string &path)
{
  eos::common::RWMutexReadLock mLock (mMutex);

  bool exact = false;
  stat_negcache_node *node = findMatch (path, false, &exact);
  //fprintf(stderr,"CHECK : %p %d  %ld  %ld\n",node,mNode2Mtime.count(node),eos::common::Timing::GetCoarseAgeInNs(mNode2Mtime[node]),mLifeTimeInNs);
  if (!node || !mNode2Mtime.count (node) || eos::common::Timing::GetCoarseAgeInNs (mNode2Mtime[node]) > mLifeTimeInNs)
    return 0;
  else if (exact)
    return node->mProbedErrno;
  else
    return ENOENT;
}

//--------------------------------------------------------------------------
//! Update stat negcache entry
//!
//! @param path - path of the entry
//!
//--------------------------------------------------------------------------
void NegStatCache::UpdateNoExist (const std::string & path, const int &erno)
{
  eos::common::RWMutexWriteLock mLock (mMutex);

  // todo: warning using coarse time stamp
  struct timespec ts;
  eos::common::Timing::GetTimeSpec (ts, true);
  long long nowNs = ts.tv_sec * 1000000000 + ts.tv_nsec;

  stat_negcache_node *node = getNode (path, true);
  node->mProbedErrno = erno;
  // erase previsou mtime if any
  eraseMtime (node);
  // set the new mtimes
  updateMtime (node, nowNs);

  expire ();
}

//--------------------------------------------------------------------------
//! Forget stat negcache entry
//!
//! @param path - path of the entry
//!
//--------------------------------------------------------------------------
void NegStatCache::Forget (const std::string & path)
{
  eos::common::RWMutexWriteLock mLock (mMutex);

  stat_negcache_node *node = findMatch (path, true);
  //if (node) printf ("Forget node path=%s  node=%p  name=%s\n", path.c_str (), node, node ? node->mName.c_str () : "");
  if (node) rmNode (node);

  expire ();
}

//--------------------------------------------------------------------------
//! Dump the content of cache in a stream
//!
//! @param os - stream to dump the information to
//! @param os - stream to dump the information to
//!
//--------------------------------------------------------------------------
void NegStatCache::Dump (std::ostream &os, bool noMtime, stat_negcache_node *node, std::string *fullname)
{
  //printf("Dump %p  %p\n",node,fullname);
  if (!node || !fullname)
  {
    eos::common::RWMutexReadLock mLock (mMutex);
    std::string fullname;
    os << "Tree:" << "\n";
    Dump (os, noMtime, &pTreeRoot, &fullname);
    if (!noMtime)
    {
      os << "mMtime2Nodes:" << "\n";
      for (auto it = mMtime2Nodes.begin (); it != mMtime2Nodes.end (); it++)
      {
        os << it->first << "  =>  ";
        for (auto it2 = it->second.begin (); it2 != it->second.end (); it2++)
          os << (*it2) << "(" << (*it2)->mName.c_str () << ") ";
        os << "\n";
      }
      os << "mNode2Mtime:" << "\n";
      for (auto it = mNode2Mtime.begin (); it != mNode2Mtime.end (); it++)
      {
        os << (it->first) << "(" << (it->first)->mName.c_str () << ") " << "  =>  " << it->second << "\n";
      }
    }

    return;
  }
  //printf("Dump %s  %s\n",node->mName.c_str(),fullname->c_str());

  auto fnsize = fullname->size ();
  if (node->mProbedErrno) os << *fullname << "\n";
  for (auto it = node->mChildren.begin (); it != node->mChildren.end (); it++)
  {
    *fullname += ("/" + it->first);
    Dump (os, noMtime, it->second, fullname);
    //printf("Dump2  fullname=%s  fnsize=%d",fullname,fnsize);
    fullname->erase (fnsize);
  }
}

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
void NegStatCache::rmNode (stat_negcache_node *node, bool uproot, bool entirebranch)
{

  //printf("rmNode  %p  %d\n",node,node->mChildren.size ());
  if (uproot && node->mChildren.empty ())
  {
    //printf("rmNode  %p  %d",node->mParent,node->mParent->mChildren.size ());
    while (node->mParent && node->mParent->mChildren.size () == 1 && node->mParent->mProbedErrno == 0)
    {
      node = node->mParent;
      //printf("%s#",node->mName.c_str());
    }
    //printf("\n");
    rmNode (node, false, true);
  }
  else
  {
    if (node->mChildren.size ())
    {
      if (entirebranch)
      {
        for (auto it = node->mChildren.begin (); it != node->mChildren.end (); it++)
          rmNode (it->second, false, true);
      }
      else
        node->mProbedErrno = 0;
    }

    if (node->mChildren.empty () && node->mParent) // we don't want to erase the root of the tree
    {
      node->mParent->mChildren.erase (node->mName);
      delete (node);
    }
    // forget the mtime
    eraseMtime (node);
  }
}

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
NegStatCache::stat_negcache_node * NegStatCache::findMatch (const std::string &path, bool exact, bool *exactout)
{
  stat_negcache_node *node = &pTreeRoot;
  std::string token;
  std::string::size_type slash1 = 0, slash2 = 0;
  //printf("findDeepestMatch: ");
  do
  {
    // compute the next token in the path
    slash2 = path.find ('/', slash1 + 1);
    token = path.substr (slash1 + 1, slash2 == std::string::npos ? slash2 : slash2 - slash1 - 1);
    if (!exact && node->mProbedErrno) //no exact match, stopping at the first non existing entry
      break;
    //printf("%s#(%p)#",token.c_str(),node);
  }
  while (node->mChildren.count (token) && (node = node->mChildren[token]) && ((slash1 = slash2) != std::string::npos));

  if (exactout) *exactout = (slash1 == std::string::npos);
  //printf("@%p\n",((slash1!=std::string::npos) && exact)?NULL:node);

  return ((slash1 != std::string::npos) && exact) ? NULL : node;
}

//--------------------------------------------------------------------------
//! get the node for a path in the negcache path tree
//!
//! @param path - path to match
//! @param create - create the node in the tree if does not exist
//!
//! @return the node to get if any, null otherwise
//!
//--------------------------------------------------------------------------
NegStatCache::stat_negcache_node * NegStatCache::getNode (const std::string &path, bool create)
{
  //printf("getLeaf2  path=%s\n",path.c_str());
  stat_negcache_node *node = &pTreeRoot, *node2 = 0;
  std::string token;
  std::string::size_type slash1 = 0, slash2 = 0;
  do
  {
    // compute the next token in the path
    slash2 = path.find ('/', slash1 + 1);
    token = path.substr (slash1 + 1, (slash2 == std::string::npos) ? slash2 : slash2 - slash1 - 1);
    slash1 = slash2;
    //printf("getLeaf2  path=%s  slash1=%d  slash2=%d  token=%s  node2=%p\n",path.c_str(),slash1,slash2,token.c_str(),node2);
  }
  while (((node->mChildren.count (token) && (node = node->mChildren[token])) // existing branch
  || (create && ((node2 = new stat_negcache_node) && (node2->mParent = node) && (node2->mName = token).size () && (node->mChildren[token] =
      node2) && (node = node2)))) && slash1 != std::string::npos);

  return node;
}

//--------------------------------------------------------------------------
//! update the mtime maps
//!
//! @param node - node to remove from the maps
//! @param timeNs - mtime in nsec.
//!
//--------------------------------------------------------------------------
void NegStatCache::updateMtime (stat_negcache_node * node, long long timeNs)
{
  mNode2Mtime[node] = timeNs;
  mMtime2Nodes[timeNs].insert (node);
}

//--------------------------------------------------------------------------
//! remove any reference to a node in the mtime maps
//!
//! @param node - node to remove from the maps
//!
//--------------------------------------------------------------------------
void NegStatCache::eraseMtime (stat_negcache_node * node)
{
  auto it = mNode2Mtime.end ();
  if ((it = mNode2Mtime.find (node)) != mNode2Mtime.end ())
  {
    auto it2 = mMtime2Nodes.end ();
    if ((it2 = mMtime2Nodes.find (it->second)) != mMtime2Nodes.end ())
    {
      // remove from the set
      it2->second.erase (node);
      // erase the set if empty
      if (it2->second.empty ()) mMtime2Nodes.erase (it2);
    }
    mNode2Mtime.erase (it);
  }
}

//--------------------------------------------------------------------------
//! Cleanup expired entires
//!
//--------------------------------------------------------------------------
void NegStatCache::expire (const struct timespec *now)
{
  struct timespec ts;
  if (!now)
  {
    // todo: warning using coarse time stamp
    eos::common::Timing::GetTimeSpec (ts, true);
    now = &ts;
  }

  std::map<long long, std::set<stat_negcache_node*> >::iterator it;

  int sizediff = mCacheSize - mNode2Mtime.size ();
  for (it = mMtime2Nodes.begin (); it != mMtime2Nodes.end (); ++it)
  {
    // todo: warning using coarse time stamp
    if (eos::common::Timing::GetCoarseAgeInNs (it->first, now) < mLifeTimeInNs && (sizediff-- >= 0)) break;
    for (auto vit = it->second.begin (); vit != it->second.end (); vit++)
      mNode2Mtime.erase (*vit);
  }
  mMtime2Nodes.erase (mMtime2Nodes.begin (), it);
}
