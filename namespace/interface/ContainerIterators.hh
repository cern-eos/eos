/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Container map iterators
//------------------------------------------------------------------------------

#ifndef EOS_NS_CONTAINER_ITERATORS_HH
#define EOS_NS_CONTAINER_ITERATORS_HH

#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FileMapIterator
//------------------------------------------------------------------------------
class FileMapIterator
{
public:
  FileMapIterator(IContainerMDPtr cont)
    : container(cont), mLock(cont->mMutex), iResized(false), iValid(false) {
    std::shared_lock<std::shared_timed_mutex> lock (mLock);
    iter = cont->filesBegin();
    iGeneration = generation();
    if (!iterEnd()) {
      iValid = true;
      iKey = iter->first;
      iValue = iter->second;
      iShown.insert(iKey);
    }
  }

  bool valid() const {
    return iValid;
  }

  bool iterEnd() const {
    return (iter == container->filesEnd());
  }

  void next() {
    std::shared_lock<std::shared_timed_mutex> lock (mLock);

    // check for a re-sized map
    if (generation() != iGeneration) {
      iResized = true;
      // the hash_map has been re-organized
      iter = container->filesBegin();
      if (iterEnd()) {
	iValid = false;
	return;
      }

      do {
	if (!iShown.count(iter->first)) {
	  break;
	} else {
	  iter++;
	  if (iterEnd()) {
	    iValid = false;
	    return;
	  }
	}
      } while (1);

      iGeneration = generation();
    } else {
      // check for a re-sized map
      if (iResized) {
	// in this case we always have to check if a value was already shown
	iter++;
	if (!iterEnd()) {
	  do {
	    if (!iShown.count(iter->first)) {
	      break;
	    } else {
	      iter++;
	      if (iterEnd()) {
		iValid = false;
		return;
	      }
	    }
	  } while (1);
	} else {
	  iValid = false;
	  return;
	}
      } else {
	iter++;
      }
    }

    if (!iterEnd()) {
      iKey = iter->first;
      iValue = iter->second;
      iShown.insert(iKey);
    } else {
      iValid = false;
    }
  }

  std::string key() const {
    return iKey;
  }

  IFileMD::id_t value() const {
    return iValue;
  }

  uint64_t generation() {
    return container->getFileMapGeneration();
  }

private:


  IContainerMDPtr container;
  std::shared_timed_mutex &mLock;
  eos::IContainerMD::FileMap::const_iterator iter;
  std::set<std::string> iShown;
  std::string iKey;
  uint64_t iValue;
  uint64_t iGeneration;
  bool iResized;
  bool iValid;
};

//------------------------------------------------------------------------------
//! Class ContainerIterator
//------------------------------------------------------------------------------
class ContainerMapIterator
{
public:
  ContainerMapIterator(IContainerMDPtr cont)
    : container(cont), mLock(cont->mMutex), iResized(false), iValid(false) {
    iter = cont->subcontainersBegin();
    iGeneration = generation();
    if (!iterEnd()) {
      iValid = true;
      iKey = iter->first;
      iValue = iter->second;
      iShown.insert(iKey);
    }
  }

  bool valid() const {
    return iValid;
  }

  bool iterEnd() const {
    return (iter == container->subcontainersEnd());
  }

  void next() {
    std::shared_lock<std::shared_timed_mutex> lock (mLock);

    // check for a re-sized map
    if (generation() != iGeneration) {
      iResized = true;
      // the hash_map has been re-organized
      iter = container->subcontainersBegin();
      if (iterEnd()) {
	iValid = false;
	return;
      }

      do {
	if (!iShown.count(iter->first)) {
	  break;
	} else {
	  iter++;
	  if (iterEnd()) {
	    iValid = false;
	    return;
	  }
	}
      } while (1);

      iGeneration = generation();
    } else {
      // check for a re-sized map
      if (iResized) {
	// in this case we always have to check if a value was already shown
	iter++;
	if (!iterEnd()) {
	  do {
	    if (!iShown.count(iter->first)) {
	      break;
	    } else {
	      iter++;
	      if (iterEnd()) {
		iValid = false;
		return;
	      }
	    }
	  } while (1);
	} else {
	  iValid = false;
	  return;
	}
      } else {
	iter++;
      }
    }

    if (!iterEnd()) {
      iKey = iter->first;
      iValue = iter->second;
      iShown.insert(iKey);
    } else {
      iValid = false;
    }
  }

  std::string key() const {
    return iKey;
  }

  IFileMD::id_t value() const {
    return iValue;
  }

  uint64_t generation() {
    return container->getContainerMapGeneration();
  }

private:

  IContainerMDPtr container;
  std::shared_timed_mutex &mLock;
  eos::IContainerMD::ContainerMap::const_iterator iter;
  std::set<std::string> iShown;
  std::string iKey;
  uint64_t iValue;
  uint64_t iGeneration;
  bool iResized;
  bool iValid;
};

EOSNSNAMESPACE_END

#endif
