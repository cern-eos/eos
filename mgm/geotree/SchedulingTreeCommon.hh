//------------------------------------------------------------------------------
// @file SchedulingTreeCommon.hh
// @author Geoffray Adde - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#ifdef DEFINE_TREECOMMON_MACRO
#define CHK1 if(pCheckLevel>=1)
#define CHK2 if(pCheckLevel>=2)
#define CHK3 if(pCheckLevel>=3)

#define DBG1 if(pDebugLevel>=1)
#define DBG2 if(pDebugLevel>=2)
#define DBG3 if(pDebugLevel>=3)
#endif

#ifdef UNDEFINE_TREECOMMON_MACRO
#undef CHK1
#undef CHK2
#undef CHK3

#undef DBG1
#undef DBG2
#undef DBG3
#endif

#ifndef __EOSMGM_TREECOMMON__H__
#define __EOSMGM_TREECOMMON__H__
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <iomanip>
#include <cassert>
#include <stdint.h>
#include "half/include/half.hpp"

#include "mgm/Namespace.hh"

#pragma pack(push,1)
using half_float::half;

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Base class of Scheduling Tree software components
 *
 *        It contains typdefs, helper classes as well as debug and display features.
 *
 */
/*----------------------------------------------------------------------------*/
struct SchedTreeBase
{
  // settings
  struct Settings
  {
    unsigned char mSpreadingFillRatioCap;
    unsigned char mFillRatioCompTol;
    size_t mDebugLevel; // 0(off)->3(full)
    size_t mCheckLevel; // 0(off)->3(full)
  };

  static Settings gSettings;

  // debug
protected:
  mutable size_t pDebugLevel; // 0(off)->3(full)
  mutable size_t pCheckLevel; // 0(off)->3(full)

  inline void
  setDebugLevel(const size_t debugLevel) const
  {
    pDebugLevel = debugLevel;
  }
  inline void
  setCheckLevel(const size_t checkLevel) const
  {
    pCheckLevel = checkLevel;
  }

  SchedTreeBase() :
      pDebugLevel(gSettings.mDebugLevel), pCheckLevel(gSettings.mCheckLevel)
  {
  }
  //SchedTreeBase() { assert(!gSettings.mDebugLevel); assert(!gSettings.mCheckLevel); pDebugLevel=gSettings.mDebugLevel; pCheckLevel=gSettings.mCheckLevel; }

public:
  // to be sure to control the trade off between structure size and speed
  // we need to control the data alignment

  // To have a fine control over the structures memory foot print
  // (which should be kept as small as possible for cache efficiency reasons),
  // this type is used to refer to the number of nodes in the FastTree
  // for unsigned char (8 bit) a placement group can have up to 256 nodes
  // for unsigned short (16 bit) a placement group can have up to 65536 nodes
  //typedef unsigned char tFastTreeIdx;
  //typedef unsigned short tFastTreeIdx;
  typedef uint8_t tFastTreeIdx;

  // the data in that structure is not included in the FastTree
  // it should NOT be necessary to the decision making process
  // though, it is accessible once the decision is taken
  // this keeps the FastTree as small as possible and necessary
  // note that this class could be derived from a base class to
  // have specific info for different node types
  struct TreeNodeInfo
  {
    typedef enum
    {
      intermediate, fs
    } tNodeType;
    tNodeType mNodeType;
    std::string mGeotag;
    std::string mFullGeotag;
    std::string mHost;
    unsigned long mFsId;

    std::ostream&
    display(std::ostream &os) const;

  };

  // the data in that structure is included in the FastTree
  // it MUST be necessary to the decision making process
  enum tStatus { Drainer = 1<<1, Draining = 1<<2, Balancer = 1<<3, Balancing = 1<<4, All = ~0, None = 0 };
  template<typename T>
    struct TreeNodeState
    {
      TreeNodeState() :
          mStatus(None), mUlScore(0), mDlScore(0), mTotalSpace(0), mFillRatio(0)
      {
      }

      tStatus mStatus;
      T mUlScore;
      T mDlScore;
      half mTotalSpace;
      //float mTotalSpace;
      T mFillRatio;
    };

  struct TreeNodeSlots
  {
    unsigned char mNbFreeSlots;
    unsigned char mNbTakenSlots;

    // the following function implements the way the state of nodes are summarize into the father branch
    // this function is not incremental because it allows to compute more advanced metrics than just incremental
    // the drawback is that it's called online when building the FastTree but as an extra step of this process.
    // this function is used in the SlowTree. !! the FastTree uses an online implementation
    bool
    aggregate(const std::vector<const TreeNodeSlots*> &branchesPlacement)
    {
      mNbTakenSlots = 0;
      mNbFreeSlots = 0;
      for (std::vector<const TreeNodeSlots*>::const_iterator it = branchesPlacement.begin(); it != branchesPlacement.end(); it++)
      {
        mNbTakenSlots += (*it)->mNbTakenSlots;
        mNbFreeSlots += (*it)->mNbFreeSlots;
      }
      return true;
    }
  };

  typedef TreeNodeState<unsigned char> TreeNodeStateChar;
  struct TreeNodeStateFloat : public TreeNodeState<float>
  {
    // the following function implements the way the state of nodes are summarize into the father branch
    // this function is not incremental because it allows to compute more advanced metrics than just incremental
    // the drawback is that it's called online when building the FastTree but as an extra step of this process.
    bool
    aggregate(const std::vector<const TreeNodeStateFloat*> &branchesState)
    {
      mDlScore = 0;
      mUlScore = 0;
      mFillRatio = 0;
      mTotalSpace = 0;
      mStatus = None;
      for (std::vector<const TreeNodeStateFloat*>::const_iterator it = branchesState.begin(); it != branchesState.end(); it++)
      {
        mDlScore += (*it)->mDlScore;
        mUlScore += (*it)->mUlScore;
        mTotalSpace += (*it)->mTotalSpace;
        mFillRatio += (*it)->mFillRatio * (*it)->mTotalSpace;
        mStatus =  (SchedTreeBase::tStatus) (mStatus  | (*it)->mStatus); // an intermediate node tell if a leave having is given status in under it or not
      }
      if (mTotalSpace)
        mFillRatio /= mTotalSpace;
      return true;
    }
    void
    writeCompactVersion(TreeNodeStateChar* target) const
    {
      target->mDlScore = (unsigned char) 255 * mDlScore;
      target->mUlScore = (unsigned char) 255 * mUlScore;
      target->mStatus = mStatus;
      target->mFillRatio = (unsigned char) 100 * mFillRatio;
    }
  };

  template<typename T>
    inline static signed char
    ComparePlct(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
        const TreeNodeSlots* const &rightp)
    {
      // this function compares the scheduling priority of two branches
      // inside a FastTree branches are in a vector which is kept sorted.
      // if after a replica is placed, the scheduling priority doesn't get higher
      // than the next priority level being present in the array,
      // a single swap is enough to keep the order
      // return value
      // -1 if left  > right
      //  0 if left == right
      //  1 if right < left

      // lexicographic order
      // 0 - Having at least one free slot
      if (!leftp->mNbFreeSlots && rightp->mNbFreeSlots)
        return 1;
      if (leftp->mNbFreeSlots && !rightp->mNbFreeSlots)
        return -1;

      //  std::cout << "1 ";

      // 1 - respect of SpreadingFillRatioCap
      if (lefts->mFillRatio > gSettings.mSpreadingFillRatioCap && rights->mFillRatio <= gSettings.mSpreadingFillRatioCap)
        return 1;
      if (lefts->mFillRatio <= gSettings.mSpreadingFillRatioCap && rights->mFillRatio > gSettings.mSpreadingFillRatioCap)
        return -1;

      //  std::cout << "2 ";
      // 2 - as few replicas as possible
      if (leftp->mNbTakenSlots > rightp->mNbTakenSlots)
        return 1;
      if (leftp->mNbTakenSlots < rightp->mNbTakenSlots)
        return -1;

      //  std::cout << "3 ";
      //#define USEMAXSLOT
#ifdef USEMAXSLOT
      // 3 - as many available slots as possible
      if(leftp->mNbFreeSlots
          < rightp->mNbFreeSlots) return 1;
      if(leftp->mNbFreeSlots
          > rightp->mNbFreeSlots) return -1;
      //  else return false;
#else
      // 3 - as empty as possible
      if (lefts->mFillRatio > rights->mFillRatio + gSettings.mFillRatioCompTol)
        return 1;
      if (lefts->mFillRatio + gSettings.mFillRatioCompTol < rights->mFillRatio)
        return -1;
      //  else return false;
#endif

      return 0;
    }

  template<typename T>
    inline static signed char
    CompareAccess(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
        const TreeNodeSlots* const &rightp)
    {
      // this function compares the scheduling priority of two branches
      // inside a FastTree branches are in a vector which is kept sorted.
      // if after a replica is placed, the scheduling priority doesn't get higher
      // than the next priority level being present in the array,
      // a single swap is enough to keep the order
      // return value
      // -1 if left  > right
      //  0 if left == right
      //  1 if right > left

      // lexicographic order

      // 0 - Having at least one free slot
	  if (!leftp->mNbFreeSlots && rightp->mNbFreeSlots)
        return 1;
      if (leftp->mNbFreeSlots && !rightp->mNbFreeSlots)
        return -1;

      // we might add a notion of depth to minimize latency
      return 0;
    }

  template<typename T>
    inline static bool
    LowerPlct(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
        const TreeNodeSlots* const &rightp)
    {
      // this comparison can end up on two distincts objects being equal.
      // It's not a strict order, so it's NOT suitable to STL
      if (ComparePlct<T>(lefts, leftp, rights, rightp) > 0)
        return true;
      else
        return false;
    }

  template<typename T>
    inline static bool
    LowerAccess(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
        const TreeNodeSlots* const &rightp)
    {
      // this comparison can end up on two distincts objects being equal.
      // It's not a strict order, so it's NOT suitable to STL
      if (CompareAccess<T>(lefts, leftp, rights, rightp) > 0)
        return true;
      else
        return false;
    }

  template<typename T>
    inline static bool
    LowerPtrPlct(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
        const TreeNodeSlots* const &rightp)
    {
      // this comparison is a strict order.
      // So it's suitable to STL
      switch (ComparePlct<T>(lefts, leftp, rights, rightp))
      {
      case -1:
        return false;
      case 1:
        return true;
      case 0:
        if (lefts < rights)
          return true;
        else
          return false;
      default:
        assert(false);
        break;
      }
    }

  template<typename T>
    inline static bool
    LowerPtrAccess(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
        const TreeNodeSlots* const &rightp)
    {
      // this comparison is a strict order.
      // So it's suitable to STL
      switch (CompareAccess<T>(lefts, leftp, rights, rightp))
      {
      case -1:
        return false;
      case 1:
        return true;
      case 0:
        if (lefts < rights)
          return true;
        else
          return false;
      default:
        assert(false);
        break;
      }
    }

  template<typename T>
    inline static bool
    EqualPlct(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
        const TreeNodeSlots* const &rightp)
    {
      return ComparePlct<T>(lefts, leftp, rights, rightp) == 0;
    }

  template<typename T>
    inline static bool
    EqualAccess(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
        const TreeNodeSlots* const &rightp)
    {
      return CompareAccess<T>(lefts, leftp, rights, rightp) == 0;
    }

#if 0
  struct FastTreeNodeInfo : public TreeNodeInfo
  {
    // we might need to add something
  };
#else
  typedef TreeNodeInfo FastTreeNodeInfo;
#endif

  typedef std::vector<FastTreeNodeInfo> FastTreeInfo;

  //typedef std::map<unsigned long,tFastTreeIdx> Fs2TreeIdxMap;

};
// class SchedTreeBase

inline std::ostream&
operator <<(std::ostream &os, const SchedTreeBase::TreeNodeInfo &info)
{
  return info.display(os);
}
std::ostream&
operator <<(std::ostream &os, const SchedTreeBase::FastTreeInfo &info);

EOSMGMNAMESPACE_END

#pragma pack(pop)
#endif  /* __EOSMGM_TREECOMMON__H__ */
