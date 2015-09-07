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

#define __EOSMGM_TREECOMMON_CHK1__ if(pCheckLevel>=1)
#define __EOSMGM_TREECOMMON_CHK2__ if(pCheckLevel>=2)
#define __EOSMGM_TREECOMMON_CHK3__ if(pCheckLevel>=3)

#define __EOSMGM_TREECOMMON_DBG1__ if(pDebugLevel>=1)
#define __EOSMGM_TREECOMMON_DBG2__ if(pDebugLevel>=2)
#define __EOSMGM_TREECOMMON_DBG3__ if(pDebugLevel>=3)

#ifndef __EOSMGM_TREECOMMON__H__
#define __EOSMGM_TREECOMMON__H__

#define __EOSMGM_TREECOMMON__PACK__STRUCTURE__ 0

#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <iomanip>
#include <cassert>
#include <stdint.h>
#include <sstream>
//#include "half/include/half.hpp"
#include <iterator>
#include "common/FileSystem.hh"
#include "mgm/Namespace.hh"
#include "common/Logging.hh"

#if __EOSMGM_TREECOMMON__PACK__STRUCTURE__==1
#pragma pack(push,1)
#endif

//using half_float::half;

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
    //char spreadingFillRatioCap;
    //char fillRatioCompTol;
    size_t debugLevel;// 0(off)->3(full)
    size_t checkLevel;// 0(off)->3(full)
  };

  static Settings gSettings;

  // debug
protected:
  mutable size_t pDebugLevel;// 0(off)->3(full)
  mutable size_t pCheckLevel;// 0(off)->3(full)

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
  pDebugLevel(gSettings.debugLevel), pCheckLevel(gSettings.checkLevel)
  {
  }

public:
  SchedTreeBase& operator = (const SchedTreeBase &model)
  {
    pDebugLevel = model.pDebugLevel;
    pCheckLevel = model.pCheckLevel;
    return *this;
  }

  // to be sure to control the trade off between structure size and speed
  // we need to control the data alignment

  // To have a fine control over the structures memory foot print
  // (which should be kept as small as possible for cache efficiency reasons),
  // this type is used to refer to the number of nodes in the FastTree
  // for unsigned char (8 bit) a placement group can have up to 255 nodes
  // for unsigned short (16 bit) a placement group can have up to 65535 nodes
  //typedef uint8_t tFastTreeIdx; // 10% faster than uint16_t
  typedef uint16_t tFastTreeIdx;

  inline static size_t sGetMaxNodeCount()
  {
    return std::numeric_limits<tFastTreeIdx>::max();
  }

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
    }tNodeType;
    tNodeType nodeType;
    std::string geotag;
    std::string fullGeotag;
    std::string host;
    eos::common::FileSystem::fsid_t fsId;
    float netSpeedClass;

    std::ostream&
    display(std::ostream &os) const;

  };

  // the data in that structure is included in the FastTree
  // it MUST be necessary to the decision making process
  //enum tStatus { Drainer = 1<<1, Draining = 1<<2, Balancer = 1<<3, Balancing = 1<<4, Available = 1<<5, Readable = 1<<6, Writable = 1<<7, All = ~0, None = 0 };
  enum tStatus
  { Drainer = 1, Draining = 1<<1, Balancer = 1<<2, Balancing = 1<<3, Available = 1<<4, Readable = 1<<5, Writable = 1<<6, Disabled = 1<<7, All = ~0, None = 0};
  static std::string fsStatusToStr(int16_t s)
  {
    std::string out="";
    if(s&Disabled) out = +"DIS";
    if(!(s&Available)) out = +"Unv";
    if(s&Balancer) out = out + "Bin";
    if(s&Balancing) out = out + "Bout";
    if(s&Drainer) out = out + "Din";
    if(s&Draining) out = out + "Dout";
    if(s&Writable)
    {
      if(s&Readable)
      out = out +"RW";
      else
      out = out +"WO";
    }
    else
    {
      if(s&Readable)
      out = out +"RO";
      else
      out = out +"noIO";
    }

    return out;
  }

  static std::string intermediateStatusToStr(int16_t s)
  {
    std::string out="";
    if(s&Disabled) out = +"Dis";
    if(!(s&Available)) out = +"Unv";
    if(out.empty())
    out="OK";
    return out;
  }

  template<typename T>
  struct TreeNodeState
  {
    TreeNodeState() :
    mStatus(Available), ulScore(0), dlScore(0), totalSpace(0), fillRatio(0)
    {
    }
    typedef TreeNodeState<T> tSelf;

    int16_t mStatus;
    T ulScore;
    T dlScore;
    //half mTotalSpace; // this brings 10% speed improvement and also a lower memory footprint but add yet another dependency
    float totalSpace;
    T fillRatio;
  };

  struct TreeNodeSlots
  {
    typedef TreeNodeSlots tSelf;
    unsigned char freeSlotsCount;
    unsigned char takenSlotsCount;
    char avgDlScore;
    char avgUlScore;
    char maxDlScore;
    char maxUlScore;

    TreeNodeSlots() :
    freeSlotsCount(0),takenSlotsCount(0),
    avgDlScore(0),avgUlScore(0),
    maxDlScore(0),maxUlScore(0)
    {}
  };

  typedef TreeNodeState<char> TreeNodeStateChar;
  struct TreeNodeStateFloat : public TreeNodeState<float>
  {
    void
    writeCompactVersion(TreeNodeStateChar* target) const
    {
      target->mStatus = mStatus;
      target->ulScore = (char) ulScore;
      target->dlScore = (char) dlScore;
      target->totalSpace = totalSpace;
      target->fillRatio = (char) fillRatio;
    }
  };

  template<typename T>
  inline static signed char
  comparePlct(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
      const TreeNodeSlots* const &rightp, const char &spreadingFillRatioCap, const char &fillRatioCompTol )
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

    // -2 - Should not be disabled
    int16_t mask = Disabled;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return 1;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return -1;

    // -1 - Should be a drainer
    mask = Available|Writable;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return 1;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return -1;

    // 0 - Having at least one free slot
    if (!leftp->freeSlotsCount && rightp->freeSlotsCount)
    return 1;
    if (leftp->freeSlotsCount && !rightp->freeSlotsCount)
    return -1;

    // 1 - respect of SpreadingFillRatioCap
    if (lefts->fillRatio > spreadingFillRatioCap && rights->fillRatio <= spreadingFillRatioCap)
    return 1;
    if (lefts->fillRatio <= spreadingFillRatioCap && rights->fillRatio > spreadingFillRatioCap)
    return -1;

    // 2 - as few replicas as possible
    if (leftp->takenSlotsCount > rightp->takenSlotsCount)
    return 1;
    if (leftp->takenSlotsCount < rightp->takenSlotsCount)
    return -1;

    //#define USEMAXSLOT
#ifdef USEMAXSLOT
    // 3 - as many available slots as possible
    if(leftp->freeSlotsCount
        < rightp->freeSlotsCount) return 1;
    if(leftp->freeSlotsCount
        > rightp->freeSlotsCount) return -1;
    //  else return false;
#else
    // 3 - as empty as possible
    if (lefts->fillRatio > rights->fillRatio + fillRatioCompTol)
    return 1;
    if (lefts->fillRatio + fillRatioCompTol < rights->fillRatio)
    return -1;
    //  else return false;
#endif

    return 0;
  }

  template<typename T>
  inline static signed char
  compareAccess(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
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

    // -2 - Should not be disabled
    int16_t mask = Disabled;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return 1;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return -1;

    mask = Available|Readable;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return 1;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return -1;

    // 0 - Having at least one free slot
    if (!leftp->freeSlotsCount && rightp->freeSlotsCount)
    return 1;
    if (leftp->freeSlotsCount && !rightp->freeSlotsCount)
    return -1;

    // we might add a notion of depth to minimize latency
    return 0;
  }

  template<typename T>
  inline static signed char
  compareDrnPlct(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
      const TreeNodeSlots* const &rightp, const char &spreadingFillRatioCap, const char &fillRatioCompTol)
  {
    // -2 - Should not be disabled
    int16_t mask = Disabled;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return 1;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return -1;

    // -1 - Should be a drainer
    mask = Available|Writable|Drainer;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return 1;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return -1;

    // 0 - Having at least one free slot
    if (!leftp->freeSlotsCount && rightp->freeSlotsCount)
    return 1;
    if (leftp->freeSlotsCount && !rightp->freeSlotsCount)
    return -1;

    // 1 - respect of SpreadingFillRatioCap
    if (lefts->fillRatio > spreadingFillRatioCap && rights->fillRatio <= spreadingFillRatioCap)
    return 1;
    if (lefts->fillRatio <= spreadingFillRatioCap && rights->fillRatio > spreadingFillRatioCap)
    return -1;

    // 2 - as few replicas as possible
    if (leftp->takenSlotsCount > rightp->takenSlotsCount)
    return 1;
    if (leftp->takenSlotsCount < rightp->takenSlotsCount)
    return -1;

    //#define USEMAXSLOT
#ifdef USEMAXSLOT
    // 3 - as many available slots as possible
    if(leftp->freeSlotsCount
        < rightp->freeSlotsCount) return 1;
    if(leftp->freeSlotsCount
        > rightp->freeSlotsCount) return -1;
    //  else return false;
#else
    // 3 - as empty as possible
    if (lefts->fillRatio > rights->fillRatio + fillRatioCompTol)
    return 1;
    if (lefts->fillRatio + fillRatioCompTol < rights->fillRatio)
    return -1;
    //  else return false;
#endif

    return 0;
  }

  template<typename T>
  inline static signed char
  compareDrnAccess(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
      const TreeNodeSlots* const &rightp)
  {
    // -2 - Should not be disabled
    int16_t mask = Disabled;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return 1;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return -1;

    // lexicographic order
    // -1 - Should be a draining
    mask = Available|Readable;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return 1;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return -1;

    // 0 - Having at least one free slot
    if (!leftp->freeSlotsCount && rightp->freeSlotsCount)
    return 1;
    if (leftp->freeSlotsCount && !rightp->freeSlotsCount)
    return -1;

    // we might add a notion of depth to minimize latency
    return 0;
  }

  template<typename T>
  inline static signed char
  compareBlcPlct(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
      const TreeNodeSlots* const &rightp, const char &spreadingFillRatioCap, const char &fillRatioCompTol)
  {
    // lexicographic order
    // -2 - Should not be disabled
    int16_t mask = Disabled;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return 1;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return -1;

    // -1 - Should be a balancer
    mask = Available|Writable|Balancer;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return 1;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return -1;

    // 0 - Having at least one free slot
    if (!leftp->freeSlotsCount && rightp->freeSlotsCount)
    return 1;
    if (leftp->freeSlotsCount && !rightp->freeSlotsCount)
    return -1;

    // 1 - respect of SpreadingFillRatioCap
    if (lefts->fillRatio > spreadingFillRatioCap && rights->fillRatio <= spreadingFillRatioCap)
    return 1;
    if (lefts->fillRatio <= spreadingFillRatioCap && rights->fillRatio > spreadingFillRatioCap)
    return -1;

    // 2 - as few replicas as possible
    if (leftp->takenSlotsCount > rightp->takenSlotsCount)
    return 1;
    if (leftp->takenSlotsCount < rightp->takenSlotsCount)
    return -1;

    //#define USEMAXSLOT
#ifdef USEMAXSLOT
    // 3 - as many available slots as possible
    if(leftp->freeSlotsCount
        < rightp->freeSlotsCount) return 1;
    if(leftp->freeSlotsCount
        > rightp->freeSlotsCount) return -1;
    //  else return false;
#else
    // 3 - as empty as possible
    if (lefts->fillRatio > rights->fillRatio + fillRatioCompTol)
    return 1;
    if (lefts->fillRatio + fillRatioCompTol < rights->fillRatio)
    return -1;
    //  else return false;
#endif

    return 0;
  }

  template<typename T>
  inline static signed char
  compareBlcAccess(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
      const TreeNodeSlots* const &rightp, const char &spreadingFillRatioCap, const char &fillRatioCompTol)
  {
    // -2 - Should not be disabled
    int16_t mask = Disabled;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return 1;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return -1;

    // lexicographic order
    // -1 - Should be a balancing
    mask = Available|Readable;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return 1;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return -1;

    // 0 - Having at least one free slot
    if (!leftp->freeSlotsCount && rightp->freeSlotsCount)
    return 1;
    if (leftp->freeSlotsCount && !rightp->freeSlotsCount)
    return -1;

    // we might add a notion of depth to minimize latency
    return 0;
  }

  template<typename T>
  inline static signed char
  compareGateway(const TreeNodeState<T>* const &lefts, const TreeNodeSlots* const &leftp, const TreeNodeState<T>* const &rights,
                 const TreeNodeSlots* const &rightp)
  {
    // -2 - Should not be disabled
    int16_t mask = Disabled;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return 1;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return -1;

    // lexicographic order
    // -1 - Should be a balancing
    mask = Available;
    if ( (mask!=(mask&lefts->mStatus&mask)) && (mask==(rights->mStatus&mask)) )
    return 1;
    if ( (mask==(mask&lefts->mStatus&mask)) && (mask!=(rights->mStatus&mask)) )
    return -1;

    // we might add a notion of depth to minimize latency
    return 0;
  }

  typedef TreeNodeInfo FastTreeNodeInfo;

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

template <class RandomAccessIterator, class Compare>
void insertionSort (RandomAccessIterator first, RandomAccessIterator last, Compare comp)
{
  RandomAccessIterator it1,it2;
  typedef typename std::iterator_traits<RandomAccessIterator>::value_type
  valtype;

  for(it1=first+1; it1!=last; ++it1)
  {
    valtype data = *it1;
    for(it2=it1-1;it2>=first;--it2)
    {
      if(comp(*it2,data))
      *(it2+1) = *it2;
      else
      break;
    }
    *(it2+1) = data;
  }
  return;
}

template <class RandomAccessIterator, class Compare>
void bubbleSort (RandomAccessIterator first, RandomAccessIterator last, Compare comp)
{
  for(auto it1=first+1; it1!=last; ++it1)
  {
    for(auto it2=last-1;it2>=it1;--it2)
    {
      if( comp(*(it2-1) , *it2) )
      std::swap(*(it2-1),*it2);
    }
  }
  return;
}

inline void insertionSort(int* arr, int size)
{
  assert(arr);
  assert(size > 0);
  for (int i = 1; i < size; ++i)
  {
    int j = 0;
    int data = arr[i];
    for (j = i-1; j >= 0; --j)
    {
      if (arr[j] > data)
      arr[j+1] = arr[j];
      else
      break;
    }
    arr[j+1] = data;
  }
  return;
}

EOSMGMNAMESPACE_END

#if __EOSMGM_TREECOMMON__PACK__STRUCTURE__==1
#pragma pack(pop)
#endif

#endif  /* __EOSMGM_TREECOMMON__H__ */
