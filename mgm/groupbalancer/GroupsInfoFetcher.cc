#include "mgm/groupbalancer/GroupsInfoFetcher.hh"
#include "mgm/FsView.hh"

namespace eos::mgm::group_balancer {

group_size_map
eosGroupsInfoFetcher::fetch()
{
  group_size_map mGroupSizes;
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mSpaceGroupView.count(spaceName) == 0) {
    eos_static_err("msg=\"no such space %s\"", spaceName.c_str());
    return mGroupSizes;
  }

  auto set_fsgrp = FsView::gFsView.mSpaceGroupView[spaceName];

  for (auto it = set_fsgrp.cbegin(); it != set_fsgrp.cend(); it++) {
    if ((*it)->GetConfigMember("status") != "on") {
      continue;
    }

    uint64_t size = (*it)->AverageDouble("stat.statfs.usedbytes", false);
    uint64_t capacity = (*it)->AverageDouble("stat.statfs.capacity", false);

    if (capacity == 0) {
      continue;
    }

    mGroupSizes.emplace((*it)->mName, GroupSizeInfo(size, capacity));
  }

  return mGroupSizes;
}

} // eos::mgm::group_balancer
