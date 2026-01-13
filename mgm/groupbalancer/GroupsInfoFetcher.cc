#include "mgm/groupbalancer/GroupsInfoFetcher.hh"
#include "mgm/fsview/FsView.hh"

namespace eos::mgm::group_balancer
{

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
    auto group_status = getGroupStatus((*it)->GetConfigMember("status"));

    if (!is_valid_status(group_status)) {
      continue;
    }

    uint64_t size {0}, capacity {0};

    // TODO - this might be dropped in favour of summing
    if (do_average) {
      size = (*it)->AverageDouble("stat.statfs.usedbytes", false);
      capacity = (*it)->AverageDouble("stat.statfs.capacity", false);
    } else {
      size = (*it)->SumLongLong("stat.statfs.usedbytes", false);
      capacity = (*it)->SumLongLong("stat.statfs.capacity", false);
    }
    if (capacity == 0) {
      continue;
    }

    mGroupSizes.emplace((*it)->mName, GroupSizeInfo{group_status, size, capacity});
  }

  return mGroupSizes;
}

} // eos::mgm::group_balancer
