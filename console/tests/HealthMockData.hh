//------------------------------------------------------------------------------
//! @file HealthMockData.hh
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef __HEALTHMOCKDATA__HH__
#define __HEALTHMOCKDATA__HH__

#include <unordered_map>
#include <vector>
#include <string>
#include <sstream>
#include <utility>
#include "MgmExecuteTest.hh"
#include "../commands/HealthCommand.hh"

using FSInfoVec      = std::vector<FSInfo>;
using MgmExecs       = std::unordered_map<std::string, MgmExecute>;
using GroupsInfo     = std::unordered_map<std::string, FSInfoVec>;
using TestOutputs    = std::unordered_map<std::string,  std::string>;
using GroupsInfoData = std::unordered_map<std::string,  GroupsInfo>;

class HealthMockData
{
private:
  GroupsInfo LoadGroupInfoFromString(const std::string& data)
  {
    GroupsInfo ret;
    std::stringstream file(data);
    std::string line;
    std::string group;
    FSInfoVec fss;

    while (std::getline(file, line)) {
      if (line.at(0) == '#') {
        fss.clear();
        group = std::string(line.begin() + 1,  line.end());
        continue;
      } else {
        FSInfo temp;
        temp.ReadFromString(line);
        ret[group].push_back(temp);
      }
    }

    return ret;
  }
public:
  TestOutputs m_outputs;
  GroupsInfoData m_info_data;
  MgmExecs m_mexecs;

  void GenerateInfoData()
  {
    m_info_data["good"] = LoadGroupInfoFromString(std::string(
                            "#default.1\n"
                            "fst1.localdomain 1095 1 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                            "fst2.localdomain 1095 5 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                            "fst3.localdomain 1095 9 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                            "fst4.localdomain 1095 13 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                            "#default.2\n"
                            "fst1.localdomain 1095 2 online /var/eos/fs/2 1000000000 10000000000 2000000000 12000000000\n"
                            "fst2.localdomain 1095 6 online /var/eos/fs/2 1000000000 10000000000 2000000000 12000000000\n"
                            "fst3.localdomain 1095 10 online /var/eos/fs/2 1000000000 10000000000 2000000000 12000000000\n"
                            "fst4.localdomain 1095 14 online /var/eos/fs/2 1000000000 10000000000 2000000000 12000000000\n"
                            "#default.3\n"
                            "fst1.localdomain 1095 3 online /var/eos/fs/3 1000000000 10000000000 2000000000 12000000000\n"
                            "fst2.localdomain 1095 7 online /var/eos/fs/3 1000000000 10000000000 2000000000 12000000000\n"
                            "fst3.localdomain 1095 11 online /var/eos/fs/3 1000000000 10000000000 2000000000 12000000000\n"
                            "fst4.localdomain 1095 15 online /var/eos/fs/3 1000000000 10000000000 2000000000 12000000000\n"
                            "#default.4\n"
                            "fst1.localdomain 1095 4 online /var/eos/fs/4 1000000000 10000000000 2000000000 12000000000\n"
                            "fst2.localdomain 1095 8 online /var/eos/fs/4 1000000000 10000000000 2000000000 12000000000\n"
                            "fst3.localdomain 1095 12 online /var/eos/fs/4 1000000000 10000000000 2000000000 12000000000\n"
                            "fst4.localdomain 1095 16 online /var/eos/fs/4 1000000000 10000000000 2000000000 12000000000\n"));
    m_info_data["bad"] = LoadGroupInfoFromString(std::string(
                           "#default.1\n"
                           "fst1.localdomain 1095 1 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                           "fst2.localdomain 1095 5 online /var/eos/fs/1 1000000000 2000000000 10000000000 12000000000\n"
                           "fst3.localdomain 1095 9 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                           "fst4.localdomain 1095 13 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                           "#default.2\n"
                           "fst1.localdomain 1095 2 online /var/eos/fs/2 1000000000 2000000000 10000000000 12000000000\n"
                           "fst2.localdomain 1095 6 online /var/eos/fs/2 1000000000 2000000000 10000000000 12000000000\n"
                           "fst3.localdomain 1095 10 online /var/eos/fs/2 1000000000 10000000000 2000000000 12000000000\n"
                           "fst4.localdomain 1095 14 online /var/eos/fs/2 1000000000 10000000000 2000000000 12000000000\n"
                           "#default.3\n"
                           "fst1.localdomain 1095 3 online /var/eos/fs/3 1000000000 10000000000 2000000000 12000000000\n"
                           "fst2.localdomain 1095 7 online /var/eos/fs/3 1000000000 2000000000 10000000000 12000000000\n"
                           "fst3.localdomain 1095 11 online /var/eos/fs/3 1000000000 2000000000 10000000000 12000000000\n"
                           "fst4.localdomain 1095 15 online /var/eos/fs/3 1000000000 2000000000 10000000000 12000000000\n"
                           "#default.4\n"
                           "fst1.localdomain 1095 4 online /var/eos/fs/4 1000000000 10000000000 2000000000 12000000000\n"
                           "fst2.localdomain 1095 8 online /var/eos/fs/4 1000000000 2000000000 10000000000 12000000000\n"
                           "fst3.localdomain 1095 12 online /var/eos/fs/4 1000000000 2000000000 10000000000 12000000000\n"
                           "fst4.localdomain 1095 16 online /var/eos/fs/4 1000000000 10000000000 2000000000 12000000000\n"));
    m_info_data["bad_drain"] = LoadGroupInfoFromString(std::string(
                                 "#default.1\n"
                                 "fst1.localdomain 1095 1 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                                 "fst2.localdomain 1095 5 offline /var/eos/fs/1 1000000000 2000000000 10000000000 12000000000\n"
                                 "fst3.localdomain 1095 9 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                                 "fst4.localdomain 1095 13 online /var/eos/fs/1 1000000000 10000000000 2000000000 12000000000\n"
                                 "#default.2\n"
                                 "fst1.localdomain 1095 2 offline /var/eos/fs/2 1000000000 2000000000 10000000000 12000000000\n"
                                 "fst2.localdomain 1095 6 offline /var/eos/fs/2 1000000000 2000000000 10000000000 12000000000\n"
                                 "fst3.localdomain 1095 10 online /var/eos/fs/2 1000000000 10000000000 2000000000 12000000000\n"
                                 "fst4.localdomain 1095 14 online /var/eos/fs/2 1000000000 10000000000 2000000000 12000000000\n"
                                 "#default.3\n"
                                 "fst1.localdomain 1095 3 online /var/eos/fs/3 1000000000 10000000000 2000000000 12000000000\n"
                                 "fst2.localdomain 1095 7 offline /var/eos/fs/3 1000000000 2000000000 10000000000 12000000000\n"
                                 "fst3.localdomain 1095 11 offline /var/eos/fs/3 1000000000 2000000000 10000000000 12000000000\n"
                                 "fst4.localdomain 1095 15 offline /var/eos/fs/3 1000000000 2000000000 10000000000 12000000000\n"
                                 "#default.4\n"
                                 "fst1.localdomain 1095 4 online /var/eos/fs/4 1000000000 10000000000 2000000000 12000000000\n"
                                 "fst2.localdomain 1095 8 online /var/eos/fs/4 1000000000 2000000000 10000000000 12000000000\n"
                                 "fst3.localdomain 1095 12 online /var/eos/fs/4 1000000000 2000000000 10000000000 12000000000\n"
                                 "fst4.localdomain 1095 16 online /var/eos/fs/4 1000000000 10000000000 2000000000 12000000000\n"));
  }

  void GenerateOutputs()
  {
    m_outputs["drain_bad_-a"] =
      std::string(
        "#-------------------------------------------------------------------\n"
        "              Group:  Offline Used(GB)   Online Free(GB)   Status:\n"
        "#-------------------------------------------------------------------\n"
        "\33[32m           default.4\33[0m                 0\33[0m           18.6265\33[0m        OK\33[0m\n"
        "\33[31m           default.3\33[0m           27.9397\33[0m            8.3819\33[0m      FULL\33[0m\n"
        "\33[31m           default.2\33[0m           18.6265\33[0m           16.7638\33[0m      FULL\33[0m\n"
        "\33[32m           default.1\33[0m           9.31323\33[0m           25.1457\33[0m        OK\33[0m\n"
        "\n"
        "\n");
    m_outputs["drain_good_-a"] =
      std::string(
        "#-------------------------------------------------------------------\n"
        "              Group:  Offline Used(GB)   Online Free(GB)   Status:\n"
        "#-------------------------------------------------------------------\n"
        "\33[32m           default.4\33[0m                 0\33[0m           33.5276\33[0m        OK\33[0m\n"
        "\33[32m           default.3\33[0m                 0\33[0m           33.5276\33[0m        OK\33[0m\n"
        "\33[32m           default.2\33[0m                 0\33[0m           33.5276\33[0m        OK\33[0m\n"
        "\33[32m           default.1\33[0m                 0\33[0m           33.5276\33[0m        OK\33[0m\n"
        "\n"
        "\n");
    m_outputs["drain_bad"] =
      std::string(
        "#-------------------------------------------------------------------\n"
        "              Group:  Offline Used(GB)   Online Free(GB)   Status:\n"
        "#-------------------------------------------------------------------\n"
        "\33[31m           default.3\33[0m           27.9397\33[0m            8.3819\33[0m      FULL\33[0m\n"
        "\33[31m           default.2\33[0m           18.6265\33[0m           16.7638\33[0m      FULL\33[0m\n"
        "\n"
        "\n");
    m_outputs["drain_good"] =
      std::string(
        "#-------------------------------------------------------------------\n"
        "              Group:  Offline Used(GB)   Online Free(GB)   Status:\n"
        "#-------------------------------------------------------------------\n"
        "\n"
        "\n");
    m_outputs["drain_bad_-m"] =
      std::string(
        "type=FullDrainCheck\n"
        "group=default.4 offline_used_space=0 online_free_space=18.6265 status=ok \n"
        "group=default.3 offline_used_space=27.9397 online_free_space=8.3819 status=full \n"
        "group=default.2 offline_used_space=18.6265 online_free_space=16.7638 status=full \n"
        "group=default.1 offline_used_space=9.31323 online_free_space=25.1457 status=ok \n"
        "\n");
    m_outputs["drain_good_-m"] =
      std::string(
        "type=FullDrainCheck\n"
        "group=default.4 offline_used_space=0 online_free_space=33.5276 status=ok \n"
        "group=default.3 offline_used_space=0 online_free_space=33.5276 status=ok \n"
        "group=default.2 offline_used_space=0 online_free_space=33.5276 status=ok \n"
        "group=default.1 offline_used_space=0 online_free_space=33.5276 status=ok \n"
        "\n");
    m_outputs["placement_bad_-a"] =
      std::string(
        "#-----------------------------------------------------\n"
        "               Group      Free      Full  Contention\n"
        "#-----------------------------------------------------\n"
        "\33[31m           default.4\33[0m         2\33[0m         2\33[0m         50%\33[0m\n"
        "\33[31m           default.3\33[0m         1\33[0m         3\33[0m         75%\33[0m\n"
        "\33[31m           default.2\33[0m         2\33[0m         2\33[0m         50%\33[0m\n"
        "\33[32m           default.1\33[0m         3\33[0m         1\33[0m         25%\33[0m\n"
        "#------------------------------------------------------\n"
        "   min   avg   max  min-placement               group\n"
        "#------------------------------------------------------\n"
        "   25%\33[0m   50%\33[0m   75%\33[0m              1\33[0m           default.3\33[0m\n"
        "\n"
        "\n");
    m_outputs["placement_bad_-m"] =
      std::string(
        "type=PlacementContentionCheck\n"
        "group=default.4 free_fs=2 full_fs=2 status=full\n"
        "group=default.3 free_fs=1 full_fs=3 status=full\n"
        "group=default.2 free_fs=2 full_fs=2 status=full\n"
        "group=default.1 free_fs=3 full_fs=1 status=fine\n"
        "\n");
    m_outputs["placement_good_-a"] =
      std::string(
        "#-----------------------------------------------------\n"
        "               Group      Free      Full  Contention\n"
        "#-----------------------------------------------------\n"
        "\33[32m           default.4\33[0m         4\33[0m         0\33[0m          0%\33[0m\n"
        "\33[32m           default.3\33[0m         4\33[0m         0\33[0m          0%\33[0m\n"
        "\33[32m           default.2\33[0m         4\33[0m         0\33[0m          0%\33[0m\n"
        "\33[32m           default.1\33[0m         4\33[0m         0\33[0m          0%\33[0m\n"
        "#------------------------------------------------------\n"
        "   min   avg   max  min-placement               group\n"
        "#------------------------------------------------------\n"
        "    0%\33[0m    0%\33[0m    0%\33[0m              4\33[0m           default.4\33[0m\n"
        "\n"
        "\n");
    m_outputs["placement_bad"] =
      std::string(
        "#-----------------------------------------------------\n"
        "               Group      Free      Full  Contention\n"
        "#-----------------------------------------------------\n"
        "\33[31m           default.4\33[0m         2\33[0m         2\33[0m         50%\33[0m\n"
        "\33[31m           default.3\33[0m         1\33[0m         3\33[0m         75%\33[0m\n"
        "\33[31m           default.2\33[0m         2\33[0m         2\33[0m         50%\33[0m\n"
        "#------------------------------------------------------\n"
        "   min   avg   max  min-placement               group\n"
        "#------------------------------------------------------\n"
        "   25%\33[0m   50%\33[0m   75%\33[0m              1\33[0m           default.3\33[0m\n"
        "\n"
        "\n");
    m_outputs["placement_good"] =
      std::string(
        "#-----------------------------------------------------\n"
        "               Group      Free      Full  Contention\n"
        "#-----------------------------------------------------\n"
        "#------------------------------------------------------\n"
        "   min   avg   max  min-placement               group\n"
        "#------------------------------------------------------\n"
        "    0%\33[0m    0%\33[0m    0%\33[0m              4\33[0m           default.4\33[0m\n"
        "\n"
        "\n");
    m_outputs["placement_good_-m"] =
      std::string(
        "type=PlacementContentionCheck\n"
        "group=default.4 free_fs=4 full_fs=0 status=fine\n"
        "group=default.3 free_fs=4 full_fs=0 status=fine\n"
        "group=default.2 free_fs=4 full_fs=0 status=fine\n"
        "group=default.1 free_fs=4 full_fs=0 status=fine\n"
        "\n");
    m_outputs["nodes_good_-a"] =
      std::string(
        "#-----------------------------------------\n"
        "                     Hostport:   Status:\n"
        "#-----------------------------------------\n"
        "\33[32m         fst1.localdomain:1095\33[0m    online\33[0m\n"
        "\33[32m         fst2.localdomain:1095\33[0m    online\33[0m\n"
        "\33[32m         fst3.localdomain:1095\33[0m    online\33[0m\n"
        "\33[32m         fst4.localdomain:1095\33[0m    online\33[0m\n"
        "\n"
        "\n");
    m_outputs["nodes_good"] =
      std::string(
        "#-----------------------------------------\n"
        "                     Hostport:   Status:\n"
        "#-----------------------------------------\n"
        "\n"
        "\n");
    m_outputs["nodes_bad_-a"] =
      std::string(
        "#-----------------------------------------\n"
        "                     Hostport:   Status:\n"
        "#-----------------------------------------\n"
        "\33[32m         fst1.localdomain:1095\33[0m    online\33[0m\n"
        "\33[31m         fst2.localdomain:1095\33[0m   offline\33[0m\n"
        "\33[31m         fst3.localdomain:1095\33[0m   offline\33[0m\n"
        "\33[31m         fst4.localdomain:1095\33[0m   offline\33[0m\n"
        "\n"
        "\n");
    m_outputs["nodes_bad"] =
      std::string(
        "#-----------------------------------------\n"
        "                     Hostport:   Status:\n"
        "#-----------------------------------------\n"
        "\33[31m         fst2.localdomain:1095\33[0m   offline\33[0m\n"
        "\33[31m         fst3.localdomain:1095\33[0m   offline\33[0m\n"
        "\33[31m         fst4.localdomain:1095\33[0m   offline\33[0m\n"
        "\n"
        "\n");
  }

  void GenerateMgms()
  {
    MgmExecute temp;
    temp.m_queue.push(std::make_pair(
                        "mgm.cmd=fs&mgm.subcmd=ls&mgm.outformat=m",
                        "host=fst1.localdomain port=1095 id=1 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst1.localdomain port=1095 id=2 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst1.localdomain port=1095 id=3 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst1.localdomain port=1095 id=4 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst2.localdomain port=1095 id=5 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst2.localdomain port=1095 id=6 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst2.localdomain port=1095 id=7 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst2.localdomain port=1095 id=8 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst3.localdomain port=1095 id=9 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst3.localdomain port=1095 id=10 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst3.localdomain port=1095 id=11 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst3.localdomain port=1095 id=12 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst4.localdomain port=1095 id=13 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst4.localdomain port=1095 id=14 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst4.localdomain port=1095 id=15 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"
                        "host=fst4.localdomain port=1095 id=16 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0"));
    m_mexecs["good"] = temp;
    temp.m_queue.pop();
    temp.m_queue.push(std::make_pair(
                        "mgm.cmd=fs&mgm.subcmd=ls&mgm.outformat=m",
                        "host=fst1.localdomain port=1095 id=1 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst1.localdomain port=1095 id=2 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst1.localdomain port=1095 id=3 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst1.localdomain port=1095 id=4 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst2.localdomain port=1095 id=5 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst2.localdomain port=1095 id=6 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst2.localdomain port=1095 id=7 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst2.localdomain port=1095 id=8 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst3.localdomain port=1095 id=9 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst3.localdomain port=1095 id=10 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst3.localdomain port=1095 id=11 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst3.localdomain port=1095 id=12 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst4.localdomain port=1095 id=13 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst4.localdomain port=1095 id=14 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst4.localdomain port=1095 id=15 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst4.localdomain port=1095 id=16 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"));
    m_mexecs["bad"] = temp;
    temp.m_queue.pop();
    temp.m_queue.push(std::make_pair(
                        "mgm.cmd=fs&mgm.subcmd=ls&mgm.outformat=m",
                        "host=fst1.localdomain port=1095 id=1 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst1.localdomain port=1095 id=2 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=offline scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst1.localdomain port=1095 id=3 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst1.localdomain port=1095 id=4 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst2.localdomain port=1095 id=5 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=offline scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst2.localdomain port=1095 id=6 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=offline scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst2.localdomain port=1095 id=7 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst2.localdomain port=1095 id=8 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst3.localdomain port=1095 id=9 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=offline scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst3.localdomain port=1095 id=10 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=offline scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst3.localdomain port=1095 id=11 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=offline scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst3.localdomain port=1095 id=12 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst4.localdomain port=1095 id=13 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/1 schedgroup=default.1 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst4.localdomain port=1095 id=14 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/2 schedgroup=default.2 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst4.localdomain port=1095 id=15 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/3 schedgroup=default.3 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=2000000000 stat.statfs.usedbytes=10000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"
                        "host=fst4.localdomain port=1095 id=16 uuid=ab24ec6f-a448-434f-ae80-d2f2869b2d79 path=/var/eos/fs/4 schedgroup=default.4 stat.boot=booted configstatus=rw headroom=1000000000  stat.errc=0 stat.errmsg=  stat.disk.load=0.00 stat.disk.readratemb=0 stat.disk.writeratemb=0 stat.net.ethratemib=119 stat.net.inratemib=0 stat.net.outratemib=0 stat.ropen=0 stat.wopen=0 stat.statfs.freebytes=10000000000 stat.statfs.usedbytes=2000000000 stat.statfs.capacity=12000000000 stat.usedfiles=0 stat.statfs.ffree=40270268 stat.statfs.fused=258228224 stat.statfs.files=40333312 stat.drain=nodrain stat.drainprogress=0 stat.drainfiles=0 stat.drainbytesleft=0 stat.drainretry=0 graceperiod=86400 stat.timeleft=0 stat.active=online scaninterval=604800 stat.balancer.running=0 stat.disk.iops=240 stat.disk.bw=127.00 stat.geotag=Serbia stat.health=unknown%20raid stat.health.redundancy_factor=0 stat.health.drives_failed=0 stat.health.drives_total=0 stat.health.indicator=0\n"));
    m_mexecs["bad_drain"] = temp;
    temp.m_queue.pop();
    temp.m_queue.push(std::make_pair(
                        "mgm.cmd=node&mgm.subcmd=ls&mgm.outformat=m",
                        "type=nodesview hostport=fst1.localdomain:1095 status=online cfg.status=on cfg.txgw=off heartbeatdelta=3 nofs=6 avg.stat.disk.load=0.00 sig.stat.disk.load=0.00 sum.stat.disk.readratemb=0 sum.stat.disk.writeratemb=0 sum.stat.net.ethratemib=119 sum.stat.net.inratemib=0 sum.stat.net.outratemib=0 sum.stat.ropen=0 sum.stat.wopen=0 sum.stat.statfs.freebytes=237439672320 sum.stat.statfs.usedbytes=10247208960 sum.stat.statfs.capacity=247686881280 sum.stat.usedfiles=0 sum.stat.statfs.ffree=241621608 sum.stat.statfs.fused=1549369344 sum.stat.statfs.files=241999872 sum.stat.balancer.running=0 stat.gw.queued=  cfg.stat.sys.vsize=350724096 cfg.stat.sys.rss=45793280 cfg.stat.sys.threads=54 cfg.stat.sys.sockets=8 cfg.stat.sys.eos.version=4.1.11-1 cfg.stat.sys.kernel=3.10.0-327.36.3.el7.x86_64 cfg.stat.sys.eos.start=Mon Dec  5 10:17:12 2016 cfg.stat.sys.uptime= 21:54:52 up 12:20,  1 user,  load average: 0.00, 0.01, 0.05 sum.stat.disk.iops?configstatus@rw=1122 sum.stat.disk.bw?configstatus@rw=634 cfg.stat.geotag=Serbia cfg.gw.rate=120 cfg.gw.ntx=10\n"
                        "type=nodesview hostport=fst2.localdomain:1095 status=online cfg.status=on cfg.txgw=off heartbeatdelta=3 nofs=6 avg.stat.disk.load=0.00 sig.stat.disk.load=0.00 sum.stat.disk.readratemb=0 sum.stat.disk.writeratemb=0 sum.stat.net.ethratemib=119 sum.stat.net.inratemib=0 sum.stat.net.outratemib=0 sum.stat.ropen=0 sum.stat.wopen=0 sum.stat.statfs.freebytes=237439672320 sum.stat.statfs.usedbytes=10247208960 sum.stat.statfs.capacity=247686881280 sum.stat.usedfiles=0 sum.stat.statfs.ffree=241621608 sum.stat.statfs.fused=1549369344 sum.stat.statfs.files=241999872 sum.stat.balancer.running=0 stat.gw.queued=  cfg.stat.sys.vsize=350724096 cfg.stat.sys.rss=45793280 cfg.stat.sys.threads=54 cfg.stat.sys.sockets=8 cfg.stat.sys.eos.version=4.1.11-1 cfg.stat.sys.kernel=3.10.0-327.36.3.el7.x86_64 cfg.stat.sys.eos.start=Mon Dec  5 10:17:12 2016 cfg.stat.sys.uptime= 21:54:52 up 12:20,  1 user,  load average: 0.00, 0.01, 0.05 sum.stat.disk.iops?configstatus@rw=1122 sum.stat.disk.bw?configstatus@rw=634 cfg.stat.geotag=Serbia cfg.gw.rate=120 cfg.gw.ntx=10\n"
                        "type=nodesview hostport=fst3.localdomain:1095 status=online cfg.status=on cfg.txgw=off heartbeatdelta=3 nofs=6 avg.stat.disk.load=0.00 sig.stat.disk.load=0.00 sum.stat.disk.readratemb=0 sum.stat.disk.writeratemb=0 sum.stat.net.ethratemib=119 sum.stat.net.inratemib=0 sum.stat.net.outratemib=0 sum.stat.ropen=0 sum.stat.wopen=0 sum.stat.statfs.freebytes=237439672320 sum.stat.statfs.usedbytes=10247208960 sum.stat.statfs.capacity=247686881280 sum.stat.usedfiles=0 sum.stat.statfs.ffree=241621608 sum.stat.statfs.fused=1549369344 sum.stat.statfs.files=241999872 sum.stat.balancer.running=0 stat.gw.queued=  cfg.stat.sys.vsize=350724096 cfg.stat.sys.rss=45793280 cfg.stat.sys.threads=54 cfg.stat.sys.sockets=8 cfg.stat.sys.eos.version=4.1.11-1 cfg.stat.sys.kernel=3.10.0-327.36.3.el7.x86_64 cfg.stat.sys.eos.start=Mon Dec  5 10:17:12 2016 cfg.stat.sys.uptime= 21:54:52 up 12:20,  1 user,  load average: 0.00, 0.01, 0.05 sum.stat.disk.iops?configstatus@rw=1122 sum.stat.disk.bw?configstatus@rw=634 cfg.stat.geotag=Serbia cfg.gw.rate=120 cfg.gw.ntx=10\n"
                        "type=nodesview hostport=fst4.localdomain:1095 status=online cfg.status=on cfg.txgw=off heartbeatdelta=3 nofs=6 avg.stat.disk.load=0.00 sig.stat.disk.load=0.00 sum.stat.disk.readratemb=0 sum.stat.disk.writeratemb=0 sum.stat.net.ethratemib=119 sum.stat.net.inratemib=0 sum.stat.net.outratemib=0 sum.stat.ropen=0 sum.stat.wopen=0 sum.stat.statfs.freebytes=237439672320 sum.stat.statfs.usedbytes=10247208960 sum.stat.statfs.capacity=247686881280 sum.stat.usedfiles=0 sum.stat.statfs.ffree=241621608 sum.stat.statfs.fused=1549369344 sum.stat.statfs.files=241999872 sum.stat.balancer.running=0 stat.gw.queued=  cfg.stat.sys.vsize=350724096 cfg.stat.sys.rss=45793280 cfg.stat.sys.threads=54 cfg.stat.sys.sockets=8 cfg.stat.sys.eos.version=4.1.11-1 cfg.stat.sys.kernel=3.10.0-327.36.3.el7.x86_64 cfg.stat.sys.eos.start=Mon Dec  5 10:17:12 2016 cfg.stat.sys.uptime= 21:54:52 up 12:20,  1 user,  load average: 0.00, 0.01, 0.05 sum.stat.disk.iops?configstatus@rw=1122 sum.stat.disk.bw?configstatus@rw=634 cfg.stat.geotag=Serbia cfg.gw.rate=120 cfg.gw.ntx=10\n"));
    m_mexecs["good_nodes"] = temp;
    temp.m_queue.pop();
    temp.m_queue.push(std::make_pair(
                        "mgm.cmd=node&mgm.subcmd=ls&mgm.outformat=m",
                        "type=nodesview hostport=fst1.localdomain:1095 status=online cfg.status=on cfg.txgw=off heartbeatdelta=3 nofs=6 avg.stat.disk.load=0.00 sig.stat.disk.load=0.00 sum.stat.disk.readratemb=0 sum.stat.disk.writeratemb=0 sum.stat.net.ethratemib=119 sum.stat.net.inratemib=0 sum.stat.net.outratemib=0 sum.stat.ropen=0 sum.stat.wopen=0 sum.stat.statfs.freebytes=237439672320 sum.stat.statfs.usedbytes=10247208960 sum.stat.statfs.capacity=247686881280 sum.stat.usedfiles=0 sum.stat.statfs.ffree=241621608 sum.stat.statfs.fused=1549369344 sum.stat.statfs.files=241999872 sum.stat.balancer.running=0 stat.gw.queued=  cfg.stat.sys.vsize=350724096 cfg.stat.sys.rss=45793280 cfg.stat.sys.threads=54 cfg.stat.sys.sockets=8 cfg.stat.sys.eos.version=4.1.11-1 cfg.stat.sys.kernel=3.10.0-327.36.3.el7.x86_64 cfg.stat.sys.eos.start=Mon Dec  5 10:17:12 2016 cfg.stat.sys.uptime= 21:54:52 up 12:20,  1 user,  load average: 0.00, 0.01, 0.05 sum.stat.disk.iops?configstatus@rw=1122 sum.stat.disk.bw?configstatus@rw=634 cfg.stat.geotag=Serbia cfg.gw.rate=120 cfg.gw.ntx=10\n"
                        "type=nodesview hostport=fst2.localdomain:1095 status=offline cfg.status=on cfg.txgw=off heartbeatdelta=3 nofs=6 avg.stat.disk.load=0.00 sig.stat.disk.load=0.00 sum.stat.disk.readratemb=0 sum.stat.disk.writeratemb=0 sum.stat.net.ethratemib=119 sum.stat.net.inratemib=0 sum.stat.net.outratemib=0 sum.stat.ropen=0 sum.stat.wopen=0 sum.stat.statfs.freebytes=237439672320 sum.stat.statfs.usedbytes=10247208960 sum.stat.statfs.capacity=247686881280 sum.stat.usedfiles=0 sum.stat.statfs.ffree=241621608 sum.stat.statfs.fused=1549369344 sum.stat.statfs.files=241999872 sum.stat.balancer.running=0 stat.gw.queued=  cfg.stat.sys.vsize=350724096 cfg.stat.sys.rss=45793280 cfg.stat.sys.threads=54 cfg.stat.sys.sockets=8 cfg.stat.sys.eos.version=4.1.11-1 cfg.stat.sys.kernel=3.10.0-327.36.3.el7.x86_64 cfg.stat.sys.eos.start=Mon Dec  5 10:17:12 2016 cfg.stat.sys.uptime= 21:54:52 up 12:20,  1 user,  load average: 0.00, 0.01, 0.05 sum.stat.disk.iops?configstatus@rw=1122 sum.stat.disk.bw?configstatus@rw=634 cfg.stat.geotag=Serbia cfg.gw.rate=120 cfg.gw.ntx=10\n"
                        "type=nodesview hostport=fst3.localdomain:1095 status=offline cfg.status=on cfg.txgw=off heartbeatdelta=3 nofs=6 avg.stat.disk.load=0.00 sig.stat.disk.load=0.00 sum.stat.disk.readratemb=0 sum.stat.disk.writeratemb=0 sum.stat.net.ethratemib=119 sum.stat.net.inratemib=0 sum.stat.net.outratemib=0 sum.stat.ropen=0 sum.stat.wopen=0 sum.stat.statfs.freebytes=237439672320 sum.stat.statfs.usedbytes=10247208960 sum.stat.statfs.capacity=247686881280 sum.stat.usedfiles=0 sum.stat.statfs.ffree=241621608 sum.stat.statfs.fused=1549369344 sum.stat.statfs.files=241999872 sum.stat.balancer.running=0 stat.gw.queued=  cfg.stat.sys.vsize=350724096 cfg.stat.sys.rss=45793280 cfg.stat.sys.threads=54 cfg.stat.sys.sockets=8 cfg.stat.sys.eos.version=4.1.11-1 cfg.stat.sys.kernel=3.10.0-327.36.3.el7.x86_64 cfg.stat.sys.eos.start=Mon Dec  5 10:17:12 2016 cfg.stat.sys.uptime= 21:54:52 up 12:20,  1 user,  load average: 0.00, 0.01, 0.05 sum.stat.disk.iops?configstatus@rw=1122 sum.stat.disk.bw?configstatus@rw=634 cfg.stat.geotag=Serbia cfg.gw.rate=120 cfg.gw.ntx=10\n"
                        "type=nodesview hostport=fst4.localdomain:1095 status=offline cfg.status=on cfg.txgw=off heartbeatdelta=3 nofs=6 avg.stat.disk.load=0.00 sig.stat.disk.load=0.00 sum.stat.disk.readratemb=0 sum.stat.disk.writeratemb=0 sum.stat.net.ethratemib=119 sum.stat.net.inratemib=0 sum.stat.net.outratemib=0 sum.stat.ropen=0 sum.stat.wopen=0 sum.stat.statfs.freebytes=237439672320 sum.stat.statfs.usedbytes=10247208960 sum.stat.statfs.capacity=247686881280 sum.stat.usedfiles=0 sum.stat.statfs.ffree=241621608 sum.stat.statfs.fused=1549369344 sum.stat.statfs.files=241999872 sum.stat.balancer.running=0 stat.gw.queued=  cfg.stat.sys.vsize=350724096 cfg.stat.sys.rss=45793280 cfg.stat.sys.threads=54 cfg.stat.sys.sockets=8 cfg.stat.sys.eos.version=4.1.11-1 cfg.stat.sys.kernel=3.10.0-327.36.3.el7.x86_64 cfg.stat.sys.eos.start=Mon Dec  5 10:17:12 2016 cfg.stat.sys.uptime= 21:54:52 up 12:20,  1 user,  load average: 0.00, 0.01, 0.05 sum.stat.disk.iops?configstatus@rw=1122 sum.stat.disk.bw?configstatus@rw=634 cfg.stat.geotag=Serbia cfg.gw.rate=120 cfg.gw.ntx=10\n"));
    m_mexecs["bad_nodes"] = temp;
  }
};

#endif
