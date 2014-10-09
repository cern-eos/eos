// ----------------------------------------------------------------------
// File: FsView.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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
#include "mgm/FsView.hh"
#include "common/StringConversion.hh"
#include "XrdSys/XrdSysTimer.hh"

/*----------------------------------------------------------------------------*/
#include <math.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
FsView FsView::gFsView;
std::string FsSpace::gConfigQueuePrefix;
std::string FsGroup::gConfigQueuePrefix;
std::string FsNode::gConfigQueuePrefix;
std::string FsNode::gManagerId;

bool FsSpace::gDisableDefaults = false;

#ifndef EOSMGMFSVIEWTEST
ConfigEngine* FsView::ConfEngine = 0;
#endif

/*----------------------------------------------------------------------------*/
std::string
/*----------------------------------------------------------------------------*/
/**
 * @brief return's the printout format for a given option
 * @param option see the implementation for valid options
 * @return std;:string with format line passed to the printout routine
 */
/*----------------------------------------------------------------------------*/
FsView::GetNodeFormat (std::string option)
{

  if (option == "m")
  {
    // monitoring format
    return "member=type:width=1:format=os|sep= |member=hostport:width=1:format=os|sep= |member=status:width=1:format=os|sep= |member=cfg.status:width=1:format=os|sep= |member=cfg.txgw:width=1:format=os|sep= |member=heartbeatdelta:width=1:format=os|sep= |member=nofs:width=1:format=ol|sep= |avg=stat.disk.load:width=1:format=of|sep= |sig=stat.disk.load:width=1:format=of|sep= |sum=stat.disk.readratemb:width=1:format=ol|sep= |sum=stat.disk.writeratemb:width=1:format=ol|sep= |sum=stat.net.ethratemib:width=1:format=ol|sep= |sum=stat.net.inratemib:width=1:format=ol|sep= |sum=stat.net.outratemib:width=1:format=ol|sep= |sum=stat.ropen:width=1:format=ol|sep= |sum=stat.wopen:width=1:format=ol|sep= |sum=stat.statfs.freebytes:width=1:format=ol|sep= |sum=stat.statfs.usedbytes:width=1:format=ol|sep= |sum=stat.statfs.capacity:width=1:format=ol|sep= |sum=stat.usedfiles:width=1:format=ol|sep= |sum=stat.statfs.ffree:width=1:format=ol|sep= |sum=stat.statfs.fused:width=1:format=ol|sep= |sum=stat.statfs.files:width=1:format=ol|sep= |sum=stat.balancer.running:width=1:format=ol:tag=stat.balancer.running|sep= |sum=stat.drainer.running:width=1:format=ol:tag=stat.drainer.running|sep= |member=stat.gw.queued:width=1:format=os:tag=stat.gw.queued|sep= |member=cfg.stat.sys.vsize:width=1:format=ol|sep= |member=cfg.stat.sys.rss:width=1:format=olsep= |member=cfg.stat.sys.threads:width=1:format=ol|sep= |member=cfg.stat.sys.sockets:width=1:format=os|sep= |member=cfg.stat.sys.eos.version:width=1:format=os|sep= |member=cfg.stat.sys.kernel:width=1:format=os|sep= |member=cfg.stat.sys.eos.start:width=1:format=os|sep= |member=cfg.stat.sys.uptime:width=1:format=os|sep= |sum=stat.disk.iops?configstatsu@rw:width=1:format=ol|sep= |sum=stat.disk.bw?configstatus@rw:width=1:format=ol|sep= |member=cfg.stat.geotag:width=1:format=os|";
  }

  if (option == "io")
  {
    // io format
    return "header=1:member=hostport:width=32:format=s|sep= |member=cfg.stat.geotag:width=16:format=s|sep= |avg=stat.disk.load:width=10:format=f:tag=diskload|sep= |sum=stat.disk.readratemb:width=12:format=+l:tag=diskr-MB/s|sep= |sum=stat.disk.writeratemb:width=12:format=+l:tag=diskw-MB/s|sep= |sum=stat.net.ethratemib:width=10:format=l:tag=eth-MiB/s|sep= |sum=stat.net.inratemib:width=10:format=l:tag=ethi-MiB|sep= |sum=stat.net.outratemib:width=10:format=l:tag=etho-MiB|sep= |sum=stat.ropen:width=6:format=l:tag=ropen|sep= |sum=stat.wopen:width=6:format=l:tag=wopen|sep= |sum=stat.statfs.usedbytes:width=12:format=+l:unit=B:tag=used-bytes|sep= |sum=stat.statfs.capacity:width=12:format=+l:unit=B:tag=max-bytes|sep= |sum=stat.usedfiles:width=12:format=+l:tag=used-files|sep= |sum=stat.statfs.files:width=11:format=+l:tag=max-files|sep= |sum=stat.balancer.running:width=10:format=l:tag=bal-shd|sep= |sum=stat.drainer.running:width=10:format=l:tag=drain-shd|sep= |member=inqueue:width=10:format=s:tag=gw-queue|sep= |sum=stat.disk.iops?configstatus@rw:width=6:format=l:tag=iops|sep= |sum=stat.disk.bw?configstatus@rw:width=9:format=+l:unit=MB:tag=bw|";
  }

  if (option == "sys")
  {
    // system format
    return "header=1:member=hostport:width=32:format=s|sep= |member=cfg.stat.geotag:width=16:format=s|sep= |member=cfg.stat.sys.vsize:width=12:format=+l|tag=vsize|sep= |member=cfg.stat.sys.rss:width=12:format=+l:tag=rss|sep= |member=cfg.stat.sys.threads:width=12:format=+l:tag=threads|sep= |member=cfg.stat.sys.sockets:width=10:format=s:tag=sockets|sep= |member=cfg.stat.sys.eos.version:width=12:format=s:tag=eos|sep= |member=cfg.stat.sys.kernel:width=30:format=s:tag=kernel version|sep= |member=cfg.stat.sys.eos.start:width=32:format=s:tag=start|sep= |member=cfg.stat.sys.uptime:width=80:format=s:tag=uptime";
  }

  if (option == "fsck")
  {
    // fsck format
    return "header=1:member=hostport:width=32:format=s|sep= |sum=stat.fsck.mem_n:width=8:format=l:tag=n(mem)|sep= |sum=stat.fsck.d_sync_n:width=8:format=l:tag=n(disk)|sep= |sum=stat.fsck.m_sync_n:width=8:format=l:tag=n(mgm)|sep= |sum=stat.fsck.orphans_n:width=12:format=l:tag=e(orph)|sep= |sum=stat.fsck.unreg_n:width=12:format=l:tag=e(unreg)|sep= |sum=stat.fsck.rep_diff_n:width=12:format=l:tag=e(layout)|sep= |sum=stat.fsck.rep_missing_n:width=12:format=l:tag=e(miss)|sep= |sum=stat.fsck.d_mem_sz_diff:width=12:format=l:tag=e(disksize)|sep= |sum=stat.fsck.m_mem_sz_diff:width=12:format=l:tag=e(mgmsize)|sep= |sum=stat.fsck.d_cx_diff:width=12:format=l:tag=e(disk-cx)|sep= |sum=stat.fsck.m_cx_diff:width=12:format=l:tag=e(mgm-cx)";
  }

  if (option == "l")
  {
    // long format
    return "header=1:member=type:width=10:format=-s|sep= |member=hostport:width=32:format=s|sep= |member=cfg.stat.geotag:width=16:format=s|sep= |member=status:width=10:format=s|sep= |member=cfg.status:width=12:format=s|sep= |member=cfg.txgw:width=6:format=s|sep= |member=heartbeatdelta:width=16:format=s|sep= |member=nofs:width=5:format=s|sep= |sum=stat.balancer.running:width=10:format=l:tag=balan-shd|sep= |sum=stat.drainer.running:width=10:format=l:tag=drain-shd|sep= |member=inqueue:width=10:format=s:tag=gw-queue";
  }
  // default format
  return "header=1:member=type:width=10:format=-s|sep= |member=hostport:width=32:format=s|sep= |member=cfg.stat.geotag:width=16:format=s|sep= |member=status:width=10:format=s|sep= |member=cfg.status:width=12:format=s|sep= |member=cfg.txgw:width=6:format=s|sep= |member=inqueue:width=10:format=s:tag=gw-queued|sep= |member=cfg.gw.ntx:width=8:format=s:tag=gw-ntx|sep= |member=cfg.gw.rate:width=8:format=s:tag=gw-rate|sep= |member=heartbeatdelta:width=16:format=s|sep= |member=nofs:width=5:format=s";
}

/*----------------------------------------------------------------------------*/
std::string
FsView::GetFileSystemFormat (std::string option)
/*----------------------------------------------------------------------------*/
/**
 * @brief return's the printout format for a given option
 * @param option see the implementation for valid options
 * @return std;:string with format line passed to the printout routine
 */
/*----------------------------------------------------------------------------*/
{

  if (option == "m")
  {
    // monitoring format
    return "key=host:width=1:format=os|sep= |key=port:width=1:format=os|sep= |key=id:width=1:format=os|sep= |key=uuid:width=1:format=os|sep= |key=path:width=1:format=os|sep= |key=schedgroup:width=1:format=os|sep= |key=stat.boot:width=1:format=os|sep= |key=configstatus:width=1:format=os|sep= |key=headroom:width=1:format=os|sep= |key=stat.errc:width=1:format=os|sep= |key=stat.errmsg:width=1:format=oqs|sep= |key=stat.disk.load:width=1:format=of|sep= |key=stat.disk.readratemb:width=1:format=ol|sep= |key=stat.disk.writeratemb:width=1:format=ol|sep= |key=stat.net.ethratemib:width=1:format=ol|sep= |key=stat.net.inratemib:width=1:format=ol|sep= |key=stat.net.outratemib:width=1:format=ol|sep= |key=stat.ropen:width=1:format=ol|sep= |key=stat.wopen:width=1:format=ol|sep= |key=stat.statfs.freebytes:width=1:format=ol|sep= |key=stat.statfs.usedbytes:width=1:format=ol|sep= |key=stat.statfs.capacity:width=1:format=ol|sep= |key=stat.usedfiles:width=1:format=ol|sep= |key=stat.statfs.ffree:width=1:format=ol|sep= |key=stat.statfs.fused:width=1:format=ol|sep= |key=stat.statfs.files:width=1:format=ol|sep= |key=stat.drain:width=1:format=os|sep= |key=stat.drainprogress:width=1:format=ol:tag=progress|sep= |key=stat.drainfiles:width=1:format=ol|sep= |key=stat.drainbytesleft:width=1:format=ol|sep= |key=stat.drainretry:width=1:format=ol|sep= |key=graceperiod:width=1:format=ol|sep= |key=stat.timeleft:width=1:format=ol|sep= |key=stat.active:width=1:format=os|sep= |key=scaninterval:width=1:format=os|sep= |key=stat.balancer.running:width=1:format=ol:tag=stat.balancer.running|sep= |key=stat.drainer.running:width=1:format=ol:tag=stat.drainer.running|sep= |key=stat.disk.iops:width=1:format=ol|sep= |key=stat.disk.bw:width=1:format=of|sep= |key=stat.geotag:width=1:format=os";
  }

  if (option == "io")
  {
    // io format
    return "header=1:key=hostport:width=32:format=s|sep= |key=id:width=5:format=s|sep= |key=schedgroup:width=16:format=s|sep= |key=stat.geotag:width=16:format=s|sep= |key=stat.disk.load:width=10:format=f:tag=diskload|sep= |key=stat.disk.readratemb:width=12:format=+l:tag=diskr-MB/s|sep= |key=stat.disk.writeratemb:width=12:format=+l:tag=diskw-MB/s|sep= |key=stat.net.ethratemib:width=10:format=l:tag=eth-MiB/s|sep= |key=stat.net.inratemib:width=10:format=l:tag=ethi-MiB|sep= |key=stat.net.outratemib:width=10:format=l:tag=etho-MiB|sep= |key=stat.ropen:width=6:format=l:tag=ropen|sep= |key=stat.wopen:width=6:format=l:tag=wopen|sep= |key=stat.statfs.usedbytes:width=12:format=+l:unit=B:tag=used-bytes|sep= |key=stat.statfs.capacity:width=12:format=+l:unit=B:tag=max-bytes|sep= |key=stat.usedfiles:width=12:format=+l:tag=used-files|sep= |key=stat.statfs.files:width=11:format=+l:tag=max-files|sep= |key=stat.balancer.running:width=10:format=l:tag=bal-shd|sep= |key=stat.drainer.running:width=14:format=l:tag=drain-shd|key=stat.drainer:width=12:format=s:tag=drainpull|sep= |key=stat.disk.iops:width=6:format=l:tag=iops|sep= |key=stat.disk.bw:width=9:format=+l:unit=MB:tag=bw";
  }

  if (option == "fsck")
  {
    // fsck format
    return "header=1:key=hostport:width=32:format=s|sep= |key=id:width=6:format=s|sep= |key=stat.fsck.mem_n:width=8:format=l:tag=n(mem)|sep= |key=stat.fsck.d_sync_n:width=8:format=l:tag=n(disk)|sep= |key=stat.fsck.m_sync_n:width=8:format=l:tag=n(mgm)|sep= |key=stat.fsck.orphans_n:width=12:format=l:tag=e(orph)|sep= |key=stat.fsck.unreg_n:width=12:format=l:tag=e(unreg)|sep= |key=stat.fsck.rep_diff_n:width=12:format=l:tag=e(layout)|sep= |key=stat.fsck.rep_missing_n:width=12:format=l:tag=e(miss)|sep= |key=stat.fsck.d_mem_sz_diff:width=12:format=l:tag=e(disksize)|sep= |key=stat.fsck.m_mem_sz_diff:width=12:format=l:tag=e(mgmsize)|sep= |key=stat.fsck.d_cx_diff:width=12:format=l:tag=e(disk-cx)|sep= |key=stat.fsck.m_cx_diff:width=12:format=l:tag=e(mgm-cx)";
  }

  if (option == "d")
  {
    // drain format
    return "header=1:key=host:width=24:format=s:condition=stat.drain=!nodrain|sep= (|key=port:width=4:format=-s|sep=) |key=id:width=6:format=s|sep= |key=path:width=16:format=s|sep= |key=stat.drain:width=12:format=s|sep= |key=stat.drainprogress:width=12:format=l:tag=progress|sep= |key=stat.drainfiles:width=12:format=+l:tag=files|sep= |key=stat.drainbytesleft:width=12:format=+l:tag=bytes-left:unit=B|sep= |key=stat.timeleft:width=11:format=l:tag=timeleft|sep= |key=stat.drainretry:width=6:format=l:tag=retry|sep= |key=stat.wopen:width=6:format=l:tag=wopen";

  }
  if (option == "l")
  {
    // long format
    return "header=1:key=host:width=24:format=-s|sep= |key=port:width=5:format=s|sep= |key=id:width=6:format=s|sep= |key=uuid:width=36:format=s|sep= |key=path:width=16:format=s|sep= |key=schedgroup:width=16:format=s|sep= |key=headroom:width=10:format=+l|sep= |key=stat.boot:width=12:format=s|sep= |key=configstatus:width=14:format=s|sep= |key=stat.drain:width=12:format=s|sep= |key=stat.active:width=8:format=s|key=scaninterval:width=14:format=s";
  }

  if (option == "e")
  {
    // error format
    return "header=1:key=host:width=24:format=s:condition=stat.errc=!0|sep= |key=id:width=6:format=s|sep= |key=path:width=10:format=s|sep= |key=stat.boot:width=12:format=s|sep= |key=configstatus:width=14:format=s|sep= |key=stat.drain:width=12:format=s|sep= |key=stat.errc:width=3:format=s|sep= |key=stat.errmsg:width=0:format=s";
  }
  // default format
  return "header=1:key=host:width=24:format=s|sep= (|key=port:width=4:format=-s|sep=) |key=id:width=6:format=s|sep= |key=path:width=16:format=s|sep= |key=schedgroup:width=16:format=s|sep= |key=stat.geotag:width=16:format=s|sep= |key=stat.boot:width=12:format=s|sep= |key=configstatus:width=14:format=s|sep= |key=stat.drain:width=12:format=s|sep= |key=stat.active:width=8:format=s";
}

/*----------------------------------------------------------------------------*/
std::string
FsView::GetSpaceFormat (std::string option)
/*----------------------------------------------------------------------------*/
/**
 * @brief return's the printout format for a given option
 * @param option see the implementation for valid options
 * @return std;:string with format line passed to the printout routine
 */
/*----------------------------------------------------------------------------*/
{
  if (option == "m")
  {
    // monitoring format
    return "member=type:width=1:format=os|sep= |member=name:width=1:format=os|sep= |member=cfg.groupsize:width=1:format=ol|sep= |member=cfg.groupmod:width=1:format=ol|sep= |member=nofs:width=1:format=ol|sep= |member=cfg.quota:width=1|sep= |avg=stat.disk.load:width=1:format=of|sep= |sig=stat.disk.load:width=1:format=of|sep= |sum=stat.disk.readratemb:width=1:format=ol|sep= |sum=stat.disk.writeratemb:width=1:format=ol|sep= |sum=stat.net.ethratemib:width=1:format=ol|sep= |sum=stat.net.inratemib:width=1:format=ol|sep= |sum=stat.net.outratemib:width=1:format=ol|sep= |sum=stat.ropen:width=1:format=ol|sep= |sum=stat.wopen:width=1:format=ol|sep= |sum=stat.statfs.usedbytes:width=1:format=ol|sep= |sum=stat.statfs.freebytes:width=1:format=ol|sep= |sum=stat.statfs.capacity:width=1:format=ol|sep= |sum=stat.usedfiles:width=1:format=ol|sep= |sum=stat.statfs.ffiles:width=1:format=ol|sep= |sum=stat.statfs.files:width=1:format=ol|sep= |sum=stat.statfs.capacity?configstatus@rw:width=1:format=ol|sep= |sum=<n>?configstatus@rw:width=1:format=ol|sep= |member=cfg.quota:width=1:format=os|sep= |member=cfg.nominalsize:width=1:format=ol|sep= |member=cfg.balancer:width=1:format=os|sep= |member=cfg.balancer.threshold:width=1:format=ol|sep= |sum=stat.balancer.running:width=1:format=ol:tag=stat.balancer.running|sep= |sum=stat.drainer.running:width=1:format=ol:tag=stat.drainer.running|sep= |sum=stat.disk.iops?configstatus@rw:width=1:format=ol|sep= |sum=stat.disk.bw?configstatus@rw:width=1:format=ol";
  }

  if (option == "io")
  {
    return "header=1:member=name:width=10:format=s|sep= |avg=stat.disk.load:width=10:format=f:tag=diskload|sep= |sum=stat.disk.readratemb:width=12:format=+l:tag=diskr-MB/s|sep= |sum=stat.disk.writeratemb:width=12:format=+l:tag=diskw-MB/s|sep= |sum=stat.net.ethratemib:width=10:format=l:tag=eth-MiB/s|sep= |sum=stat.net.inratemib:width=10:format=l:tag=ethi-MiB|sep= |sum=stat.net.outratemib:width=10:format=l:tag=etho-MiB|sep= |sum=stat.ropen:width=6:format=l:tag=ropen|sep= |sum=stat.wopen:width=6:format=l:tag=wopen|sep= |sum=stat.statfs.usedbytes:width=12:format=+l:unit=B:tag=used-bytes|sep= |sum=stat.statfs.capacity:width=12:format=+l:unit=B:tag=max-bytes|sep= |sum=stat.usedfiles:width=12:format=+l:tag=used-files|sep= |sum=stat.statfs.files:width=11:format=+l:tag=max-files|sep= |sum=stat.balancer.running:width=10:format=l:tag=bal-shd|sep= |sum=stat.drainer.running:width=10:format=l:tag=drain-shd|sep= |sum=stat.disk.iops?configstatus@rw:width=6:format=l:tag=iops|sep= |sum=stat.disk.bw?configstatus@rw:width=9:format=+l:unit=MB:tag=bw";
  }

  if (option == "fsck")
  {
    return "header=1:member=name:width=10:format=s|sep= |sum=stat.fsck.mem_n:width=8:format=l:tag=n(mem)|sep= |sum=stat.fsck.d_sync_n:width=8:format=l:tag=n(disk)|sep= |sum=stat.fsck.m_sync_n:width=8:format=l:tag=n(mgm)|sep= |sum=stat.fsck.orphans_n:width=12:format=l:tag=e(orph)|sep= |sum=stat.fsck.unreg_n:width=12:format=l:tag=e(unreg)|sep= |sum=stat.fsck.rep_diff_n:width=12:format=l:tag=e(layout)|sep= |sum=stat.fsck.rep_missing_n:width=12:format=l:tag=e(miss)|sep= |sum=stat.fsck.d_mem_sz_diff:width=12:format=l:tag=e(disksize)|sep= |sum=stat.fsck.m_mem_sz_diff:width=12:format=l:tag=e(mgmsize)|sep= |sum=stat.fsck.d_cx_diff:width=12:format=l:tag=e(disk-cx)|sep= |sum=stat.fsck.m_cx_diff:width=12:format=l:tag=e(mgm-cx)";
  }

  if (option == "l")
  {
    // long output formag
    return "header=1:member=type:width=10:format=-s|sep= |member=name:width=16:format=s|sep= |member=cfg.groupsize:width=12:format=s|sep= |member=cfg.groupmod:width=12:format=s|sep= |member=nofs:width=6:format=s:tag=N(fs)|sep= |sum=<n>?configstatus@rw:width=9:format=l:tag=N(fs-rw)|sep= |sum=stat.statfs.usedbytes:width=15:format=+l|sep= |sum=stat.statfs.capacity:width=14:format=+l|sep= |sum=stat.statfs.capacity?configstatus@rw:width=13:format=+l:tag=capacity(rw)|sep= |member=cfg.nominalsize:width=13:format=+l:tag=nom.capacity|sep= |member=cfg.quota:width=6:format=s";
  }

  return "header=1:member=type:width=10:format=-s|sep= |member=name:width=16:format=s|sep= |member=cfg.groupsize:width=12:format=s|sep= |member=cfg.groupmod:width=12:format=s|sep= |member=nofs:width=6:format=s:tag=N(fs)|sep= |sum=<n>?configstatus@rw:width=9:format=l:tag=N(fs-rw)|sep= |sum=stat.statfs.usedbytes:width=15:format=+l|sep= |sum=stat.statfs.capacity:width=14:format=+l|sep= |sum=stat.statfs.capacity?configstatus@rw:width=13:format=+l:tag=capacity(rw)|sep= |member=cfg.nominalsize:width=13:format=+l:tag=nom.capacity|sep= |member=cfg.quota:width=6:format=s|sep= |member=cfg.balancer:width=10:format=s:tag=balancing|sep= |member=cfg.balancer.threshold:width=11:format=+l:tag=threshold|sep= |member=cfg.converter:width=11:format=s:tag=converter|sep= |member=cfg.converter.ntx:width=6:format=+l:tag=ntx|sep= |member=cfg.stat.converter.active:width=8:format=+l:tag=active|sep= |member=cfg.groupbalancer:width=11:format=s:tag=intergroup|";
}

/*----------------------------------------------------------------------------*/
std::string
FsView::GetGroupFormat (std::string option)
/*----------------------------------------------------------------------------*/
/**
 * @brief return's the printout format for a given option
 * @param option see the implementation for valid options
 * @return std;:string with format line passed to the printout routine
 */
/*----------------------------------------------------------------------------*/
{
  if (option == "m")
  {
    // monitoring format
    return "member=type:width=1:format=os|sep= |member=name:width=1:format=os|sep= |member=nofs:width=1:format=os|sep= |avg=stat.disk.load:width=1:format=of|sep= |sig=stat.disk.load:width=1:format=of|sep= |sum=stat.disk.readratemb:width=1:format=ol|sep= |sum=stat.disk.writeratemb:width=1:format=ol|sep= |sum=stat.net.ethratemib:width=1:format=ol|sep= |sum=stat.net.inratemib:width=1:format=ol|sep= |sum=stat.net.outratemib:width=1:format=ol|sep= |sum=stat.ropen:width=1:format=ol|sep= |sum=stat.wopen:width=1:format=ol|sep= |sum=stat.statfs.usedbytes:width=1:format=ol|sep= |sum=stat.statfs.freebytes:width=1:format=ol|sep= |sum=stat.statfs.capacity:width=1:format=ol|sep= |sum=stat.usedfiles:width=1:format=ol|sep= |sum=stat.statfs.ffree:width=1:format=ol|sep= |sum=stat.statfs.files:width=1:format=ol|sep= |maxdev=stat.statfs.filled:width=1:format=of|sep= |avg=stat.statfs.filled:width=1:format=of|sep= |sig=stat.statfs.filled:width=1:format=of|sep= |member=cfg.stat.balancing:width=1:format=os:tag=stat.balancing|sep= |sum=stat.balancer.running:width=1:format=ol:tag=stat.balancer.running|sep= |sum=stat.drainer.running:width=1:format=ol:tag=stat.drainer.running";
  }

  if (option == "io")
  {
    // io format
    return "header=1:member=name:width=16:format=-s|sep= |avg=stat.disk.load:width=10:format=f:tag=diskload|sep= |sum=stat.disk.readratemb:width=12:format=+l:tag=diskr-MB/s|sep= |sum=stat.disk.writeratemb:width=12:format=+l:tag=diskw-MB/s|sep= |sum=stat.net.ethratemib:width=10:format=l:tag=eth-MiB/s|sep= |sum=stat.net.inratemib:width=10:format=l:tag=ethi-MiB|sep= |sum=stat.net.outratemib:width=10:format=l:tag=etho-MiB|sep= |sum=stat.ropen:width=6:format=l:tag=ropen|sep= |sum=stat.wopen:width=6:format=l:tag=wopen|sep= |sum=stat.statfs.usedbytes:width=12:format=+l:unit=B:tag=used-bytes|sep= |sum=stat.statfs.capacity:width=12:format=+l:unit=B:tag=max-bytes|sep= |sum=stat.usedfiles:width=12:format=+l:tag=used-files|sep= |sum=stat.statfs.files:width=11:format=+l:tag=max-files|sep= |sum=stat.balancer.running:width=10:format=l:tag=bal-shd|sep= |sum=stat.drainer.running:width=10:format=l:tag=drain-shd";
  }

  if (option == "l")
  {
    // long format
    return "header=1:member=type:width=10:format=-s|sep= |member=name:width=16:format=s|sep= |member=cfg.status:width=12:format=s|sep= |key=stat.geotag:width=16:format=s|sep= |member=nofs:width=5:format=s";
  }
  // default format
  return "header=1:member=type:width=10:format=-s|sep= |member=name:width=16:format=-s|sep= |member=cfg.status:width=12:format=s|sep= |member=nofs:width=5:format=s|sep= |maxdev=stat.statfs.filled:width=12:format=f:unit=p|sep= |avg=stat.statfs.filled:width=12:format=f:unit=p|sep= |sig=stat.statfs.filled:width=12:format=f:unit=p|sep= |member=cfg.stat.balancing:width=10:format=-s|sep= |sum=stat.balancer.running:width=10:format=l:tag=bal-shd|sep= |sum=stat.drainer.running:width=10:format=l:tag=drain-shd";
}

/*----------------------------------------------------------------------------*/
bool
FsView::Register (FileSystem* fs)
/*----------------------------------------------------------------------------*/
/**
 * @brief register a filesystem object in the filesystem view
 * @param fs filesystem to register
 * @return true if done, otherwise false
 */
/*----------------------------------------------------------------------------*/
{
  if (!fs)
    return false;

  // create a snapshot of the current variables of the fs
  eos::common::FileSystem::fs_snapshot snapshot;

  if (fs->SnapShotFileSystem(snapshot))
  {
    //--------------------------------------------------------------------------
    // align view by filesystem object and filesystem id
    //--------------------------------------------------------------------------

    // check if there is already a filesystem with the same path on the same node

    if (mNodeView.count(snapshot.mQueue))
    {
      // loop over all attached filesystems and compare the queue path
      std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
      for (it = mNodeView[snapshot.mQueue]->begin(); it != mNodeView[snapshot.mQueue]->end(); it++)
      {
        if (FsView::gFsView.mIdView[*it]->GetQueuePath() == snapshot.mQueuePath)
        {
          // this queue path was already existing, we cannot register
          return false;
        }
      }
    }

    // check if this is already in the view
    if (mFileSystemView.count(fs))
    {
      // this filesystem is already there, this might be an update
      eos::common::FileSystem::fsid_t fsid;
      fsid = mFileSystemView[fs];
      if (fsid != snapshot.mId)
      {
        // remove previous mapping
        mIdView.erase(fsid);
        // setup new two way mapping
        mFileSystemView[fs] = snapshot.mId;
        mIdView[snapshot.mId] = fs;
        eos_debug("updating mapping %u<=>%lld", snapshot.mId, fs);
      }
    }
    else
    {
      mFileSystemView[fs] = snapshot.mId;
      mIdView[snapshot.mId] = fs;
      eos_debug("registering mapping %u<=>%lld", snapshot.mId, fs);
    }

    //--------------------------------------------------------------------------
    // align view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst
    //--------------------------------------------------------------------------

    // check if we have already a node view
    if (mNodeView.count(snapshot.mQueue))
    {
      mNodeView[snapshot.mQueue]->insert(snapshot.mId);
      eos_debug("inserting into node view %s<=>%u", snapshot.mQueue.c_str(), snapshot.mId, fs);
    }
    else
    {
      FsNode* node = new FsNode(snapshot.mQueue.c_str());
      mNodeView[snapshot.mQueue] = node;
      node->insert(snapshot.mId);
      node->SetNodeConfigDefault();
      eos_debug("creating/inserting into node view %s<=>%u", snapshot.mQueue.c_str(), snapshot.mId, fs);
    }

    //--------------------------------------------------------------------------
    // align view by groupname
    //--------------------------------------------------------------------------

    // check if we have already a group view
    if (mGroupView.count(snapshot.mGroup))
    {
      mGroupView[snapshot.mGroup]->insert(snapshot.mId);
      eos_debug("inserting into group view %s<=>%u", snapshot.mGroup.c_str(), snapshot.mId, fs);
    }
    else
    {
      FsGroup* group = new FsGroup(snapshot.mGroup.c_str());
      mGroupView[snapshot.mGroup] = group;
      group->insert(snapshot.mId);
      group->mIndex = snapshot.mGroupIndex;
      eos_debug("creating/inserting into group view %s<=>%u", snapshot.mGroup.c_str(), snapshot.mId, fs);
    }


    mSpaceGroupView[snapshot.mSpace].insert(mGroupView[snapshot.mGroup]);

    //--------------------------------------------------------------------------
    // align view by spacename
    //--------------------------------------------------------------------------

    // check if we have already a space view
    if (mSpaceView.count(snapshot.mSpace))
    {
      mSpaceView[snapshot.mSpace]->insert(snapshot.mId);
      eos_debug("inserting into space view %s<=>%u %x", snapshot.mSpace.c_str(), snapshot.mId, fs);
    }
    else
    {
      FsSpace* space = new FsSpace(snapshot.mSpace.c_str());
      mSpaceView[snapshot.mSpace] = space;
      space->insert(snapshot.mId);
      eos_debug("creating/inserting into space view %s<=>%u %x", snapshot.mSpace.c_str(), snapshot.mId, fs);
    }
  }

  StoreFsConfig(fs);

  return true;
}

/*----------------------------------------------------------------------------*/
void
FsView::StoreFsConfig (FileSystem* fs)
/*----------------------------------------------------------------------------*/
/**
 * @brief Store the filesystem configuration in the configuration engine
 * @param fs filesystem object to store
 */
/*----------------------------------------------------------------------------*/
{
#ifndef EOSMGMFSVIEWTEST
  if (fs)
  {
    // -------------------------------------------------------------------------
    // register in the configuration engine
    // -------------------------------------------------------------------------
    std::string key;
    std::string val;
    fs->CreateConfig(key, val);
    if (FsView::ConfEngine)
      FsView::ConfEngine->SetConfigValue("fs", key.c_str(), val.c_str());
  }
#endif
  return;
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Move a filesystem in to a target group
 * @param fs filesystem object to move
 * @param group target group 
 * @return true if moved otherwise false
 */

/*----------------------------------------------------------------------------*/
bool
FsView::MoveGroup (FileSystem* fs, std::string group)
{
  if (!fs)
    return false;

  eos::common::FileSystem::fs_snapshot snapshot1;
  eos::common::FileSystem::fs_snapshot snapshot;
  if (fs->SnapShotFileSystem(snapshot1))
  {
#ifndef EOSMGMFSVIEWTEST
    fs->SetString("schedgroup", group.c_str());
#endif
    if (fs->SnapShotFileSystem(snapshot))
    {
      // remove from the original space
      if (mSpaceView.count(snapshot1.mSpace))
      {
        FsSpace* space = mSpaceView[snapshot1.mSpace];
        space->erase(snapshot1.mId);
        eos_debug("unregister space %s from space view",
                  space->GetMember("name").c_str());
        if (!space->size())
        {
          mSpaceView.erase(snapshot1.mSpace);
          delete space;
        }
      }

      // remove from the original group
      if (mGroupView.count(snapshot1.mGroup))
      {
        FsGroup* group = mGroupView[snapshot1.mGroup];
        group->erase(snapshot1.mId);
        eos_debug("unregister group %s from group view",
                  group->GetMember("name").c_str());
        if (!group->size())
        {
          if (mSpaceGroupView.count(snapshot1.mSpace))
          {
            mSpaceGroupView[snapshot1.mSpace].erase(mGroupView[snapshot1.mGroup]);
          }
          mGroupView.erase(snapshot1.mGroup);
          delete group;
        }
      }

      // check if we have already a group view
      if (mGroupView.count(snapshot.mGroup))
      {
        mGroupView[snapshot.mGroup]->insert(snapshot.mId);
        eos_debug("inserting into group view %s<=>%u",
                  snapshot.mGroup.c_str(), snapshot.mId, fs);
      }
      else
      {
        FsGroup* group = new FsGroup(snapshot.mGroup.c_str());
        mGroupView[snapshot.mGroup] = group;
        group->insert(snapshot.mId);
        group->mIndex = snapshot.mGroupIndex;
        eos_debug("creating/inserting into group view %s<=>%u",
                  snapshot.mGroup.c_str(), snapshot.mId, fs);
      }

      mSpaceGroupView[snapshot.mSpace].insert(mGroupView[snapshot.mGroup]);

      // check if we have already a space view
      if (mSpaceView.count(snapshot.mSpace))
      {
        mSpaceView[snapshot.mSpace]->insert(snapshot.mId);
        eos_debug("inserting into space view %s<=>%u %x",
                  snapshot.mSpace.c_str(), snapshot.mId, fs);
      }
      else
      {
        FsSpace* space = new FsSpace(snapshot.mSpace.c_str());
        mSpaceView[snapshot.mSpace] = space;
        space->insert(snapshot.mId);
        eos_debug("creating/inserting into space view %s<=>%u %x",
                  snapshot.mSpace.c_str(), snapshot.mId, fs);
      }

      StoreFsConfig(fs);

      return true;
    }
  }
  return false;
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Unregister a filesystem in the filesystem view
 * @param fs filesystem to unregister
 * @return true if done otherwise false
 */

/*----------------------------------------------------------------------------*/
bool
FsView::UnRegister (FileSystem* fs)
{
  if (!fs)
    return false;

#ifndef EOSMGMFSVIEWTEST
  // delete in the configuration engine
  std::string key = fs->GetQueuePath();

  if (FsView::ConfEngine)
    FsView::ConfEngine->DeleteConfigValue("fs", key.c_str());
#endif



  // create a snapshot of the current variables of the fs
  eos::common::FileSystem::fs_snapshot snapshot;

  if (fs->SnapShotFileSystem(snapshot))
  {
    //--------------------------------------------------------------------------
    // remove view by filesystem object and filesystem id
    //--------------------------------------------------------------------------

    // check if this is in the view
    if (mFileSystemView.count(fs))
    {
      mFileSystemView.erase(fs);
      mIdView.erase(snapshot.mId);
      eos_debug("unregister %lld from filesystem view", fs);
    }

    //----------------------------------------------------------------
    // remove fs from node view & evt. remove node view
    //----------------------------------------------------------------
    if (mNodeView.count(snapshot.mQueue))
    {
      FsNode* node = mNodeView[snapshot.mQueue];
      node->erase(snapshot.mId);
      eos_debug("unregister node %s from node view", node->GetMember("name").c_str());
      if (!node->size())
      {
        mNodeView.erase(snapshot.mQueue);
        delete node;
      }
    }

    //----------------------------------------------------------------
    //! remove fs from group view & evt. remove group view
    //----------------------------------------------------------------
    if (mGroupView.count(snapshot.mGroup))
    {
      FsGroup* group = mGroupView[snapshot.mGroup];
      group->erase(snapshot.mId);
      eos_debug("unregister group %s from group view", group->GetMember("name").c_str());
      if (!group->size())
      {
        mSpaceGroupView[snapshot.mSpace].erase(mGroupView[snapshot.mGroup]);
        mGroupView.erase(snapshot.mGroup);
        delete group;
      }
    }

    //----------------------------------------------------------------
    //! remove fs from space view & evt. remove space view
    //----------------------------------------------------------------
    if (mSpaceView.count(snapshot.mSpace))
    {
      FsSpace* space = mSpaceView[snapshot.mSpace];
      space->erase(snapshot.mId);
      eos_debug("unregister space %s from space view", space->GetMember("name").c_str());
      if (!space->size())
      {
        mSpaceView.erase(snapshot.mSpace);
        delete space;
      }
    }

    //----------------------------------------------------------------
    //! remove mapping
    //----------------------------------------------------------------
    RemoveMapping(snapshot.mId, snapshot.mUuid);


    //----------------------------------------------------------------
    //! delete fs object
    //----------------------------------------------------------------
    delete fs;

    return true;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
bool
FsView::ExistsQueue (std::string queue, std::string queuepath)
{
  //----------------------------------------------------------------
  //! checks if a node has already a filesystem registered 
  //----------------------------------------------------------------

  if (mNodeView.count(queue))
  {
    // loop over all attached filesystems and compare the queue path
    std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
    for (it = mNodeView[queue]->begin(); it != mNodeView[queue]->end(); it++)
    {
      if (FsView::gFsView.mIdView[*it]->GetQueuePath() == queuepath)
      {
        // this queue path was already existing, we cannot register
        return true;
      }
    }
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool
FsView::RegisterNode (const char* nodename)
{
  //----------------------------------------------------------------
  //! add view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------

  std::string nodequeue = nodename;

  // check if we have already a node view
  if (mNodeView.count(nodequeue))
  {
    eos_debug("node is existing");
    return false;
  }
  else
  {
    FsNode* node = new FsNode(nodequeue.c_str());
    mNodeView[nodequeue] = node;
    node->SetNodeConfigDefault();
    eos_debug("creating node view %s", nodequeue.c_str());
    return true;
  }
}

/*----------------------------------------------------------------------------*/
void
FsView::UnRegisterNodes ()
{
  //----------------------------------------------------------------
  //! remove all nodes
  //----------------------------------------------------------------
  std::map<std::string, FsNode* >::iterator it;
  for (it = mNodeView.begin(); it != mNodeView.end(); it++)
  {
    delete (it->second);
  }
}

/*----------------------------------------------------------------------------*/
bool
FsView::UnRegisterNode (const char* nodename)
{
  //----------------------------------------------------------------
  //! remove view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------

  // we have to remove all the connected filesystems via UnRegister(fs) to keep space, group, node view in sync
  std::map<std::string, FsNode* >::iterator it;
  bool retc = true;
  bool hasfs = false;
  if (mNodeView.count(nodename))
  {
    while (mNodeView.count(nodename) && (mNodeView[nodename]->begin() != mNodeView[nodename]->end()))
    {
      eos::common::FileSystem::fsid_t fsid = *(mNodeView[nodename]->begin());
      FileSystem* fs = mIdView[fsid];
      if (fs)
      {
        hasfs = true;
        eos_static_debug("Unregister filesystem fsid=%llu node=%s queue=%s", (unsigned long long) fsid, nodename, fs->GetQueue().c_str());
        retc |= UnRegister(fs);
      }
    }
    if (!hasfs)
    {
      // we have to explicitly remove the node from the view here because no fs was removed
      delete mNodeView[nodename];
      retc = (mNodeView.erase(nodename) ? true : false);
    }
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
bool
FsView::RegisterSpace (const char* spacename)
{
  //----------------------------------------------------------------
  //! add view by spacename (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------

  std::string spacequeue = spacename;

  // check if we have already a space view
  if (mSpaceView.count(spacequeue))
  {
    eos_debug("space is existing");
    return false;
  }
  else
  {
    FsSpace* space = new FsSpace(spacequeue.c_str());
    mSpaceView[spacequeue] = space;
    eos_debug("creating space view %s", spacequeue.c_str());
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool
FsView::UnRegisterSpace (const char* spacename)
{
  //----------------------------------------------------------------
  //! remove view by spacename (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------

  // we have to remove all the connected filesystems via UnRegister(fs) to keep space, group, space view in sync
  std::map<std::string, FsSpace* >::iterator it;
  bool retc = true;
  bool hasfs = false;
  if (mSpaceView.count(spacename))
  {
    while (mSpaceView.count(spacename) && (mSpaceView[spacename]->begin() != mSpaceView[spacename]->end()))
    {
      std::map<eos::common::FileSystem::fsid_t, FileSystem*>::iterator it;
      eos::common::FileSystem::fsid_t fsid = *(mSpaceView[spacename]->begin());
      FileSystem* fs = mIdView[fsid];
      if (fs)
      {
        hasfs = true;
        eos_static_debug("Unregister filesystem fsid=%llu space=%s queue=%s", (unsigned long long) fsid, spacename, fs->GetQueue().c_str());
        retc |= UnRegister(fs);
      }
    }
    if (!hasfs)
    {
      // we have to explicitly remove the space from the view here because no fs was removed
      delete mSpaceView[spacename];
      retc = (mSpaceView.erase(spacename) ? true : false);
    }
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
bool
FsView::RegisterGroup (const char* groupname)
{
  //----------------------------------------------------------------
  //! add view by groupname  e.g. default or default.0
  //----------------------------------------------------------------

  std::string groupqueue = groupname;

  // check if we have already a group view
  if (mGroupView.count(groupqueue))
  {
    eos_debug("group is existing");
    return false;
  }
  else
  {
    FsGroup* group = new FsGroup(groupqueue.c_str());
    mGroupView[groupqueue] = group;
    eos_debug("creating group view %s", groupqueue.c_str());
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool
FsView::UnRegisterGroup (const char* groupname)
{
  //----------------------------------------------------------------
  //! remove view by groupname e.g. default or default.0
  //----------------------------------------------------------------

  // we have to remove all the connected filesystems via UnRegister(fs) to keep group, group, group view in sync
  std::map<std::string, FsGroup* >::iterator it;
  bool retc = true;
  bool hasfs = false;
  if (mGroupView.count(groupname))
  {
    while (mGroupView.count(groupname) && (mGroupView[groupname]->begin() != mGroupView[groupname]->end()))
    {
      eos::common::FileSystem::fsid_t fsid = *(mGroupView[groupname]->begin());
      FileSystem* fs = mIdView[fsid];
      if (fs)
      {
        hasfs = true;
        eos_static_debug("Unregister filesystem fsid=%llu group=%s queue=%s", (unsigned long long) fsid, groupname, fs->GetQueue().c_str());
        retc |= UnRegister(fs);
      }
    }
    if (!hasfs)
    {
      std::string sgroupname = groupname;
      std::string spacename = "";
      std::string index = "";

      // remove the direct group reference here
      if (mSpaceGroupView.count(spacename))
      {
        mSpaceGroupView[spacename].erase(mGroupView[groupname]);
      }
      // we have to explicitly remove the group from the view here because no fs was removed
      delete mGroupView[groupname];
      retc = (mGroupView.erase(groupname) ? true : false);
      eos::common::StringConversion::SplitByPoint(groupname, spacename, index);
    }
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
void
FsView::Reset ()
{
  //----------------------------------------------------------------
  //! remove all filesystems by erasing all spaces
  //----------------------------------------------------------------

  std::map<std::string, FsSpace* >::iterator it;

  {
    eos::common::RWMutexReadLock viewlock(ViewMutex);
    // stop all the threads having only a read-lock
    for (auto it = mSpaceView.begin(); it != mSpaceView.end(); it++)
      it->second->Stop();
  }

  eos::common::RWMutexWriteLock viewlock(ViewMutex);
  while (mSpaceView.size())
  {
    UnRegisterSpace(mSpaceView.begin()->first.c_str());
  }

  //  UnRegisterNodes();

  eos::common::RWMutexWriteLock maplock(MapMutex);

  // remove all mappins
  Fs2UuidMap.clear();
  Uuid2FsMap.clear();

  SetNextFsId(0);

  // although this shouldn't be necessary, better run an additional cleanup
  mSpaceView.clear();
  mGroupView.clear();
  mNodeView.clear();
  {
    eos::common::RWMutexWriteLock gwlock(GwMutex);
    mGwNodes.clear();
  }
  mIdView.clear();
  mFileSystemView.clear();
}

/*----------------------------------------------------------------------------*/
void
FsView::SetNextFsId (eos::common::FileSystem::fsid_t fsid)
{
  //----------------------------------------------------------------
  //! stores the next fsid into the global config
  //----------------------------------------------------------------
  NextFsId = fsid;

  std::string key = "nextfsid";
  char value[1024];
  snprintf(value, sizeof (value) - 1, "%llu", (unsigned long long) fsid);
  std::string svalue = value;

#ifndef EOSMGMFSVIEWTEST
  if (!SetGlobalConfig(key, value))
  {
    eos_static_err("unable to set nextfsid in global config");
  }
#endif
}

/*----------------------------------------------------------------------------*/
FileSystem*
FsView::FindByQueuePath (std::string &queuepath)
{
  //----------------------------------------------------------------
  //! find a filesystem specifying a queuepath
  //----------------------------------------------------------------

  // needs an external ViewMutex lock !!!!
  std::map<eos::common::FileSystem::fsid_t, FileSystem*>::iterator it;
  for (it = mIdView.begin(); it != mIdView.end(); it++)
  {
    if (it->second->GetQueuePath() == queuepath)
      return it->second;
  }
  return 0;
}

#ifndef EOSMGMFSVIEWTEST

/*----------------------------------------------------------------------------*/
bool
FsView::SetGlobalConfig (std::string key, std::string value)
{
  // we need to store this in the shared hash between MGMs
  XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
  XrdMqSharedHash* hash = eos::common::GlobalConfig::gConfig.Get(MgmConfigQueueName.c_str());
  if (hash)
  {
    hash->Set(key, value);
  }
#ifndef EOSMGMFSVIEWTEST
  // register in the configuration engine
  std::string ckey = MgmConfigQueueName.c_str();
  ckey += "#";
  ckey += key;

  if (FsView::ConfEngine)
    FsView::ConfEngine->SetConfigValue("global", ckey.c_str(), value.c_str());
#endif
  return true;
}

/*----------------------------------------------------------------------------*/
std::string
FsView::GetGlobalConfig (std::string key)
{
  XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
  XrdMqSharedHash* hash = eos::common::GlobalConfig::gConfig.Get(MgmConfigQueueName.c_str());

  if (hash)
  {
    return hash->Get(key);
  }
  return "";
}

#endif

/*----------------------------------------------------------------------------*/
void*
FsView::StaticHeartBeatCheck (void* arg)
{
  //----------------------------------------------------------------
  //! static thread startup function calling HeartBeatCheck
  //----------------------------------------------------------------
  return reinterpret_cast<FsView*> (arg)->HeartBeatCheck();
}

/*----------------------------------------------------------------------------*/
void*
FsView::HeartBeatCheck ()
{
  //----------------------------------------------------------------
  //! heart beat checker set's filesystem to down if the heart beat is missing
  //----------------------------------------------------------------

  XrdSysThread::SetCancelOn();
  while (1)
  {
    {
      // quickly go through all heartbeats
      eos::common::RWMutexReadLock lock(ViewMutex);
      std::map<eos::common::FileSystem::fsid_t, FileSystem*>::const_iterator it;
      // iterator over all filesystems
      for (it = mIdView.begin(); it != mIdView.end(); it++)
      {
        if (!it->second)
          continue;
        eos::common::FileSystem::fs_snapshot_t snapshot;
        snapshot.mHeartBeatTime = (time_t) it->second->GetLongLong("stat.heartbeattime");

        if (!it->second->HasHeartBeat(snapshot))
        {
          // mark as offline
          if (it->second->GetActiveStatus() != eos::common::FileSystem::kOffline)
            it->second->SetActiveStatus(eos::common::FileSystem::kOffline);
        }
        else
        {
          std::string queue = it->second->GetString("queue");
          std::string group = it->second->GetString("schedgroup");

          if ((FsView::gFsView.mNodeView.count(queue)) &&
              (FsView::gFsView.mGroupView.count(group)) &&
              (FsView::gFsView.mNodeView[queue]->GetConfigMember("status") == "on") &&
              (FsView::gFsView.mGroupView[group]->GetConfigMember("status") == "on"))
          {
            if (it->second->GetActiveStatus() != eos::common::FileSystem::kOnline)
              it->second->SetActiveStatus(eos::common::FileSystem::kOnline);
          }
          else
          {
            if (it->second->GetActiveStatus() != eos::common::FileSystem::kOffline)
              it->second->SetActiveStatus(eos::common::FileSystem::kOffline);
          }
        }
      }
    }
    XrdSysTimer sleeper;
    sleeper.Snooze(10);
    XrdSysThread::CancelPoint();
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
std::string
BaseView::GetMember (std::string member)
{
  //----------------------------------------------------------------
  //! return a view member variable
  //----------------------------------------------------------------

  if (member == "name")
    return mName;
  if (member == "type")
    return mType;
  if (member == "nofs")
  {
    char line[1024];
    snprintf(line, sizeof (line) - 1, "%llu", (unsigned long long) size());
    mSize = line;
    return mSize;
  }

  if (member == "inqueue")
  {
    XrdOucString s = "";
    s += (int) mInQueue;
    return s.c_str();
  }

  if (member == "heartbeat")
  {
    char line[1024];
    snprintf(line, sizeof (line) - 1, "%llu", (unsigned long long) mHeartBeat);
    mHeartBeatString = line;
    return mHeartBeatString;
  }

  if (member == "heartbeatdelta")
  {
    char line[1024];
    if (labs(time(NULL) - mHeartBeat) > 86400)
    {
      snprintf(line, sizeof (line) - 1, "~");
    }
    else
    {
      snprintf(line, sizeof (line) - 1, "%llu", (unsigned long long) (time(NULL) - mHeartBeat));
    }
    mHeartBeatDeltaString = line;
    return mHeartBeatDeltaString;
  }

  if (member == "status")
  {
    return mStatus;
  }

  // return global config value
  std::string prefix = member;
  prefix.erase(4);
  if (prefix == "cfg.")
  {
    std::string val = "???";
    member.erase(0, 4);
    eos::common::GlobalConfig::gConfig.SOM()->HashMutex.LockRead();
    std::string nodeconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(GetConfigQueuePrefix(), mName.c_str());
    XrdMqSharedHash* hash = eos::common::GlobalConfig::gConfig.Get(nodeconfigname.c_str());
    if (hash)
    {
      val = hash->Get(member.c_str());
    }
    eos::common::GlobalConfig::gConfig.SOM()->HashMutex.UnLockRead();

    // it's otherwise hard to get the default into place
    if (((val == "") || (val == "???")) && (member == "stat.balancing"))
    {
      val = "idle";
    }

    return val;
  }

  return "";
}

/*----------------------------------------------------------------------------*/
FsNode::~FsNode ()
{
  if (mGwQueue) delete mGwQueue;
  FsView::gFsView.mGwNodes.erase(mName); // unregister evt. gateway node
};

/*----------------------------------------------------------------------------*/
std::string
FsNode::GetMember (std::string member)
{
  if (member == "hostport")
  {
    std::string hostport = eos::common::StringConversion::GetStringHostPortFromQueue(mName.c_str());
    return hostport;
  }
  else
  {
    return BaseView::GetMember(member);
  }
}

/*----------------------------------------------------------------------------*/
bool
BaseView::SetConfigMember (std::string key, std::string value, bool create, std::string broadcastqueue, bool isstatus)
{
  //----------------------------------------------------------------
  //! set a configuration member variable (stored in the config engine)
  //! - if 'isstatus'=true we just store the value in the shared hash but don't flush it into the configuration engine 
  //!   => is used to set status variables on config queues (baseview queues)
  //----------------------------------------------------------------

  bool success = false;
#ifndef EOSMGMFSVIEWTEST
  eos::common::GlobalConfig::gConfig.SOM()->HashMutex.LockRead();
  std::string nodeconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(GetConfigQueuePrefix(), mName.c_str());
  XrdMqSharedHash* hash = eos::common::GlobalConfig::gConfig.Get(nodeconfigname.c_str());
  if (!hash && create)
  {
    eos::common::GlobalConfig::gConfig.SOM()->HashMutex.UnLockRead();
    if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(nodeconfigname.c_str(), broadcastqueue.c_str()))
    {
      success = false;
    }
    eos::common::GlobalConfig::gConfig.SOM()->HashMutex.LockRead();
    hash = eos::common::GlobalConfig::gConfig.Get(nodeconfigname.c_str());
  }

  if (hash)
  {
    success = hash->Set(key, value);
    if (key == "txgw")
    {
      eos::common::RWMutexWriteLock gwlock(FsView::gFsView.GwMutex);
      if (value == "on")
      {
        // we have to register this queue into the gw set for fast lookups
        FsView::gFsView.mGwNodes.insert(broadcastqueue);
        // clear the queue if a machine is enabled
        FsView::gFsView.mNodeView[broadcastqueue]->mGwQueue->Clear();
      }
      else
      {
        FsView::gFsView.mGwNodes.erase(broadcastqueue);
      }
    }
  }

  eos::common::GlobalConfig::gConfig.SOM()->HashMutex.UnLockRead();

  // register in the configuration engine
  if ((!isstatus) && (FsView::ConfEngine))
  {
    nodeconfigname += "#";
    nodeconfigname += key;
    std::string confval = value;
    FsView::ConfEngine->SetConfigValue("global", nodeconfigname.c_str(), confval.c_str());
  }
#endif

  return success;
}

/*----------------------------------------------------------------------------*/
std::string
BaseView::GetConfigMember (std::string key)
{
#ifndef EOSMGMFSVIEWTEST
  //----------------------------------------------------------------
  //! get a configuration member variable (stored in the config engine)
  //----------------------------------------------------------------

  XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
  std::string nodeconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(GetConfigQueuePrefix(), mName.c_str());

  XrdMqSharedHash* hash = eos::common::GlobalConfig::gConfig.Get(nodeconfigname.c_str());

  if (hash)
  {
    return hash->Get(key);
  }
#endif
  return "";
}

/*----------------------------------------------------------------------------*/
bool
BaseView::GetConfigKeys (std::vector<std::string> &keys)
{
#ifndef EOSMGMFSVIEWTEST
  XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
  std::string nodeconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(GetConfigQueuePrefix(), mName.c_str());
  XrdMqSharedHash* hash = eos::common::GlobalConfig::gConfig.Get(nodeconfigname.c_str());
  if (hash)
  {
    hash->GetKeys(keys);
    return true;
  }
#endif
  return false;
}

/*----------------------------------------------------------------------------*/
eos::common::FileSystem::fsid_t
FsView::CreateMapping (std::string fsuuid)
{
  //----------------------------------------------------------------
  //! creates a new filesystem id based on a uuid
  //----------------------------------------------------------------

  eos::common::RWMutexWriteLock lock(MapMutex);
  if (Uuid2FsMap.count(fsuuid))
  {
    return Uuid2FsMap[fsuuid];
  }
  else
  {
    if (!NextFsId)
      SetNextFsId(1);

    std::map<eos::common::FileSystem::fsid_t, std::string>::const_iterator it;

    // use the maximum fsid
    for (it = Fs2UuidMap.begin(); it != Fs2UuidMap.end(); it++)
    {
      if (it->first > NextFsId)
      {
        NextFsId = it->first;
      }
    }

    if (NextFsId > 64000)
    {
      // we don't support more than 64.000 filesystems
      NextFsId = 1;
    }

    while (Fs2UuidMap.count(NextFsId))
    {
      NextFsId++;
      if (NextFsId > 640000)
      {
        // if all filesystem id's are exhausted we better abort the program to avoid a mess!
        eos_static_crit("all filesystem id's exhausted (64.000) - aborting the program");
        exit(-1);
      }
    }

    SetNextFsId(NextFsId);

    Uuid2FsMap[fsuuid] = NextFsId;
    Fs2UuidMap[NextFsId] = fsuuid;
    return NextFsId;
  }
}

/*----------------------------------------------------------------------------*/
bool
FsView::ProvideMapping (std::string fsuuid, eos::common::FileSystem::fsid_t fsid)
{
  //----------------------------------------------------------------
  //! adds a fsid=uuid pair to the mapping
  //----------------------------------------------------------------

  eos::common::RWMutexWriteLock lock(MapMutex);
  if (Uuid2FsMap.count(fsuuid))
  {
    if (Uuid2FsMap[fsuuid] == fsid)
      return true; // we accept if it is consistent with the existing mapping
    else
      return false; // we reject if it is in contradiction to an existing mapping
  }
  else
  {
    Uuid2FsMap[fsuuid] = fsid;
    Fs2UuidMap[fsid] = fsuuid;
    return true;
  }
}

/*----------------------------------------------------------------------------*/
eos::common::FileSystem::fsid_t
FsView::GetMapping (std::string fsuuid)
{
  //----------------------------------------------------------------
  //! returns an fsid for a uuid
  //----------------------------------------------------------------

  eos::common::RWMutexReadLock lock(MapMutex);
  if (Uuid2FsMap.count(fsuuid))
  {
    return Uuid2FsMap[fsuuid];
  }
  else
  {
    return 0; // 0 means there is no mapping
  }
}

bool
FsView::RemoveMapping (eos::common::FileSystem::fsid_t fsid)
{
  //----------------------------------------------------------------
  //! removes a mapping entry by fsid
  //----------------------------------------------------------------

  eos::common::RWMutexWriteLock lock(MapMutex);
  bool removed = false;
  std::string fsuuid;
  if (Fs2UuidMap.count(fsid))
  {
    fsuuid = Fs2UuidMap[fsid];
    Fs2UuidMap.erase(fsid);
    removed = true;
  }

  if (Uuid2FsMap.count(fsuuid))
  {
    Uuid2FsMap.erase(fsuuid);
    removed = true;
  }
  return removed;
}

bool
FsView::RemoveMapping (eos::common::FileSystem::fsid_t fsid, std::string fsuuid)
{
  //----------------------------------------------------------------
  //! removes a mapping entry by providing fsid + uuid
  //----------------------------------------------------------------

  eos::common::RWMutexWriteLock lock(MapMutex);
  bool removed = false;
  if (Uuid2FsMap.count(fsuuid))
  {
    Uuid2FsMap.erase(fsuuid);
    removed = true;
  }
  if (Fs2UuidMap.count(fsid))
  {
    Fs2UuidMap.erase(fsid);
    removed = true;
  }
  return removed;
}

/*----------------------------------------------------------------------------*/
void
FsView::PrintSpaces (std::string &out, std::string headerformat, std::string listformat, const char* selection)
{
  //----------------------------------------------------------------
  //! print space information to out
  //----------------------------------------------------------------

  std::map<std::string, FsSpace* >::iterator it;

  std::vector<std::string> selections;
  std::string selected = selection?selection:"";
  if (selection)
    eos::common::StringConversion::Tokenize(selected, selections ,",");

  for (it = mSpaceView.begin(); it != mSpaceView.end(); it++)
  {
    if (selection)
    {
      bool found=false;
      bool spacefound=false;
      for (size_t i=0; i<selections.size(); i++)
      {
	std::string sel = selections[i];
	// only apply space:... selections
	if (sel.substr(0,6) == "space:")
	{
	  spacefound=true;
	  sel.erase(0,6);
	}
	if ((it->second->mName.find(sel) != std::string::npos))
	  found=true;
      }
      if (selections.size() && (!spacefound))
	found=true;

      if (!found)
	continue;
    }
    it->second->Print(out, headerformat, listformat, selections);
    if (!listformat.length() && ((headerformat.find("header=1:")) == 0))
    {
      headerformat.erase(0, 9);
    }
  }
}

/*----------------------------------------------------------------------------*/
void
FsView::PrintGroups (std::string &out, std::string headerformat, std::string listformat, const char* selection)
{
  //----------------------------------------------------------------
  //! print group information to out
  //----------------------------------------------------------------

  std::map<std::string, FsGroup* >::iterator it;

  std::vector<std::string> selections;
  std::string selected = selection?selection:"";
  if (selection)
    eos::common::StringConversion::Tokenize(selected, selections ,",");

  for (it = mGroupView.begin(); it != mGroupView.end(); it++)
  {
    if (selection)
    {
      bool found=false;
      for (size_t i=0; i<selections.size(); i++)
      {
	if ((it->second->mName.find(selections[i])) != std::string::npos)
	  found=true;
      }
      if (!found)
	continue;
    }
    selections.clear();
    it->second->Print(out, headerformat, listformat, selections);
    if (!listformat.length() && ((headerformat.find("header=1:")) == 0))
    {
      headerformat.erase(0, 9);
    }
  }
}

/*----------------------------------------------------------------------------*/
void
FsView::PrintNodes (std::string &out, std::string headerformat, std::string listformat, const char* selection)
{
  //----------------------------------------------------------------
  //! print node information to out
  //----------------------------------------------------------------
  std::map<std::string, FsNode* >::iterator it;

  std::vector<std::string> selections;
  std::string selected = selection?selection:"";
  if (selection)
    eos::common::StringConversion::Tokenize(selected, selections ,",");

  for (it = mNodeView.begin(); it != mNodeView.end(); it++)
  {
    if (selection)
    {
      bool found=false;
      for (size_t i=0; i<selections.size(); i++)
      {
	if ((it->second->mName.find(selections[i])) != std::string::npos)
	  found=true;
      }
      if (!found)
	continue;
    }
    selections.clear();
    it->second->Print(out, headerformat, listformat, selections);
    if (!listformat.length() && ((headerformat.find("header=1:")) == 0))
    {
      headerformat.erase(0, 9);
    }
  }
}

#ifndef EOSMGMFSVIEWTEST

/*----------------------------------------------------------------------------*/
bool
FsView::ApplyFsConfig (const char* inkey, std::string &val)
{
  //----------------------------------------------------------------
  //! converts a config engine definition for a filesystem into the FsView representation
  //----------------------------------------------------------------

  if (!inkey)
    return false;

  // convert to map
  std::string key = inkey;
  std::map<std::string, std::string> configmap;
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(val, tokens);
  for (size_t i = 0; i < tokens.size(); i++)
  {
    std::vector<std::string> keyval;
    std::string delimiter = "=";
    eos::common::StringConversion::Tokenize(tokens[i], keyval, delimiter);
    configmap[keyval[0]] = keyval[1];
  }

  if ((!configmap.count("queuepath")) || (!configmap.count("queue")) || (!configmap.count("id")))
  {
    eos_static_err("config definitions missing ...");
    return false;
  }

  eos::common::RWMutexWriteLock viewlock(ViewMutex);
  eos::common::FileSystem::fsid_t fsid = atoi(configmap["id"].c_str());

  FileSystem* fs = 0;
  // apply only the registration fo a new filesystem if it does not exist
  if (!FsView::gFsView.mIdView.count(fsid))
  {
    fs = new FileSystem(configmap["queuepath"].c_str(), configmap["queue"].c_str(), eos::common::GlobalConfig::gConfig.SOM());
  }
  else
  {
    fs = FsView::gFsView.mIdView[fsid];
  }

  if (fs)
  {
    fs->OpenTransaction();
    fs->SetId(fsid);
    fs->SetString("uuid", configmap["uuid"].c_str());
    std::map<std::string, std::string>::iterator it;
    for (it = configmap.begin(); it != configmap.end(); it++)
    {
      // set config parameters
      fs->SetString(it->first.c_str(), it->second.c_str());
    }
    fs->CloseTransaction();
    if (!FsView::gFsView.Register(fs))
    {
      eos_static_err("cannot register filesystem name=%s from configuration", configmap["queuepath"].c_str());
      return false;
    }
    // insert into the mapping
    FsView::gFsView.ProvideMapping(configmap["uuid"], fsid);
    return true;
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool
FsView::ApplyGlobalConfig (const char* key, std::string &val)
{
  //----------------------------------------------------------------
  //! converts a config engine definition of a global variable into the FsView representation
  //----------------------------------------------------------------

  // global variables are stored like key='<queuename>:<variable>' val='<val>'
  std::string configqueue = key;
  std::vector<std::string> tokens;
  std::vector<std::string> paths;
  std::string delimiter = "#";
  std::string pathdelimiter = "/";
  eos::common::StringConversion::Tokenize(configqueue, tokens, delimiter);
  eos::common::StringConversion::Tokenize(configqueue, paths, pathdelimiter);
  bool success = false;

  if (tokens.size() != 2)
  {
    eos_static_err("the key definition of config <%s> is invalid", key);
    return false;
  }

  if (paths.size() < 1)
  {
    eos_static_err("the queue name does not contain any /");
    return false;
  }

  eos::common::GlobalConfig::gConfig.SOM()->HashMutex.LockRead();
  XrdMqSharedHash* hash = eos::common::GlobalConfig::gConfig.Get(tokens[0].c_str());
  if (!hash)
  {
    eos::common::GlobalConfig::gConfig.SOM()->HashMutex.UnLockRead();

    // create a global config queue
    if ((tokens[0].find("/node/")) != std::string::npos)
    {
      std::string broadcast = "/eos/";
      broadcast += paths[paths.size() - 1];
      size_t dashpos = 0;
      // remote the #<variable> 
      if ((dashpos = broadcast.find("#")) != std::string::npos)
      {
        broadcast.erase(dashpos);
      }
      broadcast += "/fst";

      if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(tokens[0].c_str(), broadcast.c_str()))
      {
        eos_static_err("cannot create config queue <%s>", tokens[0].c_str());
      }
    }
    else
    {
      if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(tokens[0].c_str(), "/eos/*/mgm"))
      {
        eos_static_err("cannot create config queue <%s>", tokens[0].c_str());
      }
    }
    eos::common::GlobalConfig::gConfig.SOM()->HashMutex.LockRead();
    hash = eos::common::GlobalConfig::gConfig.Get(tokens[0].c_str());
  }
  if (hash)
  {
    success = hash->Set(tokens[1].c_str(), val.c_str());

    // ---------------------------------------------------------------------------
    // here we build a set with the gw nodes for fast lookup in the TransferEngine
    // ---------------------------------------------------------------------------
    if ((tokens[0].find("/node/")) != std::string::npos)
    {
      if (tokens[1] == "txgw")
      {
        std::string broadcast = "/eos/";
        broadcast += paths[paths.size() - 1];
        size_t dashpos = 0;
        // remote the #<variable> 
        if ((dashpos = broadcast.find("#")) != std::string::npos)
        {
          broadcast.erase(dashpos);
        }
        broadcast += "/fst";

        FsView::gFsView.RegisterNode(broadcast.c_str()); // the node might not yet exist!

        eos::common::RWMutexWriteLock gwlock(GwMutex);
        if (val == "on")
        {
          // we have to register this queue into the gw set for fast lookups
          FsView::gFsView.mGwNodes.insert(broadcast.c_str());
        }
        else
        {
          FsView::gFsView.mGwNodes.erase(broadcast.c_str());
        }
      }
    }

  }
  else
  {
    eos_static_err("there is no global config for queue <%s>", tokens[0].c_str());
  }
  eos::common::GlobalConfig::gConfig.SOM()->HashMutex.UnLockRead();

  return success;
}
#endif

/*----------------------------------------------------------------------------*/
long long
BaseView::SumLongLong (const char* param, bool lock)
{
  //----------------------------------------------------------------
  //! computes the sum for <param> as long
  //! param="<param>[?<key>=<value] allows to select with matches
  //----------------------------------------------------------------

  if (lock)
  {
    FsView::gFsView.ViewMutex.LockRead();
  }

  long long sum = 0;
  std::string sparam = param;
  size_t qpos = 0;
  std::string key = "";
  std::string value = "";
  bool isquery = false;

  if ((qpos = sparam.find("?")) != std::string::npos)
  {
    std::string query = sparam;
    query.erase(0, qpos + 1);
    sparam.erase(qpos);
    std::vector<std::string> token;
    std::string delimiter = "@";
    eos::common::StringConversion::Tokenize(query, token, delimiter);
    key = token[0];
    value = token[1];
    isquery = true;
  }

  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it = begin(); it != end(); it++)
  {

    eos::common::FileSystem::fs_snapshot snapshot;

    // for query sum's we always fold in that a group and host has to be enabled
    if ((!key.length()) || (FsView::gFsView.mIdView[*it]->GetString(key.c_str()) == value))
    {
      if (isquery && ((!eos::common::FileSystem::GetActiveStatusFromString(FsView::gFsView.mIdView[*it]->GetString("stat.active").c_str())) ||
                      (eos::common::FileSystem::GetStatusFromString(FsView::gFsView.mIdView[*it]->GetString("stat.boot").c_str()) != eos::common::FileSystem::kBooted)))
        continue;

      long long v = FsView::gFsView.mIdView[*it]->GetLongLong(sparam.c_str());
      if (isquery && v && (sparam == "stat.statfs.capacity"))
      {
        // correct the capacity(rw) value for headroom
        v -= FsView::gFsView.mIdView[*it]->GetLongLong("headroom");
      }
      sum += v;
    }
  }

  // we have to rescale the stat.net parameters because they arrive for each filesystem
  if (!sparam.compare(0, 8, "stat.net"))
  {
    if (mType == "spaceview")
    {
      // divide by the number of "cfg.groupmod"
      std::string gsize = "";
      long long groupmod = 1;
      gsize = GetMember("cfg.groupmod");
      if (gsize.length())
      {
        groupmod = strtoll(gsize.c_str(), 0, 10);
      }
      if (groupmod)
      {
        sum /= groupmod;
      }
    }
    if ((mType == "nodesview"))
    {
      // divide by the number of entries we have summed
      if (size())
        sum /= size();
    }
  }
  if (lock)
  {
    FsView::gFsView.ViewMutex.UnLockRead();
  }
  return sum;
}

/*----------------------------------------------------------------------------*/
double
BaseView::SumDouble (const char* param, bool lock)
{
  //----------------------------------------------------------------
  //! computes the sum for <param> as double
  //----------------------------------------------------------------

  if (lock)
  {
    FsView::gFsView.ViewMutex.LockRead();
  }

  double sum = 0;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it = begin(); it != end(); it++)
  {
    sum += FsView::gFsView.mIdView[*it]->GetDouble(param);
  }
  if (lock)
  {
    FsView::gFsView.ViewMutex.UnLockRead();
  }
  return sum;
}

/*----------------------------------------------------------------------------*/
double
BaseView::AverageDouble (const char* param, bool lock)
{
  //----------------------------------------------------------------
  //! computes the average for <param>
  //----------------------------------------------------------------

  if (lock)
  {
    FsView::gFsView.ViewMutex.LockRead();
  }

  double sum = 0;
  int cnt = 0;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it = begin(); it != end(); it++)
  {
    bool consider = true;
    if (mType == "groupview")
    {
      // we only count filesystem which are >=kRO and booted for averages in the group view
      if ((FsView::gFsView.mIdView[*it]->GetConfigStatus() < eos::common::FileSystem::kRO) ||
          (FsView::gFsView.mIdView[*it]->GetStatus() != eos::common::FileSystem::kBooted) ||
          (FsView::gFsView.mIdView[*it]->GetActiveStatus() == eos::common::FileSystem::kOffline))
        consider = false;
    }

    if (consider)
    {
      cnt++;
      sum += FsView::gFsView.mIdView[*it]->GetDouble(param);
    }
  }
  if (lock)
  {
    FsView::gFsView.ViewMutex.UnLockRead();
  }
  return (cnt) ? (double) (1.0 * sum / cnt) : 0;
}

double
BaseView::MaxDeviation (const char* param, bool lock)
{
  //----------------------------------------------------------------
  //! computes the maximum deviation of <param> from the avg of <param>
  //----------------------------------------------------------------

  if (lock)
  {
    FsView::gFsView.ViewMutex.LockRead();
  }

  double avg = AverageDouble(param, false);

  double maxdev = 0;
  double dev = 0;

  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it = begin(); it != end(); it++)
  {
    bool consider = true;
    if (mType == "groupview")
    {
      // we only count filesystem which are >=kRO and booted for averages in the group view
      if ((FsView::gFsView.mIdView[*it]->GetConfigStatus() < eos::common::FileSystem::kRO) ||
          (FsView::gFsView.mIdView[*it]->GetStatus() != eos::common::FileSystem::kBooted) ||
          (FsView::gFsView.mIdView[*it]->GetActiveStatus() == eos::common::FileSystem::kOffline))
        consider = false;
    }
    dev = fabs(avg - FsView::gFsView.mIdView[*it]->GetDouble(param));
    if (consider)
    {
      if (dev > maxdev)
        maxdev = dev;
    }
  }
  if (lock)
  {
    FsView::gFsView.ViewMutex.UnLockRead();
  }
  return (double) (maxdev);
}

/*----------------------------------------------------------------------------*/
double
BaseView::SigmaDouble (const char* param, bool lock)

{
  //----------------------------------------------------------------
  //! computes the sigma for <param>
  //----------------------------------------------------------------

  if (lock)
  {
    FsView::gFsView.ViewMutex.LockRead();
  }

  double avg = AverageDouble(param, false);

  double sumsquare = 0;
  int cnt = 0;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it = begin(); it != end(); it++)
  {
    bool consider = true;
    if (mType == "groupview")
    {
      // we only count filesystem which are >=kRO and booted for averages in the group view
      if ((FsView::gFsView.mIdView[*it]->GetConfigStatus() < eos::common::FileSystem::kRO) ||
          (FsView::gFsView.mIdView[*it]->GetStatus() != eos::common::FileSystem::kBooted) ||
          (FsView::gFsView.mIdView[*it]->GetActiveStatus() == eos::common::FileSystem::kOffline))
        consider = false;
    }
    if (consider)
    {
      cnt++;
      sumsquare += pow((avg - FsView::gFsView.mIdView[*it]->GetDouble(param)), 2);
    }
  }

  sumsquare = (cnt) ? sqrt(sumsquare / cnt) : 0;

  if (lock)
  {
    FsView::gFsView.ViewMutex.UnLockRead();
  }

  return sumsquare;
}

/*----------------------------------------------------------------------------*/
void
BaseView::Print (std::string &out, std::string headerformat, std::string listformat, std::vector<std::string> &selections)
{
  //----------------------------------------------------------------
  //! print userdefined format to out
  //----------------------------------------------------------------

  //-------------------------------------------------------------------------------
  // headerformat
  //-------------------------------------------------------------------------------
  // format has to be provided as a chain (separated by "|" ) of the following tags
  // "member=<key>:width=<width>:format=[+][-][so]:unit=<unit>:tag=<tag>" -> to print a member variable of the view
  // "avg=<key>:width=<width>:format=[fo]"                      -> to print the average 
  // "sum=<key>:width=<width>:format=[lo]                       -> to print a sum
  // "sig=<key>:width=<width>:format=[lo]                       -> to print the standard deviation
  // "maxdev=<key>:width=<width>;format=[lo]                       -> to print the maxdeviation
  // "sep=<seperator>"                                          -> to put a seperator
  // "tag=<tag>"                                                -> use tag as header not the variable name
  // "header=1"                                                 -> put a header with description on top! This must be the first format tag!!!
  //-------------------------------------------------------------------------------
  // listformat
  //-------------------------------------------------------------------------------
  // format has to be provided as a chain (separated by "|" ) of the following tags
  // "key=<key>:width=<width>:format=[+][-][slfo]:unit=<unit>:tag=<tag>"  -> to print a key of the attached children
  // "sep=<seperator>"                        -> to put a seperator
  // "header=1"                                                 -> put a header with description on top! This must be the first format tag!!!
  // the formats are:
  // 's' : print as string
  // 'l' : print as long long
  // 'f' : print as double
  // 'o' : print as <key>=<val>
  // '-' : left align the printout
  // '+' : convert numbers into k,M,G,T,P ranges
  // the unit is appended to every number:
  // e.g. 1500 with unit=B would end up as '1.5 kB'
  // the command only appends to <out> and DOES NOT initialize it
  // "tag=<tag>"                                                -> use tag as header not the variable name

  std::vector<std::string> formattoken;
  bool buildheader = false;

  std::string header = "";
  std::string body = "";

  eos::common::StringConversion::Tokenize(headerformat, formattoken, "|");

  for (unsigned int i = 0; i < formattoken.size(); i++)
  {
    std::vector<std::string> tagtoken;
    std::map<std::string, std::string> formattags;

    eos::common::StringConversion::Tokenize(formattoken[i], tagtoken, ":");
    for (unsigned int j = 0; j < tagtoken.size(); j++)
    {
      std::vector<std::string> keyval;
      eos::common::StringConversion::Tokenize(tagtoken[j], keyval, "=");
      formattags[keyval[0]] = keyval[1];
    }

    //---------------------------------------------------------------------------------------
    // "key=<key>:width=<width>:format=[slfo]"

    bool alignleft = false;
    if ((formattags["format"].find("-") != std::string::npos))
    {
      alignleft = true;
    }

    if (formattags.count("header"))
    {
      // add the desired seperator
      if (formattags.count("header") == 1)
      {
        buildheader = true;
      }
    }

    if (formattags.count("width") && formattags.count("format"))
    {
      unsigned int width = atoi(formattags["width"].c_str());
      // string
      char line[1024];
      char tmpline[1024];
      char lformat[1024];
      char lenformat[1024];
      line[0] = 0;
      lformat[0] = 0;

      if ((formattags["format"].find("s")) != std::string::npos)
        snprintf(lformat, sizeof (lformat) - 1, "%%s");
      
      if ((formattags["format"].find("l")) != std::string::npos)
        snprintf(lformat, sizeof (lformat) - 1, "%%lld");


      if ((formattags["format"].find("f")) != std::string::npos)
        snprintf(lformat, sizeof (lformat) - 1, "%%.02f");

      // protect against missing format types
      if (lformat[0] == 0)
        continue;

      if (alignleft)
      {
        snprintf(lenformat, sizeof (lenformat) - 1, "%%-%ds", width);
      }
      else
      {
        snprintf(lenformat, sizeof (lenformat) - 1, "%%%ds", width);
      }

      // normal member printout
      if (formattags.count("member"))
      {

        if (((formattags["format"].find("+")) != std::string::npos))
        {
          std::string ssize;
          eos::common::StringConversion::GetReadableSizeString(ssize, (unsigned long long) (strtoll(GetMember(formattags["member"]).c_str(), 0, 10)), formattags["unit"].c_str());
          snprintf(line, sizeof (line) - 1, lenformat, ssize.c_str());
        }
        else
        {
          if (((formattags["format"].find("l")) != std::string::npos))
            snprintf(tmpline, sizeof (tmpline) - 1, lformat, strtoll(GetMember(formattags["member"]).c_str(),0,10));
          else
            snprintf(tmpline, sizeof (tmpline) - 1, lformat, GetMember(formattags["member"]).c_str());
          snprintf(line, sizeof (line) - 1, lenformat, tmpline);
        }

        if (buildheader)
        {
          char headline[1024];
          char lenformat[1024];
          XrdOucString pkey = formattags["member"].c_str();
          pkey.replace("stat.statfs.", "");
          pkey.replace("stat.", "");
          pkey.replace("cfg.", "");
          if (formattags.count("tag"))
          {
            pkey = formattags["tag"].c_str();
          }

          snprintf(lenformat, sizeof (lenformat) - 1, "%%%ds", width - 1);
          snprintf(headline, sizeof (headline) - 1, lenformat, pkey.c_str());
          std::string sline = headline;
          if (sline.length() != (width - 1))
          {
            sline.erase(0, ((sline.length() - width + 1 + 3) > 0) ? (sline.length() - width + 1 + 3) : 0);
            sline.insert(0, "...");
          }
          header += "#";
          header += sline;
        }
      }

      // sum printout
      if (formattags.count("sum"))
      {
        snprintf(tmpline, sizeof (tmpline) - 1, lformat, SumLongLong(formattags["sum"].c_str(), false));

        if (((formattags["format"].find("+")) != std::string::npos))
        {
          std::string ssize;
          eos::common::StringConversion::GetReadableSizeString(ssize, (unsigned long long) SumLongLong(formattags["sum"].c_str(), false), formattags["unit"].c_str());
          snprintf(line, sizeof (line) - 1, lenformat, ssize.c_str());
        }
        else
        {
          snprintf(line, sizeof (line) - 1, lenformat, tmpline);
        }

        if (buildheader)
        {
          char headline[1024];
          char lenformat[1024];
          XrdOucString pkey = formattags["sum"].c_str();
          pkey.replace("stat.statfs.", "");
          pkey.replace("stat.", "");
          pkey.replace("cfg.", "");
          if (formattags.count("tag"))
          {
            pkey = formattags["tag"].c_str();
            width += 5;
          }

          snprintf(lenformat, sizeof (lenformat) - 1, "%%%ds", width - 6);
          snprintf(headline, sizeof (headline) - 1, lenformat, pkey.c_str());
          std::string sline = headline;
          if (sline.length() != (width - 6))
          {
            sline.erase(0, ((sline.length() - width + 6 + 3) > 0) ? (sline.length() - width + 6 + 3) : 0);
            sline.insert(0, "...");
          }
          if (!formattags.count("tag"))
          {
            header += "#";
            header += "sum(";
            header += sline;
            header += ")";
          }
          else
          {
            header += "#";
            header += sline;
          }
        }
      }

      if (formattags.count("avg"))
      {
        snprintf(tmpline, sizeof (tmpline) - 1, lformat, AverageDouble(formattags["avg"].c_str(), false));

        if ((formattags["format"].find("+") != std::string::npos))
        {
          std::string ssize;
          eos::common::StringConversion::GetReadableSizeString(ssize, (unsigned long long) AverageDouble(formattags["avg"].c_str(), false), formattags["unit"].c_str());
          snprintf(line, sizeof (line) - 1, lenformat, ssize.c_str());
        }
        else
        {
          snprintf(line, sizeof (line) - 1, lenformat, tmpline);
        }

        if (buildheader)
        {
          char headline[1024];
          char lenformat[1024];
          XrdOucString pkey = formattags["avg"].c_str();
          pkey.replace("stat.statfs.", "");
          pkey.replace("stat.", "");
          pkey.replace("cfg.", "");
          if (formattags.count("tag"))
          {
            pkey = formattags["tag"].c_str();
            width += 5;
          }

          snprintf(lenformat, sizeof (lenformat) - 1, "%%%ds", width - 6);
          snprintf(headline, sizeof (headline) - 1, lenformat, pkey.c_str());
          std::string sline = headline;
          if (sline.length() != (width - 6))
          {
            sline.erase(0, ((sline.length() - width + 6 + 3) > 0) ? (sline.length() - width + 6 + 3) : 0);
            sline.insert(0, "...");
          }
          if (!formattags.count("tag"))
          {
            header += "#";
            header += "avg(";
            header += sline;
            header += ")";
          }
          else
          {
            header += "#";
            header += sline;
          }
        }
      }

      if (formattags.count("sig"))
      {
        snprintf(tmpline, sizeof (tmpline) - 1, lformat, SigmaDouble(formattags["sig"].c_str(), false));

        if ((formattags["format"].find("+") != std::string::npos))
        {
          std::string ssize;
          eos::common::StringConversion::GetReadableSizeString(ssize, (unsigned long long) SigmaDouble(formattags["sig"].c_str(), false), formattags["unit"].c_str());
          snprintf(line, sizeof (line) - 1, lenformat, ssize.c_str());
        }
        else
        {
          snprintf(line, sizeof (line) - 1, lenformat, tmpline);
        }
        if (buildheader)
        {
          char headline[1024];
          char lenformat[1024];
          XrdOucString pkey = formattags["sig"].c_str();
          pkey.replace("stat.statfs.", "");
          pkey.replace("stat.", "");
          pkey.replace("cfg.", "");
          if (formattags.count("tag"))
          {
            pkey = formattags["tag"].c_str();
            width += 5;
          }

          snprintf(lenformat, sizeof (lenformat) - 1, "%%%ds", width - 6);
          snprintf(headline, sizeof (headline) - 1, lenformat, pkey.c_str());
          std::string sline = headline;
          if (sline.length() != (width - 6))
          {
            sline.erase(0, ((sline.length() - width + 6 + 3) > 0) ? (sline.length() - width + 6 + 3) : 0);
            sline.insert(0, "...");
          }
          if (!formattags.count("tag"))
          {
            header += "#";
            header += "sig(";
            header += sline;
            header += ")";
          }
          else
          {
            header += "#";
            header += sline;
          }
        }
      }

      if (formattags.count("maxdev"))
      {
        snprintf(tmpline, sizeof (tmpline) - 1, lformat, MaxDeviation(formattags["maxdev"].c_str(), false));

        if ((formattags["format"].find("+") != std::string::npos))
        {
          std::string ssize;
          eos::common::StringConversion::GetReadableSizeString(ssize, (unsigned long long) MaxDeviation(formattags["maxdev"].c_str(), false), formattags["unit"].c_str());
          snprintf(line, sizeof (line) - 1, lenformat, ssize.c_str());
        }
        else
        {
          snprintf(line, sizeof (line) - 1, lenformat, tmpline);
        }
        if (buildheader)
        {
          char headline[1024];
          char lenformat[1024];
          XrdOucString pkey = formattags["maxdev"].c_str();
          pkey.replace("stat.statfs.", "");
          pkey.replace("stat.", "");
          pkey.replace("cfg.", "");
          if (formattags.count("tag"))
          {
            pkey = formattags["tag"].c_str();
            width += 5;
          }

          snprintf(lenformat, sizeof (lenformat) - 1, "%%%ds", width - 6);
          snprintf(headline, sizeof (headline) - 1, lenformat, pkey.c_str());
          std::string sline = headline;
          if (sline.length() != (width - 6))
          {
            sline.erase(0, ((sline.length() - width + 6 + 3) > 0) ? (sline.length() - width + 6 + 3) : 0);
            sline.insert(0, "...");
          }
          if (!formattags.count("tag"))
          {
            header += "#";
            header += "dev(";
            header += sline;
            header += ")";
          }
          else
          {
            header += "#";
            header += sline;
          }
        }
      }

      if ((formattags["format"].find("o") != std::string::npos))
      {
        char keyval[4096];
        buildheader = false; // auto disable header
        
        XrdOucString noblankline=line;
        {
          // replace all inner blanks with %20
          std::string snoblankline=line;
          size_t pos=snoblankline.find_last_not_of(" ");
          if (noblankline.length()>1)
            while (noblankline.replace(" ", "%20", 0, (pos==std::string::npos)?-1:pos)) {}
        }
        
        if (formattags.count("member"))
        {
          snprintf(keyval, sizeof (keyval) - 1, "%s=%s", formattags["member"].c_str(), noblankline.c_str());
        }
        if (formattags.count("sum"))
        {
          snprintf(keyval, sizeof (keyval) - 1, "sum.%s=%s", formattags["sum"].c_str(), noblankline.c_str());
        }
        if (formattags.count("avg"))
        {
          snprintf(keyval, sizeof (keyval) - 1, "avg.%s=%s", formattags["avg"].c_str(), noblankline.c_str());
        }
        if (formattags.count("sig"))
        {
          snprintf(keyval, sizeof (keyval) - 1, "sig.%s=%s", formattags["sig"].c_str(), noblankline.c_str());
        }
        if (formattags.count("maxdev"))
        {
          snprintf(keyval, sizeof (keyval) - 1, "dev.%s=%s", formattags["maxdev"].c_str(), noblankline.c_str());
        }
        body += keyval;
      }
      else
      {
        std::string sline = line;
        if (sline.length() > width)
        {
          sline.erase(0, ((sline.length() - width + 3) > 0) ? (sline.length() - width + 3) : 0);
          sline.insert(0, "...");
        }
        body += sline;
      }
    }

    if (formattags.count("sep"))
    {
      // add the desired seperator
      body += formattags["sep"];
      if (buildheader)
      {
        header += formattags["sep"];
      }
    }
  }

  body += "\n";

  //---------------------------------------------------------------------------------------
  if (listformat.length())
  {
    bool first = true;
    std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
    // if a format was given for the filesystem children, forward the print to the filesystems
    for (it = begin(); it != end(); it++)
    {
      std::string lbody;
      bool matches=true;

      FsView::gFsView.mIdView[*it]->Print(lbody, listformat);
      // apply each selection as a find match in the string
      if (selections.size())
      {
	for (size_t i=0; i< selections.size(); i++)
	{
	  if (selections[i].substr(0,6) == "space:")
	    continue;
	  if (lbody.find(selections[i]) == std::string::npos)
	    matches=false;
	}
      }

      if (matches)
	body+=lbody;

      if (first)
      {
        // put the header format only in the first node printout
        first = false;
        if ((listformat.find("header=1:")) == 0)
        {
          listformat.erase(0, 9);
        }
      }
    }
  }

  if (buildheader)
  {
    std::string line = "";
    line += "#";
    for (unsigned int i = 0; i < (header.length() - 1); i++)
    {
      line += "-";
    }
    line += "\n";
    out += line;
    out += header;
    out += "\n";
    out += line;
    out += body;
  }
  else
  {
    out += body;
  }
}

#ifndef EOSMGMFSVIEWTEST

/*----------------------------------------------------------------------------*/
FsGroup::~FsGroup () { }

/*----------------------------------------------------------------------------*/
bool
FsSpace::ApplySpaceDefaultParameters (eos::mgm::FileSystem* fs, bool force)
{
  // -----------------------------------------
  // ! if a filesystem has not yet these parameters defined, we inherit them from the space configuration
  // ! this function has to be called with the a read lock on the View Mutex !
  // ! return's true if the fs was modified and the caller should evt. store the modification to the config
  // -----------------------------------------

  if (!fs)
    return false;

  bool modified = false;
  eos::common::FileSystem::fs_snapshot_t snapshot;
  if (fs->SnapShotFileSystem(snapshot, false))
  {
    if (force || (!snapshot.mScanInterval))
    {
      // try to apply the default
      if (GetConfigMember("scaninterval").length())
      {
        fs->SetString("scaninterval", GetConfigMember("scaninterval").c_str());
        modified = true;
      }
    }
    if (force || (!snapshot.mGracePeriod))
    {
      // try to apply the default
      if (GetConfigMember("graceperiod").length())
      {
        fs->SetString("graceperiod", GetConfigMember("graceperiod").c_str());
        modified = true;
      }
    }
    if (force || (!snapshot.mDrainPeriod))
    {
      // try to apply the default
      if (GetConfigMember("drainperiod").length())
      {
        fs->SetString("drainperiod", GetConfigMember("drainperiod").c_str());
        modified = true;
      }
    }
    if (force || (!snapshot.mHeadRoom))
    {
      // try to apply the default
      if (GetConfigMember("headroom").length())
      {
        fs->SetString("headroom", GetConfigMember("headroom").c_str())
          ;
        modified = true;
      }
    }
  }
  return modified;
}

/*----------------------------------------------------------------------------*/
void
FsSpace::ResetDraining ()
{
  // ---------------------------------------------------------------------------
  //! re-evaluates the drainnig states in all groups and resets the state
  // ------------------- -------------------------------------------------------
  eos_static_info("msg=\"reset drain state\" space=\"%s\"", mName.c_str());
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  // iterate over all groups in this space
  for (auto sgit = FsView::gFsView.mSpaceGroupView[mName].begin();
    sgit != FsView::gFsView.mSpaceGroupView[mName].end();
    sgit++)
  {
    bool setactive = false;
    std::string lGroup = (*sgit)->mName;

    FsGroup::const_iterator git;

    for (git = (*sgit)->begin();
      git != (*sgit)->end(); git++)
    {
      if (FsView::gFsView.mIdView.count(*git))
      {
        int drainstatus =
          (eos::common::FileSystem::GetDrainStatusFromString(
                                                             FsView::gFsView.mIdView[*git]->GetString("stat.drain").c_str())
           );

        if ((drainstatus == eos::common::FileSystem::kDraining) ||
            (drainstatus == eos::common::FileSystem::kDrainStalling))
        {
          // if any mGroup filesystem is draining, all the others have 
          // to enable the pull for draining!
          setactive = true;
        }
      }
    }
    // if the mGroup get's disabled we stop the draining
    if (FsView::gFsView.mGroupView[lGroup]->GetConfigMember("status") != "on")
    {
      setactive = false;
    }
    for (git = (*sgit)->begin();
      git != (*sgit)->end(); git++)
    {
      eos::mgm::FileSystem* fs = FsView::gFsView.mIdView[*git];
      if (fs)
      {
        if (setactive)
        {
          if (fs->GetString("stat.drainer") != "on")
          {
            fs->SetString("stat.drainer", "on");
          }
        }
        else
        {
          if (fs->GetString("stat.drainer") != "off")
          {
            fs->SetString("stat.drainer", "off");
          }
        }
        eos_static_info("fsid=%05d state=%s",fs->GetId(), fs->GetString("stat.drainer").c_str());
      }
    }
  }
}
#endif

EOSMGMNAMESPACE_END
