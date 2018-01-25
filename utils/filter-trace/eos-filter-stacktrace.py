#!/usr/bin/env python3

################################################################################
## Script to parse and filter EOS stacktraces.                                ##
## Author: Georgios Bitzes - CERN                                             ##
##                                                                            ##
## Copyright (C) 2018 CERN/Switzerland                                        ##
## This program is free software: you can redistribute it and/or modify       ##
## it under the terms of the GNU General Public License as published by       ##
## the Free Software Foundation, either version 3 of the License, or          ##
## (at your option) any later version.                                        ##
##                                                                            ##
## This program is distributed in the hope that it will be useful,            ##
## but WITHOUT ANY WARRANTY; without even the implied warranty of             ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the              ##
## GNU General Public License for more details.                               ##
##                                                                            ##
## You should have received a copy of the GNU General Public License          ##
## along with this program.  If not, see <http://www.gnu.org/licenses/>.      ##
################################################################################

import argparse
import re

VERBOSE=False

def debug(string):
    global VERBOSE
    if VERBOSE:
        print(string)

class ExclusionFilter:
    def __init__(self, match):
        self.eliminations = 0
        self.description = "exclude ''".format(match)
        self.match = match

    def check(self, thread):
        for i in range(0, thread.getNumberOfFrames()):
            if self.match in thread.getFrame(i):
                self.eliminations += 1
                return True

        return False

class ZmqFilter:
    def __init__(self):
        self.eliminations = 0
        self.description = "zmq poller"

    def check(self, thread):
        if thread.getNumberOfFrames() <= 2:
            return False

        if ("epoll_wait" in thread.getFrame(0) and
            "zmq::epoll_t::loop" in thread.getFrame(1)):

            self.eliminations += 1
            return True

        if ("poll" in thread.getFrame(0) and
            "zmq::signaler_t::wait" in thread.getFrame(1)):

            self.eliminations += 1
            return True

        if ("poll" in thread.getFrame(0) and
            "zmq_poll" in thread.getFrame(1) ):

            self.eliminations += 1
            return True

        return False

class MicroHttpdFilter:
    def __init__(self):
        self.eliminations = 0
        self.description = "microhttpd poller"

    def check(self, thread):
        if thread.getNumberOfFrames() <= 3:
            return False

        if ("epoll_wait" in thread.getFrame(0) and
            "MHD_epoll" in thread.getFrame(1) and
            "MHD_select_thread" in thread.getFrame(2)):

            self.eliminations += 1
            return True

        return False

class QClientFilter:
    def __init__(self):
        self.eliminations = 0
        self.description = "qclient pollers"

    def check(self, thread):
        if thread.getNumberOfFrames() <= 3:
            return False

        if "pthread_cond_timedwait" in thread.getFrame(0):
            for i in range(0, thread.getNumberOfFrames()):
                if "qclient::WriterThread::eventLoop" in thread.getFrame(i):
                    self.eliminations += 1
                    return True

        if ("poll" in thread.getFrame(0) and
            "qclient::QClient::eventLoop" in thread.getFrame(1) ):

            self.eliminations += 1
            return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            ExclusionFilter("qclient::BackgroundFlusher::monitorAckReception").check(thread) ):

            self.eliminations += 1
            return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            ExclusionFilter("qclient::BackgroundFlusher::processPipeline").check(thread) ):

            self.eliminations += 1
            return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            ExclusionFilter("qclient::BackgroundFlusher::main").check(thread) ):
            self.eliminations += 1
            return True

        return False

class XrdSleeperFilter:
    def __init__(self):
        self.eliminations = 0
        self.description = "xrootd sleeping background threads"

    def check(self, thread):
        if thread.getNumberOfFrames() <= 4:
            return False

        if ("clock_nanosleep" in thread.getFrame(0) and
            "XrdSysTimer::Wait4Midnight" in thread.getFrame(1) and
            "XrdSysLogger::zHandler" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            "XrdSysCondVar::WaitMS" in thread.getFrame(1) and
            "XrdSysCondVar::Wait" in thread.getFrame(2) and
            "XrdBuffManager::Reshape" in thread.getFrame(3) ):

            self.eliminations += 1
            return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            "XrdSysCondVar::WaitMS" in thread.getFrame(1) and
            "XrdSysCondVar::Wait" in thread.getFrame(2) and
            "XrdScheduler::TimeSched" in thread.getFrame(3) ):

            self.eliminations += 1
            return True

        if ("epoll_wait" in thread.getFrame(0) and
            "XrdPollE::Start" in thread.getFrame(1) and
            "XrdStartPolling" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        if ("nanosleep" in thread.getFrame(0) and
            "XrdSecsssKTRefresh" in thread.getFrame(1) and
            "XrdSysThread_Xeq" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        if ("accept4" in thread.getFrame(0) and
            "XrdSysFD_Accept" in thread.getFrame(1) and
            "XrdNetSocket::Accept" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        if ("accept4" in thread.getFrame(0) and
            "XrdSysFD_Accept" in thread.getFrame(1) and
            "XrdNet::do_Accept_TCP" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        if ("nanosleep" in thread.getFrame(0) and
            "XrdSysTimer::Wait" in thread.getFrame(1) and
            "XrdCl::TaskManager::RunTasks" in thread.getFrame(2) and
            "RunRunnerThread" in thread.getFrame(3) ):

            self.eliminations += 1
            return True

        if ("syscall" in thread.getFrame(0) and
            "Wait" in thread.getFrame(1) and
            "Get" in thread.getFrame(2) and
            "XrdCl::JobManager::RunJobs" in thread.getFrame(3) and
            "RunRunnerThread" in thread.getFrame(4) ):

            self.eliminations += 1
            return True

        if ("do_futex_wait" in thread.getFrame(0) and
            "__new_sem_wait_slow" in thread.getFrame(1) and
            "sem_wait" in thread.getFrame(2) and
            "Wait" in thread.getFrame(3) and
            "XrdScheduler::Run" in thread.getFrame(4) ):

            self.eliminations += 1
            return True

        if ("epoll_wait" in thread.getFrame(0) and
            "XrdSys::IOEvents::PollE::Begin" in thread.getFrame(1) and
            "XrdSys::IOEvents::BootStrap::Start" in thread.getFrame(2) and
            "XrdSysThread_Xeq" in thread.getFrame(3) ):

            self.eliminations += 1
            return True


        return False

class BackgroundSleeperFilter:
    def __init__(self):
        self.eliminations = 0
        self.description = "mgm sleeping background threads"

    def check(self, thread):
        if thread.getNumberOfFrames() <= 3:
            return False

        # LRU background threads
        if ("nanosleep" in thread.getFrame(0) and
            "XrdSysTimer::Snooze" in thread.getFrame(1) and
            "eos::mgm::LRU::LRUr" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        # Drainer
        if ("nanosleep" in thread.getFrame(0) and
            "XrdSysTimer::Wait" in thread.getFrame(1) and
            "eos::mgm::Drainer::Drain" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        # Heartbeat check
        if ("nanosleep" in thread.getFrame(0) and
            "XrdSysTimer::Snooze" in thread.getFrame(1) and
            "eos::mgm::FsView::HeartBeatCheck" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        # Fuse server monitor heartbeat
        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Snooze" in thread.getFrame(1) and
             "eos::mgm::FuseServer::Clients::MonitorHeartBeat" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Snooze" in thread.getFrame(1) and
             "eos::mgm::FuseServer::MonitorCaps" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Wait" in thread.getFrame(1) and
             "eos::mgm::GeoBalancer::GeoBalance" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Snooze" in thread.getFrame(1) and
             "eos::mgm::GroupBalancer::GroupBalance" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Snooze" in thread.getFrame(1) and
             "eos::mgm::Balancer::Balance" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Wait" in thread.getFrame(1) and
             "eos::mgm::Stat::Circulate" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Wait" in thread.getFrame(1) and
             "eos::mgm::TransferEngine::Watch" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Wait" in thread.getFrame(1) and
             "eos::mgm::TransferEngine::Scheduler" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Snooze" in thread.getFrame(1) and
             "eos::mgm::Iostat::Receive" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Wait" in thread.getFrame(1) and
             "XrdMgmOfs::ArchiveSubmitter" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Wait" in thread.getFrame(1) and
             "eos::mgm::Master::Supervisor" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Wait" in thread.getFrame(1) and
             "eos::mgm::GeoTreeEngine::listenFsChange" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ( "nanosleep" in thread.getFrame(0) and
             "XrdSysTimer::Wait" in thread.getFrame(1) and
             "eos::mgm::Iostat::Circulate" in thread.getFrame(2) ):

             self.eliminations += 1
             return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            ExclusionFilter("eos::MetadataFlusher::queueSizeMonitoring").check(thread) ):
            return True

        if ("pthread_cond_wait" in thread.getFrame(0) and
            "XrdSysCondVar::Wait" in thread.getFrame(1) and
            "eos::mgm::Egroup::Refresh" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            "XrdSysCondVar::WaitMS" in thread.getFrame(1) and
            "XrdSysCondVar::Wait" in thread.getFrame(2) and
            "eos::common::LvDbDbLogInterface::archiveThread" in thread.getFrame(3) ):

            self.eliminations += 1
            return True

        if ("pause" in thread.getFrame(0) and
            "eos::common::HttpServer::Run" in thread.getFrame(1) and
            "XrdSysThread_Xeq" in thread.getFrame(2) ):

            self.eliminations += 1
            return True

        if ("pthread_cond_wait" in thread.getFrame(0) and
            "eos::common::ConcurrentQueue" in thread.getFrame(1) ):

            self.eliminations += 1
            return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            ExclusionFilter("eos::common::ThreadPool::ThreadPool").check(thread) ):
            self.eliminations += 1
            return True

        if ("nanosleep" in thread.getFrame(0) and
            "XrdSysTimer::Wait" in thread.getFrame(1) and
            "eos::mgm::Messaging::Listen" in thread.getFrame(2) ):
            self.eliminations += 1
            return True

        if ("pthread_cond_wait" in thread.getFrame(0) and
            "XrdSysCondVar::Wait" in thread.getFrame(1) and
            "XrdMgmOfs::FsConfigListener" in thread.getFrame(2) and
            "XrdMgmOfs::StartMgmFsConfigListener" in thread.getFrame(3) ):
            self.eliminations += 1
            return True

        if ("pthread_cond_timedwait" in thread.getFrame(0) and
            "XrdSysCondVar::WaitMS" in thread.getFrame(1) and
            "XrdSysCondVar::Wait" in thread.getFrame(2) and
            "eos::mgm::Converter::Convert" in thread.getFrame(3) ):
            self.eliminations += 1
            return True

        if ("pthread_cond_wait" in thread.getFrame(0) and
            "XrdSysCondVar::Wait" in thread.getFrame(1) and
            "Wait" in thread.getFrame(2) and
            "XrdMqSharedObjectChangeNotifier::SomListener" in thread.getFrame(3) ):
            self.eliminations += 1
            return True

        return False

class RocksLevelDBFilter:
    def __init__(self):
        self.eliminations = 0
        self.description = "rocksdb/leveldb sleeping background threads"

    def check(self, thread):
        if thread.getNumberOfFrames() <= 3:
            return False

        if ("pthread_cond_wait" in thread.getFrame(0) and
            "std::condition_variable::wait" in thread.getFrame(1) and
            "rocksdb::ThreadPoolImpl::Impl::BGThread" in thread.getFrame(2) and
            "rocksdb::ThreadPoolImpl::Impl::BGThreadWrapper" in thread.getFrame(3) ):
            return True

        if ("pthread_cond_wait" in thread.getFrame(0) and
            "leveldb::(anonymous namespace)::PosixEnv::BGThreadWrapper" in thread.getFrame(1) ):
            return True

        return False

class ThreadStack:
    def __init__(self, text):
        self.text = text

        for i in range(len(self.text)):
            self.text[i] = self.text[i].strip()

        while self.text[0] == "":
            self.text = self.text[1:]

        match = re.search(r'^Thread (\d+)', self.text[0])

        if not match:
            raise Exception("Cannot parse thread stack, couldn't extract thread id: {}".format(self.text))

        self.header = self.text[0]
        self.threadId = int(match.group(1))

        self.frames = []

        currentLine = ""
        for line in self.text[1:]:
            match = re.search(r'^#(\d+)  ', line)

            # Frame spans two lines
            if not match:
                if not currentLine.strip() == "" and not line.strip() == "":
                    currentLine = currentLine + " " + line
                else:
                    currentLine += line
                continue

            # Empty line?
            if line.strip() == "":
                continue

            # Legit new frame - retire previous line
            if currentLine.strip() != "":
                self.frames += [currentLine]

            # Replace currentLine
            currentLine = line

        self.frames += [currentLine]
        debug("Parsed thread stack with id = {} and {} frames".format(self.threadId, len(self.frames)))

    def getThreadID(self):
        return self.threadId

    def getFrame(self, i):
        return self.frames[i]

    def getNumberOfFrames(self):
        return len(self.frames)

    def tostr(self):
        return self.header + "\n" + "\n".join(self.frames)

    def getHeader(self):
        return self.header

class StackTrace:
    def __init__(self, text):
        self.text = text
        self.threads = []

        # Split thread boundaries
        tmp = []
        for line in self.text:
            if line.strip() == "": continue

            # New thread boundary? Clear tmp.
            if line.startswith("Thread "):
                if len(tmp) != 0:
                    self.threads += [ThreadStack(tmp)]
                tmp = []

            tmp += [line]

        # Final thread?
        if len(tmp) != 0:
            self.threads += [ThreadStack(tmp)]

    def getThread(self, i):
        return self.threads[i]

    def getNumberOfThreads(self):
        return len(self.threads)

def build_filters(args):
    filters = []

    if not args.want_zmq:
        filters += [ZmqFilter()]

    if not args.want_mhd:
        filters += [MicroHttpdFilter()]

    if not args.want_qcl:
        filters += [QClientFilter()]

    if not args.want_xrd_sleepers:
        filters += [XrdSleeperFilter()]

    if not args.want_mgm_sleepers:
        filters += [BackgroundSleeperFilter()]

    if not args.want_rocksdb_sleepers:
        filters += [RocksLevelDBFilter()]

    if args.exclusions:
        for excl in args.exclusions:
            print(excl)
            filters += [ExclusionFilter(excl)]

    return filters

def parseTrace(args):
    threads = []

    with open(args.path, "r") as f:
        content = f.readlines()

    while content[0].strip() == "":
        content = content[1:]

    trace = StackTrace(content)
    filters = build_filters(args)

    for thread in trace.threads:
        eliminated = False

        for filt in filters:
            if filt.check(thread):
                eliminated = True
                break

        if not eliminated:
            print(thread.tostr())
            print("")

def getargs():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description="Goes through an MGM stacktrace and weeds out un-interesting functions.\n")

    parser.add_argument('--path', type=str, required=True, help="Stacktrace path")
    parser.add_argument('--verbose', dest='verbose', action='store_true', help="Verbose output")

    group = parser.add_argument_group('Built-in filters enabled by default', 'A certain number of filters are enabled by default to greatly reduce the initial noise, pass these flags to disable them. Only sleeping threads will be filtered out - if some poller, for example, is doing work at the time of capturing the stacktrace, it will be shown by default, unless you filter it out manually using a custom filter.')
    group.add_argument('--show-zmq-pollers', dest='want_zmq', action='store_true', help="Show sleeping ZMQ background threads.")
    group.add_argument('--show-microhttpd-pollers', dest='want_mhd', action='store_true', help="Show sleeping MicroHttpd background threads.")
    group.add_argument('--show-qclient-pollers', dest='want_qcl', action='store_true', help="Show sleeping QClient background threads.")
    group.add_argument('--show-xrd-threads', dest='want_xrd_sleepers', action='store_true', help="Show sleeping XRootD background threads.")
    group.add_argument('--show-background-mgm-threads', dest='want_mgm_sleepers', action='store_true', help="Show sleeping MGM background threads. (Balancer, GroupBalancer, GeoGalancer, Converter, etc)")
    group.add_argument('--show-rocksdb-threads', dest='want_rocksdb_sleepers', action='store_true', help="Show sleeping rocksdb / leveldb background threads.")


    group = parser.add_argument_group('Custom filters', '')
    group.add_argument('--exclude', dest='exclusions', nargs='+', help="Exclude threads whose stacktraces contain the passed string. You can pass multiple arguments.")

    args = parser.parse_args()

    global VERBOSE
    VERBOSE = args.verbose
    return args

def main():
    args = getargs()
    parseTrace(args)

if __name__ == '__main__':
    main()
