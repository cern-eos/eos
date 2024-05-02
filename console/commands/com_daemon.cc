// ----------------------------------------------------------------------
// File: com_daemon.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/Config.hh"
#include "common/Path.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/
#include <sys/wait.h>
#include <sched.h>
#include <sys/mount.h>
#include <sys/ptrace.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
/*----------------------------------------------------------------------------*/

#ifdef __APPLE__
#define EOS_PTRACE_TRACEME PT_TRACEME
#define EOS_PTRACE_ATTACH   PT_ATTACH
#define EOS_PTRACE_DETACH   PT_DETACH
#else
#define EOS_PTRACE_TRACEME PTRACE_TRACEME
#define EOS_PTRACE_ATTACH   PTRACE_ATTACH
#define EOS_PTRACE_DETACH   PTRACE_DETACH
#endif //__APPLE__

/* Steer a service */
int
com_daemon(char* arg)
{
#ifdef __APPLE__
  fprintf(stderr, "error: daemon command is not support on OSX\n");
  global_retc = EINVAL;
  return (0);
#else
  eos::common::StringTokenizer subtokenizer(arg);
  eos::common::Config cfg;
  XrdOucString option = "";
  XrdOucString name = "";
  XrdOucString service = "";
  XrdOucString subcmd;
  XrdOucString modules;
  std::vector<std::string> mods;
  std::string chapter;
  std::string executable;
  std::string pidfile;
  std::string envfile;
  std::string cfile;
  bool ok = false;
  subtokenizer.GetLine();

  if (wants_help(arg)) {
    goto com_daemon_usage;
  }

  option = subtokenizer.GetToken();

  if (!option.length()) {
    goto com_daemon_usage;
  }

  if (option == "sss") {
    subcmd = subtokenizer.GetToken();

    if (!subcmd.length()) {
      goto com_daemon_usage;
    }

    if (subcmd == "recreate") {
      if (geteuid()) {
        std::cerr << "error: you have to run this command as root!" << std::endl;
        global_retc = EPERM;
        return (0);
      }

      struct stat buf;

      std::cout <<
                "info: you are going to (re-)create the instance sss key. A previous key will be moved to /etc/eos.keytab.<unixtimestamp>"
                << std::endl;

      if (!::stat("/etc/eos.keytab", &buf)) {
        std::string oldkeytab = "/etc/eos.keytab.";
        oldkeytab += std::to_string(time(NULL));

        if (::rename("/etc/eos.keytab", oldkeytab.c_str())) {
          std::cerr <<
                    "error: renaming of existing old keytab file /etc/eos.keytab failed!" <<
                    std::endl;
          global_retc = errno;
          return (0);
        }
      }

      bool interactive = false;

      if (isatty(STDOUT_FILENO)) {
        interactive = true;
      }

      if (!interactive || ICmdHelper::ConfirmOperation()) {
        if (!::stat("/opt/eos/xrootd/bin/xrdsssadmin", &buf)) {
          system("yes | /opt/eos/xrootd/bin/xrdsssadmin -u daemon -g daemon -k eosmaster add /etc/eos.keytab");
          system("yes | /opt/eos/xrootd/bin/xrdsssadmin -u eosnobody -g eosnobody -k eosnobody add /etc/eos.keytab");
        } else {
          system("yes | xrdsssadmin -u daemon -g daemon -k eosmaster add /etc/eos.keytab");
          system("yes | xrdsssadmin -u eosnobody -g eosnobody -k eosnobody add /etc/eos.keytab");
        }

        system("mkdir -p /etc/eos/; cat /etc/eos.keytab | grep eosnobody > /etc/eos/fuse.sss.keytab; chmod 400 /etc/eos/fuse.sss.keytab");
        std::cout << "info: recreated /etc/eos.keytab /etc/eos/fuse.sss.keytab" <<
                  std::endl;
      } else {
        global_retc = EINVAL;
        return (0);
      }

      return (0);
    }
  }

  if (option == "seal") {
    XrdOucString toseal = subtokenizer.GetToken();
    XrdOucString sealed;
    std::string key;

    if (!toseal.length()) {
      return (0);
    }

    if (toseal.beginswith("/")) {
      // treat it as a file
      std::string contents;
      eos::common::StringConversion::LoadFileIntoString(toseal.c_str(), contents);
      toseal = contents.c_str();
    }

    const char* pkey = subtokenizer.GetToken();

    if (!pkey) {
      key = eos::common::StringConversion::StringFromShellCmd("cat /etc/eos.keytab | grep u:daemon | md5sum");
    } else {
      key = pkey;
    }

    std::string shakey = eos::common::SymKey::HexSha256(key);
    eos::common::SymKey::SymmetricStringEncrypt(toseal, sealed,
        (char*)shakey.c_str());
    fprintf(stderr, "enc:%s\n", sealed.c_str());
    return (0);
  }

  if ((option != "run") &&
      (option != "config") &&
      (option != "stack") &&
      (option != "stop") &&
      (option != "kill") &&
      (option != "jwk") &&
      (option != "module-init")) {
    goto com_daemon_usage;
  }

  service = subtokenizer.GetToken();

  if ((option != "jwk") && ((!service.length()) ||
      ((service != "mgm") &&
       (service != "mq") &&
       (service != "fst") &&
       (service != "qdb")))) {
    goto com_daemon_usage;
  }

  name = subtokenizer.GetToken();

  if (!name.length()) {
    name = service.c_str();
  }

  modules = name;
  modules += ".modules";
  executable = "eos-";
  executable += service.c_str();
  envfile = "/var/run/eos/";
  envfile += executable.c_str() ;
  envfile += ".";
  envfile += name.c_str();
  envfile += ".env";
  pidfile = "/var/run/eos/xrd.";
  pidfile += service.c_str();
  pidfile += ".";
  pidfile += name.c_str();
  pidfile += ".pid";
  cfg.Load("generic", "all");
  ok |= cfg.ok();
  cfg.Load(service.c_str(), name.c_str(), false);
  ok |= cfg.ok();
  // this might fail if there are no modules
  cfg.Load(service.c_str(), modules.c_str(), false);
  {
    // load all the modules:
    eos::common::StringConversion::Tokenize(cfg.Dump("modules", true), mods, "\n");

    for (size_t i = 0; i < mods.size(); ++i) {
      if (mods[i].front() == '#') {
        // ignore comments
        continue;
      }

      if (mods[i].find(" ") != std::string::npos) {
        fprintf(stderr, "warning: ignoring module line '%s' (contains space)\n",
                mods[i].c_str());
        continue;
      }

      if (mods[i].empty()) {
        // ignore empty lines
        continue;
      }

      if (!cfg.Load("modules", mods[i].c_str(), false)) {
        fprintf(stderr, "error: failed to load module '%s'\n", mods[i].c_str());
        global_retc = EINVAL;
        return 0;
      }
    }
  }
  chapter = service.c_str();
  chapter += ":xrootd:";
  chapter += name.c_str();
  cfile = "/var/run/eos/xrd.cf.";
  cfile += name.c_str();

  if (option == "config") {
    char** const envv = cfg.Env("sysconfig");

    for (size_t i = 0; i < 1024; ++i) {
      if (envv[i]) {
        putenv(envv[i]);
        fprintf(stderr, "[putenv] %s\n", envv[i]);
      } else {
        break;
      }
    }

    if (service == "qdb") {
      XrdOucString subcmd = subtokenizer.GetToken();

      if (subcmd == "coup") {
        std::string kline;
        kline = "export REDISCLI_AUTH=`cat /etc/eos.keytab`; redis-cli -p `cat ";
        kline += cfile;
        kline += "|grep xrd.port | cut -d ' ' -f 2` <<< raft-attempt-coup";
        int rc = system(kline.c_str());
        fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
        global_retc = WEXITSTATUS(rc);
        return (0);
      } else if (subcmd == "info") {
        std::string kline;
        kline = "export REDISCLI_AUTH=`cat /etc/eos.keytab`; redis-cli -p `cat ";
        kline += cfile;
        kline += "|grep xrd.port | cut -d ' ' -f 2` <<< raft-info";
        int rc = system(kline.c_str());
        fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
        global_retc = WEXITSTATUS(rc);
        return (0);
      } else if (subcmd == "remove") {
        XrdOucString member = subtokenizer.GetToken();

        if (!member.length()) {
          fprintf(stderr,
                  "error: remove misses member argument host:port : 'eos daemon config qdb qdb remove host:port'\n");
          global_retc = EINVAL;
          return 0;
        } else {
          std::string kline;
          kline = "export REDISCLI_AUTH=`cat /etc/eos.keytab`; redis-cli -p `cat ";
          kline += cfile;
          kline += "|grep xrd.port | cut -d ' ' -f 2` <<< \"raft-remove-member ";
          kline += member.c_str();
          kline += "\"";
          int rc = system(kline.c_str());
          fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
          global_retc = WEXITSTATUS(rc);
          return (0);
        }
      } else if (subcmd == "add") {
	XrdOucString member = subtokenizer.GetToken();
	if (!member.length()) {
	  fprintf(stderr,"error: add misses member argument host:port : 'eos daemon config qdb qdb add host:port'\n");
	  global_retc = EINVAL;
	  return 0;
	} else {
	  std::string kline;
	  kline = "export REDISCLI_AUTH=`cat /etc/eos.keytab`; redis-cli -p `cat "; kline += cfile; kline += "|grep xrd.port | cut -d ' ' -f 2` \"<<< raft-add-observer ";
	  kline += member.c_str();
	  kline += "\"";
	  int rc = system(kline.c_str());
	  fprintf(stderr,"info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
	  global_retc = WEXITSTATUS(rc);
	  return (0);
	}
      } else if (subcmd == "promote") {
        XrdOucString member = subtokenizer.GetToken();

        if (!member.length()) {
          fprintf(stderr,
                  "error: promote misses member argument host:port : 'eos daemon config qdb qdb promote host:port'\n");
          global_retc = EINVAL;
          return 0;
        } else {
          std::string kline;
          kline = "export REDISCLI_AUTH=`cat /etc/eos.keytab`; redis-cli -p `cat ";
          kline += cfile;
          kline += "|grep xrd.port | cut -d ' ' -f 2` <<< \"raft-promote-observer ";
          kline += member.c_str();
          kline += "\"";
          int rc = system(kline.c_str());
          fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
          global_retc = WEXITSTATUS(rc);
          return (0);
        }
      } else if (subcmd == "new") {
	XrdOucString member = subtokenizer.GetToken();
	if (member != "observer") {
	  fprintf(stderr,"error: new misses 'observer' arguement : 'eos daemon config qdb qdb new observer'\n");
	  global_retc = EINVAL;
	  return 0;
	} else {
	  std::string stopqdb = "systemctl stop qdb ";
	  stopqdb += name.c_str();
	  system(stopqdb.c_str());
	  
	  std::string qdbpath=getenv("QDB_PATH")?getenv("QDB_PATH"):"";
	  std::string qdbcluster=getenv("QDB_CLUSTER_ID")?getenv("QDB_CLUSTER_ID"):"";
	  std::string qdbnode=getenv("QDB_NODE")?getenv("QDB_NODE"):"";
	  if (qdbpath.empty())    {
	    fprintf(stderr,"error: QDB_PATH is undefined in your configuration\n");
	    global_retc = EINVAL;
	    return 0;
	  }
	  if (qdbcluster.empty()) {
	    fprintf(stderr,"error: QDB_CLUSTER_ID is undefined in your configuration\n");
	    global_retc = EINVAL;
	    return 0;
	  }
	  if (qdbnode.empty()) {
	    fprintf(stderr,"error: QDB_NODE is undefined in your configuration\n");
	    global_retc = EINVAL;
	    return 0;
	  }
	  struct stat buf;
	  if (!::stat(qdbpath.c_str(), &buf)) {
	    fprintf(stderr,"error: path '%s' exists - to create a new observer this path has to be changed or removed\n",qdbpath.c_str());
	    global_retc = EINVAL;
	    return 0;
	  } else {
	    fprintf(stderr,"info: creating QDB under %s ...\n", qdbpath.c_str());
	  }
	  
	  std::string kline;
	  kline = "quarkdb-create --path ";
	  kline += qdbpath;
	  kline += " --clusterID ";
	  kline += qdbcluster;
	  int rc = system(kline.c_str());
	  fprintf(stderr,"info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
	  global_retc = WEXITSTATUS(rc);
	  if(!global_retc) {
	    fprintf(stderr,"info: to get this node joining the cluster you do:\n");
	    fprintf(stderr,"1 [ this node ] : systemctl start eos5-@qdb@%s\n",name.c_str());
	    fprintf(stderr,"2 [ leader    ] : eos daemon config qdb %s add %s\n",name.c_str(),qdbnode.c_str());
	    fprintf(stderr,"3 [ leader    ] : eos daemon config qdb %s promote %s\n",name.c_str(),qdbnode.c_str());
	  }
	  return (0);
	}
      } else if (subcmd == "backup") {
        std::string qdbpath = "/var/lib/qdb1";

        for (auto it : cfg[chapter.c_str()]) {
          size_t pos;

          if ((pos = it.find("redis.database")) != std::string::npos) {
            qdbpath = it;
            qdbpath.erase(pos, 15);
          }
        }

        std::string kline;
        std::string qdblocation = qdbpath;
        qdblocation += "/backup/";
        qdblocation += std::to_string(time(NULL));
        kline += "export REDISCLI_AUTH=`cat /etc/eos.keytab`; redis-cli -p `cat ";
        kline += cfile;
        kline += "|grep xrd.port | cut -d ' ' -f 2` <<< \"quarkdb-checkpoint ";
        kline += qdblocation;
        kline += "\"";
        int rc = system(kline.c_str());
        fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
        global_retc = WEXITSTATUS(rc);
        return (0);
      }
    }

    fprintf(stderr, "# ---------------------------------------\n");
    fprintf(stderr, "# ------------- i n i t -----------------\n");
    fprintf(stderr, "# ---------------------------------------\n");
    fprintf(stderr, "%s\n", cfg.Dump("init", true).c_str());
    fprintf(stderr, "# ---------------------------------------\n");
    fprintf(stderr, "# ------------- s y s c o n f i g -------\n");
    fprintf(stderr, "# ---------------------------------------\n");
    fprintf(stderr, "%s\n", cfg.Dump("sysconfig", true).c_str());
    fprintf(stderr, "# ---------------------------------------\n");
    fprintf(stderr, "# ------------- m o d u l e s -----------\n");
    fprintf(stderr, "# ---------------------------------------\n");
    fprintf(stderr, "%s\n", cfg.Dump("modules", true).c_str());
    fprintf(stderr, "# ---------------------------------------\n");
    fprintf(stderr, "# ------------- x r o o t d  ------------\n");
    fprintf(stderr, "# ---------------------------------------\n");
    fprintf(stderr, "# running config file: %s\n", cfile.c_str());
    fprintf(stderr, "%s\n", cfg.Dump(chapter.c_str(), true).c_str());
    fprintf(stderr, "#########################################\n");
  } else if (option == "module-init") {
    std::string initfile = "/tmp/.eos.daemon.init";
    std::string initsection = name.c_str();
    initsection += ":init";

    if (!eos::common::StringConversion::SaveStringIntoFile(initfile.c_str(),
        cfg.Dump(initsection.c_str(), true))) {
      fprintf(stderr, "error: unable to create startup config file '%s'\n",
              initfile.c_str());
      global_retc = errno;
      return (0);
    } else {
      chmod(initfile.c_str(), S_IRWXU);
      // run the init file
      int rc = system(initfile.c_str());

      if (WEXITSTATUS(rc)) {
        fprintf(stderr, "error: init script '%s' failed with errc=%d\n",
                initsection.c_str(), WEXITSTATUS(rc));
        global_retc = WEXITSTATUS(rc);
        return (0);
      }
    }
  } else if (option == "stack") {
    std::string kline;
    kline = "test -e ";
    kline += envfile.c_str();
    kline += " && eu-stack -p `cat ";
    kline += envfile.c_str();
    kline += "| cut -d '&' -f 1 | cut -d '=' -f 2`";
    int rc = system(kline.c_str());
    fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
    global_retc = WEXITSTATUS(rc);
    return (0);
  } else if (option == "jwk") {
    XrdOucString jwkfile = name.c_str();
    struct stat buf;
    if (::stat(jwkfile.c_str(), &buf)) {
      fprintf(stderr, "error: jwk key file '%s' does not exist!\n", jwkfile.c_str());
      global_retc = ENOENT;
      return (0);
    }
    std::string kline;
    kline = "env EOS_JWK=\"$(cat \"";
    kline += jwkfile.c_str();
    kline += "\")\" /sbin/eos-jwk-https";
    int rc = system(kline.c_str());
    fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
    global_retc = WEXITSTATUS(rc);
    return (0);
  } else if (option == "kill") {
    std::string kline;
    kline = "test -e ";
    kline += envfile.c_str();
    kline += " && kill -9 `cat ";
    kline += envfile.c_str();
    kline += "| cut -d '&' -f 1 | cut -d '=' -f 2`";
    int rc = system(kline.c_str());
    fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
    global_retc = WEXITSTATUS(rc);
    return (0);
  } else if (option == "stop") {
    std::string kline;
    kline = "test -e ";
    kline += envfile.c_str();
    kline += " && kill -15 `cat ";
    kline += envfile.c_str();
    kline += "| cut -d '&' -f 1 | cut -d '=' -f 2`";
    int rc = system(kline.c_str());
    fprintf(stderr, "info: run '%s' retc=%d\n", kline.c_str(), WEXITSTATUS(rc));
    global_retc = WEXITSTATUS(rc);
    return (0);
  } else if (option == "run") {
    if (!cfg.Has(chapter.c_str())) {
      fprintf(stderr,
              "error: missing service configuration [%s] in generic config file '/etc/eos/config/generic/all' or '/etc/eos/config/%s/%s'\n",
              chapter.c_str(), service.c_str(), name.c_str());
      global_retc = EINVAL;
      return (0);
    }

    char** const envv = cfg.Env("sysconfig");

    for (size_t i = 0; i < 1024; ++i) {
      if (envv[i]) {
        fprintf(stderr, "%s\n", envv[i]);
      } else {
        break;
      }
    }

    if (cfg.Has("init")) {
      fprintf(stderr, "# ---------------------------------------\n");
      fprintf(stderr, "# ------------- i n i t -----------------\n");
      fprintf(stderr, "# ---------------------------------------\n");

      if (cfg.Has("unshare")) {
        fprintf(stderr, "# ---------------------------------------\n");
        fprintf(stderr, "# ------------- u n s h a r e -----------\n");

        if (unshare(CLONE_NEWNS)) {
          fprintf(stderr, "warning: failed to unshare mount namespace errno=%d\n", errno);
        }

        if (mount("none", "/", NULL, MS_REC | MS_PRIVATE, NULL)) {
          fprintf(stderr, "warning: failed none mount / - errno=%d\n", errno);
        }

        fprintf(stderr, "# ---------------------------------------\n");
      }

      for (auto it : cfg["init"]) {
        bool exit_on_failure = false;
        fprintf(stderr, "# run: %s\n", it.c_str());
        pid_t pid;
        std::string cline = it;

        if (cline.substr(0, 4) == "enc:") {
          std::string key;
          const char* pkey = subtokenizer.GetToken();

          if (!pkey) {
            key = eos::common::StringConversion::StringFromShellCmd("cat /etc/eos.keytab | grep u:daemon | md5sum");
          } else {
            key = pkey;
          }

          std::string shakey = eos::common::SymKey::HexSha256(key);
          XrdOucString in = cline.substr(4).c_str();;
          XrdOucString out;
          eos::common::SymKey::SymmetricStringDecrypt(in, out, (char*)shakey.c_str());

          if (!out.c_str()) {
            fprintf(stderr, "error: encoded init line '%s' cannot be decoded\n",
                    in.c_str());
            continue;
          }

          cline = out.c_str();
          exit_on_failure = true;
        }

        if (exit_on_failure) {
          // test that nobody traces us ..
          if (!(pid = fork())) {
            pause();
            exit(0);
          } else {
            if (ptrace(EOS_PTRACE_ATTACH, pid, 0, 0)) {
              kill(pid, SIGKILL);
              fprintf(stderr,
                      "error: failed to attach to forked process pid=%d errno=%d - we are untraceable\n",
                      pid, errno);

              if (exit_on_failure) {
                exit(-1);
              }
            } else {
              ptrace(EOS_PTRACE_DETACH, pid, 0, 0);
              kill(pid, 9);
              waitpid(pid, 0, 0);
            }
          }
        }

        if (!(pid = fork())) {
          execle("/bin/bash", "eos-bash", "-c", cline.c_str(), NULL, envv);
          exit(0);
        } else {
          waitpid(pid, 0, 0);
        }
      }
    }

    if (ok) {
      fprintf(stderr, "# ---------------------------------------\n");
      fprintf(stderr, "# ------------- x r o o t d  ------------\n");
      fprintf(stderr, "# ---------------------------------------\n");
      fprintf(stderr, "# running config file: %s\n", cfile.c_str());
      fprintf(stderr, "# ---------------------------------------\n");
      fprintf(stderr, "%s\n", cfg.Dump(chapter.c_str(), true).c_str());
      fprintf(stderr, "#########################################\n");
      eos::common::Path cPath(cfile.c_str());

      if (!cPath.MakeParentPath(0x1ed)) {
        fprintf(stderr, "error: unable to create run directory '%s'\n",
                cPath.GetParentPath());
        global_retc = errno;
        return (0);
      }

      if (!eos::common::StringConversion::SaveStringIntoFile(cfile.c_str(),
          cfg.Dump(chapter.c_str(), true))) {
        fprintf(stderr, "error: unable to create startup config file '%s'\n",
                cfile.c_str());
        global_retc = errno;
        return (0);
      }

      ::chdir(cPath.GetParentPath());
      std::string logfile = "/var/log/eos/xrdlog.";
      logfile += service.c_str();

      if (service == "qdb") {
        execle("/opt/eos/xrootd/bin/xrootd", executable.c_str(), "-n", name.c_str(),
               "-c", cfile.c_str(), "-l", logfile.c_str(), "-R", "daemon", "-k", "fifo", "-s",
               pidfile.c_str(), NULL, envv);
      } else {
        execle("/opt/eos/xrootd/bin/xrootd", executable.c_str(), "-n", name.c_str(),
               "-c", cfile.c_str(), "-l", logfile.c_str(), "-R", "daemon", "-s",
               pidfile.c_str(), NULL, envv);
      }

      return (0);
    } else {
      fprintf(stderr, "error: rc=%d msg=%s\n", cfg.getErrc(), cfg.getMsg().c_str());
      global_retc = cfg.getErrc();
      return (0);
    }
  }

  return (0);
com_daemon_usage:
  fprintf(stdout,
          "usage: daemon config|sss|kill|run|stack|stop|jwk|module-init <service> [name] [subcmd]                                     :  \n");
  fprintf(stdout,
          "                <service> := mq | mgm | fst | qdb\n");
  fprintf(stdout,
          "                config                                                -  configure a service / show configuration\n");
  fprintf(stdout,
          "                kill                                                  -  kill -9 a given service\n");
  fprintf(stdout,
          "                run                                                   -  run the given service daemon optionally identified by name\n");
  fprintf(stdout,
          "                sss recreate                                          -  re-create an instance sss key and the eosnobody keys (/etc/eos.keytab,/etc/eos/fuse.sss.keytab)'\n");
  fprintf(stdout,
          "                stack                                                 -  print an 'eu-stack'\n");
  fprintf(stdout,
          "                stop                                                  -  kill -15 a given service\n");
  fprintf(stdout,
	  "                jwk                                                   -  run a 'jwk' public key server on port 4443\n");
  fprintf(stdout,
          "                module-init                                           -  run the init procedure for a module\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "      examples: eos daemon config qdb qdb coup                        -  try to make instance [qdb] a leader of QDB\n");
  fprintf(stdout,
          "                eos daemon config qdb qdb info                        -  show raft-info for the [qdb] QDB instance\n");
  fprintf(stdout,
          "                eos daemon config qdb qdb remove host:port            -  remove a member of the qdb cluster\n");
  fprintf(stdout,
          "                eos daemon config qdb qdb add host:port               -  add an observer to the qdb cluster\n");
  fprintf(stdout,
          "                eos daemon config qdb qdb promote host:port           -  promote an observer to a full member of the qdb cluster\n");
  fprintf(stdout,
          "                eos daemon config qdb qdb new observer                -  create a new observer\n");
  fprintf(stdout,
          "                eos daemon config fst fst.1                           -  show the init,sysconfig and xrootd config for the [fst.1] FST service\n");
  fprintf(stdout,
          "                eos daemon kill mq                                    -  shoot the MQ service with signal -9\n");
  fprintf(stdout,
          "                eos daemon stop mq                                    -  gracefully shut down the MQ service with signal -15\n");
  fprintf(stdout,
          "                eos daemon stack mgm                                  -  take an 'eu-stack' of the MGM service\n");
  fprintf(stdout,
          "                eos daemon run fst fst.1                              -  run the fst.1 subservice FST\n");
  global_retc = EINVAL;
  return (0);
#endif
}
