//------------------------------------------------------------------------------
//! @file ProcCommand.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "ProcCommand.hh"
#include "common/Path.hh"
#include "mgm/XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ProcCommand::ProcCommand():
  pVid(0), mPath(""),  mCmd(""), mSubCmd(""), mArgs(""), stdOut(""), stdErr(""),
  stdJson(""), retc(0), mResultStream(""), pOpaque(0), ininfo(0), mDoSort(false),
  mSelection(0), mOutFormat(""), mOutDepth(0), fstdout(0), fstderr(0),
  fresultStream(0), fstdoutfilename(""), fstderrfilename(""),
  fresultStreamfilename(""), mError(0), mComment(""), mLen(0), mAdminCmd(false),
  mUserCmd(false), mFuseFormat(false), mJsonFormat(false), mHttpFormat(false),
  mClosed(false), mJsonCallback("")
{
  mExecTime = time(NULL);
}

//------------------------------------------------------------------------------
// Constructor with parameter
//------------------------------------------------------------------------------
ProcCommand::ProcCommand(eos::common::Mapping::VirtualIdentity& vid):
  ProcCommand()
{
  pVid = &vid;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ProcCommand::~ProcCommand()
{
  if (fstdout) {
    fclose(fstdout);
    fstdout = 0;
    unlink(fstdoutfilename.c_str());
  }

  if (fstderr) {
    fclose(fstderr);
    fstderr = 0;
    unlink(fstderrfilename.c_str());
  }

  if (fresultStream) {
    fclose(fresultStream);
    fresultStream = 0;
    unlink(fresultStreamfilename.c_str());
  }

  if (pOpaque) {
    delete pOpaque;
    pOpaque = 0;
  }
}

//------------------------------------------------------------------------------
// Open temporary output files for results of find commands
//------------------------------------------------------------------------------
bool
ProcCommand::OpenTemporaryOutputFiles()
{
  char tmpdir [4096];
  snprintf(tmpdir, sizeof(tmpdir) - 1, "/tmp/eos.mgm/%llu",
           (unsigned long long) XrdSysThread::ID());
  fstdoutfilename = tmpdir;
  fstdoutfilename += ".stdout";
  fstderrfilename = tmpdir;
  fstderrfilename += ".stderr";
  fresultStreamfilename = tmpdir;
  fresultStreamfilename += ".mResultstream";
  eos::common::Path cPath(fstdoutfilename.c_str());

  if (!cPath.MakeParentPath(S_IRWXU)) {
    eos_err("Unable to create temporary outputfile directory %s", tmpdir);
    return false;
  }

  // own the directory by daemon
  if (::chown(cPath.GetParentPath(), 2, 2)) {
    eos_err("Unable to own temporary outputfile directory %s",
            cPath.GetParentPath());
  }

  fstdout = fopen(fstdoutfilename.c_str(), "w");
  fstderr = fopen(fstderrfilename.c_str(), "w");
  fresultStream = fopen(fresultStreamfilename.c_str(), "w+");

  if ((!fstdout) || (!fstderr) || (!fresultStream)) {
    if (fstdout) {
      fclose(fstdout);
    }

    if (fstderr) {
      fclose(fstderr);
    }

    if (fresultStream) {
      fclose(fresultStream);
    }

    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Open a proc command e.g. call the appropriate user or admin commmand and
// store the output in a resultstream of in case of find in temporary output
// files.
//------------------------------------------------------------------------------
int
ProcCommand::open(const char* inpath, const char* info,
                  eos::common::Mapping::VirtualIdentity& vid_in,
                  XrdOucErrInfo* error)
{
  pVid = &vid_in;
  mClosed = false;
  mPath = inpath;
  mDoSort = false;
  mError = error;
  ininfo = info;

  if ((mPath.beginswith("/proc/admin"))) {
    mAdminCmd = true;
  }

  if (mPath.beginswith("/proc/user")) {
    mUserCmd = true;
  }

  // Deal with '&' ... sigh
  XrdOucString sinfo = ininfo;

  for (int i = 0; i < sinfo.length(); i++) {
    if (sinfo[i] == '&') {
      // figure out if this is a real separator or
      XrdOucString follow = sinfo.c_str() + i + 1;

      if (!follow.beginswith("mgm.") && (!follow.beginswith("eos.")) &&
          (!follow.beginswith("xrd.")) && (!follow.beginswith("callback"))) {
        sinfo.erase(i, 1);
        sinfo.insert("#AND#", i);
      }
    }
  }

  pOpaque = new XrdOucEnv(sinfo.c_str());

  if (!pOpaque) {
    // alloc failed
    return SFS_ERROR;
  }

  mOutFormat = "";
  mOutDepth = 0;
  mCmd = pOpaque->Get("mgm.cmd");
  mSubCmd = pOpaque->Get("mgm.subcmd");
  mOutFormat = pOpaque->Get("mgm.outformat");
  long depth = pOpaque->GetInt("mgm.outdepth");

  if (depth > 0) {
    mOutDepth = (unsigned)depth;
  }

  mSelection = pOpaque->Get("mgm.selection");
  mComment = pOpaque->Get("mgm.comment") ? pOpaque->Get("mgm.comment") : "";
  mJsonCallback = pOpaque->Get("callback") ? pOpaque->Get("callback") : "";
  eos_static_debug("json-callback=%s opaque=%s", mJsonCallback.c_str(),
                   sinfo.c_str());
  int envlen = 0;
  mArgs = pOpaque->Env(envlen);
  mFuseFormat = false;
  mJsonFormat = false;
  mHttpFormat = false;
  // If set to FUSE, don't print the stdout,stderr tags and we guarantee a line
  // feed in the end
  XrdOucString format = pOpaque->Get("mgm.format");

  if (format == "fuse") {
    mFuseFormat = true;
  }

  if (format == "json") {
    mJsonFormat = true;
  }

  if (format == "http") {
    mHttpFormat = true;
  }

  stdOut = "";
  stdErr = "";
  retc = 0;
  mResultStream = "";
  mLen = 0;
  mDoSort = true;

  if (mJsonCallback.length()) {
    mJsonFormat = true;
  }

  // Admin command section
  if (mAdminCmd) {
    if (mCmd == "archive") {
      Archive();
      mDoSort = false;
    } else if (mCmd == "backup") {
      Backup();
      mDoSort = false;
    } else if (mCmd == "access") {
      Access();
      mDoSort = false;
    } else if (mCmd == "config") {
      Config();
      mDoSort = false;
    } else if (mCmd == "node") {
      Node();
      mDoSort = false;
    } else if (mCmd == "space") {
      Space();
      mDoSort = false;
    } else if (mCmd == "geosched") {
      GeoSched();
      mDoSort = false;
    } else if (mCmd == "group") {
      Group();
      mDoSort = false;
    } else if (mCmd == "fs") {
      Fs();
      mDoSort = false;
    } else if (mCmd == "ns") {
      Ns();
      mDoSort = false;
    } else if (mCmd == "io") {
      Io();
      mDoSort = false;
    } else if (mCmd == "fsck") {
      Fsck();
      mDoSort = false;
    } else if (mCmd == "quota") {
      AdminQuota();
      mDoSort = false;
    } else if (mCmd == "transfer") {
      Transfer();
      mDoSort = false;
    } else if (mCmd == "debug") {
      Debug();
    } else if (mCmd == "vid") {
      Vid();
    } else if (mCmd == "vst") {
      Vst();
      mDoSort = false;
    } else if (mCmd == "rtlog") {
      Rtlog();
      mDoSort = false;
    } else {
      // command is not implemented
      stdErr += "error: no such admin command '";
      stdErr += mCmd;
      stdErr += "'";
      retc = EINVAL;
    }

    MakeResult();
    return SFS_OK;
  }

  // User command section
  if (mUserCmd) {
    if (mCmd == "accounting") {
      Accounting();
      mDoSort = false;
    } else if (mCmd == "archive") {
      Archive();
      mDoSort = false;
    } else if (mCmd == "motd") {
      Motd();
      mDoSort = false;
    } else if (mCmd == "version") {
      Version();
      mDoSort = false;
    } else if (mCmd == "quota") {
      Quota();
      mDoSort = false;
    } else if (mCmd == "who") {
      Who();
      mDoSort = false;
    } else if (mCmd == "fuse") {
      return Fuse();
    } else if (mCmd == "fusex") {
      return FuseX();
    } else if (mCmd == "file") {
      File();
      mDoSort = false;
    } else if (mCmd == "fileinfo") {
      Fileinfo();
      mDoSort = false;
    } else if (mCmd == "mkdir") {
      Mkdir();
    } else if (mCmd == "rmdir") {
      Rmdir();
    } else if (mCmd == "cd") {
      Cd();
      mDoSort = false;
    } else if (mCmd == "chown") {
      Chown();
    } else if (mCmd == "ls") {
      Ls();
      mDoSort = false;
    } else if (mCmd == "rm") {
      Rm();
    } else if (mCmd == "whoami") {
      Whoami();
      mDoSort = false;
    } else if (mCmd == "find") {
      Find();
    } else if (mCmd == "map") {
      Map();
    } else if (mCmd == "member") {
      Member();
    } else if (mCmd == "attr") {
      Attr();
      mDoSort = false;
    } else if (mCmd == "chmod") {
      Chmod();
    } else if (mCmd == "recycle") {
      Recycle();
      mDoSort = false;
    } else {
      // Command not implemented
      stdErr += "errro: no such user command '";
      stdErr += mCmd;
      stdErr += "'";
      retc = EINVAL;
    }

    MakeResult();
    return SFS_OK;
  }

  // If neither admin nor proc command
  return gOFS->Emsg((const char*) "open", *error, EINVAL,
                    "execute command - not implemented ", ininfo);
}

//------------------------------------------------------------------------------
// Read a part of the result stream produced during open
//------------------------------------------------------------------------------
int
ProcCommand::read(XrdSfsFileOffset boff, char* buff, XrdSfsXferSize blen)
{
  if (fresultStream) {
    // file based results go here ...
    if ((fseek(fresultStream, boff, 0)) == 0) {
      size_t nread = fread(buff, 1, blen, fresultStream);

      if (nread > 0) {
        return nread;
      }
    } else {
      eos_err("seek to %llu failed\n", boff);
    }

    return 0;
  } else {
    if (mLen - boff <= 0) {
      return 0;
    }

    // Memory based results go here ...
    if (((unsigned int) blen <= (mLen - boff))) {
      memcpy(buff, mResultStream.c_str() + boff, blen);
      return blen;
    } else {
      memcpy(buff, mResultStream.c_str() + boff, (mLen - boff));
      return (mLen - boff);
    }
  }
}

//------------------------------------------------------------------------------
// Return stat information for the result stream to tell the client the size
// of the proc output.
//------------------------------------------------------------------------------
int
ProcCommand::stat(struct stat* buf)
{
  memset(buf, 0, sizeof(struct stat));
  buf->st_size = mLen;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Close the proc stream and store the clients comment for the command in the
// comment log file.
//------------------------------------------------------------------------------
int
ProcCommand::close()
{
  if (!mClosed) {
    // Only instance users or sudoers can add to the log book
    if ((pVid->uid <= 2) || (pVid->sudoer)) {
      if (mComment.length() && gOFS->commentLog) {
        if (!gOFS->commentLog->Add(mExecTime, mCmd.c_str(), mSubCmd.c_str(),
                                   mArgs.c_str(), mComment.c_str(), stdErr.c_str(), retc)) {
          eos_err("failed to log to comment log file");
        }
      }
    }

    mClosed = true;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Build the inmemory result of the stdout,stderr & retc of the proc command.
// Depending on the output format the key-value CGI returned changes => see
// implementation.
//------------------------------------------------------------------------------
void
ProcCommand::MakeResult()
{
  mResultStream = "";

  if (!fstdout) {
    if (mDoSort) {
      eos::common::StringConversion::SortLines(stdOut);
    }

    if ((!mFuseFormat && !mJsonFormat && !mHttpFormat)) {
      // The default format
      mResultStream = "mgm.proc.stdout=";
      mResultStream += XrdMqMessage::Seal(stdOut);
      mResultStream += "&mgm.proc.stderr=";
      mResultStream += XrdMqMessage::Seal(stdErr);
      mResultStream += "&mgm.proc.retc=";
      mResultStream += retc;
    }

    if (mFuseFormat || mHttpFormat) {
      if (mFuseFormat) {
        mResultStream += stdOut;
      } else {
        mResultStream +=
          "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\" \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">\n";
        mResultStream += "<html>\n";
        mResultStream +=
          "<TITLE>EOS-HTTP</TITLE> <link rel=\"stylesheet\" href=\"http://www.w3.org/StyleSheets/Core/Midnight\"> \n";
        mResultStream += "<meta charset=\"utf-8\"> \n";
        mResultStream += "<div class=\"httptable\" id=\"";
        mResultStream += mCmd;
        mResultStream += "_";
        mResultStream += mSubCmd;
        mResultStream += "\">\n";

        // FUSE format contains only STDOUT
        if (stdOut.length() && KeyValToHttpTable(stdOut)) {
          mResultStream += stdOut;
        } else {
          if (stdErr.length() || retc) {
            mResultStream += stdOut;
            mResultStream += "<h3>&#9888;&nbsp;<font color=\"red\">";
            mResultStream += stdErr;
            mResultStream += "</font></h3>";
          } else {
            if (!stdOut.length()) {
              mResultStream += "<h3>&#10004;&nbsp;";
              mResultStream += "Success!";
              mResultStream += "</h3>";
            } else {
              mResultStream += stdOut;
            }
          }
        }

        mResultStream += "</div>";
      }
    }

    if (mJsonFormat) {
      if (!stdJson.length()) {
        Json::Value json;
        Json::Value jsonresult;
        json["errormsg"] = stdErr.c_str();
        std::stringstream ss;
        ss << retc;
        json["retc"] = ss.str();
        ss.str(stdOut.c_str());
        std::string line;

        do {
          Json::Value jsonentry;
          line.clear();

          if (!std::getline(ss, line)) {
            break;
          }

          if (!line.length()) {
            continue;
          }

          XrdOucString sline = line.c_str();

          while (sline.replace("<n>", "n")) {}

          while (sline.replace("?configstatus@rw", "_rw")) {}

          line = sline.c_str();
          std::map <std::string , std::string> map;
          eos::common::StringConversion::GetKeyValueMap(line.c_str(), map, "=", " ");
          // these values violate the JSON hierarchy and have to be rewritten
          eos::common::StringConversion::ReplaceMapKey(map, "cfg.balancer",
              "cfg.balancer.status");
          eos::common::StringConversion::ReplaceMapKey(map, "cfg.geotagbalancer",
              "cfg.geotagbalancer.status");
          eos::common::StringConversion::ReplaceMapKey(map, "cfg.geobalancer",
              "cfg.geobalancer.status");
          eos::common::StringConversion::ReplaceMapKey(map, "cfg.groupbalancer",
              "cfg.groupbalancer.status");
          eos::common::StringConversion::ReplaceMapKey(map, "cfg.wfe", "cfg.wfe.status");
          eos::common::StringConversion::ReplaceMapKey(map, "cfg.lru", "cfg.lru.status");
          eos::common::StringConversion::ReplaceMapKey(map, "stat.health",
              "stat.health.status");
          eos::common::StringConversion::ReplaceMapKey(map, "balancer",
              "balancer.status");
          eos::common::StringConversion::ReplaceMapKey(map, "converter",
              "converter.status");
          eos::common::StringConversion::ReplaceMapKey(map, "geotagbalancer",
              "geotagbalancer.status");
          eos::common::StringConversion::ReplaceMapKey(map, "geobalancer",
              "geobalancer.status");
          eos::common::StringConversion::ReplaceMapKey(map, "groupbalancer",
              "groupbalancer.status");

          for (auto it = map.begin(); it != map.end(); ++it) {
            std::vector<std::string> token;
            eos::common::StringConversion::Tokenize(it->first, token, ".");
            char* conv;
            double val;
            errno = 0;
            val = strtod(it->second.c_str(), &conv);
            std::string value;

            if (it->second.length()) {
              value = it->second.c_str();
            } else {
              value = "NULL";
            }

            if (token.empty()) {
              continue;
            }

            auto* jep = &(jsonentry[token[0]]);

            for (int i = 1; i < (int)token.size(); i++) {
              jep = &((*jep)[token[i]]);
            }

            if (errno || (!val && (conv  == it->second.c_str())) ||
                ((conv - it->second.c_str()) != (long long)it->second.length())) {
              // non numeric
              (*jep) = value;
            } else {
              // numeric
              (*jep) = val;
            }
          }

          jsonresult.append(jsonentry);
        } while (1);

        if (mCmd.length()) {
          if (mSubCmd.length()) {
            json[mCmd.c_str()][mSubCmd.c_str()] = jsonresult;
          } else {
            json[mCmd.c_str()] = jsonresult;
          }
        } else {
          json["result"] = jsonresult;
        }

        std::stringstream r;
        r << json;

        if (mJsonCallback.length()) {
          // JSONP
          mResultStream = mJsonCallback;
          mResultStream += "([\n";
          mResultStream += r.str().c_str();
          mResultStream += "\n]);";
        } else {
          // JSON
          if (!vid.prot.beginswith("http")) {
            mResultStream = "mgm.proc.json=";
          }

          mResultStream += r.str().c_str();
        }
      } else {
        if (mJsonCallback.length()) {
          // JSONP
          mResultStream = mJsonCallback;
          mResultStream += "([\n";
          mResultStream += stdJson;
          mResultStream += "\n]);";
        } else {
          if (!vid.prot.beginswith("http")) {
            mResultStream = "mgm.proc.json=";
          }

          mResultStream += stdJson;
        }
      }
    }

    if (!mResultStream.endswith('\n')) {
      mResultStream += "\n";
    }

    if (retc) {
      eos_static_err("%s (errno=%u)", stdErr.c_str(), retc);
    }

    mLen = mResultStream.length();
  } else {
    // File based results CANNOT be sorted and don't have mFuseFormat
    if (!mFuseFormat) {
      // Create the stdout result
      if (!fseek(fstdout, 0, 0) &&
          !fseek(fstderr, 0, 0) &&
          !fseek(fresultStream, 0, 0)) {
        fprintf(fresultStream, "&mgm.proc.stdout=");
        std::ifstream inStdout(fstdoutfilename.c_str());
        std::ifstream inStderr(fstderrfilename.c_str());
        std::string entry;

        while (std::getline(inStdout, entry)) {
          XrdOucString sentry = entry.c_str();
          sentry += "\n";

          if (!mFuseFormat) {
            XrdMqMessage::Seal(sentry);
          }

          fprintf(fresultStream, "%s", sentry.c_str());
        }

        // Close and remove - if this fails there is nothing to recover anyway
        fclose(fstdout);
        fstdout = 0;
        unlink(fstdoutfilename.c_str());
        // Create the stderr result
        fprintf(fresultStream, "&mgm.proc.stderr=");

        while (std::getline(inStderr, entry)) {
          XrdOucString sentry = entry.c_str();
          sentry += "\n";
          XrdMqMessage::Seal(sentry);
          fprintf(fresultStream, "%s", sentry.c_str());
        }

        // Close and remove - if this fails there is nothing to recover anyway
        fclose(fstderr);
        fstderr = 0;
        unlink(fstderrfilename.c_str());
        fprintf(fresultStream, "&mgm.proc.retc=%d", retc);
        mLen = ftell(fresultStream);
        // Spool the resultstream to the beginning
        fseek(fresultStream, 0, 0);
      } else {
        eos_static_err("cannot seek to position 0 in result files");
      }
    }
  }
}

//------------------------------------------------------------------------------
// Try to detect and convert a monitor output format and convert it into a
// nice http table
//------------------------------------------------------------------------------
bool
ProcCommand::KeyValToHttpTable(XrdOucString& stdOut)
{
  while (stdOut.replace("= ", "=\"\"")) {
  }

  std::string stmp = stdOut.c_str();
  XrdOucTokenizer tokenizer((char*) stmp.c_str());
  const char* line;
  bool ok = true;
  std::vector<std::string> keys;
  std::vector < std::map < std::string, std::string >> keyvaluetable;
  std::string table;

  while ((line = tokenizer.GetLine())) {
    if (strlen(line) <= 1) {
      continue;
    }

    std::map<std::string, std::string> keyval;

    if (eos::common::StringConversion::GetKeyValueMap(line,
        keyval,
        "=",
        " ",
        &keys)) {
      keyvaluetable.push_back(keyval);
    } else {
      ok = false;
      break;
    }
  }

  if (ok) {
    table +=
      R"literal(<style>
table
{
  table-layout:auto;
}
</style>
)literal";

    table += "<table border=\"8\" cellspacing=\"10\" cellpadding=\"20\">\n";
    // build the header
    table += "<tr>\n";
    for (size_t i = 0; i < keys.size(); i++)
    {
      table += "<th>";
      table += "<font size=\"2\">";
      // for keys don't print lengthy strings like a.b.c.d ... just print d
      std::string dotkeys = keys[i];
      size_t pos = dotkeys.rfind(".");
      if (pos != std::string::npos)
        dotkeys.erase(0, pos + 1);
      //table += dotkeys;
      table += keys[i];
      table += "</font>";
      table += "</th>";
      table += "\n";
    }
    table += "</tr>\n";

    // build the rows

    for (size_t i = 0; i < keyvaluetable.size(); i++)
    {
      table += "<tr>\n";
      for (size_t j = 0; j < keys.size(); j++)
      {
        table += "<td nowrap=\"nowrap\">";
        table += "<font size=\"2\">";
        XrdOucString sizestring = keyvaluetable[i][keys[j]].c_str();
        unsigned long long val = eos::common::StringConversion::GetSizeFromString(sizestring);
        if (errno || val == 0 || (!sizestring.isdigit()))
        {
          XrdOucString decodeURI = keyvaluetable[i][keys[j]].c_str();
          // we need to remove URI encoded spaces now
          while (decodeURI.replace("%20", " "))
          {
          }
          table += decodeURI.c_str();
        }
        else
        {
          eos::common::StringConversion::GetReadableSizeString(sizestring, val, "");
          table += sizestring.c_str();
        }
        table += "</font>";
        table += "</td>";
      }
      table += "</tr>\n";
      table += "\n";
    }


    table += "</table>\n";
    stdOut = table.c_str();
  }
  return ok;
}

//------------------------------------------------------------------------------
// Get a file's full path using the fid information stored in the opaque data
//------------------------------------------------------------------------------
void
ProcCommand::GetPathFromFid(XrdOucString& path, XrdOucEnv* opaque,
                            const std::string& err_msg)
{
  std::string tag = "mgm.file.id";

  if (path == "") {
    unsigned long long fid = strtoull(pOpaque->Get(tag.c_str()), 0, 10);

    if (fid == 0ULL) {
      stdErr += "error: fid unknown!";
      retc = errno;
      return;
    }

    try {
      std::string temp =
        gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(fid).get());
      path = XrdOucString(temp.c_str());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      stdErr = err_msg.c_str();
      stdErr += e.getMessage().str().c_str();
      stdErr += "\n";
      eos_debug("caught exception %d %s\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }
}

EOSMGMNAMESPACE_END
