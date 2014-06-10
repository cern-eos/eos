//------------------------------------------------------------------------------
// File: Archive.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

static const std::string ARCH_INIT = "archive.init";
static const std::string ARCH_MIG_DONE = "archive.migrate.done";
static const std::string ARCH_MIG_ERR = "archive.migrate.err";
static const std::string ARCH_STAGE_DONE = "archive.stage.done";
static const std::string ARCH_STAGE_ERR = "archive.stage.err";
static const std::string ARCH_PURGE_DONE = "archive.purge.done";
static const std::string ARCH_PURGE_ERR = "archive.purge.err";
static const std::string ARCH_LOG = "archive.log";


//------------------------------------------------------------------------------
// Archive command
//------------------------------------------------------------------------------
int
ProcCommand::Archive()
{
  
  struct stat statinfo;
  std::ostringstream cmd_json;
  std::string option = (pOpaque->Get("mgm.archive.option") ?
                        pOpaque->Get("mgm.archive.option") : "");

  // For listing we don't need an EOS path
  if (mSubCmd == "list") 
  {
    if (option.empty())
    {
      stdErr = "error: need to provide the archive listing type";
      retc = EINVAL;
    }
    else
    {
      cmd_json << "{\"cmd\": " << "\"" << mSubCmd.c_str() << "\", "
               << "\"opt\": " <<  "\"" << option << "\""
               << "}";
    }
  }
  else
  {
    XrdOucString spath = pOpaque->Get("mgm.archive.path");
    const char* inpath = spath.c_str();
    NAMESPACEMAP;
    if (info) info = 0;
    PROC_BOUNCE_ILLEGAL_NAMES;
    PROC_BOUNCE_NOT_ALLOWED;
    eos::common::Path cPath(path);
    spath = cPath.GetPath();
    std::ostringstream dir_stream;
    dir_stream << "root://" << gOFS->ManagerId.c_str() << "/" << spath.c_str();
    std::string dir_url = dir_stream.str();

    // Check that the requested path exists and is a directory
    if (gOFS->_stat(spath.c_str(), &statinfo, *mError, *pVid))
    {
      stdErr = "error: requested path does not exits";
      retc = EINVAL;
      return SFS_OK;
    }

    if (!S_ISDIR(statinfo.st_mode))
    {
      stdErr = "error:archive path is not a directory";
      retc = EINVAL;
      return SFS_OK;
    }

    if (mSubCmd == "create")
    {
      if (!pOpaque->Get("mgm.archive.dst"))
      {
        stdErr = "error: need to provide destination for archive creation";
        retc = EINVAL;
      }
      else
      {
        // Check that the directory is not already archived
        std::ostringstream oss;
        std::vector<std::string> vect_files; 
        std::vector<std::string> vect_paths;
        vect_files.push_back(ARCH_INIT); 
        vect_files.push_back(ARCH_MIG_DONE);
        vect_files.push_back(ARCH_MIG_ERR); 
        vect_files.push_back(ARCH_STAGE_DONE);
        vect_files.push_back(ARCH_STAGE_ERR); 
        vect_files.push_back(ARCH_PURGE_DONE);
        vect_files.push_back(ARCH_PURGE_ERR);
        vect_files.push_back(ARCH_LOG);
        
        // Create all possible file paths
        for (auto it = vect_files.begin(); it != vect_files.end(); ++it)
        {
          oss.str("");
          oss.clear();
          oss << spath.c_str() << "/" << *it;
          vect_paths.push_back(oss.str());
        }

        // Check that none of the "special" files exists
        for (auto it = vect_paths.begin(); it != vect_paths.end(); ++it)
        {
          if (!gOFS->stat(it->c_str(), &statinfo, *mError))
          {
            stdErr = "error: directory is already archived ";
            stdErr += spath.c_str();
            retc = EINVAL;
            break;
          }
        }

        if (!retc)
        {
          XrdOucString dst = pOpaque->Get("mgm.archive.dst");
          ArchiveCreate(spath, dst);
          return SFS_OK;
        }
      }
    }
    else if ((mSubCmd == "migrate") || 
             (mSubCmd == "stage") || 
             (mSubCmd == "purge"))
    {
      std::string arch_url = dir_url;
      arch_url += "/";
      
      if (option == "r")
      {
        // Retry failed migration/stage
        option = "retry";
        std::string arch_err = spath.c_str();
        arch_err += "/";
        
        if (mSubCmd == "migrate") 
        {
          arch_err += ARCH_MIG_ERR;
          arch_url += ARCH_MIG_ERR;
        }
        else if (mSubCmd == "stage") // stage retry
        {
          arch_err += ARCH_STAGE_ERR;
          arch_url += ARCH_STAGE_ERR;
        }
        else if (mSubCmd == "purge")
        {
          arch_err += ARCH_PURGE_ERR;
          arch_url += ARCH_PURGE_ERR;
        }
        
        if (gOFS->stat(arch_err.c_str(), &statinfo, *mError))
        {
          stdErr = "error: no failed migration/stage/purge file in directory: ";
          stdErr += spath.c_str();
          retc = EINVAL;
        }
      }
      else 
      {
        // Check that the init/migrate archive file exists
        option = ""; 
        std::string arch_path = spath.c_str();
        arch_path += "/"; 
        
        if (mSubCmd == "migrate") // migrate
        {
          arch_path += ARCH_INIT;
          arch_url += ARCH_INIT;
        }
        else if (mSubCmd == "stage") // stage
        {
          arch_path += ARCH_PURGE_DONE;
          arch_url += ARCH_PURGE_DONE;
        }
        else if (mSubCmd == "purge")
        {
          arch_path += ARCH_MIG_DONE;
          
          if (gOFS->stat(arch_path.c_str(), &statinfo, *mError))
          {
            arch_path = spath.c_str(); 
            arch_path += "/";
            arch_path += ARCH_STAGE_DONE;
        
            if (gOFS->stat(arch_path.c_str(), &statinfo, *mError))
            {
              stdErr = "error: purge can be done only after a successful " \
                "migration or stage operation";
              retc = EINVAL;
            }
            else 
            {
              arch_url += ARCH_STAGE_DONE;
            }
          }
          else
          {
            arch_url += ARCH_MIG_DONE;
          }
        }
        
        if ((mSubCmd != "purge") && gOFS->stat(arch_path.c_str(), &statinfo, *mError))
        {
          stdErr = "error: no archive purge file in directory: ";
          stdErr += spath.c_str();
          retc = EINVAL;
        }
      }
      
      cmd_json << "{\"cmd\": " << "\"" << mSubCmd.c_str() << "\", "
               << "\"src\": " << "\"" << arch_url << "\", "
               << "\"opt\": " << "\"" << option  << "\""
               << "}";
    }
    else if (mSubCmd == "delete")
    {
      cmd_json << "{ \"cmd\": " << "\"" << mSubCmd.c_str() << "\", "
               << "\"src\": " << "\"" << dir_url << "\", "
               << "\"opt\": " << " \"\"" 
               << " }";
    }
    else
    {
      stdErr = "error: operation not supported, needs to be one of the following: "
        "create, migrate, stage or list";
      retc = EINVAL;
    }
  }
  
  // Send request to archiver process if no error occured
  if (!retc)
  {
    int sock_linger = 0;
    int sock_timeout = 1000; // 1s
    zmq::context_t zmq_ctx;
    zmq::socket_t socket(zmq_ctx, ZMQ_REQ);
    socket.setsockopt(ZMQ_RCVTIMEO, &sock_timeout, sizeof(sock_timeout));
    socket.setsockopt(ZMQ_LINGER, &sock_linger, sizeof(sock_linger));
 
    try
    {
      socket.connect(XrdMgmOfs::msArchiveEndpoint.c_str());
    }
    catch (zmq::error_t& zmq_err)
    {
      eos_static_err("connect to archiver failed");
      stdErr = "error: connect to archiver failed";
      retc = EINVAL;
    }

    if (!retc)
    {
      std::string cmd = cmd_json.str();
      zmq::message_t msg((void*)cmd.c_str(), cmd.length(), NULL);

      try
      {
        if (!socket.send(msg))
        {
          stdErr = "error: send request to archiver";
          retc = EINVAL;
        }
        else if (!socket.recv(&msg))
        {
          stdErr = "error: no response from archiver";
          retc = EINVAL;
        }
        else 
        {
          // Parse response from the archiver
          XrdOucString msg_str((const char*) msg.data(), msg.size());
          eos_info("Msg_str:%s", msg_str.c_str());
          std::istringstream iss(msg_str.c_str());
          std::string status, line, response;
          iss >> status;
          
          while (getline(iss, line))
          {
            response += line;

            if (iss.good())
              response += '\n';
          }

          if (status == "OK")
          {
            stdOut = response.c_str();
          }
          else if (status == "ERROR")
          {
            stdErr = response.c_str();
            retc = EINVAL;            
          }
          else
          {
            stdErr = "error: unknown response format from archiver";
            retc = EINVAL;            
          } 
        }
      }
      catch (zmq::error_t& zmq_err)
      {
        stdErr = "error: timeout getting response from archiver, msg: ";
        stdErr += zmq_err.what();
        retc = EINVAL;
      }
    }
  }

  eos_static_info("retc=%i, stdOut=%s, stdErr=%s", retc, stdOut.c_str(), stdErr.c_str());
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Create archive file.
//------------------------------------------------------------------------------
void
ProcCommand::ArchiveCreate(const XrdOucString& arch_dir,
                           const XrdOucString& dst_url)
{
  int num_dirs = 0;
  int num_files = 0;

  if (ArchiveGetNumEntries(arch_dir, num_dirs, num_files))
    return;

  std::ostringstream sstr;
  sstr << "/tmp/eos.mgm/archive." << XrdSysThread::ID();
  std::string arch_fn = sstr.str();
  std::ofstream arch_ofs(arch_fn.c_str());

  if (!arch_ofs.is_open())
  {
    eos_err("failed to open local archive file:%s", arch_fn.c_str());
    stdErr = "failed to open archive file at MGM ";
    retc = EIO;
    return;
  }
    
  // Write archive JSON header
  arch_ofs << "{"
           << "\"src\": \"" << "root://" << gOFS->ManagerId << "/" << arch_dir << "\", "
           << "\"dst\": \"" << dst_url.c_str() << "\", "
           << "\"dir_meta\": [\"uid\", \"gid\", \"attr\"], "
           << "\"file_meta\": [\"size\", \"mtime\", \"ctime\", \"uid\", \"gid\", "
           << "\"xstype\", \"xs\"], "
           << "\"num_dirs\": " << num_dirs << ", "
           << "\"num_files\": " << num_files << "}" << std::endl; 

  // Add directories info
  if (ArchiveAddEntries(arch_dir, arch_ofs, false))
  {
    arch_ofs.close();
    unlink(arch_fn.c_str());
    return;
  }

  // Add files info
  if (ArchiveAddEntries(arch_dir, arch_ofs, true))
  {
    arch_ofs.close();
    unlink(arch_fn.c_str());
    return;
  }

  arch_ofs.close();

  // TODO: adapt the CopyProcess to XRootD 4.0
  // Copy local archive file to archive directory in EOS
  struct XrdCl::JobDescriptor copy_job;
  copy_job.source.SetProtocol("file");
  copy_job.source.SetPath(arch_fn.c_str());

  copy_job.target.SetProtocol("root");
  copy_job.target.SetHostName("localhost");
  std::string dst_path = arch_dir.c_str();
  dst_path += "/"; 
  dst_path += ARCH_INIT;
  copy_job.target.SetPath(dst_path);
  copy_job.target.SetParams("eos.ruid=0&eos.rgid=0");
  
  XrdCl::CopyProcess copy_proc;
  copy_proc.AddJob(&copy_job);
  XrdCl::XRootDStatus status_prep = copy_proc.Prepare();

  if (status_prep.IsOK())
  {
    XrdCl::XRootDStatus status_run = copy_proc.Run(0);

    if (!status_run.IsOK())
    {
      stdErr = "error: failed run for copy process, msg=";
      stdErr += status_run.ToStr().c_str();
      retc = EIO;        
    }
  }
  else
  {
    stdErr = "error: failed prepare for copy process, msg=";
    stdErr += status_prep.ToStr().c_str();
    retc = EIO;        
  }

  // Remove local archive file
  unlink(arch_fn.c_str());
}


//------------------------------------------------------------------------------
// Get number of entries(files/directories) in the archive subtree
// i.e. run "find --count /dir/"
//------------------------------------------------------------------------------
int
ProcCommand::ArchiveGetNumEntries(const XrdOucString& arch_dir,
                                  int& num_dirs,
                                  int& num_files)
{
  int nread = 0;
  XrdSfsFileOffset offset = 0;
  XrdSfsXferSize sz_buf = 4096;
  char buffer[sz_buf + 1];
  std::string rstr = "";
  ProcCommand* cmd_count = new ProcCommand();
  XrdOucString info = "&mgm.cmd=find&mgm.path=";
  info += arch_dir.c_str();
  info += "&mgm.option=Z";
  cmd_count->open("/proc/user", info.c_str(), *pVid, mError);

  // Response should be short
  while ((nread = cmd_count->read(offset, buffer, sz_buf)))
  {
    buffer[nread] = '\0';
    rstr += buffer;
    offset += nread;
  }

  int ret = cmd_count->close();
  delete cmd_count;

  if (ret)
  {
    eos_err("find --count on directory=%s failed", arch_dir.c_str());
    stdErr = "error: find --count on directory failed";
    retc = ret;
    return retc;
  }

  size_t spos = 0;
  XrdOucEnv renv(rstr.c_str());
  std::istringstream sstream(renv.Get("mgm.proc.stdout"));
  std::string sfiles, sdirs;
  sstream >> sfiles >> sdirs;
  std::string tag = "nfiles=";
      
  if ((spos = sfiles.find(tag)) == 0)
  {
    num_files = atoi(sfiles.substr(tag.length()).c_str());
  }
  else
  {
    stdErr = "error: nfiles not found";
    retc = ENOENT;
    return retc;
  }

  tag = "ndirectories=";

  if ((spos = sdirs.find(tag)) == 0)
  {
    num_dirs = atoi(sdirs.substr(tag.length()).c_str());
    num_dirs -= 1;  // don't count current dir
  }
  else
  {
    stdErr = "error: ndirectories not found";
    retc = ENOENT;
    return retc;
  }
  
  return retc;
}


//------------------------------------------------------------------------------
// Get fileinfo for all files/dirs in the subtree and add it to the archive.
// "find -d --fileinfo /dir/" for directories or 
// "find -f --fileinfo /dir/ for files.
//------------------------------------------------------------------------------
int
ProcCommand::ArchiveAddEntries(const XrdOucString& arch_dir,
                               std::ofstream& arch_ofs, 
                               bool is_file)
{
  std::map<std::string, std::string> info_map;
  std::map<std::string, std::string> attr_map; // only for dirs

  // These keys should match the ones in the header dictionary
  if (is_file)
  {
    info_map.insert(std::make_pair("file", "")); // file name
    info_map.insert(std::make_pair("size", ""));
    info_map.insert(std::make_pair("mtime", ""));
    info_map.insert(std::make_pair("ctime", ""));
    info_map.insert(std::make_pair("uid", ""));
    info_map.insert(std::make_pair("gid", ""));
    info_map.insert(std::make_pair("xstype", ""));
    info_map.insert(std::make_pair("xs", ""));
  }
  else // dir 
  {
    info_map.insert(std::make_pair("file", "")); // dir name
    info_map.insert(std::make_pair("uid", ""));
    info_map.insert(std::make_pair("gid", ""));
    info_map.insert(std::make_pair("xattrn", ""));
    info_map.insert(std::make_pair("xattrv", ""));
  }

  // In C++11 one can simply do:
  /*
    std::map<std::string, std::string> info_map = 
    {
      {"size" : ""}, {"mtime": ""}, {"ctime": ""}, 
      {"uid": ""}, {"gid": ""}, {"xstype": ""}, {"xs": ""}
    };

    or 

    std::vector<std::string, std::sting> info_map =  {
      {"uid": ""}, {"gid": ""}, {"attr": ""} };
  */    

  std::string line;
  ProcCommand* cmd_find = new ProcCommand();
  XrdOucString info = "&mgm.cmd=find&mgm.path=";
  info += arch_dir.c_str();
  
  if (is_file)
    info += "&mgm.option=fI";
  else 
    info += "&mgm.option=dI";

  cmd_find->open("/proc/user", info.c_str(), *pVid, mError);
  int ret = cmd_find->close();

  if (ret)
  {
    delete cmd_find;
    eos_err("find fileinfo on directory=%s failed", arch_dir.c_str());
    stdErr = "error: find fileinfo failed";
    retc = ret;
    return retc;
  }

  size_t spos = 0;
  std::string rel_path;
  std::string key, value, pair;
  std::istringstream line_iss;
  std::ifstream result_ifs(cmd_find->GetResultFn());

  if (!result_ifs.good())
  {
    delete cmd_find;
    eos_err("failed to open find fileinfo result file on MGM");
    stdErr = "failed to open find fileinfo result file on MGM";
    retc = EIO;
    return retc; 
  }

  while (std::getline(result_ifs, line))
  {
    if (line.find("&mgm.proc.stderr=") == 0)
      continue;
    
    if (line.find("&mgm.proc.stdout=") == 0)
      line.erase(0, 17); 

    line_iss.clear();
    line_iss.str(line);

    while(line_iss.good())
    {
      line_iss >> pair;
      spos = pair.find('=');
      
      if ((spos == std::string::npos) || (!line_iss.good()))
        continue; // not in key=value format
    
      key = pair.substr(0, spos);
      value = pair.substr(spos + 1);
      eos_info("key=%s, value=%s", key.c_str(), value.c_str());

      if (info_map.find(key) == info_map.end())
        continue; // not what we look for

      if (key == "xattrn")
      {
        // The next token must be an xattrv
        std::string xattrn = value;
        line_iss >> pair;
        spos = pair.find('=');
      
        if ((spos == std::string::npos) || (!line_iss.good()))
        {
          delete cmd_find;
          eos_err("not expected xattr pair format");
          stdErr = "not expected xattr pair format";
          retc = EINVAL;
          return retc;
        }

        key = pair.substr(0, spos);
        value = pair.substr(spos + 1);

        if (key != "xattrv")
        {
          delete cmd_find;
          eos_err("not found expected xattrv");
          stdErr = "not found expected xattrv";
          retc = EINVAL;
          return retc;
        }

        attr_map[xattrn] = value;        
      }
      else 
        info_map[key] = value;
    }

    // Add entry info to the archive file with the path names relative to the
    // current archive directory
    rel_path = info_map["file"];
    rel_path.erase(0, arch_dir.length() + 1); // +1 for the "/"

    if (rel_path.empty())
      rel_path = "./";

    if (is_file)
    {
      arch_ofs << "[\"f\", \"" << rel_path << "\", "
               << "\"" << info_map["size"] << "\", "
               << "\"" << info_map["mtime"] << "\", "
               << "\"" << info_map["ctime"] << "\", "
               << "\"" << info_map["uid"] << "\", "
               << "\"" << info_map["gid"] << "\", "
               << "\"" << info_map["xstype"] << "\", "
               << "\"" << info_map["xs"] << "\"]" 
               << std::endl;
    }
    else
    {
      arch_ofs << "[\"d\", \"" << rel_path << "\", "
               << "\"" << info_map["uid"] << "\", "
               << "\"" << info_map["gid"] << "\", " 
               << "{";

      for (auto it = attr_map.begin(); it != attr_map.end(); /*empty*/)
      {
        arch_ofs << "\"" << it->first << "\": \"" << it->second << "\"";
        ++it;

        if (it != attr_map.end())
          arch_ofs << ", ";
      }

      arch_ofs << "}]" << std::endl;
    }
  }

  delete cmd_find;
  return retc;
}

EOSMGMNAMESPACE_END
