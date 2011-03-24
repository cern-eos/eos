#include "mq/XrdMqSharedObject.hh"
#include "mq/XrdMqMessaging.hh"
#include "mgm/FsView.hh"
#include "mgm/ConfigEngine.hh"

using namespace eos::common;
using namespace eos::mgm;

int main() {
  Logging::Init();

  Logging::SetUnit("MgmViewTest");
  Logging::SetLogPriority(LOG_INFO);

  XrdMqMessage::Configure("");
  
  XrdMqSharedObjectManager ObjectManager;

  ObjectManager.SetDebug(true);

  XrdMqMessaging messaging("root://localhost:1097//eos/test/worker", "/eos/*/worker", false, false, &ObjectManager);

  messaging.StartListenerThread();

  std::string queuepath;
  std::string queue;
  std::string schedgroup;

  int iloop = 10;
  int jloop = 10;

  for (int i=0; i< iloop; i++) {
    char n[1024];
    sprintf(n,"%03d",i);
    std::string queue="/eos/test"; queue += n;
    queue += "/fst";
    
    for (int j=0; j< jloop; j++) {
      schedgroup = "default.";
      char m[1024];
      sprintf(m,"%03d",j);
      queuepath = queue;
      queuepath += "/data"; queuepath += m;
      schedgroup += m;
      //printf("Setting up schedgroup %s\n", schedgroup.c_str());

      ObjectManager.CreateSharedHash(queuepath.c_str(), queue.c_str());
      XrdMqSharedHash* hash = ObjectManager.GetObject(queuepath.c_str(),"hash");
      hash->OpenTransaction();
      hash->SetLongLong("id", (i*iloop) + j);
      hash->Set("schedgroup", schedgroup.c_str());
      hash->Set("queuepath", queuepath.c_str());
      hash->Set("queue", queue.c_str());
      hash->Set("errmsg","");
      hash->Set("errc",0);
      hash->SetLongLong("errc",0);
      hash->SetLongLong("status",FileSystem::kDown);
      hash->SetLongLong("configstatus",FileSystem::kUnknown);
      hash->SetLongLong("bootSentTime",0);
      hash->SetLongLong("bootDoneTime",0);
      hash->SetLongLong("lastHeartBeat",0);
      hash->SetLongLong("statfs.disk.load",0);
      hash->SetLongLong("statfs.disk.in",0);
      hash->SetLongLong("statfs.disk.out",0);
      hash->SetLongLong("statfs.net.load",0);
      hash->SetLongLong("statfs.type",0);
      hash->SetLongLong("statfs.bsize",0);
      hash->SetLongLong("statfs.blocks",1000000.0*rand() / RAND_MAX);
      hash->SetLongLong("statfs.bfree",0);
      hash->SetLongLong("statfs.bavail",0);
      hash->SetLongLong("statfs.files",0);
      hash->SetLongLong("statfs.ffree",0);
      hash->SetLongLong("statfs.namelen",0);
      hash->SetLongLong("statfs.ropen",0);
      hash->SetLongLong("statfs.wopen",0);
      hash->CloseTransaction();

      FileSystem* fs = new FileSystem(queuepath.c_str(),queue.c_str(), &ObjectManager);
      FsView::gFsView.Register(fs);
    }
  }

  // test the print function
  std::string output = "";
  std::string format1 = "header=1:member=type:width=20:format=-s|sep=   |member=name:width=20:format=-s|sep=   |sum=statfs.blocks:width=20:format=-l|sep=   |avg=statfs.blocks:width=20:format=-f |sep=   |sig=statfs.blocks:width=20:format=-f";
  std::string format2 = "header=1:member=type:width=20:format=+s|sep=   |member=name:width=20:format=+s|sep=   |sum=statfs.blocks:width=20:format=+l:unit=B|sep=   |avg=statfs.blocks:width=20:format=+f:unit=B|sep=   |sig=statfs.blocks:width=20:format=+f:unit=B";
  std::string format3 = "header=1:member=type:width=1:format=os|sep=&|member=name:width=1:format=os|sep=&|sum=statfs.blocks:width=1:format=ol|sep=&|avg=statfs.blocks:width=1:format=ol|sep=&|sig=statfs.blocks:width=1:format=ol";

  std::string listformat1 = "header=1:key=queuepath:width=30:format=s|sep=   |key=schedgroup:width=10:format=s|sep=   |key=blocks:width=10:format=l|sep=   |key=statfs.wopen:width=10:format=l";

  std::string listformat2 = "key=queuepath:width=2:format=os|sep=&|key=schedgroup:width=1:format=os|sep=&|key=blocks:width=1:format=os|sep=&|key=statfs.wopen:width=1:format=os";

  output += "[ next test ]\n";
  FsView::gFsView.mSpaceView["default"]->Print(output, format1,"");
  output += "[ next test ]\n";
  FsView::gFsView.PrintSpaces(output, format1, "");
  output += "[ next test ]\n";
  FsView::gFsView.PrintGroups(output, format1, "");
  output += "[ next test ]\n";
  FsView::gFsView.PrintNodes(output, format1, "");
  output += "[ next test ]\n";
  FsView::gFsView.mSpaceView["default"]->Print(output, format2,"");
  output += "[ next test ]\n";
  FsView::gFsView.PrintSpaces(output, format2, "");
  output += "[ next test ]\n";
  FsView::gFsView.PrintGroups(output, format2, "");
  output += "[ next test ]\n";
  FsView::gFsView.PrintNodes(output, format2, "");
  output += "[ next test ]\n";
  FsView::gFsView.PrintNodes(output, format3, "");
  output += "[ next test ]\n";
  FsView::gFsView.PrintGroups(output, format2, listformat1);
  output += "[ next test ]\n";
  FsView::gFsView.PrintGroups(output, format2, listformat2);
  output += "[ next test ]\n";
  FsView::gFsView.PrintSpaces(output, format2, listformat1);

  fprintf(stdout,"%s\n", output.c_str());

  // remove filesystems
  for (int i=0; i< iloop; i++) {
    char n[1024];
    sprintf(n,"%02d",i);
    std::string queue="/eos/test"; queue += n;
    queue += "/fst";
    
    for (int j=0; j< jloop; j++) {
      schedgroup = "default.";
      char m[1024];
      sprintf(m,"%02d",j);
      queuepath = queue;
      queuepath += "/data"; queuepath += m;
      schedgroup += m;
      //      printf("Setting up %s\n", queuepath.c_str());
      unsigned int fsid= (i*iloop) + j;
      FsView::gFsView.ViewMutex.LockRead();
      FileSystem* fs = FsView::gFsView.mIdView[fsid];
      FsView::gFsView.ViewMutex.UnLockRead();
      if (fs) {
	FsView::gFsView.UnRegister(fs);
      }
    }
  }
}
