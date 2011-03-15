/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/


/* Test Interface */
int
com_test (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
 
  do {
    XrdOucString tag  = subtokenizer.GetToken();
    if (! tag.length()) 
      break;

    XrdOucString sn = subtokenizer.GetToken();
    if (! sn.length()) {
      goto com_test_usage;
    }

    int n = atoi(sn.c_str());
    printf("info: doing directory test with loop <n>=%d", n);

    if (tag == "mkdir") {
      XrdMqTiming timing("mkdir");
      
      TIMING("start",&timing);

      for (int i=0; i< 10; i++) {
        char dname[1024];
        sprintf(dname,"/test/%02d", i);
        XrdOucString cmd = ""; cmd += dname;
        //      printf("===> %s\n", cmd.c_str());
        com_mkdir((char*)cmd.c_str());

        for (int j=0; j< n/10; j++) {
          sprintf(dname,"/test/%02d/%05d", i,j);
          XrdOucString cmd = ""; cmd += dname;
          //      printf("===> %s\n", cmd.c_str());
          com_mkdir((char*)cmd.c_str());
        }
      }
      TIMING("stop",&timing);
      timing.Print();
    }

    if (tag == "rmdir") {
      XrdMqTiming timing("rmdir");   
      TIMING("start",&timing);

      for (int i=0; i< 10; i++) {
        char dname[1024];
        sprintf(dname,"/test/%02d", i);
        XrdOucString cmd = ""; cmd += dname;
        //printf("===> %s\n", cmd.c_str());

        for (int j=0; j< n/10; j++) {
          sprintf(dname,"/test/%02d/%05d", i,j);
          XrdOucString cmd = ""; cmd += dname;
          //printf("===> %s\n", cmd.c_str());
          com_rmdir((char*)cmd.c_str());
        }
        com_rmdir((char*)cmd.c_str());
      }
      TIMING("stop",&timing);
      timing.Print();
    }

    if (tag == "ls") {
      XrdMqTiming timing("ls");   
      TIMING("start",&timing);

      for (int i=0; i< 10; i++) {
        char dname[1024];
        sprintf(dname,"/test/%02d", i);
        XrdOucString cmd = ""; cmd += dname;
        com_ls((char*)cmd.c_str());
      }
      TIMING("stop",&timing);
      timing.Print();
    }

    if (tag == "lsla") {
      XrdMqTiming timing("lsla");   
      TIMING("start",&timing);

      for (int i=0; i< 10; i++) {
        char dname[1024];
        sprintf(dname,"/test/%02d", i);
        XrdOucString cmd = "-la "; cmd += dname;
        com_ls((char*)cmd.c_str());
      }
      TIMING("stop",&timing);
      timing.Print();
    }
  } while (1);

  return (0);
 com_test_usage:
  printf("usage: test [mkdir|rmdir|ls|lsla <N> ]                                             :  run performance test\n");
  return (0);

}
