// ----------------------------------------------------------------------
// File: DbMapTest.cc
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2012 CERN/Switzerland                                  *
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
/**
 * @file   DbMapTestFunc.cc
 *
 * @brief  This program performs some basic operations with the DbMap class.
 *         It's a functional test requiring a human check of the outputs.
 *         Before running this program it might be need to rm -rf /tmp/testlog*
 *
 */

#include<iostream>
#include<fstream>
#include<string>
#include<common/DbMap.hh>
#include<pthread.h>

#define NUM_THREADS     5

using namespace std;
using namespace eos::common;

DbMap dbm;
DbMap dbm_no_slice;

void *FillTheMap(void *threadid) {
  long tid;
  tid = (long) threadid;
  char buffer[32];
  sprintf(buffer, "/tmp/testlog_%ld.db", tid);
  DbMap dbm_local;
  dbm_local.AttachLog("/tmp/testlog.db", 10); // I don't need to detach because the map is dying at the end of the function
  dbm_local.AttachLog(buffer, 10); 			// I don't need to detach because the map is dying at the end of the function
  sprintf(buffer, "thread #%ld", tid);
  dbm_local.Set("Key1", "Value1", buffer);
  usleep(0 * tid * 100000);
  dbm_local.Set("Key2", "Value2", buffer);
  usleep(0 * tid * 100000);
  dbm_local.Set("Key3", "Value3", buffer);
  pthread_exit(NULL);
}

void *FillTheMap2(void *threadid) {
  // slow filling 1 entry every 0.2 sec
  long tid;
  tid = (long) threadid;
  char buffer0[16];
  sprintf(buffer0, "thread #%ld", tid);
  printf("FillTheMap2 : thread #%ld begins\n", tid);
  for (int k = 0; k < 100; k++) {
    char buffer[16];
    sprintf(buffer, "k=%d", k);
    dbm.Set("KeySeq-" + string(buffer0), "ValSeq-" + string(buffer0),
        buffer);
    dbm_no_slice.Set("KeySeq-" + string(buffer0),
        "ValSeq-" + string(buffer0), buffer);
    usleep(200000);
  }
  printf("FillTheMap2 : thread #%ld ends\n", tid);
  fflush(stdout);
  pthread_exit(NULL);
}

void *FillTheMap3(void *threadid) {
  // fast filling using SetSequence
  long tid;
  tid = (long) threadid;
  char buffer0[16];
  sprintf(buffer0, "thread #%ld", tid);
  dbm.BeginSetSequence(); dbm_no_slice.BeginSetSequence();
  printf("FillTheMap2 : thread #%ld begins\n", tid);
  for (int k = 100; k < 200; k++) {
    char buffer[16];
    sprintf(buffer, "k=%d", k);
    dbm.Set("KeySeq-" + string(buffer0), "ValSeq-" + string(buffer0),
        buffer);
    dbm_no_slice.Set("KeySeq-" + string(buffer0),
        "ValSeq-" + string(buffer0), buffer);
  }
  printf("FillTheMap3 : thread #%ld ends\n", tid);
  fflush(stdout);
  dbm.EndSetSequence(); dbm_no_slice.EndSetSequence();
  pthread_exit(NULL);
}

int main() {
  //SqliteInterfaceBase::SetDebugMode(true); //LvDbInterfaceBase::SetDebugMode(true);
#ifdef EOS_SQLITE_DBMAP
  printf("Using SQLITE3 DbMap/DbLog implementation\n\n");
#else
  printf("Using LEVELDB DbMap/DbLog implementation\n\n");
#endif
  dbm.SetName("TestMap");
  dbm_no_slice.SetName("TestMap_no_slice");
  dbm.AttachLog("/tmp/testlog.db", 10);
  dbm_no_slice.AttachLog("/tmp/testlog_no_slice.db");
  printf("attach is OK\n");
  dbm.Set("k1", "v1", "r1");
  dbm_no_slice.Set("k1", "v1", "r1");
  dbm.Set("k2", "v2", "r2");
  dbm_no_slice.Set("k2", "v2", "r2");

  // some fillings
  pthread_t threads[NUM_THREADS];
  int rc;
  long t;
  void *ret;
  for (t = 0; t < NUM_THREADS; t++) {
    printf("In main: creating thread %ld\n", t);
    rc = pthread_create(&threads[t], NULL, FillTheMap, (void *) t);
    if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);

  dbm.Remove("Key2");
  dbm_no_slice.Remove("Key2");
  dbm_no_slice.SetName("NewName_no_slice");

  for (t = 0; t < NUM_THREADS; t++) {
    printf("In main: creating thread %ld\n", t);
    rc = pthread_create(&threads[t], NULL, FillTheMap2, (void *) t);
    if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);

  for (t = 0; t < NUM_THREADS; t++) {
    printf("In main: creating thread %ld\n", t);
    rc = pthread_create(&threads[t], NULL, FillTheMap3, (void *) t);
    if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);

  // some printings
  cout << "Content of the dbm is : \n" << dbm;
  cout << "Content of the dbm_no_slice is : \n" << dbm_no_slice;
  cout << "Number of reads for dbm : " << dbm.GetReadCount()
			    << "\tnumber of writes for dbm : " << dbm.GetWriteCount()
			    << std::endl;
  cout << "Number of reads for dbm_no_slice : " << dbm_no_slice.GetReadCount()
			    << "\tnumber of writes for dbm_no_slice : "
			    << dbm_no_slice.GetWriteCount() << std::endl;
  dbm.DetachLog("/tmp/testlog.db");
  dbm_no_slice.DetachLog("/tmp/testlog_no_slice.db");

  DbLog dbl("/tmp/testlog.db");
  DbMapTypes::TlogentryVec retvec;
  dbl.GetAll(retvec);
  cout << retvec;
  cout << "*************************************************************************************************"<< endl;
  retvec.clear();
#ifdef EOS_SQLITE_DBMAP
  dbl.GetByRegex("(seqid<10) AND (writer REGEXP \"NewName\") AND NOT (value REGEXP \"[^#]+#[0-2]\")",retvec);
  dbl.GetByRegex("NOT (writer REGEXP \"NewName\")", retvec);
#else
  dbl.GetByRegex("(.*)k=1(.*)", retvec);
#endif
  cout << retvec;

  cout<< "#################################################################################################"<< endl;

  DbLog dbl_no_slice("/tmp/testlog_no_slice.db");
  retvec.clear();
  dbl_no_slice.GetAll(retvec);
  cout << retvec;
  cout<< "*************************************************************************************************"<< endl;
  retvec.clear();
#ifdef EOS_SQLITE_DBMAP
  dbl_no_slice.GetByRegex("(seqid<10) AND (writer REGEXP \"NewName\") AND NOT (value REGEXP \"[^#]+#[0-2]\")",retvec);
  dbl_no_slice.GetByRegex("NOT (writer REGEXP \"NewName\")", retvec);
#else
  dbl_no_slice.GetByRegex("(.*)k=[13](.*)", retvec);
#endif
  cout << retvec;

  //******************** check the content of the map ********************//
  //**********************************************************************//
  printf("Checking the log tables...\n");
  // timestampstr \t seqid \t writer \t key \t val \t comment
  // at this point
  // the content of /tmp/testlog_no_slice.db should be
  // writer=TestMap_no_slice key=k1 value=v1 comment=r1
  int totalcount=0;
  retvec.clear();
#ifdef EOS_SQLITE_DBMAP
  dbl_no_slice.GetByRegex("writer='TestMap_no_slice' AND key='k1' AND value='v1'",retvec);
#else
  dbl_no_slice.GetByRegex("([^\t]+[\t]){3,3}k1\tv1\tr1",retvec);
#endif
  totalcount+=retvec.size();
  assert(retvec.size()==1);
  // writer=TestMap key=k2 value=v2 comment=r2
  retvec.clear();
#ifdef EOS_SQLITE_DBMAP
  dbl_no_slice.GetByRegex("writer='TestMap_no_slice' AND key='k2' AND value='v2'",retvec);
#else
  dbl_no_slice.GetByRegex("([^\t]+[\t]){3,3}k2\tv2\tr2",retvec);
#endif
  totalcount+=retvec.size();
  assert(retvec.size()==1);
  // writer=NewName_no_slice seqid=N key=KeySeq-thread #P value=ValSeq-thread #4 comment=k=N-1    with N=[1,200] and P=[1,NUM_THREADS]
  retvec.clear();
#ifdef EOS_SQLITE_DBMAP
  dbl_no_slice.GetByRegex("writer='NewName_no_slice' AND (key REGEXP \"KeySeq-thread[ ]#[0-NUMTHREADS]\") AND (value REGEXP \"ValSeq-thread[ ]#[0-NUMTHREADS]\") ",retvec);
  //dbl_no_slice.GetByRegex("(key REGEXP \"KeySeq-thread[ ]#[0-NUMTHREADS]\")",retvec);
#else
  dbl_no_slice.GetByRegex("([^\t]+[\t]){3,3}KeySeq-thread[ ]#[0-NUMTHREADS]\tValSeq-thread[ ]#[0-NUMTHREADS]",retvec);
#endif
  totalcount+=retvec.size();
  assert(retvec.size()==100*NUM_THREADS*2);
  // check that there is nothing else
  retvec.clear();
  dbl_no_slice.GetAll(retvec);
  assert((int)retvec.size()==totalcount);

  // at this point
  // the content of /tmp/testlog.db is splitten among several archives
  system("rm -f /tmp/dbmaptestfunc_list.txt");
  system("\\ls -1d /tmp/testlog.db* > /tmp/dbmaptestfunc_list.txt");
  ifstream filelist("/tmp/dbmaptestfunc_list.txt");
  vector<string> files;
  string newname; while(getline(filelist,newname)) { files.push_back(newname); }
  filelist.close();
  vector<DbLog*> dblogs;
  for(vector<string>::const_iterator it=files.begin();it!=files.end();it++) {
    dblogs.push_back(new DbLog(*it));
  }

#define arch_loop for(vector<DbLog*>::const_iterator it=dblogs.begin();it!=dblogs.end();it++)
#define arch_testloop(pattern,count) retvec.clear(); arch_loop { (*it)->GetByRegex(pattern,retvec); } /*cout<< retvec.size()<<endl;*/ assert(retvec.size()==count);
  // the content of /tmp/testlog.db (including all the archive volumes) should be
  // writer=TestMap_no_slice key=k1 value=v1 comment=r1
#ifdef EOS_SQLITE_DBMAP
  arch_testloop("key='k1' AND value='v1'",1);
#else
  arch_testloop("([^\t]+[\t]){3,3}k1\tv1\tr1",1);
#endif
  // writer=TestMap_no_slice key=k2 value=v2 comment=r2
#ifdef EOS_SQLITE_DBMAP
  arch_testloop("key='k2' AND value='v2'",1);
#else
  arch_testloop("([^\t]+[\t]){3,3}k2\tv2\tr2",1);
#endif
  //                key=KeyN value=ValueN comment=thread #P  with N={1,2,3} and P=[1,NUM_THREADS]
#ifdef EOS_SQLITE_DBMAP
  arch_testloop("(key REGEXP \"Key[1-3]\") AND (value REGEXP \"Value[1-3]\")",3*NUM_THREADS);
#else
  arch_testloop("([^\t]+[\t]){3,3}Key[1-3]\tValue[1-3]\tthread [#][0-NUM_THREADS]",3*NUM_THREADS);
#endif
  // writer=Newname seqid=N key=KeySeq-thread #P value=ValSeq-thread #4 comment=k=N-1    with N=[1,100] and P=[1,NUM_THREADS]
#ifdef EOS_SQLITE_DBMAP
  arch_testloop("(key REGEXP \"KeySeq-thread[ ]#[0-NUMTHREADS]\") AND (value REGEXP \"ValSeq-thread[ ]#[0-NUMTHREADS]\")",100*NUM_THREADS*2);
#else
  arch_testloop("([^\t]+[\t]){3,3}KeySeq-thread[ ]#[0-NUMTHREADS]\tValSeq-thread[ ]#[0-NUMTHREADS]",100*NUM_THREADS*2);
#endif
  retvec.clear();
  // check that there is nothing else
  #ifdef EOS_SQLITE_DBMAP
  arch_testloop("(key REGEXP \"(.*)\")",203*NUM_THREADS+2);
  #else
  arch_testloop("([^\t]+[\t]){5,5}[^\t]+",203*NUM_THREADS+2);
  #endif

  // for each volume, check that all the timestamps are in the correct interval
  vector<string>::const_iterator itf=files.begin();
  arch_loop {
    if(itf->size()<17) { itf++; continue; }
    retvec.clear();
    (*it)->GetAll(retvec);
    cout<<"checking time interval comsistency for db file "<<(*itf)<<endl;
    cout<<"the following timestamps should appear in the chronological order"<<endl;
    printf("%.*s    %s    %s    %.*s\n\n", 22,itf->c_str()+17, retvec.front().timestampstr.c_str()  ,  retvec.back().timestampstr.c_str()  , 22,itf->c_str()+41);
    itf++;
  }

  // delete the DbLogs
  arch_loop delete (*it);

#ifndef EOS_SQLITE_DBMAP
  //SqliteDbMapInterface::SetDebugMode(true); SqliteDbLogInterface::SetDebugMode(true);
  DbMapT<SqliteDbMapInterface,SqliteDbLogInterface> sqdbm; sqdbm.AttachLog("/tmp/testlog_sqdbm");
  DbMapT<LvDbDbMapInterface,LvDbDbLogInterface> lvdbm;     lvdbm.AttachLog("/tmp/testlog_lvdbm");
  for(int k=0;k<10;k++) {
    char bk[8],bv[8],br[8];
    sprintf(bk,"k%2.2dsq",k);
    sprintf(bv,"v%2.2dsq",k);
    sprintf(br,"r%2.2dsq",k);
    sqdbm.Set(bk,bv,br);
    sprintf(bk,"k%2.2dlv",k);
    sprintf(bv,"v%2.2dlv",k);
    sprintf(br,"r%2.2dlv",k);
    lvdbm.Set(bk,bv,br);
  }
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::TlogentryVec sqentryvec;
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::Tlogentry    sqentry;
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::TlogentryVec lventryvec;
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::Tlogentry    lventry;
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface> sqdbl("/tmp/testlog_sqdbm");
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>     lvdbl("/tmp/testlog_lvdbm");
  cout<<"====  SqDbL before copy  ==="<<endl;
  while(sqdbl.GetAll(sqentryvec,4,&sqentry)) {
    cout<<sqentryvec<<"----------------------------"<<endl;
    sqentryvec.clear();
  }
  cout<<"============================"<<endl;
  cout<<"====  LvDbL before copy  ==="<<endl;
  while(lvdbl.GetAll(lventryvec,4,&lventry)) {
    cout<<lventryvec<<"----------------------------"<<endl;
    lventryvec.clear();
  }
  cout<<"============================"<<endl;

  cout<<"====>  Append the content of SqDbm to LvDbm"<<endl;
  lvdbm.BeginSetSequence();
  while(sqdbl.GetAll(sqentryvec,4,&sqentry)) {
    for(DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::TlogentryVec::iterator it=sqentryvec.begin();it!=sqentryvec.end();it++)
      lvdbm.Set(it->key,it->value,it->comment);
    sqentryvec.clear();
  }
  lvdbm.EndSetSequence();
  cout<<"====  LvDbL after copy  ==="<<endl;
  while(lvdbl.GetAll(lventryvec,4,&lventry)) {
    cout<<lventryvec<<"----------------------------"<<endl;
    lventryvec.clear();
  }
  cout<<"============================"<<endl;

  cout<<"====>  Append the content of LvDbm to SqDbm"<<endl;
  sqdbm.BeginSetSequence();
  while(lvdbl.GetAll(lventryvec,4,&lventry)) {
    for(DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::TlogentryVec::iterator it=lventryvec.begin();it!=lventryvec.end();it++)
      sqdbm.Set(it->key,it->value,it->comment);
    lventryvec.clear();
  }
  sqdbm.EndSetSequence();
  cout<<"====  SqDbL after copy  ==="<<endl;
  while(sqdbl.GetAll(sqentryvec,4,&sqentry)) {
    cout<<sqentryvec<<"----------------------------"<<endl;
    sqentryvec.clear();
  }
  cout<<"============================"<<endl;
  sqdbl.SetDbFile("");
  lvdbl.SetDbFile("");
  cout<<"====>  Convert LvDbl to SqDbl2"<<endl;
  ConvertLevelDb2Sqlite("/tmp/testlog_lvdbm","/tmp/testlog_lvdbm2sqdbm");
  cout<<"====>  Convert SqDbl to LvDbl2"<<endl;
  ConvertSqlite2LevelDb("/tmp/testlog_sqdbm","/tmp/testlog_sqdbm2lvdbm");
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface> sqdbl2("/tmp/testlog_lvdbm2sqdbm");
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>     lvdbl2("/tmp/testlog_sqdbm2lvdbm");
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::TlogentryVec sqentryvec2;
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::Tlogentry    sqentry2;
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::TlogentryVec lventryvec2;
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::Tlogentry    lventry2;

  cout<<"====  SqDbL2 vs LvDbl ==="<<endl;
  bool ok=true;
  lventryvec.clear(); lventry.key="";
  while(lvdbl.GetAll(lventryvec,1,&lventry)) {
    sqdbl2.GetAll(sqentryvec2,1,&sqentry2);
    if(! (sqentryvec2.back()==lventryvec.back()) ) {
      ok=false;
      cout<<"!!! non identical entry detected"<<endl<<sqentryvec2<<lventryvec;
    }
    lventryvec.clear();
    sqentryvec2.clear();
  }
  if(ok) cout<<"original and copy are identical"<<endl;
  else assert(false);
  cout<<"============================"<<endl;

  cout<<"====  LvDbL2 vs SqDbl ==="<<endl;
  ok=true;
  sqentryvec.clear(); sqentry.key=""; lventryvec2.clear(); lventry2.key="";
  while(sqdbl.GetAll(sqentryvec,1,&sqentry)) {
    lvdbl2.GetAll(lventryvec2,1,&lventry2);
    if(! (lventryvec2.back()==sqentryvec.back()) ) {
      ok=false;
      cout<<"!!! non identical entry detected"<<endl<<lventryvec2<<sqentryvec;
    }
    sqentryvec.clear();
    lventryvec2.clear();
  }
  if(ok) cout<<"original and copy are identical"<<endl;
  else assert(false);
  cout<<"============================"<<endl;
  #endif
  printf("done\n");
  fflush(stdout);

  return 0;
}
