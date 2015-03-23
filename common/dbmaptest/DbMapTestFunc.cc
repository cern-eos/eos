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
 *
 */

#include<iostream>
#include<fstream>
#include<string>
#include<common/DbMap.hh>
#include<pthread.h>
#include "google/protobuf/text_format.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "dbmaptest/test.pb.h"

#define NUM_THREADS     5
#define STRING(x) STRING2(x)
#define STRING2(x) # x

using namespace std;
using namespace eos::common;

bool outofcore = false;

DbMap dbm;
DbMap dbm_no_slice;

void *
FillTheMap(void *threadid)
{
  long tid;
  tid = (long) threadid;
  char buffer[32];
  sprintf(buffer, "/tmp/testlog_%ld.db", tid);
  DbMap dbm_local;
  dbm_local.attachLog("/tmp/testlog.db", 10); // I don't need to detach because the map is dying at the end of the function
  dbm_local.attachLog(buffer, 10); // I don't need to detach because the map is dying at the end of the function
  sprintf(buffer, "thread #%ld", tid);
  dbm_local.set("Key1", "Value1", buffer);
  usleep(0 * tid * 100000);
  dbm_local.set("Key2", "Value2", buffer);
  usleep(0 * tid * 100000);
  dbm_local.set("Key3", "Value3", buffer);
  pthread_exit(NULL);
  return NULL;
}

void *
FillTheMap2(void *threadid)
{
  // slow filling 1 entry every 0.2 sec
  long tid;
  tid = (long) threadid;
  char buffer0[16];
  sprintf(buffer0, "thread #%ld", tid);
  printf("FillTheMap2 : thread #%ld begins\n", tid);
  for (int k = 0; k < 100; k++)
  {
    char buffer[16];
    sprintf(buffer, "k=%d", k);
    dbm.set("KeySeq-" + string(buffer0), "ValSeq-" + string(buffer0), buffer);
    dbm_no_slice.set("KeySeq-" + string(buffer0), "ValSeq-" + string(buffer0), buffer);
    usleep(200000);
  }
  printf("FillTheMap2 : thread #%ld ends\n", tid);
  fflush(stdout);
  pthread_exit(NULL);
  return NULL;
}

void *
FillTheMap3(void *threadid)
{
  // fast filling using SetSequence
  long tid;
  tid = (long) threadid;
  char buffer0[16];
  sprintf(buffer0, "thread #%ld", tid);
  dbm.beginSetSequence();
  dbm_no_slice.beginSetSequence();
  printf("FillTheMap3 : thread #%ld begins\n", tid);
  for (int k = 100; k < 200; k++)
  {
    char buffer[16];
    sprintf(buffer, "k=%d", k);
    dbm.set("KeySeq-" + string(buffer0), "ValSeq-" + string(buffer0), buffer);
    dbm_no_slice.set("KeySeq-" + string(buffer0), "ValSeq-" + string(buffer0), buffer);
  }
  printf("FillTheMap3 : thread #%ld ends\n", tid);
  fflush(stdout);
  dbm.endSetSequence();
  dbm_no_slice.endSetSequence();
  pthread_exit(NULL);
  return NULL;
}

int
main()
{
  //SqliteInterfaceBase::SetDebugMode(true); //LvDbInterfaceBase::SetDebugMode(true);
#ifdef EOS_SQLITE_DBMAP
  printf("Using SQLITE3 DbMap/DbLog implementation\n\n");
#else
  printf("Using LEVELDB DbMap/DbLog implementation\n\n");
#endif
  {
    cout << "WARNING, proceeding rm -rf /tmp/testlog*, are you sure y/n ? ";
    char c;
    cin >> c;
    cout << endl;
    if (c != 'y')
      exit(1);
    system("rm -rf /tmp/testlog*");
  }

  dbm.setName("TestMap");
  dbm_no_slice.setName("TestMap_no_slice");

  if (outofcore)
  {
    dbm.attachDb("/tmp/testlogdb.db");
    dbm_no_slice.attachDb("/tmp/testlogdb_noslice.db");
    if (!(dbm.outOfCore(true) && dbm_no_slice.outOfCore(true)))
    {
      cerr << "Error moving out ot core... aborting " << endl;
      abort();
    }
  }

  dbm.attachLog("/tmp/testlog.db", 10);
  dbm_no_slice.attachLog("/tmp/testlog_no_slice.db");
  printf("attach is OK\n");
  cout << "before setting keys : count for k1: " << dbm.Count("k1") << " for k2: " << dbm.Count("k2") << endl;
  cout << "before setting keys : size of the DbMap: " << dbm.size() << endl;
  dbm.set("k1", "v1", "r1");
  dbm_no_slice.set("k1", "v1", "r1");
  dbm.set("k2", "v2", "r2");
  dbm_no_slice.set("k2", "v2", "r2");
  cout << "after setting keys : count for k1: " << dbm.Count("k1") << " for k2: " << dbm.Count("k2") << endl;
  cout << "after setting keys : size of the DbMap: " << dbm.size() << endl;

  // inserting binary data
  char bv[16];
  bv[0] = bv[2] = bv[4] = bv[6] = bv[8] = bv[10] = bv[12] = bv[14] = 0;
  bv[1] = bv[3] = bv[5] = bv[7] = bv[9] = bv[11] = bv[13] = bv[15] = 127;
  char bk[16];
  strcpy(bk, "kbinary");
  for (int h = 7; h < 16; h++)
    bk[h] = h % 2;
  dbm.set(Slice(bk, 16), Slice(bv, 16), "binary");
  dbm_no_slice.set(Slice(bk, 16), Slice(bv, 16), "binary");
  // checking binary data
  DbMap::Tval val;
  dbm.get(Slice(bk, 16), &val);
  string strbv(bv, 16);
  assert(strbv==val.value);

  // inserting a serialized protobuf struct
  tutorial::Fmd fmdin, fmdout;
  std::string sfmdin, sfmdout;
  fmdin.set_atime(123456);
  fmdin.set_atime_ns(654321);
  fmdin.set_blockcxerror(1234567890);
  fmdin.set_checksum("checksum_test");
  fmdin.set_checktime(24680);
  fmdin.set_cid(987654321);
  fmdin.set_ctime(111111);
  fmdin.set_ctime_ns(222222);
  fmdin.set_diskchecksum("diskchecksum_test");
  fmdin.set_disksize(999999999);
  size_t mykey = 123456789;
  fmdin.SerializeToString(&sfmdin);
  dbm.set(Slice((const char*) &mykey, sizeof(size_t)), sfmdin, "protobuf");
  dbm_no_slice.set(Slice((const char*) &mykey, sizeof(size_t)), sfmdin, "protobuf");
  DbMap::Tval get_out;
  dbm.get(Slice((const char*) &mykey, sizeof(size_t)), &get_out);
  fmdout.ParseFromString(get_out.value);
  assert(fmdout.DebugString()==fmdin.DebugString());
  std::string printstuff;
  google::protobuf::TextFormat::PrintToString(fmdout, &printstuff);
  std::cout << printstuff << std::endl;
  std::cout << "@@@@@@@@@@@@@@@@@@@@@@@@" << std::endl;

  // testing RegexBranch
  RegexBranch rb = (RegexAtom("key", "k.*") || RegexAtom("comment", "comment.*")) && !RegexAtom("timestampstr", "2014.*");
  RegexBranch rberror = (RegexAtom("key", "k.*") || RegexAtom("comment", "/\\^[[nt.*")) && !RegexAtom("timestampstr", "2014.*");
  DbMapTypes::Tlogentry le;
  le.key = "key.le";
  le.value = "value.le";
  le.seqid = "100";
  le.comment = "comment.le";
  le.timestampstr = "2013-06-11 10:38:16#000000009";
  cout << " result of HasError " << rb.hasError() << endl;
  cout << " result of corrupted HasError " << rberror.hasError() << endl;
  cout << " result of REGEX " << rb.eval(le) << endl;

  // some fillings
  pthread_t threads[NUM_THREADS];
  int rc;
  long t;
  void *ret;
  for (t = 0; t < NUM_THREADS; t++)
  {
    printf("In main: creating thread %ld\n", t);
    rc = pthread_create(&threads[t], NULL, FillTheMap, (void *) t);
    if (rc)
    {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);

  dbm.remove("k2");
  dbm_no_slice.remove("k2");
  dbm_no_slice.setName("NewName_no_slice");

  cout << dbm_no_slice.trimDb();
  cout << endl;

  for (t = 0; t < NUM_THREADS; t++)
  {
    printf("In main: creating thread %ld\n", t);
    rc = pthread_create(&threads[t], NULL, FillTheMap2, (void *) t);
    if (rc)
    {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);

  for (t = 0; t < NUM_THREADS; t++)
  {
    printf("In main: creating thread %ld\n", t);
    rc = pthread_create(&threads[t], NULL, FillTheMap3, (void *) t);
    if (rc)
    {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);

  // some printings
  cout << "Content of the dbm is : \n" << dbm;
  cout << "Content of the dbm_no_slice is : \n" << dbm_no_slice;
  cout << "Number of reads for dbm : " << dbm.getReadCount() << "\tnumber of writes for dbm : " << dbm.getWriteCount() << endl;
  cout << "Number of reads for dbm_no_slice : " << dbm_no_slice.getReadCount() << "\tnumber of writes for dbm_no_slice : " << dbm_no_slice.getWriteCount()
      << endl;
  dbm.detachLog("/tmp/testlog.db");
  dbm_no_slice.detachLog("/tmp/testlog_no_slice.db");

  DbLog dbl("/tmp/testlog.db");
  DbMapTypes::TlogentryVec retvec;
  dbl.getAll(&retvec);
  cout << retvec;
  cout << "*************************************************************************************************"<< endl;

  DbLog dbl_no_slice("/tmp/testlog_no_slice.db");
  retvec.clear();
  dbl_no_slice.getAll(&retvec);
  cout << retvec;
  cout<< "*************************************************************************************************"<< endl;

  //******************** check the content of the map ********************//
  //**********************************************************************//
  printf("Checking the log tables...\n");
  // timestampstr \t seqid \t writer \t key \t val \t comment
  // at this point
  // the content of /tmp/testlog_no_slice.db should be
  int totalcount = 0;

  // writer=TestMap_no_slice key=k1 value=v1 comment=r1
  retvec.clear();
  dbl_no_slice.getAll(&retvec, 0, NULL, RegexAtom("writer", "TestMap_no_slice") && RegexAtom("key", "k1") && RegexAtom("value", "v1"));
  totalcount+=retvec.size();
  //cout<<retvec.size()<<endl;
  assert(retvec.size()==1);

  // writer=TestMap_no_slice key=k2 value=v2 comment=r2
  retvec.clear();
  dbl_no_slice.getAll(&retvec, 0, NULL, RegexAtom("writer", "TestMap_no_slice") && RegexAtom("key", "k2") && RegexAtom("value", "v2"));
  totalcount+=retvec.size();
  //cout<<retvec.size()<<endl;
  assert(retvec.size()==1);

  retvec.clear();
  dbl_no_slice.getAll(&retvec, 0, NULL,
      RegexAtom("writer", "NewName_no_slice") && RegexAtom("key", "KeySeq-thread[ ]#[0-" STRING(NUM_THREADS) "]")
          && RegexAtom("value", "ValSeq-thread[ ]#[0-" STRING(NUM_THREADS) "]"));
  totalcount+=retvec.size();
  //cout<<retvec.size()<<endl;
  assert(retvec.size()==100*NUM_THREADS*2);

  retvec.clear();
  dbl_no_slice.getAll(&retvec);
  //cout<<retvec.size()<<endl;
  assert((int)retvec.size()==totalcount+3);
  // we add one because of the removal of k2 in the log, because of the key with the binary value and because of the protobuf entry

  // at this point
  vector<DbLog*> dblogs;
  vector<string> files;
  {
  system("rm -f /tmp/dbmaptestfunc_list.txt");
  system("\\ls -1d /tmp/testlog.db* > /tmp/dbmaptestfunc_list.txt");
    system("\\ls -1d /tmp/testlog_*.db* >> /tmp/dbmaptestfunc_list.txt");
  ifstream filelist("/tmp/dbmaptestfunc_list.txt");
    string newname;
    while (getline(filelist, newname))
    {
      files.push_back(newname);
    }
    filelist.close();
    cout << "list of the db files for the next check" << endl;
    for (vector<string>::const_iterator it = files.begin(); it != files.end(); it++)
    {
      if (!it->compare("/tmp/testlog_no_slice.db"))
        continue;
      dblogs.push_back(new DbLog(*it));
      cout << *it << endl;
    }
  }

#define arch_loop for(vector<DbLog*>::const_iterator it=dblogs.begin();it!=dblogs.end();it++)
#define arch_testloop(pattern,count,detailedoutput) retvec.clear(); arch_loop { int c=(*it)->getAll(&retvec,0,NULL,pattern); if(detailedoutput) cout<<(*it)->getDbFile()<<" : "<<c<<endl; } if(detailedoutput) cout<< "total : " << retvec.size()<<endl; assert(retvec.size()==count);
  // the content of /tmp/testlog.db (including all the archive volumes) should be
  // writer=TestMap key=k1 value=v1 comment=r1
  arch_testloop(RegexAtom("writer","TestMap") && RegexAtom("key","k1") && RegexAtom("value","v1"), 1, true);

  // writer=TestMap key=k2 value=v2 comment=r2
  arch_testloop(RegexAtom("writer","TestMap") && RegexAtom("key","k2") && RegexAtom("value","v2"), 1, true);

  // writer=TestMap key=Key[1-3] value=Value[1-3]
  arch_testloop(RegexAtom("key","Key[1-3]") && RegexAtom("value","Value[1-3]"), 2*3*NUM_THREADS, true);
  // once in archives (or current db) and once in testlog_x.db

  //                key=KeyN value=ValueN comment=thread #P  with N={1,2,3} and P=[1,NUM_THREADS] // only in the archives (or current db)
  arch_testloop(
      RegexAtom("writer","TestMap") && RegexAtom("key","KeySeq-thread[ ]#[0-" STRING(NUM_THREADS) "]") && RegexAtom("value","ValSeq-thread[ ]#[0-" STRING(NUM_THREADS) "]"),
      100*NUM_THREADS*2, true);

  //                check there is nothing else
  arch_testloop(RegexAtom("key",".*"), 206*NUM_THREADS+2+3, true);
  // +2 for k1 and k2 +1 for the deletion +1 for the binary, +1 for the protobuf

  // at this point, we need to consider only the current dblog and its archives to check the time ranges cohenrency
  dblogs.clear();
  files.clear();
  {
    system("rm -f /tmp/dbmaptestfunc_list.txt");
    system("\\ls -1d /tmp/testlog.db* > /tmp/dbmaptestfunc_list.txt"); // only current db plus archives
    ifstream filelist("/tmp/dbmaptestfunc_list.txt");
    string newname;
    while (getline(filelist, newname))
    {
      files.push_back(newname);
    }
    filelist.close();
    cout << "list of the db files for the next check" << endl;
    for (vector<string>::const_iterator it = files.begin(); it != files.end(); it++)
    {
      if (!it->compare("/tmp/testlog_no_slice.db"))
        continue;
      dblogs.push_back(new DbLog(*it));
      cout << *it << endl;
    }
  }

  // for each volume, check that all the timestamps are in the correct interval
  vector<string>::const_iterator itf=files.begin();
  arch_loop
  {
    if (itf->size() < 17)
    {
      itf++;
      continue;
    }
    retvec.clear();
    (*it)->getAll(&retvec);
    cout<<"checking time interval comsistency for db file "<<(*itf)<<endl;
    cout<<"the following timestamps should appear in the chronological order"<<endl;
    printf("%.*s    %s    %s    %.*s\n\n", 22, itf->c_str() + 17, retvec.front().timestampstr.c_str(), retvec.back().timestampstr.c_str(), 22,
        itf->c_str() + 41);
    itf++;
  }

  // delete the DbLogs
  arch_loop
    delete (*it);

  cout.flush();
  cout << "==== Compacting ===" << endl;
  cout.flush();
  DbMap dbm_1, dbm_2;
  // loading the uncompacted log
  dbm_1.loadDbLog("/tmp/testlog_no_slice.db");
  // compacting the log
  DbLog dbl_1("/tmp/testlog_no_slice.db");
  pair<int, int> compactStats = dbl_1.compactifyTo("/tmp/testlog_no_slice.db.compacted");
  cout << "Number of Entries Before Compacting : " << compactStats.first << endl;
  cout << "Number of Entries After  Compacting : " << compactStats.second << endl;
  // loading the compacted log
  dbm_2.loadDbLog("/tmp/testlog_no_slice.db.compacted");
  const DbMapTypes::Tkey *key1, *key2;
  const DbMapTypes::Tval *val1, *val2;
  // skip compiler warning
  key1 = key2 = 0;
  val1 = val2 = 0;
  // compare size
  assert(dbm_1.size()==dbm_1.size());
  //compare content
  bool ok = true;
  dbm_2.beginIter();
  for (dbm_1.beginIter(); dbm_1.iterate(&key1, &val1);)
  {
    dbm_2.iterate(&key2, &val2);
    if (!((*key1 == *key2) && (*val1 == *val2)))
    {
      ok = false;
      cout << "!!! non identical entry detected" << endl;
      cout << " Not Compacted " << *key1 << " --> " << *val1 << endl;
      cout << " Compacted " << *key2 << " --> " << *val2 << endl;
    }
  }
  dbm_2.endIter();
  dbm_1.endIter();
  if (ok)
    cout << "compacted and non-compacted resulting maps are identical" << endl;
  else
    assert(false);
  //  cout << " Not Compacted " << dbm_1 << endl;
  //  cout << " Compacted " << dbm_2<< endl;
  cout << "============================" << endl;

  {
    DbMap dbm2;
    cout << "==== Persistency ===" << endl;
    dbm.clear();
    dbm.attachDb("/tmp/testlog_presist.db");
    dbm.set("k1", "v1", "c1");
    dbm.set("k2", "v2", "c2");
    dbm.set("k3", "v3", "c3");
    dbm.set("k1", "v4", "c4");
    dbm.remove("k2");

    dbm.detachDb();

    dbm2.attachDb("/tmp/testlog_presist.db");

    const DbMapTypes::Tkey *key1, *key2;
    const DbMapTypes::Tval *val1, *val2;
    // skip compiler warning
    key1 = key2 = 0;
    val1 = val2 = 0;
    assert(dbm.size()==dbm2.size());
    dbm2.beginIter();
    for (dbm.beginIter(); dbm.iterate(&key1, &val1);)
    {
      dbm2.iterate(&key2, &val2);
      if (!((*key1 == *key2) && (*val1 == *val2)))
      {
        ok = false;
        cout << "!!! non identical entry detected" << endl;
        if (!(*key1 == *key2))
          cout << "keys are different" << endl;
        if (!(*val1 == *val2))
          cout << "values are different" << endl;
        cout << " Saved       " << *key1 << " --> " << *val1 << endl;
        cout << " Back Loaded " << *key2 << " --> " << *val2 << endl;
      }
    }
    dbm2.endIter();
    dbm.endIter();
    dbm.clear();
    assert(dbm.size()==0);
    if (ok)
      cout << "saved and back-loaded resulting maps are identical" << endl;
    else
      assert(false);
    cout << "============================" << endl;
  }

#ifndef EOS_SQLITE_DBMAP
  //SqliteDbMapInterface::SetDebugMode(true); SqliteDbLogInterface::SetDebugMode(true);
  DbMapT<SqliteDbMapInterface, SqliteDbLogInterface> sqdbm;
  sqdbm.attachLog("/tmp/testlog_sqdbm");
  DbMapT<LvDbDbMapInterface, LvDbDbLogInterface> lvdbm;
  lvdbm.attachLog("/tmp/testlog_lvdbm");
  for (int k = 0; k < 10; k++)
  {
    char bk[8],bv[8],br[8];
    sprintf(bk,"k%2.2dsq",k);
    sprintf(bv,"v%2.2dsq",k);
    sprintf(br,"r%2.2dsq",k);
    sqdbm.set(bk, bv, br);
    sprintf(bk,"k%2.2dlv",k);
    sprintf(bv,"v%2.2dlv",k);
    sprintf(br,"r%2.2dlv",k);
    lvdbm.set(bk, bv, br);
  }
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::TlogentryVec sqentryvec;
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::Tlogentry    sqentry;
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::TlogentryVec lventryvec;
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::Tlogentry    lventry;
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface> sqdbl("/tmp/testlog_sqdbm");
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>     lvdbl("/tmp/testlog_lvdbm");
  cout<<"====  SqDbL before copy  ==="<<endl;
  while (sqdbl.getAll(&sqentryvec, 4, &sqentry))
  {
    cout<<sqentryvec<<"----------------------------"<<endl;
    sqentryvec.clear();
  }
  cout<<"============================"<<endl;
  cout<<"====  LvDbL before copy  ==="<<endl;
  while (lvdbl.getAll(&lventryvec, 4, &lventry))
  {
    cout<<lventryvec<<"----------------------------"<<endl;
    lventryvec.clear();
  }
  cout<<"============================"<<endl;

  cout<<"====>  Append the content of SqDbm to LvDbm"<<endl;
  lvdbm.beginSetSequence();
  while (sqdbl.getAll(&sqentryvec, 4, &sqentry))
  {
    for(DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::TlogentryVec::iterator it=sqentryvec.begin();it!=sqentryvec.end();it++)
      lvdbm.set(it->key, it->value, it->comment);
    sqentryvec.clear();
  }
  lvdbm.endSetSequence();
  cout<<"====  LvDbL after copy  ==="<<endl;
  while (lvdbl.getAll(&lventryvec, 4, &lventry))
  {
    cout<<lventryvec<<"----------------------------"<<endl;
    lventryvec.clear();
  }
  cout<<"============================"<<endl;

  cout<<"====>  Append the content of LvDbm to SqDbm"<<endl;
  sqdbm.beginSetSequence();
  while (lvdbl.getAll(&lventryvec, 4, &lventry))
  {
    for(DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::TlogentryVec::iterator it=lventryvec.begin();it!=lventryvec.end();it++)
      sqdbm.set(it->key, it->value, it->comment);
    lventryvec.clear();
  }
  sqdbm.endSetSequence();
  cout<<"====  SqDbL after copy  ==="<<endl;
  while (sqdbl.getAll(&sqentryvec, 4, &sqentry))
  {
    cout<<sqentryvec<<"----------------------------"<<endl;
    sqentryvec.clear();
  }
  cout<<"============================"<<endl;
  sqdbl.setDbFile("");
  lvdbl.setDbFile("");
  cout<<"====>  Convert LvDbl to SqDbl2"<<endl;
  assert(ConvertLevelDb2Sqlite("/tmp/testlog_lvdbm","/tmp/testlog_lvdbm2sqdbm"));
  cout<<"====>  Convert SqDbl to LvDbl2"<<endl;
  assert(ConvertSqlite2LevelDb("/tmp/testlog_sqdbm","/tmp/testlog_sqdbm2lvdbm"));
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface> sqdbl2("/tmp/testlog_lvdbm2sqdbm");
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>     lvdbl2("/tmp/testlog_sqdbm2lvdbm");
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::TlogentryVec sqentryvec2;
  DbLogT<SqliteDbMapInterface,SqliteDbLogInterface>::Tlogentry    sqentry2;
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::TlogentryVec lventryvec2;
  DbLogT<LvDbDbMapInterface,LvDbDbLogInterface>::Tlogentry    lventry2;

  cout<<"====  SqDbL2 vs LvDbl ==="<<endl;
  ok = true;
  lventryvec.clear();
  lventry.key = "";
  sqentryvec2.clear();
  sqentry2.key = "";
  while (lvdbl.getAll(&lventryvec, 1, &lventry))
  {
    sqdbl2.getAll(&sqentryvec2, 1, &sqentry2);
    if (!(sqentryvec2.size() == lventryvec.size()))
    {
      ok = false;
      cout << "!!! non identical size detected" << endl << sqentryvec2 << lventryvec;
      lventryvec.clear();
      sqentryvec2.clear();
      break;
    }
    if (!(sqentryvec2.back() == lventryvec.back()))
    {
      ok=false;
      cout<<"!!! non identical entry detected"<<endl<<sqentryvec2<<lventryvec;
    }
    lventryvec.clear();
    sqentryvec2.clear();
  }
  if (ok)
    cout << "original and copy are identical" << endl;
  else
    assert(false);
  cout<<"============================"<<endl;

  cout<<"====  LvDbL2 vs SqDbl ==="<<endl;
  ok=true;
  sqentryvec.clear();
  sqentry.key = "";
  lventryvec2.clear();
  lventry2.key = "";
  while (sqdbl.getAll(&sqentryvec, 1, &sqentry))
  {
    lvdbl2.getAll(&lventryvec2, 1, &lventry2);
    if (!(lventryvec2.back() == sqentryvec.back()))
    {
      ok=false;
      cout<<"!!! non identical entry detected"<<endl<<lventryvec2<<sqentryvec;
    }
    sqentryvec.clear();
    lventryvec2.clear();
  }
  if (ok)
    cout << "original and copy are identical" << endl;
  else
    assert(false);
  cout<<"============================"<<endl;
  #endif
  cout << "done" << endl;

  return 0;
}
