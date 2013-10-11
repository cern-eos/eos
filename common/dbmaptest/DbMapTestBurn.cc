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
 * @file   DbMapTestBurn.cc
 *
 * @brief  This program performs some intensive reading and writing operations using the DbMap class.
 *         Several access schemes are tested (concurrency, block sizes) and speed measurements are provided.
 *         Note that by defining EOS_SQLITE_DBMAP while building the common lib, the test
 *		   will meter the performances of the SQLITE implementation.
 *		   Before running this program it might be need to rm -rf /tmp/testlog*
 *
 */

#include "common/DbMap.hh"
#include <sstream>
#include <cstdio>
#include <sys/resource.h>
#include "XrdSys/XrdSysAtomics.hh"
using namespace eos::common;

const unsigned long int NNUM_THREADS = 10;
pthread_t threads[NNUM_THREADS];
unsigned long int NUM_THREADS = NNUM_THREADS;
int writecount;

const char* dbfile = "/tmp/testlog.log";
int total, transacSize;
bool outofcore = true;
bool useseqid = true;
bool uselog = true;
bool prefilltest = true;
bool overwrite;
DbMap *globmap;

// for valgrind
//const int nn = 10000;
//const int nsizes = 1;
//const int transacsize[nsizes] =
//{ 1000 };
//const int maxsize = 1000;

const int nn=100000;
const int nsizes=4;
const int transacsize[nsizes]={10000,100,10,1};
const int maxsize=10000;

int n = nn;
char data_key[NNUM_THREADS][nn][32];
char data_value[NNUM_THREADS][nn][32];
char data_comment[NNUM_THREADS][nn][32];

void
PrepareDataToWrite()
{
  for (int t = 0; t < (int) NUM_THREADS; t++)
    for (int s = 0; s < nn; s++)
    {
      sprintf(data_key[t][s], "key_%6.6d_%2.2d", s, t);
      sprintf(data_value[t][s], "value_%6.6d_%2.2d", s, t);
      sprintf(data_comment[t][s], "comment_%6.6d_%2.2d", s, t);
    }
}

void*
TestWriteOnTheFly(void *threadid)
{
  unsigned long int nthr = ((unsigned long int) threadid);
  char ks[] = "key_xxxxxxx";
  char vs[] = "value_xxxxxxx";
  char cs[] = "comment_xxxxxxx";

  DbMap m;
  if (outofcore)
  {
    char dbname[256];
    sprintf(dbname, "/tmp/testlog_%lu_.db", nthr);
    //printf("Attach DB /tmp/testlog%lu.db\n",nthr
    m.useSeqId(useseqid);
    m.attachDb(dbname);
    m.outOfCore(true);
  }

  if (uselog)
    m.attachLog(dbfile);
  m.beginSetSequence();
  for (int k = 0; k < total; k++)
  {
    if (k % transacSize == 0 && k > 0)
    {
      m.endSetSequence();
      m.beginSetSequence();
    }
    if (!overwrite)
      sprintf(ks + 4, "%7.7d", k);
    sprintf(vs + 6, "%7.7d", k);
    sprintf(cs + 8, "%7.7d", k);
    m.set(ks, vs, cs);
  }
  m.endSetSequence();
  pthread_exit(NULL);
}

void*
TestWrite(void *threadid)
{
  unsigned long int nthr = ((unsigned long int) threadid);

  DbMap m;
  //m.UseSeqId(false);
  if (outofcore)
  {
    char dbname[256];
    sprintf(dbname, "/tmp/testlog_%lu_.db", nthr);
    //printf("Attach DB /tmp/testlog%lu.db\n",nthr);
    m.useSeqId(useseqid);
    m.attachDb(dbname);
    m.outOfCore(true);
  }

  if (uselog)
    m.attachLog(dbfile);

  m.beginSetSequence();
  for (int k = 0; k < total; k++)
  {
    if (k % transacSize == 0 && k > 0)
    {
      m.endSetSequence();
      m.beginSetSequence();
    }
    m.set(data_key[0][overwrite ? 0 : k], data_key[0][k], data_key[0][k]);
  }
  m.endSetSequence();
  pthread_exit(NULL);
}

void*
TestWriteGlob(void *threadid)
{
  unsigned long int thrid = ((unsigned long int) threadid);
  //unsigned long int thrid= (unsigned long int) pthread_self();

  for (int k = 0; k < total; k++)
  {
    AtomicInc(writecount);
    if (writecount % transacSize == 0 && writecount > 0)
    {
      globmap->endSetSequence();
      globmap->beginSetSequence();
    }
    globmap->set(data_key[thrid][overwrite ? 0 : k], data_key[thrid][k], data_key[thrid][k]);
  }
  pthread_exit(NULL);
}

void
RunThreads()
{
  void *ret;
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
  {
    int rc = pthread_create(&threads[t], NULL, TestWrite, (void *) t);
    if (rc)
    {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);
}

void
RunThreadsOnTheFly()
{
  void *ret;
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
  {
    int rc = pthread_create(&threads[t], NULL, TestWriteOnTheFly, (void *) t);
    if (rc)
    {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);
}

void
RunThreadsGlob()
{
  void *ret;
  globmap->beginSetSequence();
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
  {
    int rc = pthread_create(&threads[t], NULL, TestWriteGlob, (void *) t);
    if (rc)
    {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);
  globmap->endSetSequence();
}

int
main()
{
  //SqliteDbMapInterface::SetDebugMode(true);  //LvDbDbMapInterface::SetDebugMode(true);
  time_t seconds;
  int elapsed;
  string rm_cmd("rm -rf ");
  rm_cmd += dbfile;

  freopen("/dev/null", "w", stderr); // to get rid of the pending mutex messages

  {
    cout << "WARNING, proceeding rm -rf /tmp/testlog*, are you sure y/n ? ";
    char c;
    cin >> c;
    cout << endl;
    if (c != 'y')
      exit(1);
    system("rm -rf /tmp/testlog*");
  }

#ifdef EOS_SQLITE_DBMAP
  printf("Using SQLITE3 DbMap/DbLog implementation\n");
#else
  printf("Using LEVELDB DbMap/DbLog implementation\n");
#endif
  printf("Out-Of-Core is %s\n", outofcore ? "ON" : "OFF");
  printf("Use-Seq-Id  is %s\n", useseqid ? "ON" : "OFF");
  printf("Logging is     %s\n\n", uselog ? "ON" : "OFF");

  printf("Generating Data to Write Into The DB\n");
  PrepareDataToWrite();

  printf("Performing %lu writings shared among %lu threads in a new db log file according to different schemes.\n\n", n * NUM_THREADS, NUM_THREADS);
  for (int k = 0; k < nsizes; k++)
  {
    printf("\t==>> %d blocks of size %d writing from %lu maps\n", n / transacsize[k], transacsize[k], NUM_THREADS);
    system(rm_cmd.c_str());
    seconds = time(NULL);
    total = n;
    transacSize = transacsize[k];
    overwrite = false;
    RunThreads();
    elapsed = (int) time(NULL) - (int) seconds;
    printf("\t     write test took %d sec (%lf writes/sec)\n", elapsed, float(n * NUM_THREADS) / elapsed);

    system(rm_cmd.c_str());
    seconds = time(NULL);
    total = n;
    transacSize = transacsize[k];
    overwrite = true;
    RunThreads();
    elapsed = (int) time(NULL) - (int) seconds;
    printf("\t     overwrite test took %d sec (%lf writes/sec)\n", elapsed, float(n * NUM_THREADS) / elapsed);

    printf("\t==>> %d blocks of size %d writing from 1 maps\n", n / transacsize[k], transacsize[k]);
    system(rm_cmd.c_str());
    globmap = new DbMap;
    if (uselog)
      globmap->attachLog(dbfile);
    if (outofcore)
    {
      globmap->useSeqId(useseqid);
      globmap->attachDb("/tmp/testlog.db");
      globmap->outOfCore(true);
    }
    seconds = time(NULL);
    total = n;
    transacSize = transacsize[k];
    overwrite = false;
    writecount = 0;
    RunThreadsGlob();
    elapsed = (int) time(NULL) - (int) seconds;
    printf("\t     write test took %d sec (%lf writes/sec)\n", elapsed, float(n * NUM_THREADS) / elapsed);
    delete globmap;

    system(rm_cmd.c_str());
    globmap = new DbMap;
    if (uselog)
      globmap->attachLog(dbfile);
    if (outofcore)
    {
      globmap->useSeqId(useseqid);
      globmap->attachDb("/tmp/testlog.db");
      globmap->outOfCore(true);
    }
    seconds = time(NULL);
    total = n;
    transacSize = transacsize[k];
    overwrite = true;
    writecount = 0;
    RunThreadsGlob();
    elapsed = (int) time(NULL) - (int) seconds;
    printf("\t     overwrite test took %d sec (%lf writes/sec)\n", elapsed, float(n * NUM_THREADS) / elapsed);
    delete globmap;

    printf("\n");
  }

  printf("Performing %lu writings (1 thread) in a new db log file according to different schemes.\n\n", n * NUM_THREADS);
  n *= NUM_THREADS;
  NUM_THREADS = 1;
  for (int k = 0; k < nsizes; k++)
  {
    printf("\t==>> %d blocks of size %d writing\n", n / transacsize[k], transacsize[k]);
    system(rm_cmd.c_str());
    seconds = time(NULL);
    total = n;
    transacSize = transacsize[k];
    overwrite = false;
    RunThreads();
    elapsed = (int) time(NULL) - (int) seconds;
    printf("\t     write test took %d sec (%lf writes/sec)\n", elapsed, float(n) / elapsed);
    system(rm_cmd.c_str());
    seconds = time(NULL);
    total = n;
    transacSize = transacsize[k];
    overwrite = true;
    RunThreads();
    elapsed = (int) time(NULL) - (int) seconds;
    printf("\t     overwrite test took %d sec (%lf writes/sec)\n", elapsed, float(n) / elapsed);

    printf("\n");
  }

  if (prefilltest)
  {
    int nfill = 10e6;
    printf("Performing %d writings (1 thread) in a db log file already containing %d entries\n\n", n, nfill);
    system(rm_cmd.c_str());
    total = nfill;
    transacSize = transacsize[0];
    overwrite = false;
    printf("\tPrefilling...");
    fflush(stdout);
    RunThreadsOnTheFly();
    printf("done\n");
    if (outofcore)
    {
      printf("Performing %d read (1 thread) in a db log file already containing %d entries\n\n", n, nfill);
//      LvDbInterfaceBase::Option o;
//      o.BloomFilterNbits=0;
//      o.CacheSizeMb=0;
      DbMap m;

      char dbname[256];
      sprintf(dbname, "/tmp/testlog_0_.db");
      m.attachDb(dbname);
      m.outOfCore(true);
      m.useSeqId(useseqid);

      int thresh = RAND_MAX / 10 * 9;
      seconds = time(NULL);
      int kk = 0;
      for (int k = 0; k < 10 * nfill; k++)
      {
        int r = rand();
        if (r < thresh)
        { // we call just a hundredth of the data
          kk = rand() * nfill / RAND_MAX;
          kk -= kk % 100;
        }
        else
        {
          kk = rand() * nfill / RAND_MAX;
        }
        char ks[] = "key_xxxxxxx";
        sprintf(ks + 4, "%7.7d", kk);
        DbMap::Tval val;
        if (!m.get(ks, &val))
          std::cout << "Error Fetching Key " << ks << std::endl;
      }
      elapsed = (int) time(NULL) - (int) seconds;
      printf("\t     random read test took %d sec (%lf read/sec)\n", elapsed, float(10 * nfill) / elapsed);
    }
    seconds = time(NULL);
    total = n;
    transacSize = transacsize[0];
    overwrite = false;
    RunThreads();
    elapsed = (int) time(NULL) - (int) seconds;
    printf("\t     write test took %d sec (%lf writes/sec)\n", elapsed, float(n) / elapsed);
    printf("\n");
  }

  if (uselog)
  {
    printf("Performing 1000000 readings in db log file.\n\n");
    DbLog log(dbfile);
    DbLog::TlogentryVec entries;
    seconds = time(NULL);
    log.getTail(1000000, &entries);
    elapsed = (int) time(NULL) - (int) seconds;
    printf("\t     read test took %d sec (%f reads/sec)\n", elapsed, float(1000000) / elapsed);
  }
  //system(rm_cmd.c_str());

  return 0;
}
