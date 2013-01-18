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

const unsigned long int NNUM_THREADS=10;
pthread_t threads[NNUM_THREADS];
unsigned long int NUM_THREADS=NNUM_THREADS;
int writecount;

const char* dbfile="/tmp/test.log";
int total, transacSize;
bool overwrite;
DbMap *globmap;	

void* TestWrite(void *threadid) {
    char ks[]="key_xxxxxx";
    char vs[]="value_xxxxxx";
    char cs[]="comment_xxxxxx";

	DbMap m;
	m.AttachLog(dbfile);
	m.BeginSetSequence();
	for(int k=0;k<total;k++) {
		if(k%transacSize==0 && k>0) {
			m.EndSetSequence();
			m.BeginSetSequence();
		}
		if(!overwrite) sprintf(ks+4,"%6.6d",k);
		sprintf(vs+6,"%6.6d",k);
		sprintf(cs+8,"%6.6d",k);
		m.Set(ks,vs,cs);
	}
	m.EndSetSequence();
	pthread_exit(NULL);
}

void* TestWriteGlob(void *threadid) {
    char ks[]="key_xxxxxx_xx";
    char vs[]="value_xxxxxx";
    char cs[]="comment_xxxxxx";

    unsigned long int thrid= (unsigned long int) pthread_self();

	for(int k=0;k<total;k++) {
	    AtomicInc(writecount);
		if(writecount%transacSize==0 && writecount>0) {
			globmap->EndSetSequence();
			globmap->BeginSetSequence();
		}
		if(!overwrite) sprintf(ks+4,"%6.6d_%2.2lu",k,thrid);
		sprintf(vs+6,"%6.6d",k);
		sprintf(cs+8,"%6.6d",k);
		globmap->Set(ks,vs,cs);
	}
	pthread_exit(NULL);
}

void RunThreads() {
    void *ret;
    for (unsigned long int t = 0; t < NUM_THREADS; t++) {
		int rc = pthread_create(&threads[t], NULL, TestWrite, (void *) t);
		if (rc) {
			printf("ERROR; return code from pthread_create() is %d\n", rc);
			exit(-1);
		}
	}
	for (unsigned long int t = 0; t < NUM_THREADS; t++)
		pthread_join(threads[t], &ret);
}

void RunThreadsGlob() {
    void *ret;
    globmap->BeginSetSequence();
    for (unsigned long int t = 0; t < NUM_THREADS; t++) {
		int rc = pthread_create(&threads[t], NULL, TestWriteGlob, (void *) t);
		if (rc) {
			printf("ERROR; return code from pthread_create() is %d\n", rc);
			exit(-1);
		}
	}
	for (unsigned long int t = 0; t < NUM_THREADS; t++)
		pthread_join(threads[t], &ret);
	globmap->EndSetSequence();
}

int main() {
	//SqliteDbMapInterface::SetDebugMode(true);  LvDbDbMapInterface::SetDebugMode(true);
	int n=100000;
	//int n=10000;
	const int nsizes=4;
	const int transacsize[nsizes]={100000,100,10,1};
	//const int nsizes=1;
	//const int transacsize[nsizes]={100};
	time_t seconds;
	int elapsed;
	string rm_cmd("rm -rf ");
	rm_cmd+=dbfile;
	
	freopen ("/dev/null","w",stderr); // to get rid of the pending mutex messages

#ifdef EOS_SQLITE_DBMAP
	printf("Using SQLITE3 DbMap/DbLog implementation\n\n");
#else
	printf("Using LEVELDB DbMap/DbLog implementation\n\n");
#endif

	printf("Performing %lu writings shared among %lu threads in a new db log file according to different schemes.\n\n",n*NUM_THREADS,NUM_THREADS);
	for(int k=0;k<nsizes;k++) {
        printf("\t==>> %d blocks of size %d writing from %lu maps\n",n/transacsize[k],transacsize[k],NUM_THREADS);
		system(rm_cmd.c_str());
		seconds = time (NULL);
		total=n; transacSize=transacsize[k]; overwrite=false;
		RunThreads();
		elapsed=(int)time (NULL)-(int)seconds;
		printf("\t     write test took %d sec (%lf writes/sec)\n",elapsed,float(n*NUM_THREADS)/elapsed);

		system(rm_cmd.c_str());
		seconds = time (NULL);
		total=n; transacSize=transacsize[k]; overwrite=true;
		RunThreads();
		elapsed=(int)time (NULL)-(int)seconds;
		printf("\t     overwrite test took %d sec (%lf writes/sec)\n",elapsed,float(n*NUM_THREADS)/elapsed);

		printf("\t==>> %d blocks of size %d writing from 1 maps\n",n/transacsize[k],transacsize[k]);
		system(rm_cmd.c_str());
		globmap=new DbMap; globmap->AttachLog(dbfile);
		seconds = time (NULL);
		total=n; transacSize=transacsize[k]; overwrite=false; writecount=0;
		RunThreadsGlob();
		elapsed=(int)time (NULL)-(int)seconds;
		printf("\t     write test took %d sec (%lf writes/sec)\n",elapsed,float(n*NUM_THREADS)/elapsed);
		delete globmap;

		system(rm_cmd.c_str());
		globmap=new DbMap; globmap->AttachLog(dbfile);
		seconds = time (NULL);
		total=n; transacSize=transacsize[k]; overwrite=true; writecount=0;
		RunThreadsGlob();
		elapsed=(int)time (NULL)-(int)seconds;
		printf("\t     overwrite test took %d sec (%lf writes/sec)\n",elapsed,float(n*NUM_THREADS)/elapsed);
		delete globmap;

		printf("\n");
	}

	printf("Performing %lu writings (1 thread) in a new db log file according to different schemes.\n\n",n*NUM_THREADS);
	n*=NUM_THREADS; NUM_THREADS=1;
	for(int k=0;k<nsizes;k++) {
		printf("\t==>> %d blocks of size %d writing\n",n/transacsize[k],transacsize[k]);
		system(rm_cmd.c_str());
		seconds = time (NULL);
		total=n; transacSize=transacsize[k]; overwrite=false;
		RunThreads();
		elapsed=(int)time (NULL)-(int)seconds;
		printf("\t     write test took %d sec (%lf writes/sec)\n",elapsed,float(n)/elapsed);
		system(rm_cmd.c_str());
		seconds = time (NULL);
		total=n; transacSize=transacsize[k]; overwrite=true;
		RunThreads();
		elapsed=(int)time (NULL)-(int)seconds;
		printf("\t     overwrite test took %d sec (%lf writes/sec)\n",elapsed,float(n)/elapsed);

		printf("\n");
	}

	int nfill=10e6;
	printf("Performing %d writings (1 thread) in a db log file already containing %d entries\n\n",n,nfill);
	system(rm_cmd.c_str());
	total=nfill; transacSize=transacsize[0]; overwrite=false;
	printf("\tPrefilling..."); fflush(stdout);
	RunThreads();
	printf("done\n");
	seconds = time (NULL);
	total=n; transacSize=transacsize[0]; overwrite=false;
	RunThreads();
	elapsed=(int)time (NULL)-(int)seconds;
	printf("\t     write test took %d sec (%lf writes/sec)\n",elapsed,float(n)/elapsed);
	printf("\n");

	printf("Performing 1000000 readings in db log file.\n\n");
	DbLog log(dbfile);
	DbLog::TlogentryVec entries;
	seconds = time (NULL);
	log.GetTail(1000000,entries);
	elapsed=(int)time (NULL)-(int)seconds;
	printf("\t     read test took %d sec (%f reads/sec)\n",elapsed,float(1000000)/elapsed);

	system(rm_cmd.c_str());

	return 0;
}
