// ----------------------------------------------------------------------
// File: HttpHandlerFstFileCacheTests
// Author: David Smith - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include "TestEnv.hh"
#include "fst/http/HttpHandlerFstFileCache.hh"
#include "fst/XrdFstOfsFile.hh"
#include <XrdSfs/XrdSfsInterface.hh>
#include <gtest/gtest.h>
#include <unistd.h>
#include <atomic>
#include <memory>

std::atomic<uint64_t> nFakeClose = 0;
std::atomic<uint64_t> nFakeDest  = 0;
std::atomic<uint64_t> nCtr       = 0;

class FakeOfsFile : public eos::fst::XrdFstOfsFile {
public:
  FakeOfsFile(const char* user, int MonID = 0) : XrdFstOfsFile(user, MonID) { }
  virtual ~FakeOfsFile() { oh = 0; nFakeDest++; }
  int close() override { nFakeClose++; return 0; }
  int open(const char* fileName, XrdSfsFileOpenMode openMode,
           mode_t createMode, const XrdSecEntity* client,
           const char* opaque = 0) override {
    mFn = fileName; return 0;
  }

  std::string mFn;
};

TEST(FstFileCacheTest, StoreFetch)
{
  XrdSfsFileOpenMode open_mode = 0;
  eos::fst::HttpHandlerFstFileCache fc;
  eos::fst::HttpHandlerFstFileCache::Entry entry;
  std::unique_ptr<eos::fst::XrdFstOfsFile> fpP = std::make_unique<FakeOfsFile>("");
  eos::fst::XrdFstOfsFile *fp = fpP.get();

  const int dc1 = nFakeDest;

  {
    eos::fst::HttpHandlerFstFileCache::Key
       cachekey("clientname", "/myurl1", "data=val1", open_mode);

    entry.set(cachekey, fp);
    bool iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.clear();
    entry = fc.remove(cachekey);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp);
  }

  const int dc2 = nFakeDest;
  ASSERT_TRUE( dc2 == dc1 );
}

TEST(FstFileCacheTest, StoreFetchMultiSameFile)
{
  XrdSfsFileOpenMode open_mode = 0;
  eos::fst::HttpHandlerFstFileCache fc;
  eos::fst::HttpHandlerFstFileCache::Entry entry;
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp1P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp2P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp3P = std::make_unique<FakeOfsFile>("");
  eos::fst::XrdFstOfsFile *fp1 = fp1P.get();
  eos::fst::XrdFstOfsFile *fp2 = fp2P.get();
  eos::fst::XrdFstOfsFile *fp3 = fp3P.get();

  const int dc1 = nFakeDest;

  {
    eos::fst::HttpHandlerFstFileCache::Key
       cachekey("clientname", "/myurl1", "data=val1", open_mode);
    bool iret;

    entry.set(cachekey, fp1);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey, fp3);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey, fp2);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    // fetch them with most recently used (inserted) first
    entry.clear();
    entry = fc.remove(cachekey);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp2);
    entry = fc.remove(cachekey);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp3);
    entry = fc.remove(cachekey);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp1);
  }

  const int dc2 = nFakeDest;
  ASSERT_TRUE( dc2 == dc1 );
}

TEST(FstFileCacheTest, StoreFetchMultiDifferentFiles)
{
  XrdSfsFileOpenMode open_mode = 0;
  eos::fst::HttpHandlerFstFileCache fc;
  eos::fst::HttpHandlerFstFileCache::Entry entry;
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp1P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp2P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp3P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp4P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp5P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp6P = std::make_unique<FakeOfsFile>("");
  eos::fst::XrdFstOfsFile *fp1 = fp1P.get();
  eos::fst::XrdFstOfsFile *fp2 = fp2P.get();
  eos::fst::XrdFstOfsFile *fp3 = fp3P.get();
  eos::fst::XrdFstOfsFile *fp4 = fp4P.get();
  eos::fst::XrdFstOfsFile *fp5 = fp5P.get();
  eos::fst::XrdFstOfsFile *fp6 = fp6P.get();

  const int dc1 = nFakeDest;

  {
    eos::fst::HttpHandlerFstFileCache::Key
       cachekey1("clientname", "/myurl1", "data=val1", open_mode);
    eos::fst::HttpHandlerFstFileCache::Key
       cachekey2("clientname", "/myurl2", "data=val1", open_mode);
    bool iret;

    entry.set(cachekey1, fp1);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey1, fp3);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey2, fp2);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey1, fp4);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey2, fp5);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey2, fp6);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);


    // fetch them with most recently used (inserted) first
    entry.clear();
    entry = fc.remove(cachekey1);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp4);
    entry = fc.remove(cachekey2);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp6);
    entry = fc.remove(cachekey2);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp5);
    entry = fc.remove(cachekey1);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp3);
    entry = fc.remove(cachekey2);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp2);
    entry = fc.remove(cachekey1);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp1);
  }

  const int dc2 = nFakeDest;
  ASSERT_TRUE( dc2 == dc1 );
}

TEST(FstFileCacheTest, StoreFetchDifferentOpaque)
{
  XrdSfsFileOpenMode open_mode = 0;
  eos::fst::HttpHandlerFstFileCache fc;
  eos::fst::HttpHandlerFstFileCache::Entry entry;
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp1P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp2P = std::make_unique<FakeOfsFile>("");
  std::unique_ptr<eos::fst::XrdFstOfsFile> fp3P = std::make_unique<FakeOfsFile>("");
  eos::fst::XrdFstOfsFile *fp1 = fp1P.get();
  eos::fst::XrdFstOfsFile *fp2 = fp2P.get();
  eos::fst::XrdFstOfsFile *fp3 = fp3P.get();

  const int dc1 = nFakeDest;

  {
    eos::fst::HttpHandlerFstFileCache::Key
       cachekey1("clientname", "/myurl1", "data=val1", open_mode);
    eos::fst::HttpHandlerFstFileCache::Key
       cachekey2("clientname", "/myurl1", "data=val2", open_mode);
    bool iret;

    entry.set(cachekey1, fp1);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey1, fp3);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    entry.set(cachekey2, fp2);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    // fetch them with most recently used (inserted) first
    entry.clear();
    entry = fc.remove(cachekey2);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp2);
    entry = fc.remove(cachekey1);
    ASSERT_TRUE(entry.getfp() == fp3);
    entry = fc.remove(cachekey2);
    ASSERT_FALSE(entry);
    entry = fc.remove(cachekey1);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry.getfp() == fp1);
  }

  const int dc2 = nFakeDest;
  ASSERT_TRUE( dc2 == dc1 );
}


TEST(FstFileCacheTest, CacheExpire)
{
  setenv("EOS_FST_HTTP_FHCACHE_IDLETIME", "0.2", 1);
  setenv("EOS_FST_HTTP_FHCACHE_IDLERES", "0.001", 1);

  XrdSfsFileOpenMode open_mode = 0;
  eos::fst::HttpHandlerFstFileCache fc;
  eos::fst::HttpHandlerFstFileCache::Entry entry;
  eos::fst::XrdFstOfsFile *fp=0;

  {
    eos::fst::HttpHandlerFstFileCache::Key
       cachekey("clientname", "/myurl1", "data=val1", open_mode);
    bool iret;


    fp = new FakeOfsFile{"clientname"};
    const int cc1 = nFakeClose;
    const int dc1 = nFakeDest;
    entry.set(cachekey, fp);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    // entry should timeout and be deleted
    usleep(350 *1E3);

    entry = fc.remove(cachekey);
    ASSERT_FALSE(entry);
    const int cc2 = nFakeClose;
    const int dc2 = nFakeDest;
    ASSERT_TRUE( cc2 == cc1 + 1);
    ASSERT_TRUE( dc2 == dc1 + 1);

    // new OfsFile
    fp = new FakeOfsFile{"clientname"};
    entry.set(cachekey, fp);
    iret = fc.insert(entry);
    ASSERT_TRUE(iret);

    // entry should not timeout
    usleep(10 *1E3);

    entry = fc.remove(cachekey);
    ASSERT_TRUE(entry);
    ASSERT_TRUE(nFakeClose == cc2);
    delete entry.getfp();
    ASSERT_TRUE(nFakeClose == cc2);
    ASSERT_TRUE(nFakeDest == dc2+1);
  }

  unsetenv("EOS_FST_HTTP_FHCACHE_IDLETIME");
  unsetenv("EOS_FST_HTTP_FHCACHE_IDLERES");
}

TEST(FstFileCacheTest, ConcurrentCacheUse)
{
  constexpr int nth = 10;
  constexpr int nloop = 5'000;

  eos::fst::HttpHandlerFstFileCache fc;
  std::vector<std::thread> tv;

  const int cc1 = nFakeClose;
  const int dc1 = nFakeDest;

  tv.reserve(nth);
  for(int i=0;i<nth;i++) {
    tv.emplace_back([&fc]() {
      for(uint64_t l = 0;l<nloop;++l) {
        FakeOfsFile *fp = new FakeOfsFile{"clientname"};
        char fnbuf[256];
        const uint64_t n = nCtr++;
        sprintf(fnbuf,"/file%d", n%(nth/3));

        XrdSfsFileOpenMode open_mode = 0;

        eos::fst::HttpHandlerFstFileCache::Key
          cachekey("clientname", fnbuf, "data=val1", open_mode);
        eos::fst::HttpHandlerFstFileCache::Entry entry;

        fp->open(fnbuf, open_mode, 0, 0, 0);
        entry.set(cachekey, fp);
        bool iret = fc.insert(entry);
        ASSERT_TRUE(iret);

        entry.clear();
        fp = nullptr;

        // we should get back an fp for a file with our
        // filename, but not necessarily the same object
        // we inserted
        entry = fc.remove(cachekey);
        ASSERT_TRUE(entry);
        fp = (FakeOfsFile*)entry.getfp();
        ASSERT_TRUE(fp->mFn == fnbuf);

        fp->close();
        delete fp;
      }
    });
  }

  for(int i=0;i<nth;i++) {
    tv[i].join();
  }

  ASSERT_TRUE(nFakeClose == cc1 + nth*nloop);
  ASSERT_TRUE(nFakeDest == dc1 + nth*nloop);

}

TEST(FstFileCacheTest, CacheDestrTest)
{
  const int cc1 = nFakeClose;
  const int dc1 = nFakeDest;
  {
    eos::fst::HttpHandlerFstFileCache fc;
    FakeOfsFile *fp = new FakeOfsFile{"clientname"};
    XrdSfsFileOpenMode open_mode = 0;

    eos::fst::HttpHandlerFstFileCache::Key
      cachekey("clientname", "/file", "data=val1", open_mode);
    eos::fst::HttpHandlerFstFileCache::Entry entry;
    entry.set(cachekey, fp);
    bool iret = fc.insert(entry);
    ASSERT_TRUE(iret);
    // we allow the FileCache object to go out of scope while
    // containing an entry. We expect the etry to be closed
    // and destroyed.
  }

  ASSERT_TRUE(nFakeClose == cc1 + 1);
  ASSERT_TRUE(nFakeDest == dc1 + 1);
}
