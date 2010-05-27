//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog file metadata service test
//------------------------------------------------------------------------------

#ifndef CHANGE_LOG_FILE_MD_SVC_TEST_HH
#define CHANGE_LOG_FILE_MD_SVC_TEST_HH

#include <cppunit/TestCase.h>

//------------------------------------------------------------------------------
// Generic file md tests
//------------------------------------------------------------------------------
class ChangeLogFileMDSvcTest: public CppUnit::TestCase
{
  public:
    ChangeLogFileMDSvcTest(): CppUnit::TestCase() {}
    virtual ~ChangeLogFileMDSvcTest() {}
    void reloadTest();
    static CppUnit::Test *suite();
};

#endif // CHANGE_LOG_FILE_MD_SVC_TEST_HH
