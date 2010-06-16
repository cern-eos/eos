//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   FileSystemView test
//------------------------------------------------------------------------------

#ifndef FILE_SYSTEM_VIEW_TEST_HH
#define FILE_SYSTEM_VIEW_TEST_HH

#include <cppunit/TestCase.h>

//------------------------------------------------------------------------------
// Hierarchical View test
//------------------------------------------------------------------------------
class FileSystemViewTest: public CppUnit::TestCase
{
  public:
    FileSystemViewTest(): CppUnit::TestCase() {}
    virtual ~FileSystemViewTest() {}
    void fileSystemViewTest();
    static CppUnit::Test *suite();
};

#endif // FILE_SYSTEM_VIEW_TEST_HH
