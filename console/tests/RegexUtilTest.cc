#include "RegexUtilTest.hh"

CPPUNIT_TEST_SUITE_REGISTRATION(RegexUtilTest);

void RegexUtilTest::TestUtility()
{
  RegexUtil test;
  std::string origin("asdfasfsssstest12kksdjftestossskso");
  std::string temp;
  // Pass case
  CPPUNIT_ASSERT_NO_THROW(test.SetOrigin(origin));
  CPPUNIT_ASSERT_NO_THROW(test.SetRegex("test[0-9]+"));
  CPPUNIT_ASSERT_NO_THROW(test.initTokenizerMode());
  CPPUNIT_ASSERT_NO_THROW(temp = test.Match());
  CPPUNIT_ASSERT(temp ==  "test12");
  CPPUNIT_ASSERT_NO_THROW(temp = test.Match());
  CPPUNIT_ASSERT(temp ==  "test12");
  // Few fail cases
  test = RegexUtil();
  CPPUNIT_ASSERT_NO_THROW(test.SetOrigin(origin));
  CPPUNIT_ASSERT_THROW(test.SetRegex("test[0-9"),  std::string);
  test = RegexUtil();
  CPPUNIT_ASSERT_THROW(test.SetRegex("test[0-9"),  std::string);
  CPPUNIT_ASSERT_THROW(test.initTokenizerMode(),  std::string);
}
