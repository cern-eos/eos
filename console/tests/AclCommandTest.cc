//------------------------------------------------------------------------------
//! @file AclCommandTest.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "AclCommandTest.hh"

CPPUNIT_TEST_SUITE_REGISTRATION(AclCommandTest);

void AclCommandTest::TestSyntaxCommand(std::string command, bool outcome)
{
  AclCommand test(const_cast<char*>(command.c_str()));
  bool result = test.ProcessCommand();
  CPPUNIT_ASSERT(result == outcome);
}


void AclCommandTest::TestSyntax()
{
  // Basic test functionality
  TestSyntaxCommand("--sys rule path", true);
  TestSyntaxCommand("--user rule path", true);
  TestSyntaxCommand("-l path", true);
  TestSyntaxCommand("-lR path", true);
  TestSyntaxCommand("rule path", true);
  TestSyntaxCommand("-R --recursive rule path", true);
  TestSyntaxCommand("-FD --recursive rule path", false);
  TestSyntaxCommand("-Rgg --recursive rule path", false);
}

void AclCommandTest::TestFunctionality()
{
  ReqRes temp;
  AclCommand test{""};
  // Forbid cout use.
  std::cout.setstate(std::ios_base::failbit);
  // Single listing
  {
    test = AclCommand{"-l test"};
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test");
    temp.second = std::string(
                    "sys.forced.blockchecksum=\"crc32c\"\n"
                    "sys.forced.blocksize=\"4k\"\n"
                    "sys.forced.checksum=\"adler\"\n"
                    "sys.forced.layout=\"replica\"\n"
                    "sys.forced.nstripes=\"2\"\n"
                    "sys.forced.space=\"default\"\n"
                  );
    test.m_mgm_execute.m_queue.push(temp);
    test.Execute();
    CPPUNIT_ASSERT(test.m_mgm_execute.test_failed == false);
  }
  // Recursive listing
  {
    test = AclCommand{"-lR test"};
    temp.first = std::string("mgm.cmd=find&mgm.path=/test&mgm.option=d");
    temp.second = std::string(
                    "/test/\n"
                    "/test/abc/\n"
                    "/test/abc/a/\n"
                    "/test/abc/b/\n"
                    "/test/abc/c/\n"
                    "/test/test1/\n"
                    "/test/test1/d/\n"
                    "/test/test1/e/\n"
                    "/test/test1/f/\n"
                    "/test/test2/\n"
                  );
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/");
    temp.second = std::string(
                    "sys.forced.blockchecksum=\"crc32c\"\n"
                    "sys.forced.blocksize=\"4k\"\n"
                    "sys.forced.checksum=\"adler\"\n"
                    "sys.forced.layout=\"replica\"\n"
                    "sys.forced.nstripes=\"2\"\n"
                    "sys.forced.space=\"default\"\n"
                  );
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/abc/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/abc/a/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/abc/b/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/abc/c/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test1/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test1/d/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test1/e/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test1/f/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test2/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    test.Execute();
    CPPUNIT_ASSERT(test.m_mgm_execute.test_failed == false);
  }
  // Single add
  {
    test = AclCommand{"u:user1:+wr+d!d!u-r test"};
    temp.first = std::string("mgm.cmd=whoami");
    temp.second = "Virtual Identity: uid=0 (2,99,3,0) gid=0 (99,4,0) "
                  "[authz:sss] sudo* host=localhost";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test");
    temp.second = std::string(
                    "sys.forced.blockchecksum=\"crc32c\"\n"
                    "sys.forced.blocksize=\"4k\"\n"
                    "sys.forced.checksum=\"adler\"\n"
                    "sys.forced.layout=\"replica\"\n"
                    "sys.forced.nstripes=\"2\"\n"
                    "sys.forced.space=\"default\"\n"
                  );
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=u:user1:w!d+d!u&mgm.path=/test");
    temp.second =
      "success: set attribute sys.acl=\"u:user1:w!d+d!u\" in file/directory /test";
    test.m_mgm_execute.m_queue.push(temp);
    test.Execute();
    CPPUNIT_ASSERT(test.m_mgm_execute.test_failed == false);
  }
  // Recursive set
  {
    test = AclCommand{"-R g:group1=rw!uc-r++d test"};
    temp.first = std::string("mgm.cmd=whoami");
    temp.second = "Virtual Identity: uid=0 (2,99,3,0) gid=0 (99,4,0) "
                  "[authz:sss] sudo* host=localhost";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=find&mgm.path=/test&mgm.option=d");
    temp.second = std::string(
                    "/test/\n"
                    "/test/abc/\n"
                    "/test/abc/a/\n"
                    "/test/abc/b/\n"
                    "/test/abc/c/\n"
                    "/test/test1/\n"
                    "/test/test1/d/\n"
                    "/test/test1/e/\n"
                    "/test/test1/f/\n"
                    "/test/test2/\n"
                  );
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/");
    temp.second = std::string(
                    "sys.acl=\"u:user1:w!d+d!u\"\n"
                    "sys.forced.blockchecksum=\"crc32c\"\n"
                    "sys.forced.blocksize=\"4k\"\n"
                    "sys.forced.checksum=\"adler\"\n"
                    "sys.forced.layout=\"replica\"\n"
                    "sys.forced.nstripes=\"2\"\n"
                    "sys.forced.space=\"default\"\n"
                  );
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc,u:user1:w!d+d!u&"
                             "mgm.path=/test/");
    temp.second =
      "success: set attribute sys.acl=\"g:group1:w+d!uc,u:user1:w!d+d!u\""
      " in file/directory /test";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/abc/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/abc/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/abc";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/abc/a/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/abc/a/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/abc/a";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/abc/b/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/abc/b/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/abc/b";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/abc/c/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/abc/c/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/abc/c";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test1/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/test1/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/test1";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test1/d/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/test1/d/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/test1/d";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test1/e/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/test1/e/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/test1/e";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test1/f/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/test1/f/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/test1/f";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=/test/test2/");
    temp.second = "";
    test.m_mgm_execute.m_queue.push(temp);
    temp.first = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl&"
                             "mgm.attr.value=g:group1:w+d!uc&mgm.path=/test/test2/");
    temp.second = "success: set attribute sys.acl=\"g:group1:w+d!uc\" in "
                  "file/directory /test/test2";
    test.m_mgm_execute.m_queue.push(temp);
    test.Execute();
    CPPUNIT_ASSERT(test.m_mgm_execute.test_failed == false);
  }
  std::cout.clear();
}
