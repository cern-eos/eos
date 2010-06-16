AM_CPPFLAGS = @CPPUNIT_CFLAGS@
AM_LDFLAGS = @CPPUNIT_LIBS@

#changelog_filemd_test_SOURCES = ChangelogFileMDSvcTest.cc FileMDSvcTest.cc
INCLUDES = -I$(EOS_INCDIR) -I../../

changelog_test_SOURCES = ChangeLogTest.cc ChangeLogTest.hh
changelog_test_CPPFLAGS = $(INCLUDES)
changelog_test_DEPENDENCIES = ../libEosNamespace.la
changelog_test_LDADD = ../libEosNamespace.la

hierarchical_view_test_SOURCES = HierarchicalViewTest.cc HierarchicalViewTest.hh
hierarchical_view_test_CPPFLAGS = $(INCLUDES)
hierarchical_view_test_DEPENDENCIES = ../libEosNamespace.la
hierarchical_view_test_LDADD = ../libEosNamespace.la

filesystem_view_test_SOURCES = FileSystemViewTest.cc FileSystemViewTest.hh
filesystem_view_test_CPPFLAGS = $(INCLUDES)
filesystem_view_test_DEPENDENCIES = ../libEosNamespace.la
filesystem_view_test_LDADD = ../libEosNamespace.la

other_tests_SOURCES = OtherTests.cc OtherTests.hh
other_tests_CPPFLAGS = $(INCLUDES)
other_tests_DEPENDENCIES = ../libEosNamespace.la
other_tests_LDADD = ../libEosNamespace.la

changelog_containermdsvc_test_SOURCES = \
	ChangeLogContainerMDSvcTest.cc       \
	ChangeLogContainerMDSvcTest.hh
changelog_containermdsvc_test_CPPFLAGS = $(INCLUDES)
changelog_containermdsvc_test_DEPENDENCIES = ../libEosNamespace.la
changelog_containermdsvc_test_LDADD = ../libEosNamespace.la

changelog_filemdsvc_test_SOURCES = \
	ChangeLogFileMDSvcTest.cc       \
	ChangeLogFileMDSvcTest.hh
changelog_filemdsvc_test_CPPFLAGS = $(INCLUDES)
changelog_filemdsvc_test_DEPENDENCIES = ../libEosNamespace.la
changelog_filemdsvc_test_LDADD = ../libEosNamespace.la

UNITTESTS_PROGS =                      \
	changelog_containermdsvc_test  \
	changelog_filemdsvc_test       \
	changelog_test                 \
	hierarchical_view_test         \
	other_tests                    \
	filesystem_view_test

noinst_PROGRAMS = $(UNITTESTS_PROGS)
check_PROGRAMS = $(UNITTESTS_PROGS)
TESTS = $(check_PROGRAMS)

