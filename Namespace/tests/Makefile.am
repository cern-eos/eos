AM_CPPFLAGS = @CPPUNIT_CFLAGS@
AM_LDFLAGS = @CPPUNIT_LIBS@

INCLUDES = -I$(EOS_INCDIR) -I../../

libEosNamespaceTests_la_SOURCES =      \
	OtherTests.cc                  \
	ChangeLogFileMDSvcTest.cc      \
	ChangeLogContainerMDSvcTest.cc \
	FileSystemViewTest.cc          \
	HierarchicalViewTest.cc        \
	ChangeLogTest.cc


libEosNamespaceTests_CPPFLAGS = $(INCLUDES)

text_runner_SOURCES = TextRunner.cc
text_runner_CPPFLAGS = $(INCLUDES)
text_runner_DEPENDENCIES = libEosNamespaceTests.la
text_runner_LDADD = libEosNamespaceTests.la ../libEosNamespace.la

noinst_PROGRAMS = text_runner
lib_LTLIBRARIES = libEosNamespaceTests.la

