#!/bin/bash
aclocal  --force || exit 1
if libtoolize --version 1 >/dev/null 2>/dev/null; then
  libtoolize --copy --automake || exit 1
elif glibtoolize --version 1 >/dev/null 2>/dev/null; then
  glibtoolize --copy --automake || exit 1
fi

autoconf
automake -ac --add-missing --foreign || exit 1



