#!/bin/bash
aclocal -I m4 --force || exit 1
if libtoolize --version 1 >/dev/null 2>/dev/null; then
  libtoolize --copy --automake || exit 1
elif glibtoolize --version 1 >/dev/null 2>/dev/null; then
  glibtoolize --copy --automake || exit 1
fi

automake -ac --add-missing --foreign || exit 1
autoconf


