#!/bin/bash

create_dmg_with_icon() {
    set -e
    VOLNAME="$1"
    DMG="$2"
    SRC_DIR="$3"
    ICON_FILE="$4"
    CODESIGN_IDENTITY="$5"

    TMP_DMG="$(mktemp -u -t XXXXXXX)"
    trap 'RESULT=$?; rm -f "$TMP_DMG"; exit $RESULT' INT QUIT TERM EXIT
    hdiutil create -srcfolder "$SRC_DIR" -volname "$VOLNAME" -fs HFS+ \
	    -fsargs "-c c=64,a=16,e=16" -format UDRW "$TMP_DMG"
    TMP_DMG="${TMP_DMG}.dmg" # because OSX appends .dmg
    DEVICE="$(hdiutil attach -readwrite -noautoopen "$TMP_DMG" | awk 'NR==1{print$1}')"
    VOLUME="$(mount | grep "$DEVICE" | sed 's/^[^ ]* on //;s/ ([^)]*)$//')"
    # start of DMG changes
    cp "$ICON_FILE" "$VOLUME/.VolumeIcon.icns"
    SetFile -c icnC "$VOLUME/.VolumeIcon.icns"
    SetFile -a C "$VOLUME"
    # end of DMG changes
    hdiutil detach "$DEVICE"
    hdiutil convert "$TMP_DMG" -format UDZO -imagekey zlib-level=9 -o "$DMG"
    if [ -n "$CODESIGN_IDENTITY" ]; then
	codesign -s "$CODESIGN_IDENTITY" -v "$DMG"
    fi
}


if [ $# -lt 1 ]; then
    echo "Usage: $0 eos_version <cmake_opts>"
    exit 1
fi

GIT_TOP_PATH="$(git rev-parse --show-toplevel)"
CWD="$(pwd)"

if [ "${GIT_TOP_PATH}" != "${CWD}" ]; then
    echo "error: script must be launched from the root of the project"
    exit 1
fi

EOS_VERSION=$1
TMP_BUILD="${GIT_TOP_PATH}/build/"
TMP_DEST=/tmp/eos.dst
rm -rf ${TMP_BUILD}
mkdir -p ${TMP_BUILD}
rm -rf ${TMP_DEST}
mkdir -p ${TMP_DEST}/usr/local/bin/
mkdir -p ${TMP_DEST}/usr/local/lib/
CMAKE_OPT=""

if [ $# -eq 2 ]; then
   CMAKE_OPT=$2
fi

# Build eos
cd ${TMP_BUILD}
cmake -DCLIENT=1 -DCMAKE_INSTALL_PREFIX=${TMP_DEST}/usr/local -DZLIB_ROOT=/usr/local/opt/zlib/ -DOPENSSL_ROOT=/usr/local/opt/openssl/ -DNCURSES_ROOT=/usr/local/opt/ncurses/ -DZMQ_ROOT=/usr/local/opt/zeromq/ -DXROOTD_ROOT=/usr/local/opt/xrootd/ -DUUID_ROOT=/usr/local/opt/ossp-uuid -DSPARSEHASH_ROOT=/usr/local/opt/google-sparsehash/ ${CMAKE_OPT} ..

if [ $? -ne 0 ]; then
    echo "error: cmake configuration failed"
    exit 1
fi

# Build and install
make -j 4 && make install

if [ $? -ne 0 ]; then
n    echo "error: build/install failed"
    exit 1
fi

# Copy non-XRootD dependencies e.g openssl, ncurses for eos, eosd and eosxd
for EOS_EXEC in "${TMP_DEST}/usr/local/bin/eosd" "${TMP_DEST}/usr/local/bin/eosxd" "${TMP_DEST}/usr/local/bin/eos"; do
  for NAME in `otool -L $EOS_EXEC | grep -v rpath | grep /usr/local/ | awk '{print $1}' | grep -v ":" | grep -v libosxfuse | grep -v libXrd`; do
    echo $NAME
    if [ -n "$NAME" ];  then
      sn=`echo $NAME | awk -F "/" '{print $NF}'`
      cp -v $NAME /tmp/eos.dst/usr/local/lib/$sn
    fi
  done
done

# Copy XRootD dependencies and executables
cp -v /usr/local/opt/xrootd/lib/libXrd* /tmp/eos.dst/usr/local/lib/
cp -v /usr/local/opt/xrootd/bin/* /tmp/eos.dst/usr/local/bin/

# Exchange the eosx script with the eos binary
mv ${TMP_DEST}/usr/local/bin/eos ${TMP_DEST}/usr/local/bin/eos.exe
cp -v ${GIT_TOP_PATH}/utils/eosx ${TMP_DEST}/usr/local/bin/eos
chmod ugo+rx ${TMP_DEST}/usr/local/bin/eos
pkgbuild --install-location / --version $EOS_VERSION --identifier com.eos.pkg.app --root /tmp/eos.dst EOS.pkg

rm -rf dmg
mkdir dmg
cp EOS.pkg dmg/
cp ${GIT_TOP_PATH}/utils/README.osx dmg/README.txt
# cp ../var/eos/html/EOS-logo.jpg dmg/
unlink eos-osx-$EOS_VERSION.dmg >& /dev/null
create_dmg_with_icon eos-osx-${EOS_VERSION} eos-osx-${EOS_VERSION}.dmg dmg ../icons/EOS.icns
# create_dmg_with_icon Frobulator Frobulator.dmg path/to/frobulator/dir path/to/someicon.icns [ 'Andreas-Joachim Peters' ]
cd ..
