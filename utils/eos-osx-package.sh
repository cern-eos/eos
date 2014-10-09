#!/bin/bash
# give the desired versioon name like '0.3.49-beryl' and the path to the XRootD build directory as argument like '/Users/apeters/Software/xrootd-3.3.6/build'
# the third argument is an optional extra lib to add like '/opt/local/lib/libuuid.16.dylib'
VERSION=$1

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

rm -rf /tmp/eos.dst/
mkdir -p /tmp/eos.dst/

make install DESTDIR=/tmp/eos.dst/
cd $2
make install DESTDIR=/tmp/eos.dst/
cd -

if [ -n "$3" ];  then
  cp -v $3 /tmp/eos.dst/usr/lib/
fi

pkgbuild --install-location / --version $VERSION --identifier com.eos.pkg.app --root /tmp/eos.dst EOS.pkg

rm -rf dmg
mkdir dmg
cp EOS.pkg dmg/
cp ../var/eos/html/EOS-logo.jpg dmg/
unlink EOS-$VERSION.dmg
create_dmg_with_icon EOS-$VERSION EOS-$VERSION.dmg dmg ../var/eos/html/EOS-logo.icns
# create_dmg_with_icon Frobulator Frobulator.dmg path/to/frobulator/dir path/to/someicon.icns [ 'Andreas-Joachim Peters' ]

