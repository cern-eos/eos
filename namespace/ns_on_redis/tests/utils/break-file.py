#!/usr/bin/env python
#-------------------------------------------------------------------------------
# author: Lukasz Janyst <ljanyst@cern.ch>
# desc:   Overwrite random dwords in a file with random bytes
#-------------------------------------------------------------------------------

import sys, os, random, array

#-------------------------------------------------------------------------------
# Print help
#-------------------------------------------------------------------------------
def printHelp():
    print "Usage:"
    print "  ", sys.argv[0], " filename numRegions maxRegionSize"
    print "       numRegions is defaulted to 100"
    print "       maxRegionSize is defaulted to 10 bytes"


#-------------------------------------------------------------------------------
# Break the stuff
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    #---------------------------------------------------------------------------
    # Check the commandline
    #---------------------------------------------------------------------------
    maxRegionSize = 10  # maximas size of a broken region in bytes
    numRegions    = 100 # number of regions to be broken

    if len(sys.argv) < 2:
        printHelp()
        sys.exit(1)

    if len(sys.argv) >= 3:
        numRegions = int( sys.argv[2] )

    if len(sys.argv) == 4:
        numRegions = int( sys.argv[3] )

    if len(sys.argv) > 4:
        printHelp()
        sys.exit()

    #--------------------------------------------------------------------------
    # Stat the file
    #--------------------------------------------------------------------------
    try:
        size = os.stat( sys.argv[1] ).st_size
    except OSError, e:
        print "[!] There's a problem with your file:", e
        sys.exit(2)

    #---------------------------------------------------------------------------
    # Break the stuff
    #---------------------------------------------------------------------------
    try:
        f = file( sys.argv[1], "r+" )
        for i in xrange( numRegions ):
            offset  = int(random.random()*size)
            regSize = int(random.random()*maxRegionSize)
            if offset+regSize > size:
                offset = size-regSize
            f.seek( offset )

            rBytes  = array.array( 'c' )
            for j in range( regSize ):
                rBytes.append( chr(int(random.random() *255)) )
            f.write( rBytes.tostring() )
        f.close()
    except IOError, e:
        print "[!] There's a problem with your file:", e
        sys.exit(3)
