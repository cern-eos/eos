#!/bin/bash

CFSMOUNT="/cern/home/"
CFSPROC="/cern/home/.cfsd/"
alias rm="cfs_rm"

function cfs() {
    if [ "$1" == "recycle" ]; then
	shift
	cfs_recycle $@
	return 
    fi
}

function cfs_rm() {

    if [ "$1" != "-rf" ]; then
	eval '/bin/rm $@'
	return
    fi

    rpath=`realpath $2`;
    dpath=`dirname $rpath`;
    bpath=`basename $2`;
    if [ ! -e "$rpath" ]; then
       eval '/bin/rm $@'
       return
    fi
       
    CFSMOUNTLEN=${#CFSMOUNT}
    CFSPROCLEN=${#CFSPROC}
    crpath=${rpath:0:$CFSMOUNTLEN}
    prpath=${rpath:0:$CFSPROCLEN}
    if [ "$crpath" != "$CFSMOUNT" ]; then
	eval '/bin/rm $@'
	return
    fi

    if [ "$prpath" == "$CFSPROC" ]; then
	eval '/bin/rm $@'
	return
    fi
    
    cfs_checkcfs user_id

    if [ "${user_id}" == "" ]; then
	eval '/bin/rm $@'
	return 
    fi
    
    uid=`echo $user_id | tr ' ' '\n' | grep uid | cut -d ":" -f2`;
    if [ $uid == "" ]; then
	return ;
    fi
    recyclepath="$CFSMOUNT/.cfsd/recycle/uid:$uid/"

    year=`date +"%Y"`
    month=`date +"%m"`
    day=`date +"%d"`

    
    if [ -e "$recyclepath/$year/$month/$day" ]; then
	# good
	echo -n ""
    else
	if [ -d "$rpath" ]; then
	    # trigger creation of recycle dirs
	    touch "$rpath/.delete";
	    rm "$rpath/.delete";
	else
	    # trigger creation of recycle dirs
	    touch "$dpath/.delete";
	    rm "$dpath/.delete";
	fi
    fi

    if [ -e "$recyclepath/$year/$month/$day" ]; then
	inode=`stat -c '%i' $dpath`;
	targetdir="$recyclepath/$year/$month/$day/$inode.#_recycle_#/"
	targetpath="$recyclepath/$year/$month/$day/$inode.#_recycle_#/$bpath"
	mkdir -p $targetdir
	echo "# Doing fast deletion into recycle bin: restorepath='$rpath'"
	mv "$rpath" "$targetpath"
    else
	eval '/bin/rm $@'
    fi
}

function cfs_checkcfs() {
    userinfo=`getfattr -n cfs.id $CFSMOUNT 2> /dev/null | grep cfs.id | cut -d "=" -f 2 | sed s/\"//g`;
    if [ $? -ne 0 ]; then
	$1="";
    fi
    eval $1='$userinfo';
}

function cfs_byte_conversion() {
    if [ $1 -ge 1000000000000 ]; then
	echo -n "$1" | awk '{printf("%.02f TB",$1/1000000000000);}'
	return
    fi

    if [ $1 -ge 1000000000 ]; then
	echo -n "$1" | awk '{printf("%.02f GB",$1/1000000000);}'
	return
    fi

    if [ $1 -ge 1000000 ]; then
	echo -n "$1" | awk '{printf("%.02f MB",$1/1000000);}'
	return
    fi
    echo -n "$1 Bytes"
}

function cfs_recycle() {
    cfs_checkcfs user_id
    
    if [ "${user_id}" == "" ]; then
	return ;
    fi
    uid=`echo $user_id | tr ' ' '\n' | grep uid | cut -d ":" -f2`;
    if [ $uid == "" ]; then
	return ;
    fi
    recyclepath="$CFSMOUNT/.cfsd/recycle/uid:$uid/"
    
    year=`date +"%Y"`
    month=`date +"%m"`
    day=`date +"%d"`

    if [ "$1"  == "purge" ]; then
	echo "# Purging recycle bin for '$user_id' ...";
	rm -r $recyclepath 2> /dev/null
	echo "# done!";
	return
    fi
    
    if [ "$1"  == "info" ]; then
	ndirs=`getfattr -n ceph.dir.rsubdirs $recyclepath 2> /dev/null | grep ceph.dir.rsubdirs | cut -d "=" -f 2 | sed s/\"//g`
	if [ $? -ne 0 ]; then ndirs=0;fi 
	nfiles=`getfattr -n ceph.dir.rfiles $recyclepath 2> /dev/null | grep ceph.dir.rfiles | cut -d "=" -f 2 | sed s/\"//g`
	if [ $? -ne 0 ]; then nfiles=0;fi  
	bytes=`getfattr -n ceph.dir.rbytes $recyclepath 2> /dev/null | grep ceph.dir.rbytes | cut -d "=" -f 2 | sed s/\"//g`
	if [ $? -ne 0 ]; then bytes=0; fi
	echo "# Recycle bin for '$user_id' : "
	echo "#                              files       :    $nfiles"
	echo "#                              directories :    $ndirs"
	echo -n "#                              size        :    "
	cfs_byte_conversion $bytes
	echo ""
	return
    fi

    if [ "$1"  == "ls" ]; then
	find $recyclepath -type f
	return
    fi

    if [ "$1" == "restore" ]; then
	restoreinput=$2;
	restoretarget=`dirname $restoreinput`;
	restoredir=`basename $restoreinput`;
	if [ ! -d "$restoretarget" ]; then
	    echo "error: you have to provide the restore directory path as first argument!";
	    return -1;
	fi
	inode=`stat -c '%i' $restoretarget`;
	   
	targetpath="$recyclepath/$year/$month/$day/$inode.#_recycle_#/"
	if [ -d "$targetpath/$restoredir" ]; then
	    echo "# Restoring $restoredir to $restoreinput!";
	    mv $targetpath $restoreinput
	    return
	else
	    echo "error: I cannot find the directory you want to restore!"
	    return -1
	fi
    fi
}

