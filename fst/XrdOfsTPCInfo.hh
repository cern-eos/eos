#ifndef __XRDOFSTPCINFO_HH__
#define __XRDOFSTPCINFO_HH__
/******************************************************************************/
/*                                                                            */
/*                      X r d O f s T P C I n f o . h h                       */
/*                                                                            */
/* (c) 2012 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*   Produced by Andrew Hanushevsky for Stanford University under contract    */
/*              DE-AC02-76-SFO0515 with the Department of Energy              */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/* XRootD is free software: you can redistribute it and/or modify it under    */
/* the terms of the GNU Lesser General Public License as published by the     */
/* Free Software Foundation, either version 3 of the License, or (at your     */
/* option) any later version.                                                 */
/*                                                                            */
/* XRootD is distributed in the hope that it will be useful, but WITHOUT      */
/* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or      */
/* FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public       */
/* License for more details.                                                  */
/*                                                                            */
/* You should have received a copy of the GNU Lesser General Public License   */
/* along with XRootD in a file called COPYING.LESSER (LGPL license) and file  */
/* COPYING (GPL license).  If not, see <http://www.gnu.org/licenses/>.        */
/*                                                                            */
/* The copyright holder's institutional names and contributor's names may not */
/* be used to endorse or promote products derived from this software without  */
/* specific prior written permission of the institution or contributor.       */
/******************************************************************************/

#include <stdlib.h>
#include <string.h>

#include "XrdOuc/XrdOucCallBack.hh"

class XrdOucErrInfo;
class XrdSysMutex;

class XrdOfsTPCInfo
{
public:

int         Fail(XrdOucErrInfo *eRR, const char *eMsg, int eCode);

int         Match(const char *cKey, const char *cOrg,
                  const char *xLfn, const char *xDst);

void        Reply(int rC, int eC, const char *eMsg, XrdSysMutex *mP=0);

const char *Set(const char *cKey, const char *cOrg,
                const char *xLfn, const char *xDst,
                const char *xCks=0);

int         SetCB(XrdOucErrInfo *eRR);

            XrdOfsTPCInfo(const char *vKey=0, const char *vOrg=0,
                          const char *vLfn=0, const char *vDst=0,
                          const char *vCks=0) : cbP(0),
                      Cks(vCks ? strdup(vCks) :0),
                      Key(vKey ? strdup(vKey) :0),
                      Org(vOrg ? strdup(vOrg) :0),
                      Lfn(vLfn ? strdup(vLfn) :0),
                      Dst(vDst ? strdup(vDst) :0) {}

           ~XrdOfsTPCInfo() {if (Key) {free(Key); Key = 0;}
                             if (Org) {free(Org); Org = 0;}
                             if (Lfn) {free(Lfn); Lfn = 0;}
                             if (Dst) {free(Dst); Dst = 0;}
                             if (Cks) {free(Cks); Cks = 0;}
                             if (cbP) delete cbP;
                        }

XrdOucCallBack *cbP;   // Callback object
char           *Cks;   // Checksum information (only at dest)
char           *Key;   // Rendezvous key    or src  URL
char           *Org;   // Rendezvous origin
char           *Lfn;   // Rendezvous path   or dest LFN
char           *Dst;   // Rendezvous dest   or dest PFN
};
#endif
