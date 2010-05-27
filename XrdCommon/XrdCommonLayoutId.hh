#ifndef __XRDCOMMON_LAYOUTID__HH__
#define __XRDCOMMON_LAYOUTID__HH__
/*----------------------------------------------------------------------------*/
class XrdCommonLayoutId {
public:
  enum eLayoutId {kPlain=0x1,kPlainAdler=0x2, kPlainCRC32=0x3, kPlainMD5=0x4, kPlainSHA1=0x5};
  
  XrdCommonLayoutId();
  ~XrdCommonLayoutId();

};
/*----------------------------------------------------------------------------*/

#endif
