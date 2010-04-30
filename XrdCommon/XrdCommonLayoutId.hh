#ifndef __XRDCOMMON_LAYOUTID__HH__
#define __XRDCOMMON_LAYOUTID__HH__
/*----------------------------------------------------------------------------*/
class XrdCommonLayoutId {
public:
  enum eLayoutId {kPlain=0x1,kPlainAdler=0x2};
  
  XrdCommonLayoutId();
  ~XrdCommonLayoutId();

};
/*----------------------------------------------------------------------------*/

#endif
