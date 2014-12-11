// ----------------------------------------------------------------------
// File: S3Handler.cc
// Author: Justin Lewis Salmon - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "common/http/s3/S3Handler.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
std::string
S3Handler:: ContentType (const std::string &path)
{
  XrdOucString name = path.c_str();
  if (name.endswith(".3g2")) return "video/3gpp2"; 
  if (name.endswith(".3gp")) return "video/3gpp"; 
  if (name.endswith(".3gp2")) return "video/3gpp2"; 
  if (name.endswith(".3gpp")) return "video/3gpp"; 
  if (name.endswith(".aa")) return "audio/audible"; 
  if (name.endswith(".aac")) return "audio/vnd.dlna.adts"; 
  if (name.endswith(".aax")) return "audio/vnd.audible.aax"; 
  if (name.endswith(".addin")) return "text/xml"; 
  if (name.endswith(".adt")) return "audio/vnd.dlna.adts"; 
  if (name.endswith(".adts")) return "audio/vnd.dlna.adts"; 
  if (name.endswith(".ai")) return "application/postscript"; 
  if (name.endswith(".aif")) return "audio/aiff"; 
  if (name.endswith(".aifc")) return "audio/aiff"; 
  if (name.endswith(".aiff")) return "audio/aiff"; 
  if (name.endswith(".application")) return "application/x-ms-application"; 
  if (name.endswith(".asax")) return "application/xml"; 
  if (name.endswith(".ascx")) return "application/xml"; 
  if (name.endswith(".asf")) return "video/x-ms-asf"; 
  if (name.endswith(".ashx")) return "application/xml"; 
  if (name.endswith(".asmx")) return "application/xml"; 
  if (name.endswith(".aspx")) return "application/xml"; 
  if (name.endswith(".asx")) return "video/x-ms-asf"; 
  if (name.endswith(".au")) return "audio/basic"; 
  if (name.endswith(".avi")) return "video/avi"; 
  if (name.endswith(".bmp")) return "image/bmp"; 
  if (name.endswith(".btapp")) return "application/x-bittorrent-app"; 
  if (name.endswith(".btinstall")) return "application/x-bittorrent-appinst"; 
  if (name.endswith(".btkey")) return "application/x-bittorrent-key"; 
  if (name.endswith(".btsearch")) return "application/x-bittorrentsearchdescription+xml"; 
  if (name.endswith(".btskin")) return "application/x-bittorrent-skin"; 
  if (name.endswith(".cat")) return "application/vnd.ms-pki.seccat"; 
  if (name.endswith(".cd")) return "text/plain"; 
  if (name.endswith(".cer")) return "application/x-x509-ca-cert"; 
  if (name.endswith(".config")) return "application/xml"; 
  if (name.endswith(".contact")) return "text/x-ms-contact"; 
  if (name.endswith(".crl")) return "application/pkix-crl"; 
  if (name.endswith(".crt")) return "application/x-x509-ca-cert"; 
  if (name.endswith(".cs")) return "text/plain"; 
  if (name.endswith(".csproj")) return "text/plain"; 
  if (name.endswith(".css")) return "text/css"; 
  if (name.endswith(".csv")) return "application/vnd.ms-excel"; 
  if (name.endswith(".datasource")) return "application/xml"; 
  if (name.endswith(".der")) return "application/x-x509-ca-cert"; 
  if (name.endswith(".dib")) return "image/bmp"; 
  if (name.endswith(".dll")) return "application/x-msdownload"; 
  if (name.endswith(".doc")) return "application/msword"; 
  if (name.endswith(".docm")) return "application/vnd.ms-word.document.macroEnabled.12"; 
  if (name.endswith(".docx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"; 
  if (name.endswith(".dot")) return "application/msword"; 
  if (name.endswith(".dotm")) return "application/vnd.ms-word.template.macroEnabled.12"; 
  if (name.endswith(".dotx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.template"; 
  if (name.endswith(".dtd")) return "application/xml-dtd"; 
  if (name.endswith(".dtsconfig")) return "text/xml"; 
  if (name.endswith(".eps")) return "application/postscript"; 
  if (name.endswith(".exe")) return "application/x-msdownload"; 
  if (name.endswith(".fdf")) return "application/vnd.fdf"; 
  if (name.endswith(".fif")) return "application/fractals"; 
  if (name.endswith(".gif")) return "image/gif"; 
  if (name.endswith(".group")) return "text/x-ms-group"; 
  if (name.endswith(".hdd")) return "application/x-virtualbox-hdd"; 
  if (name.endswith(".hqx")) return "application/mac-binhex40"; 
  if (name.endswith(".hta")) return "application/hta"; 
  if (name.endswith(".htc")) return "text/x-component"; 
  if (name.endswith(".htm")) return "text/html"; 
  if (name.endswith(".html")) return "text/html"; 
  if (name.endswith(".hxa")) return "application/xml"; 
  if (name.endswith(".hxc")) return "application/xml"; 
  if (name.endswith(".hxd")) return "application/octet-stream"; 
  if (name.endswith(".hxe")) return "application/xml";
  if (name.endswith(".hxf")) return "application/xml"; 
  if (name.endswith(".hxh")) return "application/octet-stream"; 
  if (name.endswith(".hxi")) return "application/octet-stream"; 
  if (name.endswith(".hxk")) return "application/xml"; 
  if (name.endswith(".hxq")) return "application/octet-stream"; 
  if (name.endswith(".hxr")) return "application/octet-stream"; 
  if (name.endswith(".hxs")) return "application/octet-stream"; 
  if (name.endswith(".hxt")) return "application/xml"; 
  if (name.endswith(".hxv")) return "application/xml"; 
  if (name.endswith(".hxw")) return "application/octet-stream"; 
  if (name.endswith(".ico")) return "image/x-icon"; 
  if (name.endswith(".ics")) return "text/calendar"; 
  if (name.endswith(".ipa")) return "application/x-itunes-ipa"; 
  if (name.endswith(".ipg")) return "application/x-itunes-ipg"; 
  if (name.endswith(".ipsw")) return "application/x-itunes-ipsw"; 
  if (name.endswith(".iqy")) return "text/x-ms-iqy"; 
  if (name.endswith(".iss")) return "text/plain"; 
  if (name.endswith(".ite")) return "application/x-itunes-ite"; 
  if (name.endswith(".itlp")) return "application/x-itunes-itlp"; 
  if (name.endswith(".itls")) return "application/x-itunes-itls"; 
  if (name.endswith(".itms")) return "application/x-itunes-itms"; 
  if (name.endswith(".itpc")) return "application/x-itunes-itpc"; 
  if (name.endswith(".jfif")) return "image/jpeg"; 
  if (name.endswith(".jnlp")) return "application/x-java-jnlp-file"; 
  if (name.endswith(".jpe")) return "image/jpeg"; 
  if (name.endswith(".jpeg")) return "image/jpeg"; 
  if (name.endswith(".jpg")) return "image/jpeg"; 
  if (name.endswith(".js")) return "application/javascript"; 
  if (name.endswith(".latex")) return "application/x-latex"; 
  if (name.endswith(".library-ms")) return "application/windows-library+xml"; 
  if (name.endswith(".m1v")) return "video/mpeg"; 
  if (name.endswith(".m2t")) return "video/vnd.dlna.mpeg-tts"; 
  if (name.endswith(".m2ts")) return "video/vnd.dlna.mpeg-tts"; 
  if (name.endswith(".m2v")) return "video/mpeg"; 
  if (name.endswith(".m3u")) return "audio/mpegurl"; 
  if (name.endswith(".m3u8")) return "audio/x-mpegurl"; 
  if (name.endswith(".m4a")) return "audio/m4a"; 
  if (name.endswith(".m4b")) return "audio/m4b"; 
  if (name.endswith(".m4p")) return "audio/m4p"; 
  if (name.endswith(".m4r")) return "audio/x-m4r"; 
  if (name.endswith(".m4v")) return "video/x-m4v"; 
  if (name.endswith(".magnet")) return "application/x-magnet"; 
  if (name.endswith(".man")) return "application/x-troff-man"; 
  if (name.endswith(".master")) return "application/xml"; 
  if (name.endswith(".mht")) return "message/rfc822"; 
  if (name.endswith(".mhtml")) return "message/rfc822"; 
  if (name.endswith(".mid")) return "audio/mid"; 
  if (name.endswith(".midi")) return "audio/mid"; 
  if (name.endswith(".mod")) return "video/mpeg"; 
  if (name.endswith(".mov")) return "video/quicktime"; 
  if (name.endswith(".mp2")) return "audio/mpeg"; 
  if (name.endswith(".mp2v")) return "video/mpeg"; 
  if (name.endswith(".mp3")) return "audio/mpeg"; 
  if (name.endswith(".mp4")) return "video/mp4"; 
  if (name.endswith(".mp4v")) return "video/mp4"; 
  if (name.endswith(".mpa")) return "video/mpeg"; 
  if (name.endswith(".mpe")) return "video/mpeg"; 
  if (name.endswith(".mpeg")) return "video/mpeg"; 
  if (name.endswith(".mpf")) return "application/vnd.ms-mediapackage"; 
  if (name.endswith(".mpg")) return "video/mpeg"; 
  if (name.endswith(".mpv2")) return "video/mpeg"; 
  if (name.endswith(".mts")) return "video/vnd.dlna.mpeg-tts"; 
  if (name.endswith(".odc")) return "text/x-ms-odc"; 
  if (name.endswith(".odg")) return "application/vnd.oasis.opendocument.graphics"; 
  if (name.endswith(".odm")) return "application/vnd.oasis.opendocument.text-master"; 
  if (name.endswith(".odp")) return "application/vnd.oasis.opendocument.presentation"; 
  if (name.endswith(".ods")) return "application/vnd.oasis.opendocument.spreadsheet"; 
  if (name.endswith(".odt")) return "application/vnd.oasis.opendocument.text"; 
  if (name.endswith(".otg")) return "application/vnd.oasis.opendocument.graphics-template"; 
  if (name.endswith(".oth")) return "application/vnd.oasis.opendocument.text-web"; 
  if (name.endswith(".ots")) return "application/vnd.oasis.opendocument.spreadsheet-template"; 
  if (name.endswith(".ott")) return "application/vnd.oasis.opendocument.text-template"; 
  if (name.endswith(".ova")) return "application/x-virtualbox-ova"; 
  if (name.endswith(".ovf")) return "application/x-virtualbox-ovf"; 
  if (name.endswith(".oxt")) return "application/vnd.openofficeorg.extension"; 
  if (name.endswith(".p10")) return "application/pkcs10"; 
  if (name.endswith(".p12")) return "application/x-pkcs12"; 
  if (name.endswith(".p7b")) return "application/x-pkcs7-certificates"; 
  if (name.endswith(".p7c")) return "application/pkcs7-mime"; 
  if (name.endswith(".p7m")) return "application/pkcs7-mime"; 
  if (name.endswith(".p7r")) return "application/x-pkcs7-certreqresp"; 
  if (name.endswith(".p7s")) return "application/pkcs7-signature"; 
  if (name.endswith(".pcast")) return "application/x-podcast"; 
  if (name.endswith(".pdf")) return "application/pdf"; 
  if (name.endswith(".pdfxml")) return "application/vnd.adobe.pdfxml"; 
  if (name.endswith(".pdx")) return "application/vnd.adobe.pdx"; 
  if (name.endswith(".pfx")) return "application/x-pkcs12"; 
  if (name.endswith(".pko")) return "application/vnd.ms-pki.pko"; 
  if (name.endswith(".pls")) return "audio/scpls"; 
  if (name.endswith(".png")) return "image/png"; 
  if (name.endswith(".pot")) return "application/vnd.ms-powerpoint"; 
  if (name.endswith(".potm")) return "application/vnd.ms-powerpoint.template.macroEnabled.12"; 
  if (name.endswith(".potx")) return "application/vnd.openxmlformats-officedocument.presentationml.template"; 
  if (name.endswith(".ppa")) return "application/vnd.ms-powerpoint"; 
  if (name.endswith(".ppam")) return "application/vnd.ms-powerpoint.addin.macroEnabled.12"; 
  if (name.endswith(".pps")) return "application/vnd.ms-powerpoint"; 
  if (name.endswith(".ppsm")) return "application/vnd.ms-powerpoint.slideshow.macroEnabled.12"; 
  if (name.endswith(".ppsx")) return "application/vnd.openxmlformats-officedocument.presentationml.slideshow"; 
  if (name.endswith(".ppt")) return "application/vnd.ms-powerpoint"; 
  if (name.endswith(".pptm")) return "application/vnd.ms-powerpoint.presentation.macroEnabled.12"; 
  if (name.endswith(".pptx")) return "application/vnd.openxmlformats-officedocument.presentationml.presentation"; 
  if (name.endswith(".prf")) return "application/pics-rules"; 
  if (name.endswith(".ps")) return "application/postscript"; 
  if (name.endswith(".psc1")) return "application/PowerShell"; 
  if (name.endswith(".pwz")) return "application/vnd.ms-powerpoint"; 
  if (name.endswith(".py")) return "text/plain"; 
  if (name.endswith(".pyw")) return "text/plain"; 
  if (name.endswith(".rat")) return "application/rat-file"; 
  if (name.endswith(".rc")) return "text/plain"; 
  if (name.endswith(".rc2")) return "text/plain"; 
  if (name.endswith(".rct")) return "text/plain"; 
  if (name.endswith(".rdlc")) return "application/xml"; 
  if (name.endswith(".resx")) return "application/xml"; 
  if (name.endswith(".rmi")) return "audio/mid"; 
  if (name.endswith(".rmp")) return "application/vnd.rn-rn_music_package"; 
  if (name.endswith(".rqy")) return "text/x-ms-rqy"; 
  if (name.endswith(".rtf")) return "application/msword"; 
  if (name.endswith(".sct")) return "text/scriptlet"; 
  if (name.endswith(".settings")) return "application/xml"; 
  if (name.endswith(".shtml")) return "text/html"; 
  if (name.endswith(".sit")) return "application/x-stuffit"; 
  if (name.endswith(".sitemap")) return "application/xml"; 
  if (name.endswith(".skin")) return "application/xml"; 
  if (name.endswith(".sldm")) return "application/vnd.ms-powerpoint.slide.macroEnabled.12"; 
  if (name.endswith(".sldx")) return "application/vnd.openxmlformats-officedocument.presentationml.slide"; 
  if (name.endswith(".slk")) return "application/vnd.ms-excel"; 
  if (name.endswith(".sln")) return "text/plain"; 
  if (name.endswith(".slupkg-ms")) return "application/x-ms-license"; 
  if (name.endswith(".snd")) return "audio/basic"; 
  if (name.endswith(".snippet")) return "application/xml"; 
  if (name.endswith(".spc")) return "application/x-pkcs7-certificates"; 
  if (name.endswith(".sst")) return "application/vnd.ms-pki.certstore"; 
  if (name.endswith(".stc")) return "application/vnd.sun.xml.calc.template"; 
  if (name.endswith(".std")) return "application/vnd.sun.xml.draw.template"; 
  if (name.endswith(".stl")) return "application/vnd.ms-pki.stl"; 
  if (name.endswith(".stw")) return "application/vnd.sun.xml.writer.template"; 
  if (name.endswith(".svg")) return "image/svg+xml"; 
  if (name.endswith(".sxc")) return "application/vnd.sun.xml.calc"; 
  if (name.endswith(".sxd")) return "application/vnd.sun.xml.draw"; 
  if (name.endswith(".sxg")) return "application/vnd.sun.xml.writer.global"; 
  if (name.endswith(".sxw")) return "application/vnd.sun.xml.writer"; 
  if (name.endswith(".tga")) return "image/targa"; 
  if (name.endswith(".thmx")) return "application/vnd.ms-officetheme"; 
  if (name.endswith(".tif")) return "image/tiff"; 
  if (name.endswith(".tiff")) return "image/tiff"; 
  if (name.endswith(".torrent")) return "application/x-bittorrent"; 
  if (name.endswith(".ts")) return "video/vnd.dlna.mpeg-tts"; 
  if (name.endswith(".tts")) return "video/vnd.dlna.mpeg-tts"; 
  if (name.endswith(".txt")) return "text/plain"; 
  if (name.endswith(".user")) return "text/plain"; 
  if (name.endswith(".vb")) return "text/plain"; 
  if (name.endswith(".vbox")) return "application/x-virtualbox-vbox"; 
  if (name.endswith(".vbox-extpack")) return "application/x-virtualbox-vbox-extpack"; 
  if (name.endswith(".vbproj")) return "text/plain"; 
  if (name.endswith(".vcf")) return "text/x-vcard"; 
  if (name.endswith(".vdi")) return "application/x-virtualbox-vdi"; 
  if (name.endswith(".vdp")) return "text/plain"; 
  if (name.endswith(".vdproj")) return "text/plain"; 
  if (name.endswith(".vhd")) return "application/x-virtualbox-vhd"; 
  if (name.endswith(".vmdk")) return "application/x-virtualbox-vmdk"; 
  if (name.endswith(".vor")) return "application/vnd.stardivision.writer"; 
  if (name.endswith(".vscontent")) return "application/xml"; 
  if (name.endswith(".vsi")) return "application/ms-vsi"; 
  if (name.endswith(".vspolicy")) return "application/xml"; 
  if (name.endswith(".vspolicydef")) return "application/xml"; 
  if (name.endswith(".vspscc")) return "text/plain"; 
  if (name.endswith(".vsscc")) return "text/plain"; 
  if (name.endswith(".vssettings")) return "text/xml"; 
  if (name.endswith(".vssscc")) return "text/plain"; 
  if (name.endswith(".vstemplate")) return "text/xml"; 
  if (name.endswith(".vsto")) return "application/x-ms-vsto"; 
  if (name.endswith(".wal")) return "interface/x-winamp3-skin"; 
  if (name.endswith(".wav")) return "audio/wav"; 
  if (name.endswith(".wave")) return "audio/wav"; 
  if (name.endswith(".wax")) return "audio/x-ms-wax"; 
  if (name.endswith(".wbk")) return "application/msword"; 
  if (name.endswith(".wdp")) return "image/vnd.ms-photo"; 
  if (name.endswith(".website")) return "application/x-mswebsite"; 
  if (name.endswith(".wiz")) return "application/msword"; 
  if (name.endswith(".wlz")) return "interface/x-winamp-lang"; 
  if (name.endswith(".wm")) return "video/x-ms-wm"; 
  if (name.endswith(".wma")) return "audio/x-ms-wma"; 
  if (name.endswith(".wmd")) return "application/x-ms-wmd"; 
  if (name.endswith(".wmv")) return "video/x-ms-wmv"; 
  if (name.endswith(".wmx")) return "video/x-ms-wmx"; 
  if (name.endswith(".wmz")) return "application/x-ms-wmz"; 
  if (name.endswith(".wpl")) return "application/vnd.ms-wpl"; 
  if (name.endswith(".wsc")) return "text/scriptlet"; 
  if (name.endswith(".wsdl")) return "application/xml"; 
  if (name.endswith(".wsz")) return "interface/x-winamp-skin"; 
  if (name.endswith(".wvx")) return "video/x-ms-wvx"; 
  if (name.endswith(".xaml")) return "application/xaml+xml"; 
  if (name.endswith(".xbap")) return "application/x-ms-xbap"; 
  if (name.endswith(".xdp")) return "application/vnd.adobe.xdp+xml"; 
  if (name.endswith(".xdr")) return "application/xml"; 
  if (name.endswith(".xfdf")) return "application/vnd.adobe.xfdf"; 
  if (name.endswith(".xht")) return "application/xhtml+xml"; 
  if (name.endswith(".xhtml")) return "application/xhtml+xml"; 
  if (name.endswith(".xla")) return "application/vnd.ms-excel"; 
  if (name.endswith(".xlam")) return "application/vnd.ms-excel.addin.macroEnabled.12"; 
  if (name.endswith(".xld")) return "application/vnd.ms-excel"; 
  if (name.endswith(".xlk")) return "application/vnd.ms-excel"; 
  if (name.endswith(".xll")) return "application/vnd.ms-excel"; 
  if (name.endswith(".xlm")) return "application/vnd.ms-excel"; 
  if (name.endswith(".xls")) return "application/vnd.ms-excel"; 
  if (name.endswith(".xlsb")) return "application/vnd.ms-excel.sheet.binary.macroEnabled.12"; 
  if (name.endswith(".xlsm")) return "application/vnd.ms-excel.sheet.macroEnabled.12"; 
  if (name.endswith(".xlsx")) return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"; 
  if (name.endswith(".xlt")) return "application/vnd.ms-excel"; 
  if (name.endswith(".xltm")) return "application/vnd.ms-excel.template.macroEnabled.12"; 
  if (name.endswith(".xltx")) return "application/vnd.openxmlformats-officedocument.spreadsheetml.template"; 
  if (name.endswith(".xlw")) return "application/vnd.ms-excel"; 
  if (name.endswith(".xml")) return "text/xml"; 
  if (name.endswith(".xrm-ms")) return "text/xml"; 
  if (name.endswith(".xsc")) return "application/xml"; 
  if (name.endswith(".xsd")) return "application/xml"; 
  if (name.endswith(".xsl")) return "text/xml"; 
  if (name.endswith(".xslt")) return "application/xml"; 
  if (name.endswith(".xss")) return "application/xml";

  // default is binary/octet
  return "binary/octet-stream";
}

/*----------------------------------------------------------------------------*/
void
S3Handler::ParseHeader (eos::common::HttpRequest *request)
{
  HeaderMap header = request->GetHeaders();
  std::string header_line;
  for (auto it = header.begin(); it != header.end(); it++)
  {
    header_line += it->first;
    header_line += "=";
    header_line += it->second;
    header_line += " ";
  }
  eos_static_info("%s", header_line.c_str());

  if (header.count("authorization"))
  {
    if (header["authorization"].substr(0, 3) == "AWS")
    {
      // this is amanzon webservice authorization
      mId = header["authorization"].substr(4);
      mSignature = mId;
      size_t dpos = mId.find(":");
      if (dpos != std::string::npos)
      {
        mId.erase(dpos);
        mSignature.erase(0, dpos + 1);

        mHttpMethod = request->GetMethod();

        mPath = request->GetUrl();
        std::string subdomain = SubDomain(header["host"]);

        if (subdomain.length())
        {
          // implementation for DNS buckets
          mBucket = subdomain;
          mVirtualHost = true;
        }
        else
        {
          mVirtualHost = false;
          // implementation for non DNS buckets
          mBucket = mPath;

          if (mBucket[0] == '/')
          {
            mBucket.erase(0, 1);
          }

          size_t slash_pos = mBucket.find("/");
          if (slash_pos != std::string::npos)
          {
            // something like data/...

            mPath = mBucket;
            mPath.erase(0, slash_pos);
            mBucket.erase(slash_pos);
          }
          else
          {
            mPath = "/";
          }
        }

        mQuery = request->GetQuery();

        if (header.count("content-md5"))
        {
          mContentMD5 = header["content-md5"];
        }
        if (header.count("date"))
        {
          mDate = header["date"];
        }

        if (header.count("content-type"))
        {
          mContentType = header["content-type"];
        }

        if (header.count("host"))
        {
          mHost = header["host"];
        }
        if (header.count("user-agent"))
        {
          mUserAgent = header["user-agent"];
        }
        // canonical amz header
        for (auto it = header.begin(); it != header.end(); it++)
        {
          XrdOucString amzstring = it->first.c_str();
          XrdOucString amzfield = it->second.c_str();
          // make lower case
          amzstring.lower(0);

          if (!amzstring.beginswith("x-amz-"))
          {
            // skip everything which is not amazon style
            continue;
          }
          // trim white space in the beginning
          while (amzfield.beginswith(" "))
          {
            amzfield.erase(0, 1);
          }
          int pos;
          // remove line folding and spaces after folding
          while ((pos = amzfield.find("\r\n ")) != STR_NPOS)
          {
            amzfield.erase(pos, 3);
            while (amzfield[pos] == ' ')
            {
              amzfield.erase(pos, 1);
            }
          }
          if (!mAmzMap.count(amzstring.c_str()))
          {
            mAmzMap[amzstring.c_str()] = amzfield.c_str();
          }
          else
          {
            mAmzMap[amzstring.c_str()] += ",";
            mAmzMap[amzstring.c_str()] += amzfield.c_str();
          }
        }
        // build a canonicalized resource
        for (auto it = mAmzMap.begin(); it != mAmzMap.end(); it++)
        {
          mCanonicalizedAmzHeaders += it->first;
          mCanonicalizedAmzHeaders += ":";
          mCanonicalizedAmzHeaders += it->second;
          mCanonicalizedAmzHeaders += "\n";
        }
        mIsS3 = true;
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
bool
S3Handler::IsS3 ()
{
  // Check if S3 object is complete
  return mIsS3;
}

/*----------------------------------------------------------------------------*/
void
S3Handler::Dump (std::string & out)
{
  // Print the S3 object contents to out
  out = "id=";
  out += mId.c_str();
  out += " ";
  out += "signature=";
  out += mSignature.c_str();
  return;
}

/*----------------------------------------------------------------------------*/
std::string
S3Handler::ExtractSubResource ()
{
  // Extract everything from the query which is a sub-resource aka used for
  // signatures
  std::vector<std::string> srvec;
  eos::common::StringConversion::Tokenize(GetQuery(), srvec, "&");
  for (auto it = srvec.begin(); it != srvec.end(); it++)
  {
    std::string key;
    std::string value;
    if (!eos::common::StringConversion::SplitKeyValue(*it, key, value))
    {
      // there are subresources without assigned value
      key = *it;
      value = "";
    }

    if ((key == "acl") ||
        (key == "lifecycle") ||
        (key == "location") ||
        (key == "logging") ||
        (key == "delete") ||
        (key == "notification") ||
        (key == "uploads") ||
        (key == "partNumber") ||
        (key == "requestPayment") ||
        (key == "uploadId") ||
        (key == "versionId") ||
        (key == "versioning") ||
        (key == "versions") ||
        (key == "website") ||
        (key == "torrent"))
    {
      mSubResourceMap[key] = value;
    }
  }
  mSubResource = "";
  for (auto it = mSubResourceMap.begin(); it != mSubResourceMap.end(); it++)
  {
    if (mSubResource.length())
    {
      mSubResource += "&";
    }
    mSubResource += it->first;
    if (it->second.length())
    {
      mSubResource += "=";
      mSubResource += it->second;
    }
  }
  return mSubResource;
}

/*----------------------------------------------------------------------------*/
std::string
S3Handler::SubDomain (std::string hostname)
{
  std::string subdomain = "";
  size_t pos1 = hostname.rfind(".");
  size_t pos2 = hostname.substr(0, pos1).rfind(".");
  size_t pos3 = hostname.substr(0, pos2).rfind(".");

  if ((pos1 != pos2) &&
      (pos2 != pos3) &&
      (pos1 != pos3) &&
      (pos1 != std::string::npos) &&
      (pos2 != std::string::npos) &&
      (pos3 != std::string::npos))
  {
    subdomain = hostname;
    subdomain.erase(pos3);
  }

  return subdomain;
}

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
