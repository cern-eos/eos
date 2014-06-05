// ----------------------------------------------------------------------
// File: VstView.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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
#include "mgm/VstView.hh"
#include "common/StringConversion.hh"
#include "XrdSys/XrdSysTimer.hh"

/*----------------------------------------------------------------------------*/
#include <math.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
VstView VstView::gVstView;

/*----------------------------------------------------------------------------*/
void
VstView::Print (std::string &out,
                std::string option,
                const char* selection)
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper vLock(ViewMutex);
  bool monitoring = false;
  bool ioformating = false;

  if (option == "m")
    monitoring = true;
  if (option == "io")
    ioformating = true;

  const char* format = "%s %-16s %-4s %-40s %-16s %-6s %-10s %-8s %12s %8s %5s %6s %8s %11s %11s %8s\n";
  const char* ioformat = "%s %-20s %-4s %12s %8s %5s %11s %11s %8s %5s %5s %10s %10s %10s %10s %5s %5s\n";
  char line[4096];
  if (!monitoring)
  {
    if (ioformating)
      snprintf(line, sizeof (line) - 1, ioformat, "#", "instance", "age", "space", "used", "n-fs", "files","directories", "clients", "ropen", "wopen", "diskr-MB/s", "diskw-MB/s", "ethi-MiB/s", "etho-MiB/s", "NsR/s","NsW/s");
    else
      snprintf(line, sizeof (line) - 1, format, "#", "instance", "age", "host", "ip", "mode", "version", "uptime", "space", "used", "n(fs)", "iops", "bw-MB/s", "files", "directories", "clients");
    out += "# _______________________________________________________________________________________________________________________________________________________________________________________\n";
    out += line;
    out += "# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
  }
  if (!monitoring)
  {
    for (auto it = mView.begin(); it != mView.end(); ++it)
    {
      XrdOucString age1, age2;
      XrdOucString space;
      XrdOucString val1, val2, val3, val4, val5, val6, val7, val8, val9;
      char sused[64];
      long lage = time(NULL) - strtol(it->second["timestamp"].c_str(), 0, 10);
      long lup = time(NULL) - strtol(it->second["uptime"].c_str(), 0, 10);

      unsigned long long max_bytes = strtoull(it->second["maxbytes"].c_str(), 0, 10);
      unsigned long long free_bytes = strtoull(it->second["freebytes"].c_str(), 0, 10);
      unsigned long long diskin = strtoull(it->second["diskin"].c_str(), 0, 10);
      unsigned long long diskout = strtoull(it->second["diskout"].c_str(), 0, 10);
      unsigned long long ethin = strtoull(it->second["ethin"].c_str(), 0, 10);
      unsigned long long ethout = strtoull(it->second["ethout"].c_str(), 0, 10);
      unsigned long long ropen = strtoull(it->second["ropen"].c_str(), 0, 10);
      unsigned long long wopen = strtoull(it->second["wopen"].c_str(), 0, 10);
      unsigned long long rlock = strtoull(it->second["rlock"].c_str(), 0, 10);
      unsigned long long wlock = strtoull(it->second["wlock"].c_str(), 0, 10);
      unsigned long long nfsrw = strtoull(it->second["nfsrw"].c_str(), 0, 10);
      unsigned long long iops = strtoull(it->second["iops"].c_str(), 0, 10);
      unsigned long long bw = strtoull(it->second["bw"].c_str(), 0, 10);

      double used = 100.0 * (max_bytes - free_bytes) / max_bytes;
      if ((used < 0) || (used > 100))
        used = 100;

      if (lage < 0)
        lage = 0;

      if (lup < 0)
        lup = 0;

      if (max_bytes)
        snprintf(sused, sizeof (sused) - 1, "%.02f%%", used);
      else
        snprintf(sused, sizeof (sused) - 1, "unavail");

      std::string is = it->second["instance"];
      if (it->second["mode"]== "master")
        is+= "[W]";
      else
        is+= "[R]";

      if (ioformating)
        snprintf(line, sizeof (line) - 1, ioformat,
                 " ",
                 is.c_str(),
                 eos::common::StringConversion::GetReadableAgeString(age1, lage),
                 eos::common::StringConversion::GetReadableSizeString(space, max_bytes, "B"),
                 sused,
                 eos::common::StringConversion::GetSizeString(val9, nfsrw),
                 it->second["ns_files"].c_str(),
                 it->second["ns_container"].c_str(),
                 it->second["clients"].c_str(),
                 eos::common::StringConversion::GetSizeString(val5, ropen),
                 eos::common::StringConversion::GetSizeString(val6, wopen),
                 eos::common::StringConversion::GetSizeString(val1, diskout),
                 eos::common::StringConversion::GetSizeString(val2, diskin),
                 eos::common::StringConversion::GetSizeString(val3, ethout),
                 eos::common::StringConversion::GetSizeString(val4, ethin),
                 eos::common::StringConversion::GetSizeString(val7, rlock),
                 eos::common::StringConversion::GetSizeString(val8, wlock)
                 );
      else
        snprintf(line, sizeof (line) - 1, format,
                 " ",
                 it->second["instance"].c_str(),
                 eos::common::StringConversion::GetReadableAgeString(age1, lage),
                 it->second["host"].c_str(),
                 it->second["ip"].c_str(),
                 it->second["mode"].c_str(),
                 it->second["version"].c_str(),
                 eos::common::StringConversion::GetReadableAgeString(age2, lup),
                 eos::common::StringConversion::GetReadableSizeString(space, max_bytes, "B"),
                 sused,
                 eos::common::StringConversion::GetSizeString(val1, nfsrw),
                 eos::common::StringConversion::GetSizeString(val2, iops),
                 eos::common::StringConversion::GetSizeString(val3, bw),
                 it->second["ns_files"].c_str(),
                 it->second["ns_container"].c_str(),
                 it->second["clients"].c_str()
                 );

      out += line;
    }
  }
  else
  {
    for (auto it = mView.begin(); it != mView.end(); ++it)
    {
      if (it != mView.begin())
        out += "\n";
      for (auto sit = it->second.begin(); sit != it->second.end(); ++sit)
      {
        if (sit != it->second.begin())
          out += " ";
        out += sit->first.c_str();
        out += "=";
        out += sit->second.c_str();
      }
    }
  }
  if (!monitoring)
  {
    out += "# ........................................................................................................................................................................................\n";
  }
}


/*----------------------------------------------------------------------------*/
void
VstView::PrintHtml (XrdOucString &out, bool js)
/*----------------------------------------------------------------------------*/
{
  out += R"literal(
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="refresh" content="600">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>EOS VST MAP</title>
 )literal";

  if (js)
  {
    out += R"literal(
<link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/MarkerCluster.Default.css" type="text/css" />
<link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/MarkerCluster.css" type="text/css" />
<script type="text/javascript" src="//ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js"></script>
<script type="text/javascript" src="//maps.google.com/maps/api/js?sensor=false"></script>
   )literal";
  }

  out += R"literal(
<script type="text/javascript">

var IPMapper = {
   map: null,
    mapTypeId: google.maps.MapTypeId.SATELLITE,
    latlngbound: null,
    infowindow: null,
    baseUrl: "https://freegeoip.net/json/",
   getCircle: function(magnitude) {
     return {
       path: google.maps.SymbolPath.CIRCLE,
       fillColor: 'red',
       fillOpacity: .5,
       scale: (magnitude/10),
       strokeColor: 'white',
       strokeWeight: .9
     };
   },
    initializeMap: function(mapId){
        IPMapper.latlngbound = new google.maps.LatLngBounds();
        var latlng = new google.maps.LatLng(0, 0);
        //set Map options
        var mapOptions = {
        zoom: 2,
        minZoom:2,
        center: latlng,
        mapTypeId: IPMapper.mapTypeId,
    streetViewControl: false
      }
        //init Map
        IPMapper.map = new google.maps.Map(document.getElementById(mapId), mapOptions);
        //init info window
        IPMapper.infowindow = new google.maps.InfoWindow();
        //info window close event
        google.maps.event.addListener(IPMapper.infowindow, 'closeclick', function() {
        IPMapper.map.fitBounds(IPMapper.latlngbound);
        IPMapper.map.panToBounds(IPMapper.latlngbound);
      });
    },
    addIPArray: function(ipArray){
        ipArray = IPMapper.uniqueArray(ipArray); //get unique array elements
        //add Map Marker for each IP
        for (var i = 0; i < ipArray.length; i++){
            IPMapper.addIPMarker(ipArray[i]);
        }
    },
    addIPMarker: function(ip,site,size){
        ipRegex = /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/;
        if($.trim(ip) != '' && ipRegex.test(ip)){ //validate IP Address format
            var url = encodeURI(IPMapper.baseUrl + ip + "?callback=?"); //geocoding url
            $.getJSON(url, function(data) { //get Geocoded JSONP data
                if($.trim(data.latitude) != '' && data.latitude != '0' && !isNaN(data.latitude)){ //Geocoding successfull
                    var latitude = data.latitude;
                    var longitude = data.longitude;
                    var contentString = '<b>EOS Site:</b>' + site.toUpperCase() + '<br />';
                    $.each(data, function(key, val) {
                        contentString += '<b>' + key.toUpperCase().replace("_", " ") + ':</b> ' + val + '<br />';
                    });
                    var latlng = new google.maps.LatLng(latitude, longitude);
                    var marker = new google.maps.Marker({ //create Map Marker
                        map: IPMapper.map,
                        draggable: false,
                        position: latlng,
                        icon: IPMapper.getCircle(size)
                    });

               //marker.setAnimation(google.maps.Animation.BOUNCE);

                    IPMapper.placeIPMarker(marker, latlng, contentString); //place Marker on Map
                } else {
                    IPMapper.logError('IP Address geocoding failed!');
                    $.error('IP Address geocoding failed!');
                }
            });
        } else {
            IPMapper.logError('Invalid IP Address!');
            $.error('Invalid IP Address!');
        }
    },
    placeIPMarker: function(marker, latlng, contentString){ //place Marker on Map
        marker.setPosition(latlng);
        google.maps.event.addListener(marker, 'click', function() {
            IPMapper.getIPInfoWindowEvent(marker, contentString);
        });
        IPMapper.latlngbound.extend(latlng);
        IPMapper.map.setCenter(IPMapper.latlngbound.getCenter());
        IPMapper.map.fitBounds(IPMapper.latlngbound);
    },
    getIPInfoWindowEvent: function(marker, contentString){ //open Marker Info Window
        IPMapper.infowindow.close()
        IPMapper.infowindow.setContent(contentString);
        IPMapper.infowindow.open(IPMapper.map, marker);
    },
    uniqueArray: function(inputArray){ //return unique elements from Array
        var a = [];
        for(var i=0; i<inputArray.length; i++) {
            for(var j=i+1; j<inputArray.length; j++) {
                if (inputArray[i] === inputArray[j]) j = ++i;
            }
            a.push(inputArray[i]);
        }
        return a;
    },
    logError: function(error){
        if (typeof console == 'object') { console.error(error); }
    }
}

$(function(){
        try{
            IPMapper.initializeMap("map");

  )literal";

  XrdSysMutexHelper vLock(ViewMutex);
  for (auto it = mView.begin(); it != mView.end(); ++it) {
     if (it->second["mode"] != "master")
       continue;
     double mb = strtoull(it->second["maxbytes"].c_str(),0,10);
     mb/= 50000000000000;
     if (mb<50)
       mb=50;

     out += "          IPMapper.addIPMarker(\"";
     out += it->second["ip"].c_str();
     out += "\",\"";
     out += it->second["instance"].c_str();
     out += "\",";
     out += (int) mb;
     out += ");\n";
  }

out += R"literal(
        } catch(e){
            //handle error
        }
    });

</script>
</head>

<body>

<div id="map" style="height: 300px;"></div>
</body>
</html>
  )literal";

  return ;
}

EOSMGMNAMESPACE_END
