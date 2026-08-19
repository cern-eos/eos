#ifndef __XRDMETRICS_CONFIG_HH__
#define __XRDMETRICS_CONFIG_HH__
/******************************************************************************/
/*                                                                            */
/*                  X r d M e t r i c s C o n f i g . h h                     */
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
/******************************************************************************/

#include <set>
#include <string>
#include <vector>

#include "XrdMetrics/XrdMetricsLabels.hh" // ConstLabel

class XrdOucGatherConf;
class XrdSysError;

//-----------------------------------------------------------------------------
//! Process-wide metrics configuration, parsed from the server config file's
//! metrics.* directives. It is loaded once, early, by the server core (before
//! any subsystem registers a family) so the global labels it carries can be
//! frozen onto the default registry in time; the metrics exporter plugin then
//! reads the same settings rather than parsing the file again.
//!
//! Two concerns live here: registry-level settings that apply no matter how
//! metrics leave the process (constant global labels, per-subsystem
//! enable/disable) and exporter settings (served path, push destinations and
//! intervals). Disabling a subsystem is a *serialize-time* filter, not a
//! registration-time one: the counters still update cheaply; they are simply
//! omitted from output (see subsystemEnabled).
//-----------------------------------------------------------------------------

namespace XrdMetrics {
class Config {
public:
  // Collector-level settings ---------------------------------------------------

  bool enabled = true;                  // master switch (metrics.enable)
  std::set<std::string> disabledSubsys; // groups explicitly turned off
  std::set<std::string> enabledSubsys;  // if non-empty, the only groups on
  std::vector<ConstLabel> userLabels;   // metrics.label k v (repeatable)
  std::string role;                     // all.role (a global-label source)

  // Computed global labels (program/role/user) after a Load(); also returned by
  // buildGlobalLabels(). Applied to the default registry during Load().
  std::vector<ConstLabel> globalLabels;

  // Exporter settings ---------------------------------------------------------

  std::string path = "/metrics";  // served request path
  std::string instance;           // instance label (empty => hostname)
  std::string pushURL;            // Pushgateway base URL ("" => off)
  std::string pushJob = "xrootd"; // Pushgateway job label
  int pushEvery = 30;             // Pushgateway push period (seconds)
  std::string otelURL;            // OTLP/HTTP metrics URL ("" => off)
  int otelEvery = 30;             // OTLP push period (seconds)
  int scrapeTTL = 10;             // cached scrape body lifetime (s); 0 = disabled
  int scrapeRateLimit = 100;      // max scrapes per second; 0 = unlimited

  // Scrape endpoint authentication settings (metrics.requireauth / metrics.authtoken /
  // metrics.authpassword). When any of these is set, each request to /metrics must
  // present a matching credential or hold a valid TLS client certificate.
  //
  bool requireAuth{false};  // if true, require cert or secret
  std::string authToken;    // Bearer token shared secret ("" = disabled)
  std::string authPassword; // HTTP Basic Auth password  ("" = disabled)

  //! The process-wide instance, shared by the core load and the exporter.
  static Config& Instance();

  //! The metrics.* (and all.role) directives gathered from the config file.
  static const char* DirectiveList();

  //! Gather and parse the config file once (idempotent), then compute and apply
  //! the global labels to the default registry. Safe to call from the core early
  //! load and, as a fallback, from the exporter when loaded standalone.
  void Load(const char* cfgfn, XrdSysError* log);

  //! Fold the recognised directives from an already-gathered conf into *this.
  //! Exposed for unit tests, which drive a conf via useData() without a file.
  void parse(XrdOucGatherConf& conf);

  //! Compose the global labels from the program name (XRDPROG), the role, and the
  //! user labels, with user labels overriding the auto-seeded ones by key.
  std::vector<ConstLabel> buildGlobalLabels() const;

  //! Whether a subsystem (registry group) should be emitted: the master switch,
  //! then the deny set, then the allow set (when non-empty).
  bool subsystemEnabled(const std::string& subsystem) const;

  //! Build the per-instance Pushgateway URL:
  //!   <base>/metrics/job/<job>/instance/<instance>
  //! A single trailing slash on the base is trimmed so the path is well formed.
  static std::string PushgatewayURL(const std::string& base, const std::string& job,
                                    const std::string& instance);

private:
  void setUserLabel(const std::string& key, const std::string& val);

  bool loaded_ = false;
};
} // namespace XrdMetrics
#endif
