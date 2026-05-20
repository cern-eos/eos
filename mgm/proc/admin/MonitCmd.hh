//------------------------------------------------------------------------------
// @file: MonitCmd.hh
//------------------------------------------------------------------------------

#pragma once

#include "mgm/proc/IProcCommand.hh"
#include "proto/Monit.pb.h"

EOSMGMNAMESPACE_BEGIN

class MonitCmd : public IProcCommand {
public:
  explicit MonitCmd(eos::console::RequestProto&& req, eos::common::VirtualIdentity& vid)
      : IProcCommand(std::move(req), vid, false)
  {
  }

  ~MonitCmd() override = default;

  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  eos::console::ReplyProto
  ConfigSubcmd(const eos::console::MonitProto_ConfigProto& config) const;
  eos::console::ReplyProto ConfigLsSubcmd() const;
  eos::console::ReplyProto
  ConfigSetSubcmd(const eos::console::MonitProto_ConfigProto_SetProto& set) const;
  eos::console::ReplyProto
  EnableSubcmd(const eos::console::MonitProto_EnableProto& enable) const;
  eos::console::ReplyProto
  DisableSubcmd(const eos::console::MonitProto_DisableProto& disable) const;
};

EOSMGMNAMESPACE_END
