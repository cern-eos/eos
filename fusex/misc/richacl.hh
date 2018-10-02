//------------------------------------------------------------------------------
//! @file richacl.hh
//! @author Rainer Toebikke CERN
//! @brief richacls<=>eosacls translation functions
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

struct {
  int raclBit;
  char eosChr;
} raclEosPerms[] = {
  {RICHACE_READ_DATA, 'r'},
  {RICHACE_WRITE_DATA, 'w'},
  {RICHACE_EXECUTE, 'x'},
  {RICHACE_WRITE_ACL, 'm'},
  {RICHACE_APPEND_DATA, 'u'},
  {RICHACE_DELETE, 'd'},
  {RICHACE_WRITE_OWNER, 'c'}
};
const unsigned int raclEosPermsLen = sizeof(raclEosPerms) / sizeof(
                                       raclEosPerms[0]);

int
static racl2eos(struct richacl* acl, char* buf, int bufsz, metad::shared_md md)
{
  char* end_buf = buf + bufsz - 1;
  bool add_comma = false;
  int rc = 0;

  if (bufsz < 1) {
    return (EINVAL);
  }

  buf[0] = '\0'; /* in case there are no eos-compatible ACLs */
  /* An attempt is made to "merge" allow and deny entries into one eos ACL.
   * Here, this (silently) supposes that there is only one entry to be merged and the other one is
   * the opposite in allow/deny terms
   */
  struct masks { unsigned int allow; unsigned int deny; };
  struct masks zeromasks = {0, 0};
  std::map<std::string, struct masks> ace_mask;
  struct richace* ace;
  richacl_for_each_entry(ace, acl) {
    std::string who;
    int rc2 = 0;
    const char* ug = (ace->e_flags & RICHACE_IDENTIFIER_GROUP) ? "g" : "u";

    if (ace->e_flags & RICHACE_UNMAPPED_WHO) {
      who = ace->e_who;

      if (*ug == 'g') {
        ug = "egroup";
      }
    } else if (!(ace->e_flags & RICHACE_SPECIAL_WHO)) {
      if (*ug == 'g') {
        who = eos::common::Mapping::GidToGroupName(ace->e_id, rc2);
      } else {
        who = eos::common::Mapping::UidToUserName(ace->e_id, rc2);
      }
    } else {        /* this is a SPECIAL_WHO */
      if (ace->e_id == RICHACE_EVERYONE_SPECIAL_ID) {       /* everyone@ */
        who = "";
        ug = "z";
      } else if (ace->e_id == RICHACE_OWNER_SPECIAL_ID) {   /* owner@ */
        who = eos::common::Mapping::UidToUserName(md->uid(), rc2);
        if (EOS_LOGS_DEBUG)
            eos_static_debug("racl2eos special user id %d '%s' ug '%s'", ace->e_id, who.c_str(), ug);
      } else if (ace->e_id == RICHACE_GROUP_SPECIAL_ID) {   /* group@ */
        who = eos::common::Mapping::GidToGroupName(md->gid(), rc2);
        ug = "g"; /* RICHACE_IDENTIFIER_GROUP not necessarily set */
        if (EOS_LOGS_DEBUG)
            eos_static_debug("racl2eos special group id %d '%s' ug '%s'", ace->e_id, who.c_str(), ug);
      }
      else {
        if (EOS_LOGS_DEBUG)
            eos_static_debug("racl2eos special who %d ignored", ace->e_id);
        continue; /* silently ignored */
      }
    }

    if (rc2) {
      who = "_unknown_";
    }
    std::string id_s(ug);
    if (!who.empty()) id_s += ":" + who;

    auto a = ace_mask.find(id_s);
    if (a == ace_mask.end()) {
      ace_mask[id_s] = zeromasks;
      if (EOS_LOGS_DEBUG) eos_static_debug("racl2eos ace_mask for %s initialised to (%#x, %#x)",
                       id_s.c_str(), ace_mask[id_s].allow, ace_mask[id_s].deny);
    }

    if (richace_is_deny(ace)) {
      ace_mask[id_s].deny = ace->e_mask;
    } else {
      ace_mask[id_s].allow = ace->e_mask;
    }
  }

  for (auto map = ace_mask.begin(); map != ace_mask.end(); ++map) {
    char allowed[64];
    int pos = 0;
    struct masks allowdeny = map->second;
    unsigned int e_mask = allowdeny.allow & ~allowdeny.deny;

    if (EOS_LOGS_DEBUG) eos_static_debug("racl2eos ace_mask %s allow %#x deny %#x mask %#x", map->first.c_str(),
            allowdeny.allow, allowdeny.deny, e_mask);

    for (unsigned int j = 0; j < raclEosPermsLen; j++) {
      if (e_mask & raclEosPerms[j].raclBit) {
        if (raclEosPerms[j].eosChr == 'u') {
          allowed[pos++] = '+';
        }

        allowed[pos++] = raclEosPerms[j].eosChr;
      }
    }

    if (e_mask & RICHACE_DELETE_CHILD && !(e_mask & RICHACE_DELETE)) {
      allowed[pos++] = 'd';
    }

    allowed[pos] = '\0'; /* terminate string */
    char* b = buf;

    if ((end_buf - b) < 10) { /* don't take chances */
      rc = ENOSPC;
      break;
    }

    if (add_comma) {
      *b++ = ',';
    }

    if (EOS_LOGS_DEBUG) eos_static_debug("racl2eos %s allowed %s", map->first.c_str(), allowed);
    int l = snprintf(b, end_buf - b, "%s:%s", map->first.c_str(), allowed);

    if (b + l >= end_buf) {
      *b = '\0';
      return E2BIG;
    }

    // eos_static_debug("racl2eos: l=%d %s", l, b);
    buf = b + l;
    add_comma = true;
  }

  return rc;
}

static struct richacl*
eos2racl(const char* eosacl, metad::shared_md md)
{
  char* tacl = (char*) alloca(strlen(eosacl) + 1);
  strcpy(tacl, eosacl); /* copy, strtok_r is going to destroy this string */
  char* curr = tacl, *lasts;
  int numace = 1;

  while ((curr = strchr(curr + 1, ','))) {
    numace += 1;  /* a leading ',' would not do real harm */
  }

  curr = strtok_r(tacl, ",", &lasts);

  if (curr == NULL) {
    return richacl_from_mode(md->mode());  /* return ACL from mode bits, NULL means invalid ACL */
  }

  struct richacl* acl = richacl_alloc(numace);
  acl->a_count = 0;
  struct richace *ace;
  int idx_everyone=-1, idx_owner=-1, idx_group=-1;

  if (EOS_LOGS_DEBUG) eos_static_debug("eos2racl curr='%s' next='%s'", curr, lasts);

  int rc = 0;

  std::map<id_t, unsigned int> ace_mask_deny;

  struct richacl *denials = richacl_alloc(numace);
  denials->a_count = 0;

  do { /* one entry, e.g. u:username:rx or egroup:groupname:rw or z:wx */
    ace = &acl->a_entries[acl->a_count];
    memset(ace, 0, sizeof(
             *ace)); /* makes the ace an "allow" type entry in passing */
    char* l2;
    char* uge = strtok_r(curr, ":", &l2);
    char* qlf = strtok_r(NULL, ":", &l2);
    char* perm = strtok_r(NULL, ":", &l2);

    if (strlen(uge) == 0 || qlf == NULL) { /* badly formatted entry */
      continue;
    }

    if (perm == NULL) {         /* would be the case for z:wx */
        if (uge[0] != 'z')
            continue;           /* invalid entry */
        perm = qlf;
        qlf = NULL;
    }

    /* check all eos chars have an entry in raclEosPerms! */
    for (char* s = perm; *s != '\0'; s++) {
      if (s[0] == '+' || s[0] == '!') {
        continue;
      }

      unsigned int j;

      for (j = 0; j < raclEosPermsLen && raclEosPerms[j].eosChr != s[0]; j++);

      if (j >= raclEosPermsLen) {
        eos_static_err("eos2racl eos permission '%c' not supported in '%s'", s[0],
                       perm);
        rc = EINVAL;
        break;
      }
    }

    if (rc != 0) {
      richacl_free(acl);
      return NULL;
    }

    int qlf_num = 0;
    if (qlf != NULL) {
      char* qlf_end;
      qlf_num = strtoll((const char*) qlf, &qlf_end, 10);

      if (qlf_end[0] != '\0') {
        qlf_num = -1;  /* not a number */
      }

      if (EOS_LOGS_DEBUG) eos_static_debug("qlf='%s' qlf_num=%d qlf_end='%s'", qlf, qlf_num, qlf_end);
    }

    switch (uge[0]) { /* qualifier type, just check first char */
    case 'u': /* u in eos, meaning 'user' */
      if (qlf_num < 0) {
        ace->e_id = eos::common::Mapping::UserNameToUid(std::string(qlf), rc);
      } else {
        ace->e_id = qlf_num;
      }

      if (EOS_LOGS_DEBUG) eos_static_debug("qge=%s qlf=%s qlf_num=%d rc=%d e_id=%d", uge, qlf, qlf_num,
                       rc, ace->e_id);
      if (ace->e_id == (id_t) md->uid()) { /* owner */
        if (idx_owner >= 0) { /* already have an entry */
          acl->a_owner_mask = ace->e_mask;
          ace = &acl->a_entries[idx_owner];
          ace->e_mask = acl->a_owner_mask;
          acl->a_count--;                       /* it'll be re-incremented shortly */
        }
        ace->e_id = RICHACE_OWNER_SPECIAL_ID;
        ace->e_flags |= RICHACE_SPECIAL_WHO;
      }
      break;

    case 'g': /* g in eos, meaning 'group' */
      ace->e_flags |= RICHACE_IDENTIFIER_GROUP;

      if (qlf_num < 0) {
        ace->e_id = eos::common::Mapping::GroupNameToGid(std::string(qlf), rc);
      } else {
        ace->e_id = qlf_num;
      }

      if (EOS_LOGS_DEBUG) eos_static_debug("qge=%s qlf=%s qlf_num=%d rc=%d e_id=%d", uge, qlf, qlf_num,
                       rc, ace->e_id);
      if (ace->e_id == (id_t) md->gid()) { /* owner */
        if (idx_group >= 0) { /* already have an entry */
          acl->a_group_mask = ace->e_mask;
          ace = &acl->a_entries[idx_group];
          ace->e_mask = acl->a_group_mask;
          acl->a_count--;                       /* it'll be re-incremented shortly */
        }
        ace->e_id = RICHACE_GROUP_SPECIAL_ID;
        ace->e_flags |= RICHACE_SPECIAL_WHO;
      }
      break;

    case 'e': /* egroup in eos */
      ace->e_flags |= RICHACE_IDENTIFIER_GROUP;
      rc = richace_set_unmapped_who(ace, qlf,
                                    ace->e_flags); /* This will set RICHACE_UNMAPPED_WHO, it mustn't be set before! */
      break;

    case 'z': /* everyone@ */
      if (idx_everyone >= 0) {
        acl->a_other_mask = ace->e_mask;
        ace = &acl->a_entries[idx_everyone];
        ace->e_mask = acl->a_other_mask;
        acl->a_count--;                       /* it'll be re-incremented shortly */
      }

      ace->e_id = RICHACE_EVERYONE_SPECIAL_ID;
      ace->e_flags |= RICHACE_SPECIAL_WHO;
      break;

    default:
      eos_static_err("eos2racl invalid qualifier type: %s", uge);
    }

    if (rc) {
      eos_static_err("eos2racl parsing failed: ", strerror(rc));
      break;
    }

    /* interpret mask characters */
    unsigned int deny = 0;
    for (unsigned int j = 0; j < raclEosPermsLen; j++) {
      char* s = strchr(perm, raclEosPerms[j].eosChr);

      if (s != NULL) {
        bool has_not = (s > perm) && (s[0] == '!');

        if (has_not) {
          if (acl->a_other_mask & raclEosPerms[j].raclBit) {
            /* eos_static_err("eos2racl need a deny entry for '%c' in '%s'", s[0], perm); */
            deny |= raclEosPerms[j].raclBit;
          }

          ace->e_mask &= ~raclEosPerms[j].raclBit;
        } else {
          ace->e_mask |= raclEosPerms[j].raclBit;
        }
      } else if (acl->a_other_mask & raclEosPerms[j].raclBit) {
        eos_static_err("eos2racl need a deny entry for '%c' in '%s'",
                       raclEosPerms[j].eosChr, perm);
        deny |= raclEosPerms[j].raclBit;
      }
    }

    if (deny != 0) {
        struct richace *dace = &denials->a_entries[denials->a_count];
        richace_copy(dace, ace);
        dace->e_mask = deny;
        dace->e_type = RICHACE_ACCESS_DENIED_ACE_TYPE;
        denials->a_count++;
    }

    acl->a_count++;
    if (EOS_LOGS_DEBUG) eos_static_debug("eos2racl a_count=%d numace=%d", acl->a_count, numace);

  } while ((curr = strtok_r(NULL, ",", &lasts)));

  /* if needed, create a new ACL with denials first */
  if (denials->a_count > 0) {
      struct richacl *acl2 = richacl_alloc(denials->a_count + acl->a_count);
      if (EOS_LOGS_DEBUG) eos_static_debug("allocated new acl for %d entries", acl2->a_count);
      /*int sz = (void *) (acl->a_entries) - (void *) acl; ...this does not compile ?? */
      int sz = (char *) (acl->a_entries) - (char *) acl;
      memcpy(acl2, acl, sz);      /* copy header */
      acl2->a_count = 0;

      /* copy all useful deny entries */
      struct richace *ace;
      richacl_for_each_entry(ace, denials) {
        unsigned int deny = ace->e_mask;

        if ( ! (acl->a_other_mask & deny) &&        /* other_mask does not add rights */
             ! ( (acl->a_group_mask & deny) ||      /* group mask does not add rights... */
                 ( (ace->e_flags & RICHACE_IDENTIFIER_GROUP) && /* this is a group entry... */
                   (ace->e_flags & RICHACE_SPECIAL_WHO) &&
                   (ace->e_id == RICHACE_GROUP_SPECIAL_ID)
                 ) ) ) { 
            if (EOS_LOGS_DEBUG) eos_static_debug("deny mask %#x for id %d ignored", deny, ace->e_id);
            continue;
        }

        richace_copy(&acl2->a_entries[acl2->a_count++], ace);
        if (EOS_LOGS_DEBUG) eos_static_debug("copied mask %#x for id %d count %d", ace->e_mask, ace->e_id, acl2->a_count);
      }

      /* copy all allow entries */
      richacl_for_each_entry(ace, acl) {
        richace_copy(&acl2->a_entries[acl2->a_count++], ace);
      }

      richacl_free(acl);
      acl = acl2;
  }

  richacl_free(denials);        /* the updated a_count does not kmetter */

  if (rc == 0) {
    return acl;
  }

  richacl_free(acl);
  return NULL;
}

std::string escape(std::string src)
{
  std::string s = "";

  for (unsigned char c : src) {
    if (isprint(c)) {
      s += c;
    } else {
      char buf[64];
      sprintf(buf, "\\x%02x", c);
      s += buf;
    }
  }

  return s;
}
