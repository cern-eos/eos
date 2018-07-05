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

struct
{
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
const unsigned int raclEosPermsLen = sizeof(raclEosPerms) / sizeof(raclEosPerms[0]);

int
static racl2eos(struct richacl *acl, char *buf, int bufsz)
{
  char *end_buf = buf + bufsz - 1;
  bool add_comma = false;
  int rc = 0;

  if (bufsz < 1) return(EINVAL);
  buf[0] = '\0'; /* in case there are no eos-compatible ACLs */


  /* An attempt is made to "merge" allow and deny entries into one eos ACL.
   * Here, this (silently) supposes that there is only one entry to be merged and the other one is 
   * the opposite in allow/deny terms
   */

  std::map<std::string, int> ace_mask;
  struct richace *ace;

  richacl_for_each_entry(ace, acl)
  {
    std::string who;
    const char *ug = (ace->e_flags & RICHACE_IDENTIFIER_GROUP) ? "g" : "u";
    if (ace->e_flags & RICHACE_UNMAPPED_WHO) {
      who = ace->e_who;
      if (*ug == 'g') ug = "egroup";
    } else if (!(ace->e_flags & RICHACE_SPECIAL_WHO)) {
      if (*ug == 'g') {
        who = eos::common::Mapping::GidToGroupName(ace->e_id, rc);
      } else {
        who = eos::common::Mapping::UidToUserName(ace->e_id, rc);
      }
      if (rc) who = "_unknown_";
    } else continue; /* silently ignored */

    char id[1024];
    snprintf(id, sizeof(id), "%s:%s", ug, who.c_str());
    std::string id_s = id;

    auto a = ace_mask.find(id_s);
    if (a == ace_mask.end()) {
      ace_mask[id_s] = acl->a_other_mask;
      eos_static_debug("racl2eos ace_mask %s initialised to a_other_mask: %#x", id_s.c_str(), acl->a_other_mask);
    }
    if (richace_is_deny(ace)) {
      ace_mask[id_s] &= ~ace->e_mask;
      eos_static_debug("racl2eos ace_mask %s deny %#x: %#x", id_s.c_str(), ace->e_mask, ace_mask[id_s]);
    } else {
      ace_mask[id_s] |= ace->e_mask;
      eos_static_debug("racl2eos ace_mask %s allow %#x: %#x", id_s.c_str(), ace->e_mask, ace_mask[id_s]);
    }
  }


  for (auto map = ace_mask.begin(); map != ace_mask.end(); ++map) {

    char allowed[64];
    int pos = 0;
    int e_mask = map->second;


    for (unsigned int j = 0; j < raclEosPermsLen; j++) {
      if (e_mask & raclEosPerms[j].raclBit) {
        if (raclEosPerms[j].eosChr == 'u') {
          allowed[pos++] = '+';
        }
        allowed[pos++] = raclEosPerms[j].eosChr;
      }
    }
    if (e_mask & RICHACE_DELETE_CHILD && !(e_mask & RICHACE_DELETE)) allowed[pos++] = 'd';
    allowed[pos] = '\0'; /* terminate string */

    char *b = buf;
    if ((end_buf - b) < 10) { /* don't take chances */
      rc = ENOSPC;
      break;
    }
    if (add_comma) *b++ = ',';

    eos_static_debug("racl2eos %s allowed %s", map->first.c_str(), allowed);

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

static struct richacl *
eos2racl(const char *eosacl, int mode)
{
  char *tacl = (char *) alloca(strlen(eosacl) + 1);
  strcpy(tacl, eosacl); /* copy, strtok_r is going to destroy this string */

  char *curr = tacl, *lasts;
  int numace = 1;
  while ((curr = strchr(curr + 1, ','))) numace += 1; /* a leading ',' would not do real harm */

  curr = strtok_r(tacl, ",", &lasts);
  if (curr == NULL) return richacl_alloc(0); /* return empty ACL, NULL means invalid ACL */

  struct richacl *acl = richacl_from_mode(mode);
  numace += acl->a_count;
  int newSz = sizeof(struct richacl) +numace * sizeof(struct richace);
  acl = (struct richacl *) realloc(acl, newSz);

  eos_static_debug("eos2racl curr='%s' next='%s'", curr, lasts);


  int rc = 0;
  std::map<std::string, int> ace_mask_deny;

  do { /* one entry, e.g. u:username:rx,egroup:groupname:rw */
    struct richace *ace = &acl->a_entries[acl->a_count];
    memset(ace, 0, sizeof(*ace)); /* makes the ace an "allowed" type entry in passing */

    char *l2;
    char *uge = strtok_r(curr, ":", &l2);
    char *qlf = strtok_r(NULL, ":", &l2);
    char *perm = strtok_r(NULL, ":", &l2);

    if (strlen(uge) == 0 || perm == NULL) /* error, empty qualifier or badly formatted entry*/
      continue;

    /* check all eos chars have an entry in raclEosPerms! */
    for (char *s = perm; *s != '\0'; s++) {
      if (s[0] == '+' || s[0] == '!')
        continue;
      unsigned int j;
      for (j = 0; j < raclEosPermsLen && raclEosPerms[j].eosChr != s[0]; j++);
      if (j >= raclEosPermsLen) {
        eos_static_err("eos2racl eos permission '%c' not supported in '%s'", s[0], perm);
        rc = EINVAL;
        break;
      }
    }


    /* interpret mask characters */
    int deny = 0;
    for (unsigned int j = 0; j < raclEosPermsLen; j++) {
      char *s = strchr(perm, raclEosPerms[j].eosChr);
      if (s != NULL) {
        bool has_not = (s > perm) && (s[0] == '!');

        if (has_not) {
          if (acl->a_other_mask & raclEosPerms[j].raclBit) {
            eos_static_err("eos2racl need a deny entry for '%c' in '%s'", s[0], perm);
            deny |= raclEosPerms[j].raclBit;
          }
          ace->e_mask &= ~raclEosPerms[j].raclBit;
        } else {
          ace->e_mask |= raclEosPerms[j].raclBit;
        }
      } else if (acl->a_other_mask & raclEosPerms[j].raclBit) {
        eos_static_err("eos2racl need a deny entry for '%c' in '%s'", raclEosPerms[j].eosChr, perm);
        deny |= raclEosPerms[j].raclBit;
      }
    }

    if (rc != 0) {
      richacl_free(acl);
      return NULL;
    }

    char *qlf_end;
    int qlf_num = strtoll((const char *) qlf, &qlf_end, 10);
    if (qlf_end[0] != '\0') qlf_num = -1; /* not a number */

    eos_static_debug("qlf='%s' qlf_num=%d qlf_end='%s'", qlf, qlf_num, qlf_end);

    switch (uge[0]) { /* qualifier type, just check first char */
    case 'u': /* u in eos, meaning 'user' */
      if (qlf_num < 0) ace->e_id = eos::common::Mapping::UserNameToUid(std::string(qlf), rc);
      else ace->e_id = qlf_num;
      eos_static_debug("qge=%s qlf=%s qlf_num=%d rc=%d e_id=%d", uge, qlf, qlf_num, rc, ace->e_id);
      break;
    case 'g': /* g in eos, meaning 'group' */
      ace->e_flags |= RICHACE_IDENTIFIER_GROUP;
      if (qlf_num < 0) ace->e_id = eos::common::Mapping::GroupNameToGid(std::string(qlf), rc);
      else ace->e_id = qlf_num;
      eos_static_debug("qge=%s qlf=%s qlf_num=%d rc=%d e_id=%d", uge, qlf, qlf_num, rc, ace->e_id);
      break;
    case 'e': /* egroup in eos */
      ace->e_flags |= RICHACE_IDENTIFIER_GROUP;
      rc = richace_set_unmapped_who(ace, qlf, ace->e_flags); /* This will set RICHACE_UNMAPPED_WHO, it mustn't be set before! */
      break;
    default:
      eos_static_err("eos2racl invalid qualifier type: %s", uge);
    }

    if (rc) {
      eos_static_err("eos2racl parsing failed: ", strerror(rc));
      break;
    }

    acl->a_count++;
    eos_static_debug("eos2racl a_count=%d numace=%d", acl->a_count, numace);
    if (deny != 0) {
      numace++;

      newSz = sizeof(struct richacl) +numace * sizeof(struct richace);
      struct richacl *acl2 = (struct richacl *) realloc(acl, newSz);
      if (acl != acl2) eos_static_debug("eos2racl realloc returned %#p previous %#p", acl2, acl);
      acl = acl2;
      ace = &acl->a_entries[acl->a_count]; /* bump to new entry, and correct ace in case realloc relocated! */
      memset(ace, 0, sizeof(*ace)); /* richacl_copy acts on RICHACE_UNMAPPED_WHO in e_flags! */
      richace_copy(ace, ace - 1); /* richace_copy(target, source), copy previous entry */
      ace->e_type |= RICHACE_ACCESS_DENIED_ACE_TYPE;
      ace->e_mask = deny;

      acl->a_count++; /* finish new entry */
    }

  } while ((curr = strtok_r(NULL, ",", &lasts)));

  if (rc == 0) return acl;

  richacl_free(acl);
  return NULL;
}

std::string escape(std::string src)
{
  std::string s = "";

  for (unsigned char c : src) {
    if (isprint(c)) s += c;
    else {
      char buf[64];
      sprintf(buf, "\\x%02x", c);
      s += buf;
    }
  }
  return s;
}