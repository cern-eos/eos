// ----------------------------------------------------------------------
// File: mod_authz_key.c
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
/* http includes                                                              */
/*----------------------------------------------------------------------------*/
#include "apr_strings.h"
#include "ap_config.h"
#include "httpd.h"
#include "http_config.h"
#include "http_core.h"
#include "http_log.h"
#include "http_protocol.h"
#include "http_request.h"

/*----------------------------------------------------------------------------*/
/* openssl includes                                                           */
/*----------------------------------------------------------------------------*/
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>


/*----------------------------------------------------------------------------*/
/* structures                                                                 */
/*----------------------------------------------------------------------------*/
typedef struct {
  char *auth_keyfile;     /* file name of public key for signature verification */
  int   auth_authoritative;
} auth_config_rec;

typedef struct {
  char   path[4096];      /* lfn */
  char   sfn[4096];       /* sfn */
  char   ip[64];          /* the clients IP address */
  char   method[64];      /* http method e.g. GET/POST */
  char   keyhash[9];      /* hash of the public key to be used (not used yet) */
  time_t exptime;         /* time when the authorization expires */
  char   clientid[4096];  /* ID of the client = DN */
  char   token[8192];     /* the full token */
  char   signature[8192]; /* signature for 'token' */
  char   redirectorhost[4096]; /* hostname of the DPM redirector node */
  char   r_token[4096];   /* request token as used in dpm_get & dpm_put */
} authz_info;


/*----------------------------------------------------------------------------*/
/* b64 decoding                                                               */
/*----------------------------------------------------------------------------*/
static unsigned char* mod_keyauth_unbase64(unsigned char *input, int length, int* ublength)
{
  BIO *b64, *bmem;
  int cpcnt=0;
  char *buffer;
  char *modinput;
  int modlength;
  int i;
  int bread;

  /* add the \n every 64 characters which have been removed to be compliant with the HTTP URL syntax */
  modinput = (char *) malloc(length + (length/64 +1)+1);
  if (!modinput) {
    return 0;
  }

  memset(modinput, 0, length + (length/64 + 1)+1);

  for (i=0;i<length+1;i++) {
    /* fill a '\n' every 64 characters */
    if(i && (!(i%64))) {
      modinput[cpcnt]='\n';
      cpcnt++;
    }
    modinput[cpcnt] = input[i];
    cpcnt++;
  }
  modinput[cpcnt]=0;
  modlength=cpcnt-1;


  buffer = (char *)malloc(modlength);
  if (!buffer) {
    return 0;
  }

  memset(buffer, 0, modlength);

  b64 = BIO_new(BIO_f_base64());
  bmem = BIO_new_mem_buf(modinput, modlength);
  bmem = BIO_push(b64, bmem);

  bread=BIO_read(bmem, buffer, modlength);

  BIO_free_all(bmem);

  free(modinput);
  *ublength = bread;
  return buffer;
}

/*----------------------------------------------------------------------------*/
/* verify the request signature                                               */
/*----------------------------------------------------------------------------*/
static int mod_keyauth_verifysignature(unsigned char* data, unsigned char *base64, auth_config_rec* conf, request_rec *r) {
  int err;
  int sig_len;
  unsigned char* sig_buf;
  EVP_MD_CTX     md_ctx;
  EVP_PKEY *      pkey;
  FILE *          fp;
  X509 *        x509;

  /* base64 decode */
  sig_buf = (unsigned char*) mod_keyauth_unbase64(base64, strlen(base64), &sig_len);

  if (!sig_buf) {
    ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		  "access to %s failed, reason: base64 decoding failed",
		  r->uri);
    return 0;
  }

  /* Read public key */

  fp = fopen (conf->auth_keyfile, "r");
  if (fp == NULL) {
    ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		  "access to %s failed, reason: key file %s is not accessible",
		  r->uri, conf->auth_keyfile);
    return 0;
  }
  x509 = PEM_read_X509(fp, NULL, NULL,NULL);
  fclose (fp);

  if (x509 == NULL) {
    /*    ERR_print_errors_fp (stderr);*/
    ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		  "access to %s failed, reason: key file %s is not readable",
		  r->uri, conf->auth_keyfile);
    return 0;
  }

  /* Get public key */
  pkey=X509_get_pubkey(x509);
  if (pkey == NULL) {
    /*    ERR_print_errors_fp (stderr);*/
    ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		  "access to %s failed, reason: key file %s is not convertable",
		  r->uri, conf->auth_keyfile);
    return 0;
  }

  /* Verify the signature */

  EVP_VerifyInit   (&md_ctx, EVP_sha1());
  EVP_VerifyUpdate (&md_ctx, data, strlen((char*)data));
  err = EVP_VerifyFinal (&md_ctx, sig_buf, sig_len, pkey);
  EVP_PKEY_free (pkey);

  free(sig_buf);
  if (err != 1) {
    /*    ERR_print_errors_fp (stderr); */
    return 0;
  }

  return 1;
}

/*----------------------------------------------------------------------------*/
/* create apache default configuration                                        */
/*----------------------------------------------------------------------------*/
static void *create_auth_dir_config(apr_pool_t *p, char *d)
{
    auth_config_rec *conf = apr_palloc(p, sizeof(*conf));

    conf->auth_keyfile = "/opt/lcg/etc/dpm/https/keystore/cert.pem";
    conf->auth_authoritative = 1; /* keep the fortress secure by default */
    return conf;
}

/*----------------------------------------------------------------------------*/
/* create apache authentication slot                                          */
/*----------------------------------------------------------------------------*/
static const char *set_auth_slot(cmd_parms *cmd, void *offset, const char *f, 
                                 const char *t)
{
    if (t && strcmp(t, "publickey")) {
        return apr_pstrcat(cmd->pool, "Invalid auth file type: ", t, NULL);
    }

    return ap_set_file_slot(cmd, offset, f);
}

/*----------------------------------------------------------------------------*/
/* register configuration directives                                          */
/*----------------------------------------------------------------------------*/
static const command_rec auth_cmds[] =
{
    AP_INIT_TAKE12("AuthKeyFile", set_auth_slot,
                   (void *)APR_OFFSETOF(auth_config_rec, auth_keyfile),
                   OR_AUTHCFG, "public key to verify the https redirector signature"),
    AP_INIT_FLAG("AuthKeyAuthoritative", ap_set_flag_slot,
                 (void *)APR_OFFSETOF(auth_config_rec, auth_authoritative),
                 OR_AUTHCFG,
                 "Set to 'no' to allow access control to be passed along to "
                 "lower modules if the UserID is not known to this module"),
    {NULL}
};

module AP_MODULE_DECLARE_DATA keyauth_module;

/* These functions return 0 if client is OK, and proper error status
 * if not... either HTTP_UNAUTHORIZED, if we made a check, and it failed, or
 * HTTP_INTERNAL_SERVER_ERROR, if things are so totally confused that we
 * couldn't figure out how to tell if the client is authorized or not.
 *
 * If they return DECLINED, and all other modules also decline, that's
 * treated by the server core as a configuration error, logged and
 * reported as such.
 */

/* Determine user ID, and check if it really is that user, for HTTP
 * basic authentication...
 */

/*----------------------------------------------------------------------------*/
/* check if public key is configured - otherweise return DECLINED             */
/*----------------------------------------------------------------------------*/
static int authenticate_basic_user(request_rec *r)
{
  auth_config_rec *conf = ap_get_module_config(r->per_dir_config,
					       &keyauth_module);

  if (!conf->auth_keyfile) {
    ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		  "access to %s failed, reason: public keyfile not specified!");
    

    return DECLINED;
  }
  return OK;
}


/*----------------------------------------------------------------------------*/
/* function to parse a query strnig and fill the authz_info structure         */
/*----------------------------------------------------------------------------*/
static authz_info* get_authz_info(const char* query) {
  char* tokenize=0;
  char* keyval=0;
  char *search="&";
  char *token=0;
  char *keyend=0;
  char  lkey[8192];
  char  value[8192];
  char *tokenptr;
  char *httpstoken=0;
  char *httpsauthz=0;
  authz_info* authz=0;

  
  if (query) {
    authz = (authz_info*) malloc(sizeof(authz_info));
    memset(authz,0, sizeof(authz_info));
    /* extract httpstoken & httpsauthz */
    tokenize = strdup(query);
    tokenptr = tokenize;
    while ( (token=strtok(tokenptr, search))) {
      keyend = strchr(token,'=');
      if (!keyend) {
	tokenptr=NULL;
	continue;
      }
      
      strncpy(lkey,token,keyend-token );
      lkey[keyend-token]=0;
      strncpy(value,keyend+1,strlen(token)-(keyend-token+1));
      value[strlen(token)-(keyend-token+1)]=0;
      tokenptr=NULL;
      
      if (!strcmp(lkey,"httpstoken")) {
	httpstoken = strdup(value);
      }
      
      if (!strcmp(lkey,"httpsauthz")) {
	httpsauthz = strdup(value);
      }
    }
    free(tokenize);
  }
  
  /* if httpstoken was given, parse it ... */
  if (httpstoken) {
    /* tokens are made as <path>@<client-ip>:<sfn>:<key-hash>:<expirationtime> */
    /* tokens are made as <path>@<client-ip>:<sfn>:<key-hash>:<expirationtime>:<client-id>:<redirector-host> */
    tokenize = strdup(httpstoken);
    if ((token = strtok(tokenize, "@"))) { 
      strcpy(authz->path,token);   
      if ((token = strtok(NULL, ":"))) {
	strcpy(authz->ip,token);
	if ((token = strtok(NULL, ":"))) {
	  strcpy(authz->method,token);
	  if ((token = strtok(NULL, ":"))) {
	    strcpy(authz->sfn,token);	    
	    if ((token = strtok(NULL, ":"))) {
	      strcpy(authz->keyhash,token);	      
	      if ((token = strtok(NULL, ":"))) {
		authz->exptime = (time_t) atol(token);
		if ((token = strtok(NULL, ":"))) {
		  strcpy(authz->clientid,token);
		  if ((token = strtok(NULL, ":"))) {
		    strcpy(authz->redirectorhost,token);
		    if ((token = strtok(NULL, ":"))) {
		      strcpy(authz->r_token,token);
		    }
		  }
		} 
	      }
	    }
	  }
	}
      }
    }
  }

  if (httpstoken) {
    strcpy(authz->token,httpstoken);
    /* cert DNs come f.e. with escaped spaces  */
    ap_unescape_url(authz->token);
  }

  if (httpsauthz) {
    strcpy(authz->signature,httpsauthz);
  }

  return authz;
}

/*----------------------------------------------------------------------------*/
/* check if an http request is authorized                                     */
/*----------------------------------------------------------------------------*/
static int check_user_access(request_rec *r)
{  
  auth_config_rec *conf = ap_get_module_config(r->per_dir_config,
					       &keyauth_module);
  register int x;
  const char *t, *w;
  char *user = r->user;
  int m = r->method_number;
  const apr_array_header_t *reqs_arr = ap_requires(r);
  require_line *reqs;

  time_t now = time(NULL);

  authz_info* authz=0;

  if (!reqs_arr) {
    /* if the Apache config doesn't require key the 'publickey' authorization - we let the request pass */
    return OK;
  }
  reqs = (require_line *)reqs_arr->elts;
  
  for (x = 0; x < reqs_arr->nelts; x++) {
    
    if (!(reqs[x].method_mask & (AP_METHOD_BIT << m))) {
      continue;
    }

    t = reqs[x].requirement;
    w = ap_getword_white(r->pool, &t);
    if (!strcmp(w, "key-authorized")) {

      /* check if https authz was provided */
      authz=get_authz_info(r->args);

      /* no authz */
      if (!authz) {
	ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		      "access to %s failed, reason: user didn't provide authorization (httpsauthz/httpstoken)",
		      r->uri);

	return HTTP_UNAUTHORIZED;
      }

      /* verify the signature of authz information */
      
      if (!mod_keyauth_verifysignature(authz->token, authz->signature, conf, r)) {
	ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		      "access to %s failed, reason: cannot verify the signature of authorization information",
		      r->uri);
	free(authz);
	return HTTP_UNAUTHORIZED;
      }
    
      /* check that the accessed URL is the one which was signed for */

      if (strcmp(r->uri,authz->sfn)) {
	ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		      "access to %s failed, reason: authorization was issued for %s but URI %s was requested",
		      r->uri,authz->sfn,r->uri);
	free(authz);
	return HTTP_UNAUTHORIZED;
      }


      /* check validity time in authz */
      if (authz->exptime < now) {
	ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		      "access to %s failed, reason: user provided expired authorization",
		      r->uri);
	free(authz);
	return HTTP_UNAUTHORIZED;
      }

      /* check remote-ip with authz */
      if (strcmp(r->connection->remote_ip,authz->ip)) {
	ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		      "access to %s failed, reason: authorization was issued for ip %s but user connected as %s",
		      r->uri,authz->ip,r->connection->remote_ip);
	free(authz);
	return HTTP_UNAUTHORIZED;
      }

      /* check if the http method is the signed one */
      if (strcmp(r->method, authz->method)) {
	ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		      "access to %s failed, reason: authorization was issued for method %s but client used %s",
		      r->uri,authz->method,r->method);
	free(authz);
	return HTTP_UNAUTHORIZED;
      }
      
      ap_log_rerror(APLOG_MARK, APLOG_INFO, 0, r,
		    "access to %s granted for %s(ID:%s) ",
		    r->uri,authz->token,authz->clientid);      
      free(authz);
      return OK;
    }
  }    


  if (!(conf->auth_authoritative)) {
    return DECLINED;
  }
  
  ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r,
		"access to %s failed, reason: user %s not allowed access",
		r->uri, user);
  
  ap_note_basic_auth_failure(r);
  return HTTP_UNAUTHORIZED;
}

/*----------------------------------------------------------------------------*/
/* register authorization hooks in Apache                                     */
/*----------------------------------------------------------------------------*/
static void register_hooks(apr_pool_t *p)
{
    ap_hook_check_user_id(authenticate_basic_user,NULL,NULL,APR_HOOK_MIDDLE);
    ap_hook_auth_checker(check_user_access,NULL,NULL,APR_HOOK_MIDDLE);
}

module AP_MODULE_DECLARE_DATA keyauth_module =
{
    STANDARD20_MODULE_STUFF,
    create_auth_dir_config,     /* dir config creater */
    NULL,                       /* dir merger --- default is to override */
    NULL,                       /* server config */
    NULL,                       /* merge server config */
    auth_cmds,                  /* command apr_table_t */
    register_hooks              /* register hooks */
};
