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

//------------------------------------------------------------------------------
//! @file dsi_xrootd.hh
//! @author Geoffray Adde - CERN
//! @brief Interface of the XRootD DSI plugin
//------------------------------------------------------------------------------

#if !defined(DSI_XRD_HH)
#define DSI_XRD_HH

#include "XrdFileIo.hh"
#include "XrdUtils.hh"

extern "C" {
#include "globus_gfs_internal_hack.h"
#include "globus_gridftp_server.h"
#include "globus_logging.h"

#define IPC_RETRY 2

#define GlobusGFSErrorOpFinished(_op, _type, _result)                       \
do                                                                          \
{                                                                           \
    globus_gfs_finished_info_t          _finished_info;                     \
                                                                            \
     memset(&_finished_info, '\0', sizeof(globus_gfs_finished_info_t));     \
    _finished_info.type = _type;                                            \
    _finished_info.code = 0;                                                \
    _finished_info.msg =                                                    \
        globus_error_print_friendly(globus_error_peek(_result));            \
    _finished_info.result = _result;                                        \
                                                                            \
    globus_gridftp_server_operation_finished(                               \
        _op,                                                                \
        _result,                                                            \
        &_finished_info);                                                   \
} while(0)


  typedef struct globus_l_gfs_xrood_handle_s
  {
    // !!! WARNING
    // globus_mutex is not adapted to the XRootd async framework
    // because if threading is disbaled in the globus build
    // then, the thread model doesn't lock the mutexes because it's semantically equivalent in that case
    // but the XRootD client is multithreaded anyway
    bool isInit;
    pthread_mutex_t mutex;
    XrdFileIo *fileIo;
    // used for standalone server configuration
    globus_result_t cached_res;
    int optimal_count;
    globus_bool_t done;
    globus_off_t blk_length;
    globus_off_t blk_offset;
    globus_size_t block_size;
    globus_gfs_operation_t op;
    // if >0 , the file is mv'ed at the end of the copy to remove the suffix
    std::string *tempname;
    size_t tmpsfix_size;
    /* if use_uuid is true we will use uuid and fullDestPath in the
     file accessing commands */
    globus_bool_t use_uuid;

    //== additional data to use in the case of frontend / backend configuration ===//
    globus_mutex_t			gfs_mutex;
    globus_gfs_session_info_t session_info;
    // used for frontend / backend configuration
    //TODO: could probably be merged with cached_res but side effects are possible because of the endless callback chain
    globus_result_t cur_result;
    globus_l_gfs_xrootd_filemode_t mode;
    globus_bool_t active_delay;
    globus_gfs_data_info_t * active_data_info;
    globus_gfs_transfer_info_t * active_transfer_info;
    globus_gfs_operation_t active_op;
    void * active_user_arg;
    globus_gfs_storage_transfer_t active_callback;

    globus_l_gfs_xrood_handle_s () :
	isInit (false),fileIo(NULL),tempname(NULL)
    {
    }

  } globus_l_gfs_xrootd_handle_t;

struct globus_l_gfs_remote_node_info_s;

typedef struct globus_l_gfs_remote_ipc_bounce_s {
	globus_gfs_operation_t			op;
	void *					state;
	globus_l_gfs_xrootd_handle_t *   	my_handle;
	int					nodes_obtained;
	int					nodes_pending;
	int					begin_event_pending;
	int					event_pending;
	int *					eof_count;
	struct globus_l_gfs_remote_node_info_s * node_info;
	int					partial_eof_counts;
	int					nodes_requesting;
	int					node_ndx;
	int					node_count;
	int					finished;
	int					final_eof;
	int					cached_result;
	int					sending;
	int					events_enabled;
} globus_l_gfs_remote_ipc_bounce_t;

typedef void (*globus_l_gfs_remote_node_cb) (
	struct globus_l_gfs_remote_node_info_s * node_info,
	globus_result_t				result,
	void *					user_arg
);

typedef struct globus_l_gfs_remote_node_info_s {
  globus_l_gfs_xrootd_handle_t *		my_handle;
	globus_gfs_ipc_handle_t			ipc_handle;
	struct globus_l_gfs_remote_ipc_bounce_s * bounce;
	char *					cs;
	void *					data_arg;
	void *					event_arg;
	int					event_mask;
	int					node_ndx;
	int					stripe_count;
	int					info_needs_free;
	void *					info;
	globus_l_gfs_remote_node_cb		callback;
	void *					user_arg;
	int					error_count;
	globus_result_t				cached_result;
} globus_l_gfs_remote_node_info_t;

typedef struct globus_l_gfs_remote_request_s {
  globus_l_gfs_xrootd_handle_t *		my_handle;
	globus_l_gfs_remote_node_cb		callback;
	void *					user_arg;
	int					nodes_created;
	void *					state;
} globus_l_gfs_remote_request_t;


static void globus_l_gfs_file_net_read_cb
(globus_gfs_operation_t,
    globus_result_t,
    globus_byte_t *,
    globus_size_t,
    globus_off_t,
    globus_bool_t,
    void *);

static void
globus_l_gfs_xrootd_read_from_net(globus_l_gfs_xrootd_handle_t *);
static globus_result_t globus_l_gfs_make_error(const char*, int);

void fill_stat_array(globus_gfs_stat_t *, struct stat, char *);
void free_stat_array(globus_gfs_stat_t * ,int);

int next_read_chunk(globus_l_gfs_xrood_handle_s *xrootd_handle, int64_t &nextreadl);

int xrootd_handle_open(char *, int, int,
    globus_l_gfs_xrootd_handle_t *);
static globus_bool_t globus_l_gfs_xrootd_send_next_to_client
(globus_l_gfs_xrootd_handle_t *);
static void globus_l_gfs_net_write_cb(globus_gfs_operation_t,
    globus_result_t,
    globus_byte_t *,
    globus_size_t,
    void *);
} // end extern "C"

#endif  /* DSI_XRD_HH */
