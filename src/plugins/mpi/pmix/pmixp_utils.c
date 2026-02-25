/*****************************************************************************\
 ** pmix_utils.c - Various PMIx utility functions
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015-2017 Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com, artemp@mellanox.com>.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
 \*****************************************************************************/

#include "config.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "pmixp_common.h"
#include "pmixp_utils.h"
#include "pmixp_debug.h"

/* must come after the above pmixp includes */
#include "src/common/forward.h"

extern int pmixp_count_digits_base10(uint32_t val)
{
	int digit_count = 0;

	while (val) {
		digit_count++;
		val /= 10;
	}

	return digit_count;
}

void pmixp_free_buf(void *x)
{
	buf_t *buf = (buf_t *) x;
	FREE_NULL_BUFFER(buf);
}

int pmixp_usock_create_srv(char *path)
{
	static struct sockaddr_un sa;
	int ret = 0;

	if (strlen(path) >= sizeof(sa.sun_path)) {
		PMIXP_ERROR_STD("UNIX socket path is too long: %lu, max %lu",
				(unsigned long) strlen(path),
				(unsigned long) sizeof(sa.sun_path) - 1);
		return SLURM_ERROR;
	}

	int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0) {
		PMIXP_ERROR_STD("Cannot create UNIX socket");
		return SLURM_ERROR;
	}

	memset(&sa, 0, sizeof(sa));
	sa.sun_family = AF_UNIX;
	strcpy(sa.sun_path, path);
	if ((ret = bind(fd, (struct sockaddr *)&sa, SUN_LEN(&sa)))) {
		PMIXP_ERROR_STD("Cannot bind() UNIX socket %s", path);
		goto err_fd;
	}

	if ((ret = listen(fd, 64))) {
		PMIXP_ERROR_STD("Cannot listen(%d, 64) UNIX socket %s", fd,
				path);
		goto err_bind;

	}
	return fd;

err_bind:
	unlink(path);
err_fd:
	close(fd);
	return ret;
}

size_t pmixp_read_buf(int sd, void *buf, size_t count, int *shutdown,
		      bool blocking)
{
	ssize_t ret, offs = 0;

	*shutdown = 0;

	if (blocking) {
		fd_set_blocking(sd);
	}

	while (count - offs > 0) {
		ret = read(sd, (char *)buf + offs, count - offs);
		if (ret > 0) {
			offs += ret;
			continue;
		} else if (ret == 0) {
			/* connection closed. */
			*shutdown = 1;
			return offs;
		}
		switch (errno) {
		case EINTR:
			continue;
		case EWOULDBLOCK:
			/* we can get here in non-blocking mode only */
			return offs;
		default:
			PMIXP_ERROR_STD("blocking=%d", blocking);
			*shutdown = -errno;
			return offs;
		}
	}

	if (blocking) {
		fd_set_nonblocking(sd);
	}
	return offs;
}

int pmixp_fd_set_nodelay(int fd)
{
	int val = 1;
	if ( 0 > setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&val,
			    sizeof(val)) ) {
		PMIXP_ERROR_STD("Cannot set TCP_NODELAY on fd = %d\n", fd);
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

size_t pmixp_write_buf(int sd, void *buf, size_t count, int *shutdown,
		       bool blocking)
{
	ssize_t ret, offs = 0;

	*shutdown = 0;

	if (!blocking && !pmixp_fd_write_ready(sd, shutdown)) {
		return 0;
	}

	if (blocking) {
		fd_set_blocking(sd);
	}

	while (count - offs > 0) {
		ret = write(sd, (char *)buf + offs, count - offs);
		if (ret > 0) {
			offs += ret;
			continue;
		}
		switch (errno) {
		case EINTR:
			continue;
		case EWOULDBLOCK:
			return offs;
		default:
			*shutdown = -errno;
			return offs;
		}
	}

	if (blocking) {
		fd_set_nonblocking(sd);
	}

	return offs;
}

static int _iov_shift(struct iovec *iov, size_t iovcnt, int offset)
{
	int skip, i;
	size_t count = 0;

	/* find out how many iov's was completely sent */
	for (skip = 0; skip < iovcnt; skip++) {
		if (offset < count + iov[skip].iov_len) {
			break;
		}
		count += iov[skip].iov_len;
	}

	/* remove tose iov's from the list */
	for (i = 0; i < iovcnt - skip; i++) {
		iov[i] = iov[i + skip];
	}

	/* shift the current iov */
	offset -= count;
	iov[0].iov_base += offset;
	iov[0].iov_len -= offset;
	return iovcnt - skip;
}

size_t pmixp_writev_buf(int sd, struct iovec *iov, size_t iovcnt,
			size_t offset, int *shutdown)
{
	ssize_t ret;
	size_t size = 0, written = 0;
	int i;

	for (i=0; i < iovcnt; i++) {
		size += iov[i].iov_len;
	}

	/* Adjust initial buffer with the offset */
	iovcnt = _iov_shift(iov, iovcnt, offset);

	*shutdown = 0;

	while (size - (offset + written) > 0) {
		ret = writev(sd, iov, iovcnt);
		if (ret > 0) {
			written += ret;
			iovcnt = _iov_shift(iov, iovcnt, ret);
			continue;
		}
		switch (errno) {
		case EINTR:
			continue;
		case EWOULDBLOCK:
			return written;
		default:
			*shutdown = -errno;
			return written;
		}
	}

	return written;
}

bool pmixp_fd_read_ready(int fd, int *shutdown)
{
	struct pollfd pfd[1];
	int rc;
	pfd[0].fd = fd;
	pfd[0].events = POLLIN;
	/* Drop shutdown before the check */
	*shutdown = 0;

	rc = poll(pfd, 1, 0);
	if (rc < 0) {
		if (!(errno == EINTR)) {
			*shutdown = -errno;
			return false;
		}
	}

	bool ret = ((rc == 1) && (pfd[0].revents & POLLIN));
	if (!ret && (pfd[0].revents & (POLLERR | POLLHUP | POLLNVAL))) {
		if (pfd[0].revents & (POLLERR | POLLNVAL)) {
			*shutdown = -EBADF;
		} else {
			/* POLLHUP - normal connection close */
			*shutdown = 1;
		}
	}
	return ret;
}

bool pmixp_fd_write_ready(int fd, int *shutdown)
{
	struct pollfd pfd[1];
	int rc = 0;
	struct timeval tv;
	double start, cur;
	pfd[0].fd = fd;
	pfd[0].events = POLLOUT;
	pfd[0].revents = 0;

	gettimeofday(&tv,NULL);
	start = tv.tv_sec + 1E-6*tv.tv_usec;
	cur = start;
	while ((cur - start) < 0.01) {
		rc = poll(pfd, 1, 10);

		/* update current timestamp */
		gettimeofday(&tv,NULL);
		cur = tv.tv_sec + 1E-6*tv.tv_usec;
		if (0 > rc) {
			if (errno == EINTR) {
				continue;
			} else {
				*shutdown = -errno;
				return false;
			}
		}
		break;
	}

	if (pfd[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
		if (pfd[0].revents & (POLLERR | POLLNVAL)) {
			*shutdown = -EBADF;
		} else {
			/* POLLHUP - normal connection close */
			*shutdown = 1;
		}
	}
	return ((rc == 1) && (pfd[0].revents & POLLOUT));
}

int pmixp_stepd_send(const char *nodelist, const char *address,
		     const char *data, uint32_t len,
		     unsigned int start_delay,
		     unsigned int retry_cnt, int silent)
{

	int retry = 0, rc = SLURM_SUCCESS;
	unsigned int delay = start_delay; /* in milliseconds */
	char *copy_of_nodelist = xstrdup(nodelist);

	while (1) {
		if (!silent && retry >= 1) {
			PMIXP_DEBUG("send failed, rc=%d, try #%d", rc, retry);
		}

		rc = slurm_forward_data(&copy_of_nodelist, (char *)address,
					len, data);

		if (rc == SLURM_SUCCESS)
			break;

		retry++;
		if (retry >= retry_cnt) {
			PMIXP_ERROR("send failed, rc=%d, exceeded the retry limit", rc);
			break;
		}

		/* wait with constantly increasing delay */
		struct timespec ts =
		{(delay / MSEC_IN_SEC),
		 ((delay % MSEC_IN_SEC) * NSEC_IN_MSEC)};
		nanosleep(&ts, NULL);
		delay *= 2;
	}
	xfree(copy_of_nodelist);

	return rc;
}

/* original
static int _pmix_p2p_send_core(const char *nodename, const char *address,
			       const char *data, uint32_t len)
{
	int rc;
	slurm_msg_t msg, resp;
	forward_data_msg_t req;

	pmixp_debug_hang(0);

	slurm_msg_t_init(&msg);
	slurm_msg_t_init(&resp);

	PMIXP_DEBUG("nodelist=%s, address=%s, len=%u", nodename, address, len);
	req.address = (char *)address;
	req.len = len;

	req.data = (char*)data;

	msg.msg_type = REQUEST_FORWARD_DATA;
	msg.data = &req;

	if (slurm_conf_get_addr(nodename, &msg.address, msg.flags)
	    == SLURM_ERROR) {
		PMIXP_ERROR("Can't find address for host "
			    "%s, check slurm.conf", nodename);
		return SLURM_ERROR;
	}

	slurm_msg_set_r_uid(&msg, slurm_conf.slurmd_user_id);

	if (slurm_send_recv_node_msg(&msg, &resp, 0)) {
		PMIXP_ERROR("failed to send to %s, errno=%d", nodename, errno);
		return SLURM_ERROR;
	}

	rc = slurm_get_return_code(resp.msg_type, resp.data);
	slurm_free_msg_data(resp.msg_type, resp.data);

	return rc;
}
*/


/*
 * Parse SLURM_NODE_ALIASES environment variable to find node address
 * Format: node1,ip1,node2,ip2,...
 * Returns xstrdup'd address string or NULL if not found.
 * Caller must xfree() the result.
 */
static char* _lookup_in_node_aliases(const char *node_name, const char *aliases)
{
	char *addr = NULL;
	char *alias_copy = NULL;
	char *saveptr = NULL;
	char *token;
	char *current_node = NULL;
	char *current_addr = NULL;
	int pair_count = 0;
	
	if (!node_name || !aliases)
		return NULL;
	
	PMIXP_DEBUG("Looking up %s in SLURM_NODE_ALIASES", node_name);
	
	alias_copy = xstrdup(aliases);
	if (!alias_copy)
		return NULL;
	
	token = strtok_r(alias_copy, ",", &saveptr);
	while (token) {
		if (pair_count % 2 == 0) {
			current_node = token;
		} else {
			current_addr = token;
			if (current_node && strcmp(current_node, node_name) == 0) {
				addr = xstrdup(current_addr);
				PMIXP_DEBUG("Found address %s for node %s in aliases",
					    addr, node_name);
				break;
			}
		}
		pair_count++;
		token = strtok_r(NULL, ",", &saveptr);
	}
	
	xfree(alias_copy);
	return addr;
}



/*
 * Get node address directly from PMIx peer info
 * Compatible with PMIx 3.2.3
 */
static char* _get_addr_from_pmix_peer(const char *nodename)
{
	char *addr = NULL;
	pmix_proc_t myproc;
	pmix_proc_t target;
	pmix_value_t *val = NULL;
	pmix_info_t *info = NULL;
	size_t ninfo = 0;
	int rc;
	char *tmp_str = NULL;
	
	PMIXP_ERROR("=== PMIX DEBUG === _get_addr_from_pmix_peer: Looking up %s", nodename);
	
	/* Initialize myproc */
	memset(&myproc, 0, sizeof(pmix_proc_t));
	memset(&target, 0, sizeof(pmix_proc_t));
	
	/* Get our own proc info - different API in PMIx 3 */
	const char *nspace = NULL;
	uint32_t rank;
	
	/* In PMIx 3, we need to get namespace from environment */
	nspace = getenv("PMIX_NAMESPACE");
	if (!nspace) {
		PMIXP_ERROR("=== PMIX DEBUG === PMIX_NAMESPACE not set");
		return NULL;
	}
	
	/* Get rank from environment */
	const char *rank_str = getenv("PMIX_RANK");
	if (!rank_str) {
		PMIXP_ERROR("=== PMIX DEBUG === PMIX_RANK not set");
		return NULL;
	}
	rank = atoi(rank_str);
	
	PMIXP_ERROR("=== PMIX DEBUG === My proc: nspace=%s rank=%d", nspace, rank);
	
	/* Set up target process - use PMIX_PROC_INFO instead of PEER_INFO for PMIx 3 */
	strncpy(target.nspace, nspace, PMIX_MAX_NSLEN);
	target.rank = PMIX_RANK_WILDCARD;
	
	PMIXP_ERROR("=== PMIX DEBUG === Querying PMIx for proc info in nspace=%s", target.nspace);
	
	/* Query proc info from PMIx server - PMIX_PROC_INFO is the correct attribute for PMIx 3 */
	rc = PMIx_Get(&target, PMIX_PROC_INFO, NULL, 0, &val);
	
	if (rc != PMIX_SUCCESS) {
		PMIXP_ERROR("=== PMIX DEBUG === PMIx_Get FAILED with rc=%d", rc);
		return NULL;
	}
	
	if (!val) {
		PMIXP_ERROR("=== PMIX DEBUG === PMIx_Get returned NULL value");
		return NULL;
	}
	
	PMIXP_ERROR("=== PMIX DEBUG === PMIx_Get SUCCESS, value type=%d", val->type);
	
	/* Extract address based on PMIx value type */
	if (val->type == PMIX_STRING) {
		tmp_str = val->data.string;
		PMIXP_ERROR("=== PMIX DEBUG === Got string value: '%s'", tmp_str);
		
		/* Parse out IP address - look for hostname or IP */
		addr = xstrdup(tmp_str);
		PMIXP_ERROR("=== PMIX DEBUG === Using address: %s", addr);
	} 
	else if (val->type == PMIX_DATA_ARRAY) {
		pmix_data_array_t *arr = val->data.darray;
		PMIXP_ERROR("=== PMIX DEBUG === Got data array: type=%d size=%zu", 
			    arr->type, arr->size);
		
		if (arr->type == PMIX_STRING && arr->size > 0) {
			char **str_arr = (char **)arr->array;
			for (size_t i = 0; i < arr->size; i++) {
				PMIXP_ERROR("=== PMIX DEBUG === Array[%zu]: '%s'", i, str_arr[i]);
			}
			if (str_arr[0]) {
				addr = xstrdup(str_arr[0]);
				PMIXP_ERROR("=== PMIX DEBUG === Using first array element: %s", addr);
			}
		}
	} else {
		PMIXP_ERROR("=== PMIX DEBUG === Unhandled value type: %d", val->type);
	}
	
	PMIX_VALUE_RELEASE(val);
	
	if (addr) {
		PMIXP_ERROR("=== PMIX DEBUG === Successfully extracted address %s for node %s",
			    addr, nodename);
	} else {
		PMIXP_ERROR("=== PMIX DEBUG === Could not extract address for node %s",
			    nodename);
	}
	
	return addr;
}


static void _debug_dump_address(const char *nodename, const char *address)
{
    char filename[256];
    snprintf(filename, sizeof(filename), "/tmp/pmix_debug_%s.txt", nodename);
    
    FILE *fp = fopen(filename, "a");
    if (!fp) return;
    
    fprintf(fp, "=== Time: %ld ===\n", time(NULL));
    fprintf(fp, "nodename: %s\n", nodename ? nodename : "NULL");
    fprintf(fp, "address: '%s'\n", address ? address : "NULL");
    
    if (address) {
        fprintf(fp, "address length: %zu\n", strlen(address));
        fprintf(fp, "address hex dump:\n");
        for (int i = 0; i < 64 && i < strlen(address); i++) {
            fprintf(fp, "  [%d] = 0x%02x (%c)\n", 
                    i, (unsigned char)address[i],
                    isprint(address[i]) ? address[i] : '.');
        }
    }
    fprintf(fp, "\n");
    fclose(fp);
}



static int _pmix_p2p_send_core(const char *nodename, const char *address,
                               const char *data, uint32_t len)
{
    int rc;
    slurm_msg_t msg, resp;
    forward_data_msg_t req;
    struct addrinfo hints, *res;
    char port_str[16];
    int using_dns = 0;

    pmixp_debug_hang(0);

    slurm_msg_t_init(&msg);
    slurm_msg_t_init(&resp);

   _debug_dump_address(nodename, address);    
    PMIXP_ERROR("=== PMIX DEBUG === Entering _pmix_p2p_send_core for node=%s", nodename);
    
    /* SPECIAL CASE: This appears to be MPI peer communication (no address provided) */
    if (!address || !address[0]) {
        PMIXP_ERROR("=== PMIX DEBUG === Detected MPI peer communication, using port 6818");
        
        /* Use DNS with port 6818 for MPI peer-to-peer */
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_ADDRCONFIG;
        
        snprintf(port_str, sizeof(port_str), "%d", 6818);
        
        if (getaddrinfo(nodename, port_str, &hints, &res) == 0) {
            char addr_str[INET6_ADDRSTRLEN];
            
            if (res->ai_family == AF_INET) {
                struct sockaddr_in *sin = (struct sockaddr_in *)res->ai_addr;
                inet_ntop(AF_INET, &sin->sin_addr, addr_str, sizeof(addr_str));
                PMIXP_ERROR("=== PMIX DEBUG === MPI peer lookup SUCCESS: %s -> %s:%d", 
                            nodename, addr_str, ntohs(sin->sin_port));
            }
            
            memcpy(&msg.address, res->ai_addr, res->ai_addrlen);
            freeaddrinfo(res);
            using_dns = 1;
            goto send_message;
        } else {
            PMIXP_ERROR("=== PMIX DEBUG === MPI peer DNS lookup FAILED for %s", nodename);
        }
    }
    
    /* DEFAULT CASE: For everything else (controller health checks, etc.), use slurm.conf */
    PMIXP_ERROR("=== PMIX DEBUG === Using standard slurm.conf lookup for %s", nodename);
    if (slurm_conf_get_addr(nodename, &msg.address, msg.flags) == SLURM_ERROR) {
        PMIXP_ERROR("=== PMIX DEBUG === slurm.conf lookup FAILED for %s", nodename);
        PMIXP_ERROR("Can't find address for host %s", nodename);
        return SLURM_ERROR;
    } else {
        PMIXP_ERROR("=== PMIX DEBUG === slurm.conf lookup SUCCESS for %s", nodename);
    }

send_message:
    req.address = (char *)address;
    req.len = len;
    req.data = (char*)data;

    msg.msg_type = REQUEST_FORWARD_DATA;
    msg.data = &req;

    slurm_msg_set_r_uid(&msg, slurm_conf.slurmd_user_id);

    if (slurm_send_recv_node_msg(&msg, &resp, 0)) {
        PMIXP_ERROR("failed to send to %s, errno=%d", nodename, errno);
        return SLURM_ERROR;
    }

    rc = slurm_get_return_code(resp.msg_type, resp.data);
    slurm_free_msg_data(resp.msg_type, resp.data);

    PMIXP_ERROR("=== PMIX DEBUG === Exiting _pmix_p2p_send_core for %s with rc=%d", 
                nodename, rc);
    return rc;
}




int pmixp_p2p_send(const char *nodename, const char *address, const char *data,
		   uint32_t len, unsigned int start_delay,
		   unsigned int retry_cnt, int silent)
{
	int retry = 0, rc = SLURM_SUCCESS;
	unsigned int delay = start_delay; /* in milliseconds */

	pmixp_debug_hang(0);

	while (1) {
		if (!silent && retry >= 1) {
			PMIXP_DEBUG("send failed, rc=%d, try #%d", rc, retry);
		}

		rc = _pmix_p2p_send_core(nodename, address, data, len);

		if (rc == SLURM_SUCCESS)
			break;

		retry++;
		if (retry >= retry_cnt) {
			PMIXP_ERROR("send failed, rc=%d, exceeded the retry limit", rc);
			break;
		}

		/* wait with constantly increasing delay */
		struct timespec ts =
		{(delay / MSEC_IN_SEC),
		 ((delay % MSEC_IN_SEC) * NSEC_IN_MSEC)};
		nanosleep(&ts, NULL);
		delay *= 2;
	}

	return rc;
}

int pmixp_mkdir(char *path, bool trusted)
{
	char *base = NULL, *newdir = NULL, *slash;
	int dirfd, flags;
	mode_t rights = (S_IRUSR | S_IWUSR | S_IXUSR);

	/* NOTE: we need user who owns the job to access PMIx usock
	 * file. According to 'man 7 unix':
	 * "... In the Linux implementation, sockets which are visible in the
	 * file system honor the permissions of the directory  they are in... "
	 * Our case is the following: slurmstepd is usually running as root,
	 * user application will be "sudo'ed". To provide both of them with
	 * access to the unix socket we do the following:
	 * 1. Owner ID is set to the job owner.
	 * 2. Group ID corresponds to slurmstepd.
	 * 3. Set 0700 access mode
	 */

	base = xstrdup(path);
	/* split into base and new directory name */
	while ((slash = strrchr(base, '/'))) {
		/* fix a path with one or more trailing slashes */
		if (slash[1] == '\0')
			slash[0] = '\0';
		else
			break;
	}

	if (!slash) {
		PMIXP_ERROR_STD("Invalid directory \"%s\"", path);
		xfree(base);
		return EINVAL;
	}

	slash[0] = '\0';
	newdir = slash + 1;
	flags = O_DIRECTORY;
	if (!trusted)
		flags |= O_NOFOLLOW;

	if ((dirfd = open(base, flags)) < 0) {
		PMIXP_ERROR_STD("Could not open parent directory \"%s\"", base);
		xfree(base);
		return errno;
	}

#ifdef MULTIPLE_SLURMD
	struct stat statbuf;
	flags = 0;
	if (!trusted)
		flags |= AT_SYMLINK_NOFOLLOW;
	if (!fstatat(dirfd, newdir, &statbuf, flags)) {
		if ((statbuf.st_mode & S_IFDIR) &&
		    (statbuf.st_uid == pmixp_info_jobuid())) {
			PMIXP_ERROR_STD("Directory \"%s\" already exists, but has correct uid",
					path);
			close(dirfd);
			xfree(base);
			return 0;
		}
	}
#endif

	if (mkdirat(dirfd, newdir, rights) < 0) {
		PMIXP_ERROR_STD("Cannot create directory \"%s\"",
				path);
		close(dirfd);
		xfree(base);
		return errno;
	}

	if (fchownat(dirfd, newdir, (uid_t) pmixp_info_jobuid(), (gid_t) -1,
		     AT_SYMLINK_NOFOLLOW) < 0) {
		error("%s: fchownath(%s): %m", __func__, path);
		close(dirfd);
		xfree(base);
		return errno;
	}

	close(dirfd);
	xfree(base);
	return 0;
}
