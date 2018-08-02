/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <ftw.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

/* This should be more than enough to traverse the depth of /manta */
#define	MAX_DESCRIPTORS	10

static void usage(const char *);
static void nftw_error(const char *, ...);
static int ntfw_cb(const char *, const struct stat *, int, struct FTW *);

int
main(int argc, char **argv)
{
	int i;
	int errors = 0;

	if (argc < 2)
		usage(argv[0]);

	/*
	 * Roll through the list of caller-supplied directories and call
	 * `nftw()' on each.  Record any errors along the way.
	 */
	for (i = 1; i < argc; i++) {
		if (nftw(argv[i], ntfw_cb, MAX_DESCRIPTORS, FTW_PHYS) != 0) {
			fprintf(stderr, "%s: %s: encountered an error\n",
				argv[0], argv[i]);
			errors++;
		}
	}

	return (errors != 0);
}

static void
usage(const char *name)
{
	fprintf(stderr, "usage: %s dir1 dir2 ... dirN\n", name);
	exit(1);
}

static void
nftw_error(const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);
}

static int
ntfw_cb(const char *path, const struct stat *st, int objtype, struct FTW *ftw)
{
	int ret = 0;
	unsigned int logical;
	const char *name = path + ftw->base;

	switch (objtype) {
	case FTW_F:
		logical = st->st_blocks / 2;
		if (st->st_blocks % 2 > 0)
			logical++;
		printf("%s\t%lu\t%lu\t%d\n", path, st->st_size, st->st_mtime,
		    logical);
		break;

	/* We are not interested in directories or sym links */
	case FTW_D:
	case FTW_SL:
		break;

	case FTW_DNR:
		nftw_error("%s: unable to read\n", name);
		break;

	case FTW_NS:
		nftw_error("%s: stat failed: %s\n", name, strerror(errno));
		break;

	default:
		nftw_error("%s: unknown type (%s)", name, objtype);
		ret = 1;
		break;
	}

	return (ret);
}
