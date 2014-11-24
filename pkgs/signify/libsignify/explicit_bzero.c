/*	$OpenBSD: explicit_bzero.c,v 1.1 2014/01/22 21:06:45 tedu Exp $ */
/*
 * Public domain.
 * Written by Ted Unangst
 */

#include <string.h>
#include "util.h"

/*
 * explicit_bzero - don't let the compiler optimize away bzero
 */

static void (* volatile signify_bzero)(void *, size_t) = bzero;

void
explicit_bzero(void *p, size_t n)
{
	signify_bzero(p, n);
}
