// +build !windows

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sasl/sasl.h>

static int mgo_sasl_simple(void *context, int id, const char **result, unsigned int *len)
{
	if (!result) {
		return SASL_BADPARAM;
	}
	switch (id) {
	case SASL_CB_USER:
		*result = (char *)context;
		break;
	case SASL_CB_AUTHNAME:
		*result = (char *)context;
		break;
	case SASL_CB_LANGUAGE:
		*result = NULL;
		break;
	default:
		return SASL_BADPARAM;
	}
	if (len) {
		*len = *result ? strlen(*result) : 0;
	}
	return SASL_OK;
}

typedef int (*callback)(void);

static int mgo_sasl_secret(sasl_conn_t *conn, void *context, int id, sasl_secret_t **result)
{
	if (!conn || !result || id != SASL_CB_PASS) {
		return SASL_BADPARAM;
	}
	*result = (sasl_secret_t *)context;
	return SASL_OK;
}

sasl_callback_t *mgo_sasl_callbacks(const char *username, const char *password)
{
	sasl_callback_t *cb = malloc(4 * sizeof(sasl_callback_t));
	int n = 0;

	size_t len = strlen(password);
	sasl_secret_t *secret = (sasl_secret_t*)malloc(sizeof(sasl_secret_t) + len);
	if (!secret) {
		free(cb);
		return NULL;
	}
	strcpy((char *)secret->data, password);
	secret->len = len;

	cb[n].id = SASL_CB_PASS;
	cb[n].proc = (callback)&mgo_sasl_secret;
	cb[n].context = secret;
	n++;

	cb[n].id = SASL_CB_USER;
	cb[n].proc = (callback)&mgo_sasl_simple;
	cb[n].context = (char*)username;
	n++;

	cb[n].id = SASL_CB_AUTHNAME;
	cb[n].proc = (callback)&mgo_sasl_simple;
	cb[n].context = (char*)username;
	n++;

	cb[n].id = SASL_CB_LIST_END;
	cb[n].proc = NULL;
	cb[n].context = NULL;

	return cb;
}
