#include "sasl_windows.h"

static const LPSTR SSPI_PACKAGE_NAME = "kerberos";

SECURITY_STATUS SEC_ENTRY sspi_acquire_credentials_handle(CredHandle *cred_handle, char *username, char *password, char *domain)
{
	SEC_WINNT_AUTH_IDENTITY auth_identity;
	SECURITY_INTEGER ignored;

	auth_identity.Flags = SEC_WINNT_AUTH_IDENTITY_ANSI;
	auth_identity.User = (LPSTR) username;
	auth_identity.UserLength = strlen(username);
	auth_identity.Password = (LPSTR) password;
	auth_identity.PasswordLength = strlen(password);
	auth_identity.Domain = (LPSTR) domain;
	auth_identity.DomainLength = strlen(domain);
	return call_sspi_acquire_credentials_handle(NULL, SSPI_PACKAGE_NAME, SECPKG_CRED_OUTBOUND, NULL, &auth_identity, NULL, NULL, cred_handle, &ignored);
}

int sspi_step(CredHandle *cred_handle, int has_context, CtxtHandle *context, PVOID *buffer, ULONG *buffer_length, char *target)
{
	SecBufferDesc inbuf;
	SecBuffer in_bufs[1];
	SecBufferDesc outbuf;
	SecBuffer out_bufs[1];

	if (has_context > 0) {
		// If we already have a context, we now have data to send.
		// Put this data in an inbuf.
		inbuf.ulVersion = SECBUFFER_VERSION;
		inbuf.cBuffers = 1;
		inbuf.pBuffers = in_bufs;
		in_bufs[0].pvBuffer = *buffer;
		in_bufs[0].cbBuffer = *buffer_length;
		in_bufs[0].BufferType = SECBUFFER_TOKEN;
	}

	outbuf.ulVersion = SECBUFFER_VERSION;
	outbuf.cBuffers = 1;
	outbuf.pBuffers = out_bufs;
	out_bufs[0].pvBuffer = NULL;
	out_bufs[0].cbBuffer = 0;
	out_bufs[0].BufferType = SECBUFFER_TOKEN;

	ULONG context_attr = 0;

	int ret = call_sspi_initialize_security_context(cred_handle,
	          has_context > 0 ? context : NULL,
	          (LPSTR) target,
	          ISC_REQ_ALLOCATE_MEMORY | ISC_REQ_MUTUAL_AUTH,
	          0,
	          SECURITY_NETWORK_DREP,
	          has_context > 0 ? &inbuf : NULL,
	          0,
	          context,
	          &outbuf,
	          &context_attr,
	          NULL);

	*buffer = malloc(out_bufs[0].cbBuffer);
	*buffer_length = out_bufs[0].cbBuffer;
	memcpy(*buffer, out_bufs[0].pvBuffer, *buffer_length);

	return ret;
}

int sspi_send_client_authz_id(CtxtHandle *context, PVOID *buffer, ULONG *buffer_length, char *user_plus_realm)
{
	SecPkgContext_Sizes sizes;
	SECURITY_STATUS status = call_sspi_query_context_attributes(context, SECPKG_ATTR_SIZES, &sizes);

	if (status != SEC_E_OK) {
		return status;
	}

	size_t user_plus_realm_length = strlen(user_plus_realm);
	int msgSize = 4 + user_plus_realm_length;
	char *msg = malloc((sizes.cbSecurityTrailer + msgSize + sizes.cbBlockSize) * sizeof(char));
	msg[sizes.cbSecurityTrailer + 0] = 1;
	msg[sizes.cbSecurityTrailer + 1] = 0;
	msg[sizes.cbSecurityTrailer + 2] = 0;
	msg[sizes.cbSecurityTrailer + 3] = 0;
	memcpy(&msg[sizes.cbSecurityTrailer + 4], user_plus_realm, user_plus_realm_length);

	SecBuffer wrapBufs[3];
	SecBufferDesc wrapBufDesc;
	wrapBufDesc.cBuffers = 3;
	wrapBufDesc.pBuffers = wrapBufs;
	wrapBufDesc.ulVersion = SECBUFFER_VERSION;

	wrapBufs[0].cbBuffer = sizes.cbSecurityTrailer;
	wrapBufs[0].BufferType = SECBUFFER_TOKEN;
	wrapBufs[0].pvBuffer = msg;

	wrapBufs[1].cbBuffer = msgSize;
	wrapBufs[1].BufferType = SECBUFFER_DATA;
	wrapBufs[1].pvBuffer = msg + sizes.cbSecurityTrailer;

	wrapBufs[2].cbBuffer = sizes.cbBlockSize;
	wrapBufs[2].BufferType = SECBUFFER_PADDING;
	wrapBufs[2].pvBuffer = msg + sizes.cbSecurityTrailer + msgSize;

	status = call_sspi_encrypt_message(context, SECQOP_WRAP_NO_ENCRYPT, &wrapBufDesc, 0);
	if (status != SEC_E_OK) {
		free(msg);
		return status;
	}

	*buffer_length = wrapBufs[0].cbBuffer + wrapBufs[1].cbBuffer + wrapBufs[2].cbBuffer;
	*buffer = malloc(*buffer_length);

	memcpy(*buffer, wrapBufs[0].pvBuffer, wrapBufs[0].cbBuffer);
	memcpy(*buffer + wrapBufs[0].cbBuffer, wrapBufs[1].pvBuffer, wrapBufs[1].cbBuffer);
	memcpy(*buffer + wrapBufs[0].cbBuffer + wrapBufs[1].cbBuffer, wrapBufs[2].pvBuffer, wrapBufs[2].cbBuffer);

	free(msg);
	return SEC_E_OK;
}
