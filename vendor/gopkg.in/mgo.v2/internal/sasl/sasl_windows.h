#include <windows.h>

#include "sspi_windows.h"

SECURITY_STATUS SEC_ENTRY sspi_acquire_credentials_handle(CredHandle* cred_handle, char* username, char* password, char* domain);
int sspi_step(CredHandle* cred_handle, int has_context, CtxtHandle* context, PVOID* buffer, ULONG* buffer_length, char* target);
int sspi_send_client_authz_id(CtxtHandle* context, PVOID* buffer, ULONG* buffer_length, char* user_plus_realm);
