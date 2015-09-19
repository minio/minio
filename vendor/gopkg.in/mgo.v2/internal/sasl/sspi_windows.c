// Code adapted from the NodeJS kerberos library:
// 
//   https://github.com/christkv/kerberos/tree/master/lib/win32/kerberos_sspi.c
//
// Under the terms of the Apache License, Version 2.0:
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
#include <stdlib.h>

#include "sspi_windows.h"

static HINSTANCE sspi_secur32_dll = NULL;

int load_secur32_dll()
{
	sspi_secur32_dll = LoadLibrary("secur32.dll");
	if (sspi_secur32_dll == NULL) {
		return GetLastError();
	}
	return 0;
}

SECURITY_STATUS SEC_ENTRY call_sspi_encrypt_message(PCtxtHandle phContext, unsigned long fQOP, PSecBufferDesc pMessage, unsigned long MessageSeqNo)
{
	if (sspi_secur32_dll == NULL) {
		return -1;
	}
	encryptMessage_fn pfn_encryptMessage = (encryptMessage_fn) GetProcAddress(sspi_secur32_dll, "EncryptMessage");
	if (!pfn_encryptMessage) {
		return -2;
	}
	return (*pfn_encryptMessage)(phContext, fQOP, pMessage, MessageSeqNo);
}

SECURITY_STATUS SEC_ENTRY call_sspi_acquire_credentials_handle(
	LPSTR pszPrincipal, LPSTR pszPackage, unsigned long fCredentialUse,
        void *pvLogonId, void *pAuthData, SEC_GET_KEY_FN pGetKeyFn, void *pvGetKeyArgument,
        PCredHandle phCredential, PTimeStamp ptsExpiry)
{
	if (sspi_secur32_dll == NULL) {
		return -1;
	}
	acquireCredentialsHandle_fn pfn_acquireCredentialsHandle;
#ifdef _UNICODE
	pfn_acquireCredentialsHandle = (acquireCredentialsHandle_fn) GetProcAddress(sspi_secur32_dll, "AcquireCredentialsHandleW");
#else
	pfn_acquireCredentialsHandle = (acquireCredentialsHandle_fn) GetProcAddress(sspi_secur32_dll, "AcquireCredentialsHandleA");
#endif
	if (!pfn_acquireCredentialsHandle) {
		return -2;
	}
	return (*pfn_acquireCredentialsHandle)(
		pszPrincipal, pszPackage, fCredentialUse, pvLogonId, pAuthData,
	        pGetKeyFn, pvGetKeyArgument, phCredential, ptsExpiry);
}

SECURITY_STATUS SEC_ENTRY call_sspi_initialize_security_context(
	PCredHandle phCredential, PCtxtHandle phContext, LPSTR pszTargetName,
	unsigned long fContextReq, unsigned long Reserved1, unsigned long TargetDataRep,
	PSecBufferDesc pInput, unsigned long Reserved2, PCtxtHandle phNewContext,
	PSecBufferDesc pOutput, unsigned long *pfContextAttr, PTimeStamp ptsExpiry)
{
	if (sspi_secur32_dll == NULL) {
		return -1;
	}
	initializeSecurityContext_fn pfn_initializeSecurityContext;
#ifdef _UNICODE
	pfn_initializeSecurityContext = (initializeSecurityContext_fn) GetProcAddress(sspi_secur32_dll, "InitializeSecurityContextW");
#else
	pfn_initializeSecurityContext = (initializeSecurityContext_fn) GetProcAddress(sspi_secur32_dll, "InitializeSecurityContextA");
#endif
	if (!pfn_initializeSecurityContext) {
		return -2;
	}
	return (*pfn_initializeSecurityContext)(
		phCredential, phContext, pszTargetName, fContextReq, Reserved1, TargetDataRep,
		pInput, Reserved2, phNewContext, pOutput, pfContextAttr, ptsExpiry);
}

SECURITY_STATUS SEC_ENTRY call_sspi_query_context_attributes(PCtxtHandle phContext, unsigned long ulAttribute, void *pBuffer)
{
	if (sspi_secur32_dll == NULL) {
		return -1;
	}
	queryContextAttributes_fn pfn_queryContextAttributes;
#ifdef _UNICODE
	pfn_queryContextAttributes = (queryContextAttributes_fn) GetProcAddress(sspi_secur32_dll, "QueryContextAttributesW");
#else
	pfn_queryContextAttributes = (queryContextAttributes_fn) GetProcAddress(sspi_secur32_dll, "QueryContextAttributesA");
#endif
	if (!pfn_queryContextAttributes) {
		return -2;
	}
	return (*pfn_queryContextAttributes)(phContext, ulAttribute, pBuffer);
}
