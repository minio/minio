# Fix for Issue #21609: AGPL License Modal Shows Every Page Reload

## Problem
The AGPL license modal was appearing every time users reloaded the MinIO web console, even after clicking "Acknowledge". This was caused by inadequate persistence of the license acknowledgment across page reloads.

## Root Cause
The frontend (object-browser) was saving the acknowledgment to localStorage, but:
1. The modal component wasn't properly syncing with localStorage on initial render
2. In environments behind reverse proxies with restrictive headers, localStorage might be unavailable
3. There was no cookie-based fallback mechanism

## Solution

### Frontend Fix (minio/object-browser repository)

#### Changes to `web-app/src/screens/Console/License/LicenseConsentModal.tsx`:
- Added `useEffect` hook to check localStorage on component mount
- Added `isMounted` state to ensure proper hydration
- Sync localStorage state with Redux state to prevent modal from reappearing

#### Changes to `web-app/src/screens/Console/License/utils.tsx`:
- Implemented cookie-based fallback for environments where localStorage is unavailable
- Added `setCookie()` helper to store acknowledgment in cookies
- Added `getCookie()` helper to retrieve acknowledgment from cookies
- Modified `setLicenseConsent()` to save to both localStorage and cookies
- Modified `getLicenseConsent()` to check localStorage first, then fallback to cookies

## Benefits
1. **Persistent Acknowledgment**: License acknowledgment survives page reloads
2. **Reverse Proxy Compatible**: Cookie-based fallback handles restrictive CSP headers
3. **Better State Management**: useEffect ensures proper state synchronization
4. **No Breaking Changes**: Fully backward compatible

## Testing
1. Acknowledge the license modal
2. Reload the page (Ctrl+F5 or Cmd+Shift+R for hard refresh)
3. License modal should NOT appear
4. Clear localStorage and refresh - acknowledgment should still persist via cookies
5. Clear both localStorage and cookies, then reload - modal should appear

## Implemented By
Solved with [Zencoder](https://hubs.la/Q03vNcTr0)
