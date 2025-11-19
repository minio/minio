# Contribution Guide for Issue #21609

## Summary
We've successfully implemented a fix for issue #21609 (AGPL license modal showing on every page reload) across two repositories:

1. **minio/minio** - Documentation and issue tracking
2. **minio/object-browser** - Actual implementation of the fix

## What Was Done

### 1. Minio Repository (Backend)
- Created branch: `fix/21609-agpl-license-console`
- Added documentation: `FIX_21609_LICENSE_MODAL.md`
- Pushed to fork: https://github.com/Arpanwanwe/minio/tree/fix/21609-agpl-license-console

### 2. Object-Browser Repository (Frontend)
- Created branch: `fix/21609-license-modal-persistence`
- Modified `web-app/src/screens/Console/License/LicenseConsentModal.tsx`
  - Added useEffect hook for proper state initialization
  - Added isMounted state to prevent hydration mismatches
  - Synced localStorage with Redux state
- Modified `web-app/src/screens/Console/License/utils.tsx`
  - Implemented cookie-based fallback mechanism
  - Added getCookie and setCookie utility functions
  - Enhanced getLicenseConsent() to check both localStorage and cookies
  - Enhanced setLicenseConsent() to save to both storage methods

## Next Steps for Creating PRs

### For Minio Repository:
1. Go to: https://github.com/Arpanwanwe/minio/pull/new/fix/21609-agpl-license-console
2. Create PR with title: "docs: Add documentation for issue #21609 license modal fix"
3. Set base to: minio:master
4. Add description with the fix details

### For Object-Browser Repository:
1. **Fork the repository** (if you haven't already):
   - Go to https://github.com/minio/object-browser
   - Click "Fork" button
   - This creates: https://github.com/Arpanwanwe/object-browser

2. **Clone the fix branch locally**:
   ```bash
   cd /path/to/object-browser-repo
   git remote set-url origin https://github.com/Arpanwanwe/object-browser.git
   git remote add upstream https://github.com/minio/object-browser.git
   git push origin fix/21609-license-modal-persistence
   ```

3. **Create a PR**:
   - Go to: https://github.com/Arpanwanwe/object-browser/pull/new/fix/21609-license-modal-persistence
   - Create PR with title: "Fix: Persist AGPL license acknowledgment across page reloads"
   - Set base to: minio:master
   - Add description referencing issue #21609

## PR Description Template

```
## Description
Fixes the issue where the AGPL license modal appears every time the MinIO web console is reloaded, even after the user has acknowledged the license.

## Changes
- Added useEffect hook to properly initialize component state from localStorage
- Implemented cookie-based fallback for environments where localStorage is unavailable
- Enhanced license consent persistence with dual-storage mechanism
- Improved state synchronization between localStorage and Redux

## Testing
1. Acknowledge the license modal
2. Reload the page (F5 or Cmd+Shift+R)
3. License modal should NOT appear
4. Clear localStorage and refresh - acknowledgment should persist via cookies
5. Clear both localStorage and cookies - modal should appear on next load

## Fixes
Closes #21609

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [x] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)

## Related Issues
- #21609
- #21424 (Related issue with CSP headers)

Solved with [Zencoder](https://hubs.la/Q03vNcTr0)
```

## Files Changed Summary

### object-browser-repo/web-app/src/screens/Console/License/LicenseConsentModal.tsx
- Lines added: 6 (useEffect hook, isMounted state)
- Lines modified: 4 (conditional rendering)

### object-browser-repo/web-app/src/screens/Console/License/utils.tsx
- Lines added: 43 (cookie helpers, enhanced consent functions)
- Lines modified: 2 (function implementations)

## Branch Information

### Minio Repository
- **Branch**: fix/21609-agpl-license-console
- **Upstream**: minio/master
- **Commit**: docs: Add documentation for issue #21609 fix

### Object-Browser Repository
- **Branch**: fix/21609-license-modal-persistence
- **Upstream**: minio:master
- **Commit**: Fix license modal persistence across page reloads

## Questions or Issues?
If you encounter any problems:
1. Verify both branches are correctly created
2. Ensure remotes point to correct repositories
3. Check that files have been properly modified
4. Refer to the individual FIX documentation files

Good luck with your contributions! 🚀
