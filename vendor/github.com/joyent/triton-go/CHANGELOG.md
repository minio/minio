## Unreleased

## 0.5.2 (December 28)

- Standardise the API SSH Signers input casing and naming

## 0.5.1 (December 28)

- Include leading '/' when working with SSH Agent signers

## 0.5.0 (December 28)

- Add support for RBAC in triton-go [#82]
This is a breaking change. No longer do we pass individual parameters to the SSH Signer funcs, but we now pass an input Struct. This will guard from from additional parameter changes in the future. 
We also now add support for using `SDC_*` and `TRITON_*` env vars when working with the Default agent signer

## 0.4.2 (December 22)

- Fixing a panic when the user loses network connectivity when making a GET request to instance [#81]

## 0.4.1 (December 15)

- Clean up the handling of directory sanitization. Use abs paths everywhere [#79]

## 0.4.0 (December 15)

- Fix an issue where Manta HEAD requests do not return an error resp body [#77]
- Add support for recursively creating child directories [#78]

## 0.3.0 (December 14)

- Introduce CloudAPI's ListRulesMachines under networking
- Enable HTTP KeepAlives by default in the client.  15s idle timeout, 2x
  connections per host, total of 10x connections per client.
- Expose an optional Headers attribute to clients to allow them to customize
  HTTP headers when making Object requests.
- Fix a bug in Directory ListIndex [#69](https://github.com/joyent/issues/69)
- Inputs to Object inputs have been relaxed to `io.Reader` (formerly a
  `io.ReadSeeker`) [#73](https://github.com/joyent/issues/73).
- Add support for ForceDelete of all children of a directory [#71](https://github.com/joyent/issues/71)
- storage: Introduce `Objects.GetInfo` and `Objects.IsDir` using HEAD requests [#74](https://github.com/joyent/triton-go/issues/74)

## 0.2.1 (November 8)

- Fixing a bug where CreateUser and UpdateUser didn't return the UserID

## 0.2.0 (November 7)

- Introduce CloudAPI's Ping under compute
- Introduce CloudAPI's RebootMachine under compute instances
- Introduce CloudAPI's ListUsers, GetUser, CreateUser, UpdateUser and DeleteUser under identity package
- Introduce CloudAPI's ListMachineSnapshots, GetMachineSnapshot, CreateSnapshot, DeleteMachineSnapshot and StartMachineFromSnapshot under compute package
- tools: Introduce unit testing and scripts for linting, etc.
- bug: Fix the `compute.ListMachineRules` endpoint

## 0.1.0 (November 2)

- Initial release of a versioned SDK
