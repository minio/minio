## Unreleased

- Introduce CloudAPI's ListRulesMachines under networking
- Enable HTTP KeepAlives by default in the client.  15s idle timeout, 2x
  connections per host, total of 10x connections per client.
- Expose an optional Headers attribute to clients to allow them to customize
  HTTP headers when making Object requests.
- Fix a bug in Directory ListIndex (#69)

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
