/*
Package contractor is responsible for forming and renewing file contracts with
hosts. Its goal is to manage the low-level details of the negotiation,
revision, and renewal protocols, such that the renter can operate at a higher
level of abstraction. Ideally, the renter should be mostly ignorant of the Sia
protocol, instead focusing on file management, redundancy, and upload/download
algorithms.

Contract formation does not begin until the user first calls SetAllowance. An
allowance dictates how much money the contractor is allowed to spend on file
contracts during a given period. When the user calls SetAllowance for the
first time, the call will block until contracts have been negotiated with the
specified number of hosts. Upon subsequent calls, new contracts will only be
formed if the allowance is sufficiently greater than the previous allowance,
where "sufficiently greater" currently means "enough money to pay for at least
one additional sector on every host." This allows the user to increase the
amount of available storage immediately, at the cost of some complexity.

The contractor forms many contracts in parallel with different host, and tries
to keep all the contracts "consistent" -- that is, they should all have the
same storage capacity, and they should all end at the same height. Hosts are
selected from the HostDB; there is no support for manually specifying hosts.

Contracts are automatically renewed by the contractor at a safe threshold
before they are set to expire. When contracts are renewed, they are renewed
with the current allowance, which may differ from the allowance that was used
to form the initial contracts. In general, this means that allowance
modifications only take effect upon the next "contract cycle" (the exception
being "sufficiently greater" modifications, as defined above).

As an example, imagine that the user first sets an allowance that will cover
10 contracts of 10 sectors each for 100 blocks. The contractor will
immediately form contracts with 10 hosts, paying each host enough to cover 10
sectors for 100 blocks. Then, 20 blocks later, the user increases the
allowance, such that it now covers 10 contracts of 20 sectors for 200 blocks.
The contractor will immediately form contracts as follows:

- 10 contracts will be formed with the current hosts, each covering 10 sectors
  for 80 blocks.

- 10 contracts will be formed with new hosts, each covering 20 sectors for 80
  blocks.

Note that these newly-formed contracts are timed to expire in sync with the
existing contracts. This becomes the new "contract set," totaling 30
contracts, but only 20 hosts, with 20 sectors per host. When it comes time to
renew these contracts, only one contract will be renewed per host, and the
contracts will be renewed for the full 200-block duration. The new contract
set will thus consist of 20 contracts, 20 hosts, 20 sectors, 200 blocks.

On the other hand, if the allowance is decreased, no immediate action is
taken. Why? Because the contracts have already been paid for. The new
allowance will only take effect upon the next renewal.

Modifications to file contracts are mediated through the Editor interface. An
Editor maintains a network connection to a host, over which is sends
modification requests, such as "delete sector 12." After each modification,
the Editor revises the underlying file contract and saves it to disk.

The primary challenge of the contractor is that it must be smart enough for
the user to feel comfortable allowing it to spend their money. Because
contract renewal is a background task, it is difficult to report errors to the
user and defer to their decision. For example, what should the contractor do
in the following scenarios?

- The contract set is up for renewal, but the average host price has
  increased, and now the allowance is not sufficient to cover all of the
  user's uploaded data.

- The user sets an allowance of 10 hosts. The contractor forms 5 contracts,
  but the rest fail, and the remaining hosts in the HostDB are too expensive.

- After contract formation succeeds, 2 of 10 hosts become unresponsive. Later,
  another 4 become unresponsive.

Waiting for user input is dangerous because if the contract period elapses,
data is permanently lost. The contractor should treat this as the worst-case
scenario, and take steps to prevent it, so long as the allowance is not
exceeded. However, since the contractor has no concept of redundancy, it is
not well-positioned to determine which sectors to sacrifice and which to
preserve. The contractor also lacks the ability to reupload data; it can
download sectors, but it does not know the decryption keys or erasure coding
metadata required to reconstruct the original data. It follows that these
responsibilities must be delegated to the renter.

*/
package contractor
