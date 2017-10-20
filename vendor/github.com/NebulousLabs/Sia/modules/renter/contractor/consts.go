package contractor

import (
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
)

// Constants related to fees and fee estimation.
const (
	// estimatedFileContractTransactionSize provides the estimated size of
	// the average file contract in bytes.
	estimatedFileContractTransactionSize = 1200
)

// Constants related to contract formation parameters.
var (
	// To alleviate potential block propagation issues, the contractor sleeps
	// between each contract formation.
	contractFormationInterval = build.Select(build.Var{
		Dev:      10 * time.Second,
		Standard: 60 * time.Second,
		Testing:  10 * time.Millisecond,
	}).(time.Duration)

	// minHostsForEstimations describes the minimum number of hosts that
	// are needed to make broad estimations such as the number of sectors
	// that you can store on the network for a given allowance.
	minHostsForEstimations = build.Select(build.Var{
		// The number is set lower than standard so that it can
		// be reached/exceeded easily within development
		// environments, but set high enough that it's also
		// easy to fall short within the development
		// environments.
		Dev: 5,
		// Hosts can have a lot of variance. Selecting too many
		// hosts will high-ball the price estimation, but users
		// shouldn't be selecting rewer hosts, and if there are
		// too few hosts being selected for estimation there is
		// a risk of underestimating the actual price, which is
		// something we'd rather avoid.
		Standard: 10,
		// Testing tries to happen as fast as possible,
		// therefore tends to run with a lot fewer hosts.
		Testing: 4,
	}).(int)

	// minScoreHostBuffer defines how many extra hosts are queried when trying
	// to figure out an appropriate minimum score for the hosts that we have.
	minScoreHostBuffer = build.Select(build.Var{
		Dev:      2,
		Standard: 10,
		Testing:  1,
	}).(int)
)

// Constants related to the safety values for when the contractor is forming
// contracts.
var (
	maxCollateral    = types.SiacoinPrecision.Mul64(1e3) // 1k SC
	maxDownloadPrice = maxStoragePrice.Mul64(3 * 4320)
	maxStoragePrice  = types.SiacoinPrecision.Mul64(30e3).Div(modules.BlockBytesPerMonthTerabyte) // 30k SC / TB / Month
	maxUploadPrice   = maxStoragePrice.Mul64(4320)

	// scoreLeeway defines the factor by which a host can miss the goal score
	// for a set of hosts. To determine the goal score, a new set of hosts is
	// queried from the hostdb and the lowest scoring among them is selected.
	// That score is then divided by scoreLeeway to get the minimum score that a
	// host is allowed to have before being marked as !GoodForUpload.
	scoreLeeway = types.NewCurrency64(25)
)
