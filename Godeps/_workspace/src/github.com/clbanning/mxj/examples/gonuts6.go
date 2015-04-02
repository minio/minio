/* https://groups.google.com/forum/#!topic/golang-nuts/EMXHB1nJoBA

package main

import "fmt"
import "encoding/json"

func main() {
	var v struct {
		DBInstances []struct {
			Endpoint struct {
				Address string
			}
		}
	}
	json.Unmarshal(s, &v)
	fmt.Println(v.DBInstances[0].Endpoint.Address)
}
*/

package main

import "fmt"
import "github.com/clbanning/mxj"

func main() {
	m, err := mxj.NewMapJson(s)
	if err != nil {
		fmt.Println("err:", err.Error())
	}
	v, err := m.ValuesForKey("Address")
	// v, err := m.ValuesForPath("DBInstances[0].Endpoint.Address")
	if err != nil {
		fmt.Println("err:", err.Error())
	}
	if len(v) > 0 {
		fmt.Println(v[0].(string))
	} else {
		fmt.Println("No value.")
	}
}

var s = []byte(`{ "DBInstances": [ { "PubliclyAccessible": true, "MasterUsername": "postgres", "LicenseModel": "postgresql-license", "VpcSecurityGroups": [ { "Status": "active", "VpcSecurityGroupId": "sg-e72a4282" } ], "InstanceCreateTime": "2014-06-29T03:52:59.268Z", "OptionGroupMemberships": [ { "Status": "in-sync", "OptionGroupName": "default:postgres-9-3" } ], "PendingModifiedValues": {}, "Engine": "postgres", "MultiAZ": true, "LatestRestorableTime": "2014-06-29T12:00:34Z", "DBSecurityGroups": [ { "Status": "active", "DBSecurityGroupName": "production-dbsecuritygroup-q4f0ugxpjck8" } ], "DBParameterGroups": [ { "DBParameterGroupName": "default.postgres9.3", "ParameterApplyStatus": "in-sync" } ], "AutoMinorVersionUpgrade": true, "PreferredBackupWindow": "06:59-07:29", "DBSubnetGroup": { "Subnets": [ { "SubnetStatus": "Active", "SubnetIdentifier": "subnet-34e5d01c", "SubnetAvailabilityZone": { "Name": "us-east-1b", "ProvisionedIopsCapable": false } }, { "SubnetStatus": "Active", "SubnetIdentifier": "subnet-50759d27", "SubnetAvailabilityZone": { "Name": "us-east-1c", "ProvisionedIopsCapable": false } }, { "SubnetStatus": "Active", "SubnetIdentifier": "subnet-450a1f03", "SubnetAvailabilityZone": { "Name": "us-east-1d", "ProvisionedIopsCapable": false } } ], "DBSubnetGroupName": "default", "VpcId": "vpc-acb86cc9", "DBSubnetGroupDescription": "default", "SubnetGroupStatus": "Complete" }, "SecondaryAvailabilityZone": "us-east-1b", "ReadReplicaDBInstanceIdentifiers": [], "AllocatedStorage": 15, "BackupRetentionPeriod": 1, "DBName": "deis", "PreferredMaintenanceWindow": "fri:05:52-fri:06:22", "Endpoint": { "Port": 5432, "Address": "production.cfk8mskkbkeu.us-east-1.rds.amazonaws.com" }, "DBInstanceStatus": "available", "EngineVersion": "9.3.3", "AvailabilityZone": "us-east-1c", "DBInstanceClass": "db.m1.small", "DBInstanceIdentifier": "production" } ] }`)
