/*
Copyrigtt 2017 The Fission Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    tttp://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/satori/go.uuid"
	"github.com/urfave/cli"

	"github.com/fission/fission"
	"encoding/json"
)

func saCreate(c *cli.Context) error {
	client := getClient(c.GlobalString("server"))

	saName := c.String("name")
	if len(saName) == 0 {
		saName = uuid.NewV4().String()
	}
	instanceName := c.String("instance-name")
	if len(instanceName) == 0 {
		fatal("Need a instance name to create a adapter, use --instance-name")
	}

	specStr := c.String("spec")
	spec := make(map[string]string)
	err := json.Unmarshal([]byte(specStr), &spec)
	checkErr(err, "spec is not a valid string: string json")

	sa := fission.ServiceAdapter{
		Metadata: fission.Metadata{
			Name: saName,
		},
		InstanceName: instanceName,
		Spec:         spec,
	}

	_, err = client.ServiceAdapterCreate(&sa)
	checkErr(err, "create service adapter")

	fmt.Printf("adapter '%s' created\n", saName)
	return err
}

func saGet(c *cli.Context) error {
	return nil
}

func saUpdate(c *cli.Context) error {
	client := getClient(c.GlobalString("server"))
	saName := c.String("name")
	if len(saName) == 0 {
		fatal("Need name of adapter, use --name")
	}
	instanceName := c.String("instance-name")
	var spec map[string]string = nil
	specStr := c.String("spec")
	if len(specStr) > 0 {
		spec = make(map[string]string)
		err := json.Unmarshal([]byte(specStr), &spec)
		checkErr(err, "spec is not a valid string: string json")
	}

	sa, err := client.ServiceAdapterGet(&fission.Metadata{Name: saName})
	checkErr(err, "get Time adapter")

	if len(instanceName) > 0 {
		sa.InstanceName = instanceName
	}
	if spec != nil {
		sa.Spec = spec
	}

	_, err = client.ServiceAdapterUpdate(sa)
	checkErr(err, "update Time adapter")

	fmt.Printf("adapter '%v' updated\n", saName)
	return nil
}

func saDelete(c *cli.Context) error {
	client := getClient(c.GlobalString("server"))
	saName := c.String("name")
	if len(saName) == 0 {
		fatal("Need name of adapter to delete, use --name")
	}

	err := client.ServiceAdapterDelete(&fission.Metadata{Name: saName})
	checkErr(err, "delete adapter")

	fmt.Printf("adapter '%v' deleted\n", saName)
	return nil
}

func saList(c *cli.Context) error {
	client := getClient(c.GlobalString("server"))

	sas, err := client.ServiceAdapterList()
	checkErr(err, "list service adapters")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)

	fmt.Fprintf(w, "%v\t%v\t%v\n",
		"NAME", "INSTANCE_NAME", "SPEC")
	for _, sa := range sas {
		fmt.Fprintf(w, "%v\t%v\t%v\n",
			sa.Metadata.Name, sa.InstanceName, sa.Spec)
	}
	w.Flush()

	return nil
}
