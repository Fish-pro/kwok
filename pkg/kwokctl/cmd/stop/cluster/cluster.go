/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cluster

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/kwok/pkg/config"
	"sigs.k8s.io/kwok/pkg/kwokctl/runtime"
	"sigs.k8s.io/kwok/pkg/log"
	"sigs.k8s.io/kwok/pkg/utils/path"
	"sigs.k8s.io/kwok/pkg/utils/slices"
)

type flagpole struct {
	Name       string
	Components []string
}

// NewCommand returns a new cobra.Command for stop cluster
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Use:   "cluster",
		Short: "Stop a cluster or cluster component(s)",
		Long:  "Stop a cluster or cluster component(s)",
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.Name = config.DefaultCluster
			return runE(cmd.Context(), flags)
		},
	}

	cmd.PersistentFlags().StringArrayVar(&flags.Components, "component", flags.Components, "Component name, which can include one or more of [etcd, kube-apiserver, kube-controller-manager, kube-scheduler, kwok-controller]")

	return cmd
}

func runE(ctx context.Context, flags *flagpole) error {
	name := config.ClusterName(flags.Name)
	workdir := path.Join(config.ClustersDir, flags.Name)

	logger := log.FromContext(ctx)
	logger = logger.With("cluster", flags.Name)
	ctx = log.NewContext(ctx, logger)

	rt, err := runtime.DefaultRegistry.Load(ctx, name, workdir)
	if err != nil {
		return err
	}

	if len(flags.Components) != 0 {
		conf, err := rt.Config(ctx)
		if err != nil {
			return err
		}
		componentSet := sets.NewString()
		for _, component := range conf.Components {
			componentSet.Insert(component.Name)
		}

		unknownComponents := slices.Filter(flags.Components, func(component string) bool {
			return !componentSet.Has(component)
		})
		if len(unknownComponents) != 0 {
			return fmt.Errorf("the cluster component(s) does not contain %q", unknownComponents)
		}

		err = rt.StopComponents(ctx, flags.Components...)
		if err != nil {
			logger.Error("Stopping cluster component(s)", err, "components", flags.Components)
		} else {
			logger.Info("Cluster component(s) Stopped", "components", flags.Components)
		}
		return nil
	}

	logger.Info("Stopping cluster")
	err = rt.Stop(ctx)
	if err != nil {
		return err
	}

	logger.Info("Cluster stopped")
	return nil
}
