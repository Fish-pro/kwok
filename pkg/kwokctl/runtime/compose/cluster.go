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

package compose

import (
	"context"
	"fmt"
	"io"
	"os"

	"sigs.k8s.io/kwok/pkg/kwokctl/k8s"
	"sigs.k8s.io/kwok/pkg/kwokctl/pki"
	"sigs.k8s.io/kwok/pkg/kwokctl/runtime"
	"sigs.k8s.io/kwok/pkg/kwokctl/utils"
	"sigs.k8s.io/kwok/pkg/kwokctl/vars"
	"sigs.k8s.io/kwok/pkg/log"
)

type Cluster struct {
	*runtime.Cluster
}

func NewCluster(name, workdir string) (runtime.Runtime, error) {
	return &Cluster{
		Cluster: runtime.NewCluster(name, workdir),
	}, nil
}

func (c *Cluster) Install(ctx context.Context) error {
	conf, err := c.Config()
	if err != nil {
		return err
	}

	kubeconfigPath := utils.PathJoin(conf.Workdir, runtime.InHostKubeconfigName)
	prometheusPath := ""
	inClusterOnHostKubeconfigPath := utils.PathJoin(conf.Workdir, runtime.InClusterKubeconfigName)
	pkiPath := utils.PathJoin(conf.Workdir, runtime.PkiName)
	composePath := utils.PathJoin(conf.Workdir, runtime.ComposeName)
	auditLogPath := ""
	auditPolicyPath := ""
	if conf.AuditPolicy != "" {
		auditLogPath = utils.PathJoin(conf.Workdir, "logs", runtime.AuditLogName)
		err = utils.CreateFile(auditLogPath, 0644)
		if err != nil {
			return err
		}

		auditPolicyPath = utils.PathJoin(conf.Workdir, runtime.AuditPolicyName)
		err = utils.CopyFile(conf.AuditPolicy, auditPolicyPath)
		if err != nil {
			return err
		}
	}

	caCertPath := ""
	adminKeyPath := ""
	adminCertPath := ""
	inClusterKubeconfigPath := "/root/.kube/config"
	inClusterEtcdDataPath := "/etcd-data"
	InClusterPrometheusPath := "/etc/prometheus/prometheus.yml"
	inClusterAdminKeyPath := ""
	inClusterAdminCertPath := ""
	inClusterCACertPath := ""
	inClusterPort := uint32(8080)
	scheme := "http"

	// generate ca cert
	if conf.SecretPort {
		err := pki.GeneratePki(pkiPath)
		if err != nil {
			return fmt.Errorf("failed to generate pki: %w", err)
		}
		caCertPath = utils.PathJoin(pkiPath, "ca.crt")
		adminKeyPath = utils.PathJoin(pkiPath, "admin.key")
		adminCertPath = utils.PathJoin(pkiPath, "admin.crt")
		inClusterPkiPath := "/etc/kubernetes/pki/"
		inClusterCACertPath = utils.PathJoin(inClusterPkiPath, "ca.crt")
		inClusterAdminKeyPath = utils.PathJoin(inClusterPkiPath, "admin.key")
		inClusterAdminCertPath = utils.PathJoin(inClusterPkiPath, "admin.crt")
		inClusterPort = 6443
		scheme = "https"
	}

	// Setup prometheus
	if conf.PrometheusPort != 0 {
		prometheusPath = utils.PathJoin(conf.Workdir, runtime.Prometheus)
		prometheusData, err := BuildPrometheus(BuildPrometheusConfig{
			ProjectName:  conf.Name,
			SecretPort:   conf.SecretPort,
			AdminCrtPath: inClusterAdminCertPath,
			AdminKeyPath: inClusterAdminKeyPath,
		})
		if err != nil {
			return fmt.Errorf("failed to generate prometheus yaml: %w", err)
		}
		err = os.WriteFile(prometheusPath, []byte(prometheusData), 0644)
		if err != nil {
			return fmt.Errorf("failed to write prometheus yaml: %w", err)
		}
	}

	kubeApiserverPort := conf.KubeApiserverPort
	if kubeApiserverPort == 0 {
		kubeApiserverPort, err = utils.GetUnusedPort(ctx)
		if err != nil {
			return err
		}
	}

	// Setup compose
	compose, err := BuildCompose(BuildComposeConfig{
		ProjectName:                  conf.Name,
		KubeApiserverPort:            kubeApiserverPort,
		KubeconfigPath:               inClusterOnHostKubeconfigPath,
		AdminCertPath:                adminCertPath,
		AdminKeyPath:                 adminKeyPath,
		CACertPath:                   caCertPath,
		InClusterKubeconfigPath:      inClusterKubeconfigPath,
		InClusterAdminCertPath:       inClusterAdminCertPath,
		InClusterAdminKeyPath:        inClusterAdminKeyPath,
		InClusterCACertPath:          inClusterCACertPath,
		InClusterEtcdDataPath:        inClusterEtcdDataPath,
		InClusterPrometheusPath:      InClusterPrometheusPath,
		PrometheusPath:               prometheusPath,
		EtcdImage:                    conf.EtcdImage,
		KubeApiserverImage:           conf.KubeApiserverImage,
		KubeControllerManagerImage:   conf.KubeControllerManagerImage,
		KubeSchedulerImage:           conf.KubeSchedulerImage,
		KwokControllerImage:          conf.KwokControllerImage,
		PrometheusImage:              conf.PrometheusImage,
		SecretPort:                   conf.SecretPort,
		Authorization:                conf.Authorization,
		QuietPull:                    conf.QuietPull,
		DisableKubeScheduler:         conf.DisableKubeScheduler,
		DisableKubeControllerManager: conf.DisableKubeControllerManager,
		PrometheusPort:               conf.PrometheusPort,
		RuntimeConfig:                conf.RuntimeConfig,
		FeatureGates:                 conf.FeatureGates,
		AuditPolicy:                  auditPolicyPath,
		AuditLog:                     auditLogPath,
	})
	if err != nil {
		return err
	}

	// Setup kubeconfig
	kubeconfigData, err := k8s.BuildKubeconfig(k8s.BuildKubeconfigConfig{
		ProjectName:  conf.Name,
		SecretPort:   conf.SecretPort,
		Address:      scheme + "://127.0.0.1:" + utils.StringUint32(kubeApiserverPort),
		AdminCrtPath: adminCertPath,
		AdminKeyPath: adminKeyPath,
	})
	if err != nil {
		return err
	}
	inClusterKubeconfigData, err := k8s.BuildKubeconfig(k8s.BuildKubeconfigConfig{
		ProjectName:  conf.Name,
		SecretPort:   conf.SecretPort,
		Address:      scheme + "://" + conf.Name + "-kube-apiserver:" + utils.StringUint32(inClusterPort),
		AdminCrtPath: inClusterAdminCertPath,
		AdminKeyPath: inClusterAdminKeyPath,
	})
	if err != nil {
		return err
	}

	// Save config
	err = os.WriteFile(kubeconfigPath, []byte(kubeconfigData), 0644)
	if err != nil {
		return err
	}

	err = os.WriteFile(inClusterOnHostKubeconfigPath, []byte(inClusterKubeconfigData), 0644)
	if err != nil {
		return err
	}

	err = os.WriteFile(composePath, []byte(compose), 0644)
	if err != nil {
		return err
	}

	// set the context in default kubeconfig
	_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "set", "clusters."+conf.Name+".server", scheme+"://127.0.0.1:"+utils.StringUint32(kubeApiserverPort))
	_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "set", "contexts."+conf.Name+".cluster", conf.Name)
	if conf.SecretPort {
		_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "set", "clusters."+conf.Name+".insecure-skip-tls-verify", "true")
		_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "set", "contexts."+conf.Name+".user", conf.Name)
		_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "set", "users."+conf.Name+".client-certificate", adminCertPath)
		_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "set", "users."+conf.Name+".client-key", adminKeyPath)
	}

	var out io.Writer = os.Stderr
	if conf.QuietPull {
		out = nil
	}
	images := []string{
		conf.EtcdImage,
		conf.KubeApiserverImage,
		conf.KubeControllerManagerImage,
		conf.KubeSchedulerImage,
		conf.KwokControllerImage,
	}
	if conf.PrometheusPort != 0 {
		images = append(images, conf.PrometheusImage)
	}
	logger := log.FromContext(ctx)
	for _, image := range images {
		err = utils.Exec(ctx, "", utils.IOStreams{}, conf.Runtime, "inspect",
			image,
		)
		if err != nil {
			logger.Info("Pull image", "image", image)
			err = utils.Exec(ctx, "", utils.IOStreams{
				Out:    out,
				ErrOut: out,
			}, conf.Runtime, "pull",
				image,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Cluster) Uninstall(ctx context.Context) error {
	conf, err := c.Config()
	if err != nil {
		return err
	}

	// unset the context in default kubeconfig
	_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "unset", "clusters."+conf.Name)
	_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "unset", "users."+conf.Name)
	_ = c.Kubectl(ctx, utils.IOStreams{}, "config", "unset", "contexts."+conf.Name)

	return c.Cluster.Uninstall(ctx)
}

func (c *Cluster) Up(ctx context.Context) error {
	conf, err := c.Config()
	if err != nil {
		return err
	}

	args := []string{"up", "-d"}
	if conf.QuietPull {
		args = append(args, "--quiet-pull")
	}

	commands, err := c.buildComposeCommands(ctx, args...)
	if err != nil {
		return err
	}

	err = utils.Exec(ctx, conf.Workdir, utils.IOStreams{
		ErrOut: os.Stderr,
		Out:    os.Stderr,
	}, commands[0], commands[1:]...)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cluster) Down(ctx context.Context) error {
	conf, err := c.Config()
	if err != nil {
		return err
	}

	logger := log.FromContext(ctx)
	args := []string{"down"}
	commands, err := c.buildComposeCommands(ctx, args...)
	if err != nil {
		return err
	}

	err = utils.Exec(ctx, conf.Workdir, utils.IOStreams{
		ErrOut: os.Stderr,
		Out:    os.Stderr,
	}, commands[0], commands[1:]...)
	if err != nil {
		logger.Error("Failed to down cluster", err)
	}
	return nil
}

func (c *Cluster) Start(ctx context.Context, name string) error {
	conf, err := c.Config()
	if err != nil {
		return err
	}
	return utils.Exec(ctx, conf.Workdir, utils.IOStreams{}, conf.Runtime, "start", conf.Name+"-"+name)
}

func (c *Cluster) Stop(ctx context.Context, name string) error {
	conf, err := c.Config()
	if err != nil {
		return err
	}
	return utils.Exec(ctx, conf.Workdir, utils.IOStreams{}, conf.Runtime, "stop", conf.Name+"-"+name)
}

func (c *Cluster) logs(ctx context.Context, name string, out io.Writer, follow bool) error {
	conf, err := c.Config()
	if err != nil {
		return err
	}
	args := []string{"logs"}
	if follow {
		args = append(args, "-f")
	}
	args = append(args, conf.Name+"-"+name)
	return utils.Exec(ctx, conf.Workdir, utils.IOStreams{
		ErrOut: out,
		Out:    out,
	}, conf.Runtime, args...)
}

func (c *Cluster) Logs(ctx context.Context, name string, out io.Writer) error {
	return c.logs(ctx, name, out, false)
}

func (c *Cluster) LogsFollow(ctx context.Context, name string, out io.Writer) error {
	return c.logs(ctx, name, out, true)
}

// ListBinaries list binaries in the cluster
func (c *Cluster) ListBinaries(ctx context.Context, actual bool) ([]string, error) {
	if !actual {
		return []string{
			vars.KubectlBinary,
		}, nil
	}
	_, err := c.Config()
	if err != nil {
		return nil, err
	}

	return []string{
		vars.KubectlBinary,
	}, nil
}

// ListImages list images in the cluster
func (c *Cluster) ListImages(ctx context.Context, actual bool) ([]string, error) {
	if !actual {
		return []string{
			vars.EtcdImage,
			vars.KubeApiserverImage,
			vars.KubeControllerManagerImage,
			vars.KubeSchedulerImage,
			vars.KwokControllerImage,
			vars.PrometheusImage,
		}, nil
	}
	conf, err := c.Config()
	if err != nil {
		return nil, err
	}
	return []string{
		conf.EtcdImage,
		conf.KubeApiserverImage,
		conf.KubeControllerManagerImage,
		conf.KubeSchedulerImage,
		conf.KwokControllerImage,
		conf.PrometheusImage,
	}, nil
}

// buildComposeCommands returns the compose commands with given current runtime and args
func (c *Cluster) buildComposeCommands(ctx context.Context, args ...string) ([]string, error) {
	conf, err := c.Config()
	if err != nil {
		return nil, err
	}
	runtime := conf.Runtime
	if runtime == "docker" {
		err := utils.Exec(ctx, "", utils.IOStreams{}, runtime, "compose", "version")
		if err != nil {
			// docker compose subcommand does not exist, try to download it
			dockerComposePath := utils.PathJoin(conf.Workdir, "bin", "docker-compose"+vars.BinSuffix)
			err = utils.DownloadWithCache(ctx, conf.CacheDir, conf.DockerComposeBinary, dockerComposePath, 0755, conf.QuietPull)
			if err != nil {
				return nil, err
			}
			return append([]string{dockerComposePath}, args...), nil
		}
	}
	return append([]string{runtime, "compose"}, args...), nil
}

// EtcdctlInCluster implements the ectdctl subcommand
func (c *Cluster) EtcdctlInCluster(ctx context.Context, stm utils.IOStreams, args ...string) error {
	conf, err := c.Config()
	if err != nil {
		return err
	}

	etcdContainerName := conf.Name + "-etcd"

	return utils.Exec(ctx, "", stm, conf.Runtime, append([]string{"exec", "-i", etcdContainerName, "etcdctl"}, args...)...)
}
