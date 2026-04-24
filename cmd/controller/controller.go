package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Seagate/seagate-exos-x-csi/pkg/common"
	"github.com/Seagate/seagate-exos-x-csi/pkg/controller"
	"k8s.io/klog/v2"
)

var bind = flag.String("bind", fmt.Sprintf("unix:///var/run/%s/csi-controller.sock", common.PluginName), "RPC bind URI (can be a UNIX socket path or any URI)")
var backendConfigFile = flag.String("backend-config-file", "", "Path to a JSON file containing backend controller credentials")

func main() {
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Parse()
	if *backendConfigFile != "" {
		if err := os.Setenv(common.ControllerBackendConfigFileEnvVar, *backendConfigFile); err != nil {
			klog.Fatal(err)
		}
	}
	klog.Infof("starting storage controller (%s)", common.Version)
	c := controller.New()
	defer c.Stop()
	c.Start(*bind)
}
