/*
Copyright 2022 The Volcano Authors.

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

package rescheduling

import (
	"os"
	"time"

	"github.com/mitchellh/mapstructure"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin
	PluginName = "rescheduling"
	// DefaultInterval indicates the default interval rescheduling plugin works for
	DefaultInterval = 5 * time.Minute
	// DefaultMetricsPeriod indicates the default metrics period rescheduling plugin works for
	DefaultMetricsPeriod = "5m"
	// DefaultStrategy indicates the default strategy rescheduling plugin making use of
	DefaultStrategy = "lowNodeUtilization"
)

var (
	// Session contains all the data in session object which will be used for all the rescheduling package
	Session *framework.Session

	// RegisteredStrategyConfigs collects all the strategy configurations registered.
	RegisteredStrategyConfigs map[string]interface{}

	// VictimFn contains all the VictimTasksFn for registered the strategies
	VictimFn map[string]api.VictimTasksFn

	// MetricsPeriod indicates the metrics period will be used during this plugin. 5 minutes by default.
	MetricsPeriod string
)

func init() {
	RegisteredStrategyConfigs = make(map[string]interface{})
	VictimFn = make(map[string]api.VictimTasksFn)
	MetricsPeriod = "5m"

	// register victim functions for all strategies here
	VictimFn["lowNodeUtilization"] = victimsFnForLnu
	VictimFn[GpuFragmentationStrategy] = victimsFnForGpuFragmentation
	VictimFn[CapacityUpgradeStrategy] = victimsFnForCapacityUpgrade
}

type reschedulingPlugin struct {
	// Arguments given for rescheduling plugin
	pluginArguments framework.Arguments
}

// New function returns rescheduling plugin object
func New(arguments framework.Arguments) framework.Plugin {
	return &reschedulingPlugin{
		pluginArguments: arguments,
	}
}

// Name returns the name of rescheduling plugin
func (rp *reschedulingPlugin) Name() string {
	return PluginName
}

func (rp *reschedulingPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter rescheduling plugin ...")
	defer klog.V(5).Infof("Leaving rescheduling plugin.")

	// Parse all the rescheduling strategies and execution interval
	Session = ssn
	configs := NewReschedulingConfigs()
	for _, tier := range ssn.Tiers {
		for _, pluginOption := range tier.Plugins {
			if pluginOption.Name == PluginName {
				configs.parseArguments(pluginOption.Arguments)
				break
			}
		}
	}

	// Some strategy hooks must run in every session, not just the
	// rescheduling ones, because they act on the sessions that follow an
	// eviction: the gpuFragmentation drained-node penalty (without it an
	// equally-utilized drained node ties with the intended destination
	// under binpack) and the capacityUpgrade hold/drain predicate, hold
	// preference and move maintenance (a move is a transaction that
	// progresses one session at a time). The session keeps one hook of
	// each kind per plugin, so they are composed here.
	nodeOrderFns := make([]api.NodeOrderFn, 0)
	everySessionVictimFns := make([]api.VictimTasksFn, 0)
	for _, strategy := range configs.strategies {
		switch strategy.Name {
		case GpuFragmentationStrategy:
			if os.Getenv(KillSwitchEnv) == "true" {
				continue
			}
			conf := newGpuFragmentationConf()
			if params, ok := RegisteredStrategyConfigs[GpuFragmentationStrategy].(map[string]interface{}); ok {
				conf.parse(params)
			}
			if !conf.DryRun {
				nodeOrderFns = append(nodeOrderFns, gpuFragmentationNodeOrderFn(conf))
			}
		case CapacityUpgradeStrategy:
			gpu := v1.ResourceName(loadCapacityUpgradeConf().GpuResource)
			idx := capacityUpgradeSessionIndex()
			ssn.AddPredicateFn(rp.Name(), capacityUpgradePredicateFn(idx, gpu))
			nodeOrderFns = append(nodeOrderFns, capacityUpgradeNodeOrderFn(idx, gpu))
			everySessionVictimFns = append(everySessionVictimFns, victimsFnForCapacityUpgradeMoves)
		}
	}
	if len(nodeOrderFns) > 0 {
		ssn.AddNodeOrderFn(rp.Name(), sumNodeOrderFns(nodeOrderFns))
	}

	victimFns := everySessionVictimFns
	if timeToRun(configs.interval) {
		for _, strategy := range configs.strategies {
			if VictimFn[strategy.Name] != nil {
				klog.V(4).Infof("strategy: %s\n", strategy.Name)
				victimFns = append(victimFns, VictimFn[strategy.Name])
			}
		}
	} else {
		klog.V(3).Infof("It is not the time to execute rescheduling strategies.")
	}
	if len(victimFns) > 0 {
		ssn.AddVictimTasksFns(rp.Name(), victimFns)
	}
}

// sumNodeOrderFns composes node-order functions by adding their scores; the
// first error wins.
func sumNodeOrderFns(fns []api.NodeOrderFn) api.NodeOrderFn {
	return func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		total := 0.0
		for _, fn := range fns {
			score, err := fn(task, node)
			if err != nil {
				return 0, err
			}
			total += score
		}
		return total, nil
	}
}

func (rp *reschedulingPlugin) OnSessionClose(ssn *framework.Session) {
	Session = nil
	sessionCapacityUpgrade = nil
	for k := range RegisteredStrategyConfigs {
		delete(RegisteredStrategyConfigs, k)
	}
}

// Configs is the struct for rescheduling plugin arguments
type Configs struct {
	interval   time.Duration
	strategies []Strategy
}

// Strategy is the struct for rescheduling strategy
type Strategy struct {
	Name   string                 `json:"name"`
	Params map[string]interface{} `json:"params"`
}

// NewReschedulingConfigs creates an object of rescheduling configurations with default configuration
func NewReschedulingConfigs() *Configs {
	config := &Configs{
		interval: DefaultInterval,
		strategies: []Strategy{
			{
				Name:   DefaultStrategy,
				Params: DefaultLowNodeConf,
			},
		},
	}
	RegisteredStrategyConfigs[DefaultStrategy] = DefaultLowNodeConf
	return config
}

// parseArguments parse all the rescheduling arguments
func (rc *Configs) parseArguments(arguments framework.Arguments) {
	var intervalStr string
	var err error
	if intervalArg, ok := arguments["interval"]; ok {
		intervalStr = intervalArg.(string)
	}
	rc.interval, err = time.ParseDuration(intervalStr)
	if err != nil {
		klog.V(4).Infof("Parse rescheduling interval failed. Reset the interval to 5m by default.")
		rc.interval = DefaultInterval
	}
	if metricsPeriodArg, ok := arguments["metricsPeriod"]; ok {
		MetricsPeriod = metricsPeriodArg.(string)
	}
	if MetricsPeriod == "" {
		MetricsPeriod = DefaultMetricsPeriod
	}
	strategies, ok := arguments["strategies"]
	if ok {
		strategyArray, _ := strategies.([]interface{})
		if len(strategyArray) != 0 {
			rc.strategies = rc.strategies[0:0]
		}
		for _, strategyInterface := range strategyArray {
			strategy := new(Strategy)
			err := mapstructure.Decode(strategyInterface, strategy)
			if err != nil {
				klog.V(3).Infof("Decode error: %s\n", err.Error())
			} else {
				rc.strategies = append(rc.strategies, *strategy)
			}
		}
		for k := range RegisteredStrategyConfigs {
			delete(RegisteredStrategyConfigs, k)
		}
		for _, strategy := range rc.strategies {
			RegisteredStrategyConfigs[strategy.Name] = strategy.Params
		}
		klog.V(3).Infof("RegisteredStrategyConfigs: %v\n", RegisteredStrategyConfigs)
	}
	for _, strategy := range rc.strategies {
		klog.V(3).Infof("strategy: %s, params: %v\n", strategy.Name, strategy.Params)
	}
}
