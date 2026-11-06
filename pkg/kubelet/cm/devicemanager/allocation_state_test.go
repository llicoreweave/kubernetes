/*
Copyright 2026 The Kubernetes Authors.

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

package devicemanager

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/checkpoint"
)

const timingResource = "nvidia.com/gpu"

type timingOutcome struct {
	panicValue any
	stack      string
	err        error
}

func timingPod(name string, gpu bool) *v1.Pod {
	limits := v1.ResourceList{}
	if gpu {
		limits[v1.ResourceName(timingResource)] = resource.MustParse("1")
	}
	p := makePod(limits)
	p.Name = name
	p.UID = types.UID(name)
	p.Spec.Containers[0].Name = "gpu"
	return p
}

func timingManager(t *testing.T, pods []*v1.Pod, fn func([]string) (*pluginapi.AllocateResponse, error)) *ManagerImpl {
	t.Helper()
	res := TestResource{resourceName: timingResource, resourceQuantity: resource.MustParse("2"), devs: checkpoint.DevicesPerNUMA{0: []string{"GPU-A", "GPU-B"}}}
	w, err := getTestManager(t.TempDir(), func() []*v1.Pod { return pods }, []TestResource{res})
	if err != nil {
		t.Fatal(err)
	}
	w.endpoints[timingResource] = endpointInfo{e: &MockEndpoint{allocateFunc: fn}, opts: &pluginapi.DevicePluginOptions{}}
	return w.ManagerImpl
}

func timingResponse(devs []string) (*pluginapi.AllocateResponse, error) {
	if len(devs) != 1 {
		return nil, fmt.Errorf("expected one GPU, got %v", devs)
	}
	return &pluginapi.AllocateResponse{ContainerResponses: []*pluginapi.ContainerAllocateResponse{{Devices: []*pluginapi.DeviceSpec{{HostPath: "/dev/" + devs[0], ContainerPath: "/dev/gpu", Permissions: "rwm"}}}}}, nil
}

func timingRun(m *ManagerImpl, p *v1.Pod) (out timingOutcome) {
	defer func() {
		if r := recover(); r != nil {
			out.panicValue = r
			out.stack = string(debug.Stack())
		}
	}()
	_, out.err = m.GetDeviceRunContainerOptions(context.Background(), p, &p.Spec.Containers[0])
	return
}

func TestLateAllocationTimingSerialDelayDoesNotPanic(t *testing.T) {
	a, b := timingPod("pod-a", true), timingPod("pod-b", true)
	m := timingManager(t, []*v1.Pod{a, b}, func(devs []string) (*pluginapi.AllocateResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return timingResponse(devs)
	})
	for _, p := range []*v1.Pod{a, b} {
		o := timingRun(m, p)
		if o.err != nil || o.panicValue != nil {
			t.Fatalf("%s: err=%v panic=%v\n%s", p.Name, o.err, o.panicValue, o.stack)
		}
		if m.podDevices.containerDevices(string(p.UID), "gpu", timingResource).Len() != 1 {
			t.Fatalf("missing real allocation for %s", p.Name)
		}
	}
	t.Log("Both real late-allocation calls passed with delayed plugin responses and serial Pod execution")
}

func TestUnrelatedAllocationCompletesWhileRPCBlocked(t *testing.T) {
	for _, cpuOnlyB := range []bool{false, true} {
		t.Run(fmt.Sprintf("other_pod_cpu_only=%t", cpuOnlyB), func(t *testing.T) {
			a, b := timingPod("pod-a", true), timingPod("pod-b", !cpuOnlyB)
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			m := timingManager(t, []*v1.Pod{a, b}, func(devs []string) (*pluginapi.AllocateResponse, error) {
				if calls.Add(1) == 1 {
					close(entered)
					<-release
				}
				return timingResponse(devs)
			})
			aDone, bDone := make(chan timingOutcome, 1), make(chan timingOutcome, 1)
			go func() { aDone <- timingRun(m, a) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("A did not reach the plugin RPC")
			}
			go func() {
				if !cpuOnlyB {
					bDone <- timingRun(m, b)
					return
				}
				var result timingOutcome
				defer func() {
					if value := recover(); value != nil {
						result.panicValue = value
						result.stack = string(debug.Stack())
					}
					bDone <- result
				}()
				result.err = m.Allocate(b, &b.Spec.Containers[0])
			}()
			var bResult timingOutcome
			completedBeforeRelease := false
			select {
			case bResult = <-bDone:
				completedBeforeRelease = true
			case <-time.After(time.Second):
			}
			close(release)
			receive := func(ch <-chan timingOutcome) timingOutcome {
				select {
				case r := <-ch:
					return r
				case <-time.After(5 * time.Second):
					t.Fatal("allocation did not complete")
					return timingOutcome{}
				}
			}
			aResult := receive(aDone)
			if !completedBeforeRelease {
				bResult = receive(bDone)
				t.Error("unrelated Pod B waited for Pod A's plugin RPC")
			}
			if aResult.err != nil || aResult.panicValue != nil {
				t.Errorf("A: err=%v panic=%v\n%s", aResult.err, aResult.panicValue, aResult.stack)
			}
			if bResult.err != nil || bResult.panicValue != nil {
				t.Errorf("B: err=%v panic=%v\n%s", bResult.err, bResult.panicValue, bResult.stack)
			}
			aDevices := m.podDevices.containerDevices(string(a.UID), "gpu", timingResource)
			if aDevices.Len() != 1 {
				t.Fatal("A's allocation was lost")
			}
			if !cpuOnlyB {
				bDevices := m.podDevices.containerDevices(string(b.UID), "gpu", timingResource)
				if bDevices.Len() != 1 || aDevices.Intersection(bDevices).Len() != 0 {
					t.Fatalf("incorrect ownership: A=%v B=%v", aDevices, bDevices)
				}
			}
			t.Logf("B completed before A's RPC was released; both succeeded; plugin calls=%d", calls.Load())
		})
	}
}

func TestManyDeviceRPCsRemainConcurrent(t *testing.T) {
	const n = 16
	pods := make([]*v1.Pod, n)
	ids := make([]string, n)
	for i := range pods {
		pods[i] = timingPod(fmt.Sprintf("pod-%d", i), true)
		ids[i] = fmt.Sprintf("device-%d", i)
	}
	m, err := getTestManager(t.TempDir(), func() []*v1.Pod { return pods }, []TestResource{{resourceName: timingResource, resourceQuantity: resource.MustParse("16"), devs: checkpoint.DevicesPerNUMA{0: ids}}})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, n), make(chan struct{})
	m.endpoints[timingResource] = endpointInfo{e: &MockEndpoint{allocateFunc: func(devs []string) (*pluginapi.AllocateResponse, error) {
		entered <- struct{}{}
		<-release
		return timingResponse(devs)
	}}, opts: &pluginapi.DevicePluginOptions{}}
	done := make(chan timingOutcome, n)
	for _, p := range pods {
		go func(p *v1.Pod) { done <- timingRun(m.ManagerImpl, p) }(p)
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	reached := 0
waiting:
	for reached < n {
		select {
		case <-entered:
			reached++
		case <-timer.C:
			break waiting
		}
	}
	close(release)
	for range pods {
		select {
		case r := <-done:
			if r.err != nil || r.panicValue != nil {
				t.Errorf("err=%v panic=%v\n%s", r.err, r.panicValue, r.stack)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("workers did not finish")
		}
	}
	if reached != n {
		t.Fatalf("only %d/%d RPCs ran concurrently", reached, n)
	}
	devices := map[string]bool{}
	for _, p := range pods {
		allocated := m.podDevices.containerDevices(string(p.UID), "gpu", timingResource)
		if allocated.Len() != 1 {
			t.Fatal("missing device")
		}
		for id := range allocated {
			if devices[id] {
				t.Fatal("duplicate allocation", id)
			}
			devices[id] = true
		}
	}
	t.Log("All 16 device RPCs entered before any was released; distinct ownership preserved")
}
