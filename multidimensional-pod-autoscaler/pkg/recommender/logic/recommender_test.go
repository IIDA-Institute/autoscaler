/*
Copyright 2018 The Kubernetes Authors.

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

package logic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/recommender/model"
	vpa_model "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

func TestMinResourcesApplied(t *testing.T) {
	constEstimator := NewConstEstimator(vpa_model.Resources{
		vpa_model.ResourceCPU:    vpa_model.CPUAmountFromCores(0.001),
		vpa_model.ResourceMemory: vpa_model.MemoryAmountFromBytes(1e6),
	})
	recommender := podResourceRecommender{
		constEstimator,
		constEstimator,
		constEstimator}

	containerNameToAggregateStateMap := model.ContainerNameToAggregateStateMap{
		"container-1": &model.AggregateContainerState{},
	}

	recommendedResources := recommender.GetRecommendedPodResources(containerNameToAggregateStateMap)
	assert.Equal(t, vpa_model.CPUAmountFromCores(*podMinCPUMillicores/1000), recommendedResources["container-1"].Target[vpa_model.ResourceCPU])
	assert.Equal(t, vpa_model.MemoryAmountFromBytes(*podMinMemoryMb*1024*1024), recommendedResources["container-1"].Target[vpa_model.ResourceMemory])
}

func TestMinResourcesSplitAcrossContainers(t *testing.T) {
	constEstimator := NewConstEstimator(vpa_model.Resources{
		vpa_model.ResourceCPU:    vpa_model.CPUAmountFromCores(0.001),
		vpa_model.ResourceMemory: vpa_model.MemoryAmountFromBytes(1e6),
	})
	recommender := podResourceRecommender{
		constEstimator,
		constEstimator,
		constEstimator}

	containerNameToAggregateStateMap := model.ContainerNameToAggregateStateMap{
		"container-1": &model.AggregateContainerState{},
		"container-2": &model.AggregateContainerState{},
	}

	recommendedResources := recommender.GetRecommendedPodResources(containerNameToAggregateStateMap)
	assert.Equal(t, vpa_model.CPUAmountFromCores((*podMinCPUMillicores/1000)/2), recommendedResources["container-1"].Target[vpa_model.ResourceCPU])
	assert.Equal(t, vpa_model.CPUAmountFromCores((*podMinCPUMillicores/1000)/2), recommendedResources["container-2"].Target[vpa_model.ResourceCPU])
	assert.Equal(t, vpa_model.MemoryAmountFromBytes((*podMinMemoryMb*1024*1024)/2), recommendedResources["container-1"].Target[vpa_model.ResourceMemory])
	assert.Equal(t, vpa_model.MemoryAmountFromBytes((*podMinMemoryMb*1024*1024)/2), recommendedResources["container-2"].Target[vpa_model.ResourceMemory])
}

func TestControlledResourcesFiltered(t *testing.T) {
	constEstimator := NewConstEstimator(vpa_model.Resources{
		vpa_model.ResourceCPU:    vpa_model.CPUAmountFromCores(0.001),
		vpa_model.ResourceMemory: vpa_model.MemoryAmountFromBytes(1e6),
	})
	recommender := podResourceRecommender{
		constEstimator,
		constEstimator,
		constEstimator}

	containerName := "container-1"
	containerNameToAggregateStateMap := model.ContainerNameToAggregateStateMap{
		containerName: &model.AggregateContainerState{
			ControlledResources: &[]vpa_model.ResourceName{vpa_model.ResourceMemory},
		},
	}

	recommendedResources := recommender.GetRecommendedPodResources(containerNameToAggregateStateMap)
	assert.Contains(t, recommendedResources[containerName].Target, vpa_model.ResourceMemory)
	assert.Contains(t, recommendedResources[containerName].LowerBound, vpa_model.ResourceMemory)
	assert.Contains(t, recommendedResources[containerName].UpperBound, vpa_model.ResourceMemory)
	assert.NotContains(t, recommendedResources[containerName].Target, vpa_model.ResourceCPU)
	assert.NotContains(t, recommendedResources[containerName].LowerBound, vpa_model.ResourceCPU)
	assert.NotContains(t, recommendedResources[containerName].UpperBound, vpa_model.ResourceCPU)
}

func TestControlledResourcesFilteredDefault(t *testing.T) {
	constEstimator := NewConstEstimator(vpa_model.Resources{
		vpa_model.ResourceCPU:    vpa_model.CPUAmountFromCores(0.001),
		vpa_model.ResourceMemory: vpa_model.MemoryAmountFromBytes(1e6),
	})
	recommender := podResourceRecommender{
		constEstimator,
		constEstimator,
		constEstimator}

	containerName := "container-1"
	containerNameToAggregateStateMap := model.ContainerNameToAggregateStateMap{
		containerName: &model.AggregateContainerState{
			ControlledResources: &[]vpa_model.ResourceName{vpa_model.ResourceMemory, vpa_model.ResourceCPU},
		},
	}

	recommendedResources := recommender.GetRecommendedPodResources(containerNameToAggregateStateMap)
	assert.Contains(t, recommendedResources[containerName].Target, vpa_model.ResourceMemory)
	assert.Contains(t, recommendedResources[containerName].LowerBound, vpa_model.ResourceMemory)
	assert.Contains(t, recommendedResources[containerName].UpperBound, vpa_model.ResourceMemory)
	assert.Contains(t, recommendedResources[containerName].Target, vpa_model.ResourceCPU)
	assert.Contains(t, recommendedResources[containerName].LowerBound, vpa_model.ResourceCPU)
	assert.Contains(t, recommendedResources[containerName].UpperBound, vpa_model.ResourceCPU)
}
