/*
Copyright 2022 Haoran Qiu.

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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	mpa_types "k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1alpha1"
	mpa_clientset "k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/client/clientset/versioned"
	mpa_api "k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/client/clientset/versioned/typed/autoscaling.k8s.io/v1alpha1"
	mpa_lister "k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/client/listers/autoscaling.k8s.io/v1alpha1"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_api "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/typed/autoscaling.k8s.io/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type patchRecord struct {
	Op    string      `json:"op,inline"`
	Path  string      `json:"path,inline"`
	Value interface{} `json:"value"`
}

func patchMpa(mpaClient mpa_api.MultidimPodAutoscalerInterface, mpaName string, patches []patchRecord) (result *mpa_types.MultidimPodAutoscaler, err error) {
	bytes, err := json.Marshal(patches)
	if err != nil {
		klog.Errorf("Cannot marshal MPA status patches %+v. Reason: %+v", patches, err)
		return
	}

	return mpaClient.Patch(context.TODO(), mpaName, types.JSONPatchType, bytes, meta.PatchOptions{})
}

// NewMpasLister returns MultidimPodAutoscalerLister configured to fetch all MPA objects from
// namespace, set namespace to k8sapiv1.NamespaceAll to select all namespaces.
// The method blocks until mpaLister is initially populated.
func NewMpasLister(mpaClient *mpa_clientset.Clientset, stopChannel <-chan struct{}, namespace string) mpa_lister.MultidimPodAutoscalerLister {
	mpaListWatch := cache.NewListWatchFromClient(mpaClient.AutoscalingV1alpha1().RESTClient(), "multidimpodautoscalers", namespace, fields.Everything())
	indexer, controller := cache.NewIndexerInformer(mpaListWatch,
		&mpa_types.MultidimPodAutoscaler{},
		1*time.Hour,
		&cache.ResourceEventHandlerFuncs{},
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	mpaLister := mpa_lister.NewMultidimPodAutoscalerLister(indexer)
	go controller.Run(stopChannel)
	if !cache.WaitForCacheSync(make(chan struct{}), controller.HasSynced) {
		klog.Fatalf("Failed to sync MPA cache during initialization")
	} else {
		klog.Info("Initial MPA synced successfully")
	}
	return mpaLister
}

// CreateOrUpdateMpaCheckpoint updates the status field of the MPA Checkpoint API object.
// If object doesn't exits it is created.
func CreateOrUpdateMpaCheckpoint(mpaCheckpointClient vpa_api.VerticalPodAutoscalerCheckpointInterface,
	mpaCheckpoint *vpa_types.VerticalPodAutoscalerCheckpoint) error {
	patches := make([]patchRecord, 0)
	patches = append(patches, patchRecord{
		Op:    "replace",
		Path:  "/status",
		Value: mpaCheckpoint.Status,
	})
	bytes, err := json.Marshal(patches)
	if err != nil {
		return fmt.Errorf("Cannot marshal MPA checkpoint status patches %+v. Reason: %+v", patches, err)
	}
	_, err = mpaCheckpointClient.Patch(context.TODO(), mpaCheckpoint.ObjectMeta.Name, types.JSONPatchType, bytes, meta.PatchOptions{})
	if err != nil && strings.Contains(err.Error(), fmt.Sprintf("\"%s\" not found", mpaCheckpoint.ObjectMeta.Name)) {
		_, err = mpaCheckpointClient.Create(context.TODO(), mpaCheckpoint, meta.CreateOptions{})
	}
	if err != nil {
		return fmt.Errorf("Cannot save checkpoint for mpa %v container %v. Reason: %+v", mpaCheckpoint.ObjectMeta.Name, mpaCheckpoint.Spec.ContainerName, err)
	}
	return nil
}

// UpdateMpaStatusIfNeeded updates the status field of the MPA API object.
func UpdateMpaStatusIfNeeded(mpaClient mpa_api.MultidimPodAutoscalerInterface, mpaName string, newStatus,
	oldStatus *mpa_types.MultidimPodAutoscalerStatus) (result *mpa_types.MultidimPodAutoscaler, err error) {
	patches := []patchRecord{{
		Op:    "add",
		Path:  "/status",
		Value: *newStatus,
	}}

	if !apiequality.Semantic.DeepEqual(*oldStatus, *newStatus) {
		return patchMpa(mpaClient, mpaName, patches)
	}
	return nil, nil
}
