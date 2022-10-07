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
	"time"

	"k8s.io/apimachinery/pkg/fields"

	mpa_types "k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1alpha1"
	mpa_clientset "k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/client/clientset/versioned"
	mpa_lister "k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/client/listers/autoscaling.k8s.io/v1alpha1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// NewMpasLister returns MultidimPodAutoscalerLister configured to fetch all MPA objects from
// namespace, set namespace to k8sapiv1.NamespaceAll to select all namespaces.
// The method blocks until mpaLister is initially populated.
func NewMpasLister(mpaClient *mpa_clientset.Clientset, stopChannel <-chan struct{}, namespace string) mpa_lister.MultidimPodAutoscalerLister {
	mpaListWatch := cache.NewListWatchFromClient(mpaClient.AutoscalingV1alpha1().RESTClient(), "verticalpodautoscalers", namespace, fields.Everything())
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
