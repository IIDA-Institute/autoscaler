package model

import (
	"time"

	autoscaling "k8s.io/api/autoscaling/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	mpa_types "k8s.io/autoscaler/multidimensional-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1alpha1"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

// Map from MPA annotation key to value.
type mpaAnnotationsMap map[string]string

// Map from MPA condition type to condition.
type mpaConditionsMap map[mpa_types.MultidimPodAutoscalerConditionType]mpa_types.MultidimPodAutoscalerCondition

func (conditionsMap *mpaConditionsMap) Set(
	conditionType mpa_types.MultidimPodAutoscalerConditionType,
	status bool, reason string, message string) *mpaConditionsMap {
	oldCondition, alreadyPresent := (*conditionsMap)[conditionType]
	condition := mpa_types.MultidimPodAutoscalerCondition{
		Type:    conditionType,
		Reason:  reason,
		Message: message,
	}
	if status {
		condition.Status = apiv1.ConditionTrue
	} else {
		condition.Status = apiv1.ConditionFalse
	}
	if alreadyPresent && oldCondition.Status == condition.Status {
		condition.LastTransitionTime = oldCondition.LastTransitionTime
	} else {
		condition.LastTransitionTime = metav1.Now()
	}
	(*conditionsMap)[conditionType] = condition
	return conditionsMap
}

// Mpa (Multidimensional Pod Autoscaler) object is responsible for horizontal and vertical scaling
// of Pods matching a given label selector.
// TODO: add HPA-related fields
type Mpa struct {
	ID MpaID
	// Labels selector that determines which Pods are controlled by this MPA
	// object. Can be nil, in which case no Pod is matched.
	PodSelector labels.Selector
	// Map of the object annotations (key-value pairs).
	Annotations mpaAnnotationsMap
	// Map of the status conditions (keys are condition types).
	Conditions mpaConditionsMap
	// Most recently computed recommendation. Can be nil.
	Recommendation *vpa_types.RecommendedPodResources
	// All container aggregations that contribute to this MPA.
	// TODO: Garbage collect old AggregateContainerStates.
	aggregateContainerStates aggregateContainerStatesMap
	// Pod Resource Policy provided in the MPA API object. Can be nil.
	ResourcePolicy *vpa_types.PodResourcePolicy
	// Initial checkpoints of AggregateContainerStates for containers.
	// The key is container name.
	ContainersInitialAggregateState ContainerNameToAggregateStateMap
	// UpdateMode describes how recommendations will be applied to pods
	UpdateMode *vpa_types.UpdateMode
	// Created denotes timestamp of the original MPA object creation
	Created time.Time
	// CheckpointWritten indicates when last checkpoint for the MPA object was stored.
	CheckpointWritten time.Time
	// IsV1Beta1API is set to true if MPA object has labelSelector defined as in v1beta1 api.
	IsV1Beta1API bool
	// ScaleTargetRef points to the controller managing the set of pods.
	ScaleTargetRef *autoscaling.CrossVersionObjectReference
	// PodCount contains number of live Pods matching a given MPA object.
	PodCount int
}

// NewMpa returns a new Mpa with a given ID and pod selector. Doesn't set the
// links to the matched aggregations.
func NewMpa(id MpaID, selector labels.Selector, created time.Time) *Mpa {
	mpa := &Mpa{
		ID:                              id,
		PodSelector:                     selector,
		aggregateContainerStates:        make(aggregateContainerStatesMap),
		ContainersInitialAggregateState: make(ContainerNameToAggregateStateMap),
		Created:                         created,
		Annotations:                     make(mpaAnnotationsMap),
		Conditions:                      make(mpaConditionsMap),
		IsV1Beta1API:                    false,
		PodCount:                        0,
	}
	return mpa
}

// UseAggregationIfMatching checks if the given aggregation matches (contributes to) this MPA
// and adds it to the set of MPA's aggregations if that is the case.
func (mpa *Mpa) UseAggregationIfMatching(aggregationKey AggregateStateKey, aggregation *AggregateContainerState) {
	if mpa.UsesAggregation(aggregationKey) {
		// Already linked, we can return quickly.
		return
	}
	if mpa.matchesAggregation(aggregationKey) {
		mpa.aggregateContainerStates[aggregationKey] = aggregation
		aggregation.IsUnderVPA = true
		aggregation.UpdateMode = mpa.UpdateMode
		aggregation.UpdateFromPolicy(vpa_api_util.GetContainerResourcePolicy(aggregationKey.ContainerName(), mpa.ResourcePolicy))
	}
}

// UsesAggregation returns true iff an aggregation with the given key contributes to the MPA.
func (mpa *Mpa) UsesAggregation(aggregationKey AggregateStateKey) bool {
	_, exists := mpa.aggregateContainerStates[aggregationKey]
	return exists
}

// matchesAggregation returns true iff the MPA matches the given aggregation key.
func (mpa *Mpa) matchesAggregation(aggregationKey AggregateStateKey) bool {
	if mpa.ID.Namespace != aggregationKey.Namespace() {
		return false
	}
	return mpa.PodSelector != nil && mpa.PodSelector.Matches(aggregationKey.Labels())
}

// SetResourcePolicy updates the resource policy of the MPA and the scaling
// policies of aggregators under this MPA.
func (mpa *Mpa) SetResourcePolicy(resourcePolicy *vpa_types.PodResourcePolicy) {
	if resourcePolicy == mpa.ResourcePolicy {
		return
	}
	mpa.ResourcePolicy = resourcePolicy
	for container, state := range mpa.aggregateContainerStates {
		state.UpdateFromPolicy(vpa_api_util.GetContainerResourcePolicy(container.ContainerName(), mpa.ResourcePolicy))
	}
}

// SetUpdateMode updates the update mode of the MPA and aggregators under this MPA.
func (mpa *Mpa) SetUpdateMode(updatePolicy *mpa_types.PodUpdatePolicy) {
	if updatePolicy == nil {
		mpa.UpdateMode = nil
	} else {
		if updatePolicy.UpdateMode == mpa.UpdateMode {
			return
		}
		mpa.UpdateMode = updatePolicy.UpdateMode
	}
	for _, state := range mpa.aggregateContainerStates {
		state.UpdateMode = mpa.UpdateMode
	}
}