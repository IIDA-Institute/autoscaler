# MPA Recommender

- [Intro](#intro)
- [Running](#running)
- [Implementation](#implementation)

## Intro

Recommender is the core binary of Multi-dimensiional Pod Autoscaler (MPA) system.
It consists of both vertical and horizontal scaling of resources:
- Vertical: It computes the recommended resource requests for pods based on historical and current usage of the resources. Like VPA, the current recommendations are put in status of the MPA object, where they can be inspected.
- Horizontal: To be released.
- Combined: To be released.

## Running

### Vertical Autoscaling
* In order to have historical data pulled in by the recommender, install Prometheus in your cluster and pass its address through a flag.
* Create RBAC configuration from `../deploy/vpa-rbac.yaml`.
* Create a deployment with the recommender pod from `../deploy/recommender-deployment.yaml`.
* The recommender will start running and pushing its recommendations to MPA object statuses.

## Implementation

The recommender is based on a model of the cluster that it builds in its memory.
The model contains Kubernetes resources: *Pods*, *MultidimPodAutoscalers*, with their configuration (e.g. labels) as well as other information, e.g., usage data for each container.

After starting the binary, the recommender reads the history of running pods and their usage from Prometheus into the model.
It then runs in a loop and at each step performs the following actions:

* update model with recent information on resources (using listers based on watch),
* update model with fresh usage samples from Metrics API,
* compute new recommendation for each MPA,
* put any changed recommendations into the MPA objects.
