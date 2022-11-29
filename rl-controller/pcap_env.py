import os
import pandas as pd
import random
from util import *
from kubernetes import client, config
from kubernetes.client.rest import ApiException


MIN_INSTANCES = 1
MAX_INSTANCES = 50
MIN_CPU_LIMIT = 128   # millicore
MAX_CPU_LIMIT = 1024  # millicore
MIN_MEMORY_LIMIT = 256   # MB
MAX_MEMORY_LIMIT = 2048  # MB

MPA_DOMAIN = 'autoscaling.k8s.io'
MPA_VERSION = 'v1alpha1'
MPA_PLURAL = 'multidimpodautoscalers'


class PCAPEnvironment:
    app_name = 'pcap_controller'
    app_namespace = 'default'
    mpa_name = 'hamster-mpa'
    mpa_namespace = 'default'

    # initial resource allocations
    initial_pcap_controllers = 1
    initial_tek_controllers = 4
    initial_cpu_limit = 128
    initial_memory_limit = 256

    # states
    states = {
        # resource allocation
        'num_pcap_schedulers': 1,
        'num_pcap_controllers': 1,
        'num_tek_controllers': 4,
        'cpu_limit': 1024,
        'memory_limit': 512,
        # system-wise metrics
        'cpu_util': 0.0,
        'memory_usage': 0.0,
        'disk_io_rate': 0.0,
        'ingress_rate': 0.0,
        'egress_rate': 0.0,
        # application-wise metrics
        'pcap_file_discovery_rate': 0.0,
        'pcap_rate': 0.0,  # 1 / lag
        'pcap_processing_rate': 0.0,
        'pcap_ingestion_rate': 0.0,
        'tek_rate': 0.0,  # 1 / lag
        'tek_processing_rate': 0.0,
        'tek_ingestion_rate': 0.0
    }

    # action and reward at the previous time step
    last_action = {
        'vertical_cpu': 0,
        'vertical_memory': 0,
        'horizontal': 0
    }
    last_reward = 0

    def __init__(self, app_name, app_namespace):
        if app_name not in ['pcap_controller', 'tek_controller', 'hamster']:
            print('Application not recognized!')
        self.app_name = app_name
        self.app_namespace = app_namespace

        # Load cluster config
        if 'KUBERNETES_PORT' in os.environ:
            config.load_incluster_config()
        else:
            config.load_kube_config()

        # Get the api instance to interact with the cluster
        api_client = client.api_client.ApiClient()
        self.api_instance = client.AppsV1Api(api_client)
        self.corev1 = client.CoreV1Api(api_client)

        # current resource limit
        self.cpu_limit = self.initial_cpu_limit
        self.memory_limit = self.initial_memory_limit
        self.num_replicas = 2
        if self.app_name == 'pcap_controller':
            self.num_replicas = self.initial_pcap_controllers
        elif self.app_name == 'tek_controller':
            self.num_replicas = self.initial_tek_controllers

        self.reset()
        exit()

    # observe the current states
    def observe_states(self):
        controlled_resources = ['cpu', 'memory']
        target_containers = self.get_target_containers()
        container_queries = []
        for container in target_containers:
            container_query = "container='" + container + "'"
            container_queries.append(container_query)

            prom_client.update_period(FORECASTING_SIGHT_SEC)
            for resource in controlled_resources:
                if resource.lower() == "cpu":
                    resource_query = "rate(container_cpu_usage_seconds_total{%s}[1m])"
                elif resource.lower() == "memory":
                    resource_query = "container_memory_usage_bytes{%s}"
                elif resource.lower() == "blkio":
                    resource_query = "container_fs_usage_bytes{%s}"
                elif resource.lower() == "ingress":
                    resource_query = "rate(container_network_receive_bytes_total{%s}[1m])"
                elif resource.lower() == "egress":
                    resource_query = "rate(container_network_transmit_bytes_total{%s}[1m])"

                # retrieve the metrics for target containers in all pods
                for container_query in container_queries:
                    query_index = namespace_query + "," + container_query
                    query = resource_query % (query_index)
                    print(query)

                    # retrieve the metrics for the target container from Prometheus
                    traces = prom_client.get_promdata(query, traces, resource)
        # print('Collected Traces:', traces)
        print('Collected traces for', self.app_name)
        cpu_traces = traces[self.app_name]['cpu']
        memory_traces = traces[self.app_name]['memory']
        # blkio_traces = traces[self.app_name]['blkio']
        # ingress_traces = traces[self.app_name]['ingress']
        # egress_traces = traces[self.app_name]['egress']

        # compute the average utilizations
        if 'cpu' in controlled_resources:
            all_values = []
            for container in cpu_traces:
                cpu_utils = []
                for measurement in cpu_traces[container]:
                    cpu_utils.append(float(measurement[1]))
                print('Avg CPU Util ('+container+'):', np.mean(cpu_utils))
                all_values.append(np.mean(cpu_utils))
            self.states['cpu_util'] = np.mean(all_values)
        if 'memory' in controlled_resources:
            all_values = []
            for container in memory_traces:
                memory_usages = []
                for measurement in memory_traces[container]:
                    memory_usages.append(int(measurement[1]) / 1024 / 1024.0)
                print('Avg Memory Usage ('+container+'):', np.mean(memory_usages), 'MB')
                all_values.append(np.mean(memory_usages))
            self.states['memory_usage'] = np.mean(all_values)
        if 'blkio' in controlled_resources:
            all_values = []
            for container in blkio_traces:
                blkio_usages = []
                for measurement in blkio_traces[container]:
                    blkio_usages.append(int(measurement[1]) / 1024 / 1024.0)
                print('Avg Disk I/O Usage ('+container+'):', np.mean(blkio_usages), 'MB')
                all_values.append(np.mean(blkio_usages))
            self.states['disk_io_rate'] = np.mean(all_values)
        if 'ingress' in controlled_resources:
            all_values = []
            for container in ingress_traces:
                ingress = []
                for measurement in ingress_traces[container]:
                    ingress.append(int(measurement[1]) / 1024.0)
                print('Avg Ingress ('+container+'):', np.mean(ingress), 'KB/s')
                all_values.append(np.mean(ingress))
            self.states['ingress_rate'] = np.mean(all_values)
        if 'egress' in controlled_resources:
            all_values = []
            for container in egress_traces:
                egress = []
                for measurement in egress_traces[container]:
                    egress.append(int(measurement[1]) / 1024.0)
                print('Avg egress ('+container+'):', np.mean(egress), 'KB/s')
                all_values.append(np.mean(egress))
            self.states['egress_rate'] = np.mean(all_values)

        # get the custom metrics (PCAP-related)
        # TODO

    # return the current states
    def get_rl_states(self):
        return self.states.values()

    # reset the environment by re-initializing all states and do the overprovisioning
    def reset(self):
        # rescale the number of pcap controllers and tek controllers
        if self.app_name == 'pcap_controller':
            self.api_instance.patch_namespaced_deployment_scale(
                self.app_name,
                self.app_namespace,
                {'spec': {'replicas': self.initial_pcap_controllers}}
            )
            self.num_replicas = self.initial_pcap_controllers
        elif self.app_name == 'tek_controller':
            self.api_instance.patch_namespaced_deployment_scale(
                self.app_name,
                self.app_namespace,
                {'spec': {'replicas': self.initial_tek_controllers}}
            )
            self.num_replicas = self.initial_tek_controllers

        # reset the cpu and memory limit
        self.cpu_limit = self.initial_cpu_limit
        self.memory_limit = self.initial_memory_limit
        self.set_vertical_scaling_recommendation(self.cpu_limit, self.memory_limit)

        # get the current state
        self.observe_states()
        states = self.get_rl_states()

        return states

    # action sanity check
    def sanity_check(self, action):
        if self.app_name == 'pcap_controller':
            if self.states['num_pcap_controllers'] + action['horizontal'] < MIN_INSTANCES:
                return False
            if self.states['num_pcap_controllers'] + action['horizontal'] > MAX_INSTANCES:
                return False
        elif self.app_name == 'tek_controller':
            if self.states['num_tek_controllers'] + action['horizontal'] < MIN_INSTANCES:
                return False
            if self.states['num_tek_controllers'] + action['horizontal'] > MAX_INSTANCES:
                return False
        else:
            cpu_limit_to_set = self.cpu_limit + action['vertical_cpu']
            if cpu_limit_to_set > MAX_CPU_LIMIT or cpu_limit_to_set < MIN_INSTANCES:
                return False
            memory_limit_to_set = self.memory_limit + action['vertical_memory']
            if memory_limit_to_set > MAX_MEMORY_LIMIT or memory_limit_to_set < MIN_MEMORY_LIMIT:
                return False
        return True

    # get all target container names
    def get_target_containers(self):
        target_pods = self.corev1.list_namespaced_pod(namespace=self.app_namespace, label_selector="app=" + self.app_name)

        target_containers = []
        for pod in target_pods.items:
            for container in pod.spec.containers:
                if container.name not in target_containers:
                    target_containers.append(container.name)

        return target_containers

    # set the vertical scaling recommendation to MPA
    def set_vertical_scaling_recommendation(self, cpu_limit, memory_limit):
        # update the recommendations
        container_recommendation = {"containerName": "", "lowerBound": {}, "target": {}, "uncappedTarget": {}, "upperBound": {}}
        container_recommendation["lowerBound"]['cpu'] = str(cpu_limit) + 'm'
        container_recommendation["target"]['cpu'] = str(cpu_limit) + 'm'
        container_recommendation["uncappedTarget"]['cpu'] = str(cpu_limit) + 'm'
        container_recommendation["upperBound"]['cpu'] = str(cpu_limit) + 'm'
        container_recommendation["lowerBound"]['memory'] = str(memory_limit) + 'Mi'
        container_recommendation["target"]['memory'] = str(memory_limit) + 'Mi'
        container_recommendation["uncappedTarget"]['memory'] = str(memory_limit) + 'Mi'
        container_recommendation["upperBound"]['memory'] = str(memory_limit) + 'Mi'

        recommendations = []
        containers = self.get_target_containers()
        for container in containers:
            vertical_scaling_recommendation = container_recommendation.copy()
            vertical_scaling_recommendation['containerName'] = container
            recommendations.append(vertical_scaling_recommendation)

        patched_mpa = {"recommendation": {"containerRecommendations": recommendations}, "currentReplicas": self.num_replicas, "desiredReplicas": self.num_replicas}
        body = {"status": patched_mpa}
        mpa_api = client.CustomObjectsApi()

        # Update the MPA object
        # API call doc: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CustomObjectsApi.md#patch_namespaced_custom_object
        try:
            mpa_updated = mpa_api.patch_namespaced_custom_object(group=MPA_DOMAIN, version=MPA_VERSION, plural=MPA_PLURAL, namespace=self.mpa_namespace, name=self.mpa_name, body=body)
            print("Successfully patched MPA object with the recommendation: %s" % mpa_updated['status']['recommendation']['containerRecommendations'])
        except ApiException as e:
            print("Exception when calling CustomObjectsApi->patch_namespaced_custom_object: %s\n" % e)

    # execute the action after sanity check
    def execute_action(self, action):
        if action['vertical_cpu'] != 0:
            # vertical scaling of cpu limit
            cpu_limit = self.cpu_limit + action['vertical_cpu']
            self.set_vertical_scaling_recommendation(cpu_limit, self.memory_limit)
        elif action['vertical_memory'] != 0:
            # vertical scaling of memory limit
            memory_limit = self.memory_limit + action['vertical_memory']
            self.set_vertical_scaling_recommendation(self.cpu_limit, memory_limit)
        elif action['horizontal'] != 0:
            # scaling in/out
            num_replicas = self.num_replicas + action['horizontal']
            self.api_instance.patch_namespaced_deployment_scale(
                self.app_name,
                self.app_namespace,
                {'spec': {'replicas': num_replicas}}
            )
            print('Scaled to', num_replicas, ' replicas')
            self.num_replicas = num_replicas
        else:
            # no action to perform
            print('No action')
            pass

    # RL step function to update the environment given the input actions
    # action: +/- cpu limit; +/- memory limit; +/- number of replicas
    # return: state, reward
    def step(self, action):
        curr_state = self.get_rl_states(self.num_containers, self.arrival_rate)

        # action correctness check:
        if not self.sanity_check(action):
            self.last_reward = -1
            return curr_state, ILLEGAL_PENALTY

        # execute the action on the cluster
        self.execute_action(action)

        # observe states
        self.observe_states()

        state = self.get_rl_states()

        # calculate the reward
        reward = convert_state_action_to_reward(state, action, self.last_action)

        self.last_reward = reward
        self.last_action = action

        return state, reward

    # print state information
    def print_info(self):
        print('Application name:', self.app_name)
        print('Average CPU Util:', self.current_state[0])
        print('SLO Preservation:', self.current_state[1])
        print('Total CPU Shares (normalized):', self.current_state[2])
        print('Total CPU Shares for Other Containers (normalized):', self.current_state[3])
        print('Number of Containers:', self.current_state[4] * 20)


if __name__ == '__main__':
    env = PCAPEnvironment('hamster', 'default')
    env.print_info()
