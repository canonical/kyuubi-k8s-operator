# Enable and configure GPU support for Charmed Apache Kyuubi

Charmed Apache Kyuubi supports the [RAPIDS Accelerator](https://docs.nvidia.com/spark-rapids/index.html) for Apache Spark on K8s.
This makes possible to run hardware-accelerated queries on NVIDIA GPUs.

## Prerequisites

- Charmed Apache Kyuubi, starting from revision <to-be-filled-once-released> and more.
- Kubernetes cluster is up and running with NVIDIA GPU support

Check that NVIDIA GPU support is properly enabled by searching for a `gpu-operator` deployment.
On a non-confined MicroK8s cluster, this is done by enabling the `gpu` [add-on](https://microk8s.io/docs/addon-gpu).
Once the deployment is successful, you should see a new `gpu-operator-resource` namespace with the following similar-looking pods:

```shell
kubectl get pods -n gpu-operator-resources

NAME                                                          READY   STATUS
gpu-feature-discovery-l5g5k                                   1/1     Running
gpu-operator-85776c76f-7jzp2                                  1/1     Running
gpu-operator-node-feature-discovery-gc-d8f9f89db-77rz9        1/1     Running 
gpu-operator-node-feature-discovery-master-79978f78cf-bslkt   1/1     Running 
gpu-operator-node-feature-discovery-worker-vvg6w              1/1     Running  
nvidia-container-toolkit-daemonset-r5lq2                      1/1     Running   
nvidia-cuda-validator-76t6r                                   0/1     Completed  
nvidia-dcgm-exporter-s92ln                                    1/1     Running    
nvidia-device-plugin-daemonset-92xnm                          1/1     Running    
nvidia-operator-validator-d7hhh                               1/1     Running
```

Finally, make sure that the cluster now list at least one `nvidia.com/gpu` GPU resource under one node's capacity:

```shell
kubectl get node <node-name> -o=jsonpath="{.status.capacity."nvidia.com/gpu"}"  
# 1
```

## Configuring hardware-accelerated spark jobs

Enable hardware-accelerated spark jobs with the following configuration option:

```shell
juju config <kyuubi-app> gpu-enable=true
```

Each executor pod will now use one full GPU resource.
Use the `gpu-engine-executors-limit` to set the number of executors a Kyuubi Engine will spawn.

```shell
juju config <kyuubi-app> gpu-engine-executors-limit=2
```

To get the most out of the hardware, it is needed to adapt the pod configuration to the workload:

```shell
juju config <kyuubi-app> gpu-pinned-memory=4
juju config <kyuubi-app> executor-memory=8
juju config <kyuubi-app> executor-cores=4
```

## Checking that GPU resources are used

If you have a shell access to the machine, you can use the system management interface CLI:

```shell
nvidia-smi
```

The output should look like the following

```text
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 570.169                Driver Version: 570.169        CUDA Version: 12.8     |
|-----------------------------------------+------------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id          Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |           Memory-Usage | GPU-Util  Compute M. |
|                                         |                        |               MIG M. |
|=========================================+========================+======================|
|   0  <first GPU unit>               Off |   00000000:01:00.0  On |                  N/A |
|       .......                           |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+

+-----------------------------------------------------------------------------------------+
| Processes:                                                                              |
|  GPU   GI   CI              PID   Type   Process name                        GPU Memory |
|        ID   ID                                                               Usage      |
|=========================================================================================|
|    0   N/A  N/A            2611      G   <path to spark process>                XXX MiB |
|                                          ...                                            |
+-----------------------------------------------------------------------------------------+
```

Under the `Processes` table, you should see a process for each currently active executor.
