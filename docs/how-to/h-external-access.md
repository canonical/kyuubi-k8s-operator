# How to connect from outside of Kubernetes

To expose Charmed Apache Kyuubi K8s externally, this charm provides a configuration option `expose-external` (with options `false`, `nodeport` and `loadbalancer`) to control precisely how the service will be exposed.

By default (when `expose-external=false`), Charmed Apache Kyuubi K8s create a K8s service of type `ClusterIP` which it provides as endpoints to the related client applications.
These endpoints are only accessible from within the K8s namespace (or juju model) where the Charmed Apache Kyuubi K8s is deployed.

Below is a juju models where Charmed Apache Kyuubi K8s is related to Data Integrator, which we will later use to demonstrate the configuration of `expose-external`:

```text
juju status
Model              Controller  Cloud/Region        Version  SLA          Timestamp
jubilant-7db2fec3  microk8s    microk8s/localhost  3.6.8    unsupported  16:43:19+02:00

App                       Version  Status  Scale  Charm                      Channel        Rev  Address         Exposed  Message
auth-db                   14.15    active      1  postgresql-k8s             14/stable      281  10.152.183.19   no
data-integrator                    active      1  data-integrator            latest/stable  161  10.152.183.94   no
integration-hub                    active      1  spark-integration-hub-k8s  latest/edge     43  10.152.183.220  no
kyuubi-k8s                1.10     active      3  kyuubi-k8s                 latest/edge     92  10.152.183.84   no
s3                                 active      1  s3-integrator              1/stable       146  10.152.183.103  no
zookeeper                 3.8.2    active      1  zookeeper-k8s              3/stable        78  10.152.183.42   no

Unit                         Workload  Agent  Address       Ports  Message
auth-db/0*                   active    idle   10.1.111.95          Primary
data-integrator/0*           active    idle   10.1.111.66
integration-hub/0*           active    idle   10.1.111.101
kyuubi-k8s/0                 active    idle   10.1.111.80
kyuubi-k8s/1                 active    idle   10.1.111.78
kyuubi-k8s/2*                active    idle   10.1.111.98
s3/0*                        active    idle   10.1.111.77
zookeeper/0*                 active    idle   10.1.111.114
```

When `expose-external=false`, the following shows the endpoint returned to the client:

```shell
juju run data-integrator/0 get-credentials

Running operation 7 with 1 task
  - task 8 on unit-data-integrator-0

Waiting for task 8...
```

```yaml
kyuubi:
  data: '{"database": "test", "external-node-connectivity": "true", "provided-secrets":
    "[\"mtls-cert\"]", "requested-secrets": "[\"username\", \"password\", \"tls\",
    \"tls-ca\", \"uris\", \"read-only-uris\"]"}'
  database: test
  endpoints: kyuubi-k8s-service.jubilant-7db2fec3.svc.cluster.local:10009
  password: 31rwWzk8wpnhoZvU
  tls: "False"
  uris: jdbc:hive2://kyuubi-k8s-service.jubilant-7db2fec3.svc.cluster.local:10009/
  username: relation_id_15
  version: 1.10.2
ok: "True"
```

## External access

```{note}
We recommend exposing the service using Loadbalancer.
```

Charmed Apache Kyuubi K8s can be made externally accessible by setting `expose-external=loadbalancer` (or `expose-external=nodeport`).
When `expose-external=loadbalancer`, Charmed Apache Kyuubi K8s will provide as endpoint the K8s LoadBalancer service IP (or hostname, depending on your K8s cluster provider) and port.

```shell
juju config kyuubi-k8s expose-external=loadbalancer

# wait for active-idle
juju run data-integrator/0 get-credentials

Running operation 15 with 1 task
  - task 16 on unit-data-integrator-0

Waiting for task 16...
```

```yaml
kyuubi:
  data: '{"database": "test", "external-node-connectivity": "true", "provided-secrets":
    "[\"mtls-cert\"]", "requested-secrets": "[\"username\", \"password\", \"tls\",
    \"tls-ca\", \"uris\", \"read-only-uris\"]"}'
  database: test
  endpoints: 10.64.140.43:10009
  password: 31rwWzk8wpnhoZvU
  tls: "False"
  uris: jdbc:hive2://10.64.140.43:10009/
  username: relation_id_15
  version: 1.10.2
ok: "True"
```
