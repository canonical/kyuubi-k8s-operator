# How to connect from outside of Kubernetes

To expose Charmed Apache Kyuubi K8s externally, use the `expose-external` configuration option. Possible values are: `false`, `nodeport`, and `loadbalancer`.

By default (when `expose-external=false`), Charmed Apache Kyuubi K8s creates a K8s service of type `ClusterIP` which it provides as endpoints to the related client applications.
These endpoints are only accessible from within the K8s namespace (or Juju model) where the Charmed Apache Kyuubi K8s is deployed.

To make Charmed Apache Kyuubi K8s accessible from outside of Kubernetes, set the `expose-external` parameter to `loadbalancer` or `nodeport`.

[note]
We recommend exposing the service using the `loadbalancer` option.
[/note]

When `expose-external` is set to `loadbalancer`, Charmed Apache Kyuubi K8s will provide as endpoint the K8s LoadBalancer service IP (or hostname, depending on your K8s cluster provider) and port.

```shell
juju config kyuubi-k8s expose-external=loadbalancer
```

Wait for the application to settle up with the `active` / `idle` status and retrieve credentials:

```shell
juju run data-integrator/0 get-credentials
```

The output of this command contains both endpoint and credentials, for example:

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
