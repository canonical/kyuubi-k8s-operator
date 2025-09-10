# Enable and configure monitoring for Charmed Apache Kyuubi

Charmed Apache Kyuubi K8s come with the [JMX exporter](https://github.com/prometheus/jmx_exporter/).
The metrics can be queried by accessing the `http://<kyuubi-unit-ip>:10019/metrics endpoint.

Additionally, Charmed Apache Kyuubi supports native integration with the Canonical Observability Stack (COS).

This section is about monitoring the Apache Kyuubi server, for more information about the underlying Spark jobs and their COS integration, refer to the [COS documentation](https://charmhub.io/topics/canonical-observability-stack) and the [monitoring explanation section](/t/charmed-spark-documentation-explanation-monitoring/14299).

## Integrating with COS

To deploy COS on MicroK8s, follow the [step-by-step tutorial](https://charmhub.io/topics/canonical-observability-stack/tutorials/install-microk8s).

### Offer interfaces via the COS controller

Switch to COS K8s environment and offer COS interfaces to be cross-model integrated with Charmed Apache Kyuubi K8s model:

```shell
juju switch <k8s_controller>:<cos_model_name>

juju offer grafana:grafana-dashboard grafana-dashboards
juju offer loki:logging loki-logging
juju offer prometheus:receive-remote-write prometheus-receive-remote-write
```

### Consume offers via the Apache Kyuubi model

Switch back to the Charmed Apache Kyuubi K8s model, find offers and integrate with them:

```shell
juju switch <spark_model_name>

juju find-offers <k8s_controller>:
```

A similar output should appear, if `k8s` is the K8s controller name and `cos` the model where `cos-lite` has been deployed:

```shell
Store      URL                                        Access  Interfaces
k8s        admin/cos.grafana-dashboards               admin   grafana_dashboard:grafana-dashboard
k8s        admin/cos.loki-logging                     admin   loki_push_api:logging
k8s        admin/cos.prometheus-receive-remote-write  admin   prometheus-receive-remote-write:receive-remote-write
...
```

Consume offers to be reachable in the current model:

```shell
juju consume <k8s_controller>:admin/<cos_model_name>.prometheus-receive-remote-write
juju consume <k8s_controller>:admin/<cos_model_name>.loki-logging
juju consume <k8s_controller>:admin/<cos_model_name>.grafana-dashboards
```

### Deploy and integrate Grafana agent

Deploy `grafana-agent-k8s`:

```shell
juju deploy grafana-agent-k8s --trust
```

Integrate it with consumed COS offers:

```shell
juju integrate grafana-agent-k8s grafana-dashboards
juju integrate grafana-agent-k8s loki-logging
juju integrate grafana-agent-k8s prometheus-receive-remote-write
```

Finally, integrate `grafana-agent` it with Charmed Apache Kyuubi K8s:

```shell
juju integrate grafana-agent-k8s kyuubi-k8s:grafana-dashboard
juju integrate grafana-agent-k8s kyuubi-k8s:logging
juju integrate grafana-agent-k8s kyuubi-k8s:metrics-endpoint
```

Wait for all components to settle down to the `active/idle` state on both models.

After this is complete, the monitoring COS stack should be up and running and ready to be used.

### Connect Grafana web interface

To connect to the Grafana web interface, follow the [Browse dashboards](https://charmhub.io/topics/canonical-observability-stack/tutorials/install-microk8s?_ga=2.201254254.1948444620.1704703837-757109492.1701777558#heading--browse-dashboards) section of the MicroK8s "Getting started" guide.

```shell
juju run grafana/leader get-admin-password --model <k8s_cos_controller>:<cos_model_name>
```

Then, browse to the **Dashboards** section, where you can find a **Kyuubi** one.
