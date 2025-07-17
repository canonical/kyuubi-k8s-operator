# Charmed Apache Kyuubi K8s documentation

Charmed Apache Kyuubi K8s is an open-source [Kubernetes charm](https://juju.is/docs/olm/charmed-operator) for Apache Kyuubi, which is a distributed and multi-tenant gateway to provide serverless SQL capabilities on Data Warehouses and Lakehouses leveraging on Apache Spark computational engines.

Charmed Apache Kyuubi K8s provides a JDBC endpoint which can be used by JDBC-compliant tools to execute SQL queries on a Charmed Apache Spark cluster. 
It is part of an open source, end-to-end, production-ready [data platform](https://canonical.com/data) on top of cloud native technologies provided by Canonical.

Using Charmed Apache Kyuubi K8s minimizes the barriers and costs for end-users to use Charmed Apache Spark at the client side. At the server-side, multi-tenant architecture of Apache Kyuubi provides the administrators a way to achieve computing resource isolation, data security, high availability, high client concurrency, etc.

Charmed Apache Kyuubi K8s is useful for administrators of Charmed Apache Spark solutions who want to provide serverless SQL query capability or multi-tenancy for their end-users.

<!--
# Navigation

SEE TEMPLATE
-->

## Usage

<!--TODO: Remove this entire section once we have a dedicated page-->

To deploy this charm, use:

```shell
juju add-model <my-model>

juju deploy s3-integrator --channel latest/edge
juju config s3-integrator bucket=<bucket> endpoint=<endpoint> credentials=<credentials-secret-uri>

juju deploy spark-integration-hub-k8s --channel edge --trust
juju integrate spark-integration-hub-k8s s3-integrator

juju deploy postgresql-k8s --channel 14/stable --trust

juju deploy data-integrator --channel latest/edge --config database-name=test

juju deploy kyuubi-k8s --channel edge --trust
juju integrate kyuubi-k8s spark-integration-hub-k8s 
juju integrate kyuubi-k8s:auth-db postgresql-k8s
juju integrate kyuubi-k8s data-integrator
```

> **Note**: You may use a different object storage, such as `azure-storage-integrator`.

The Integration Hub charm will take care of adding relevant configuration to the
Charmed Spark properties.
Get the JDBC endpoint and its credentials with the following:

```shell
juju run data-integrator/leader get-credentials
```

You may use the endpoint with a JDBC-compliant client, such as `beeline`:

```shell
beeline -u "<jdbc-endpoint>" -n <username> -p <password>
```

## Project and community

Charmed Apache Kyuubi K8s is a member of the Ubuntu family. It’s an open-source project that warmly welcomes community projects, contributions, suggestions, fixes and constructive feedback.

- [Code of conduct](https://ubuntu.com/community/code-of-conduct)
- [Get support](https://canonical.com/data)
- Meet the community and chat with us on [Matrix](https://matrix.to/#/#charmhub-data-platform:ubuntu.com)
- [Contribute](https://github.com/canonical/kyuubi-k8s-operator/blob/main/CONTRIBUTING.md) and report [issues](https://github.com/canonical/kyuubi-k8s-operator/issues/new)

Thinking about using Charmed Apache Spark for your next project? [Get in touch!](https://canonical.com/data)

## License

The Charmed Apache Kyuubi K8s charm is free software, distributed under the Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/kyuubi-k8s-operator/blob/main/LICENSE) for more information.

Apache Kyuubi is a free, open-source software project by the Apache Software Foundation. Users can find out more at the [Apache Kyuubi project page](https://kyuubi.apache.org/).
