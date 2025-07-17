# Tutorial

This hands-on tutorial aims to help you learn how to deploy Charmed Apache Kyuubi K8s and become familiar with its available operations.

## Prerequisites

TODO

## Set up the environment

TODO

## Deploy Charmed Apache Kyuubi K8s

Run the following commands to create a new model, deploy the mandatory charms and relate them:

```shell
juju add-model tutorial

juju deploy s3-integrator --channel 1/stable
juju config s3-integrator bucket=mybucket
juju run s3-integrator/0 sync-s3-credentials access-key=ACCESS_KEY secret-key=SECRET_KEY

juju deploy spark-integration-hub-k8s --channel 3/edge --trust
juju integrate spark-integration-hub-k8s s3-integrator

juju deploy postgresql-k8s --channel 14/stable --trust

juju deploy data-integrator --channel latest/edge --config database-name=test

juju deploy kyuubi-k8s --channel 3.4/edge --trust
juju integrate kyuubi-k8s spark-integration-hub-k8s 
juju integrate kyuubi-k8s:auth-db postgresql-k8s
juju integrate kyuubi-k8s data-integrator
```

> **Note**: You may use a different object storage, such as `azure-storage-integrator`.

## Access Charmed Apache Kyuubi K8s

Get the JDBC endpoint and its credentials with the following command:

```shell
juju run data-integrator/0 get-credentials
```

You may use the endpoint with a JDBC-compliant client, such as `beeline`.

```{note}
We recommend using the [spark-client](https://snapcraft.io/spark-client) snap, which exposes a beeline client under the `spark-client.beeline` command.
```

```shell
spark-client.beeline -u "<jdbc-endpoint>" -n <username> -p <password>
```

## Enable encryption with TLS

[Transport Layer Security (TLS)](https://en.wikipedia.org/wiki/Transport_Layer_Security) is a protocol used to encrypt data exchanged between two applications.
Essentially, it secures data transmitted over a network.

Typically, enabling TLS between a highly available database and client/server applications requires a high level of expertise.
This has all been encoded into Charmed Apache Kyuubi K8s so that configuring TLS requires minimal effort on your end.

TLS is enabled by integrating Charmed Apache Kyuubi K8s with the [Self-signed certificates charm](https://charmhub.io/self-signed-certificates).
This charm centralises TLS certificate management consistently and handles operations like providing, requesting, and renewing TLS certificates.

```{caution}
**[Self-signed certificates](https://en.wikipedia.org/wiki/Self-signed_certificate) are not recommended for a production environment.**

Check [this guide](https://discourse.charmhub.io/t/security-with-x-509-certificates/11664) for an overview of the TLS certificates charms available.
```

Before enabling TLS on Charmed Apache Kyuubi K8s, we must deploy the `self-signed-certificates` charm:

```shell
juju deploy self-signed-certificates --config ca-common-name="Tutorial CA"
```

Wait for the charm settle into an `active/idle` state, as shown by the `juju status`.

To enable TLS on Charmed Apache Kyuubi K8s, relate the `kyuubi-k8s` charm with the `self-signed-certificates` charm:

```shell
juju relate kyuubi-k8s self-signed-certificates
```

After the charms settle into `active/idle` states, the Charmed Apache Kyuubi K8s endpoint should now accept encrypted traffic.
This can be tested by requesting the server certificate using `openssl`:

```shell
openssl s_client -showcerts -connect <IP>:10009 < /dev/null
```

Requesting the credentials again should now display the certificate to use:

```shell
juju run data-integrator/0 get-credentials
```

To connect to Charmed Apache Kyuubi K8s using the spark-client's bundled beeline client, import the certificate in the spark-client snap:

```shell
juju run data-integrator/0 get-credentials | yq ".[].certificate" > cert.pem
spark-client.import-certificate tutorial-cert cert.pem
```

Then, add `;ssl=true` to the JDBC endpoint you got from the data-integrator charm.

```shell
spark-client.beeline -u "<jdbc-endpoint>;ssl=true" -n <username> -p <password>
```

Congratulations! You are now connected to Charmed Apache Kyuubi K8s using TLS.

To remove the external TLS, remove the integration:

```shell
juju remove-relation kyuubi-k8s self-signed-certificates
```
