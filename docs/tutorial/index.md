# 6. Query datasets with Apache Kyuubi

Apache Kyuubi is a gateway to serverless SQL running on Kubernetes, bridging the gap between Apache Spark as a data processing framework and a data lakehouse platform.

This hands-on tutorial aims to help you learn how to deploy Charmed Apache Kyuubi K8s and become familiar with its available operations.

## Environment variables

At this step of the tutorial, you’ll need some environment variables that were set earlier during the environment setup.
If you’ve restarted the VM and lost those variables, you can refresh them by running the following commands:

```bash
export ACCESS_KEY=$(kubectl get secret -n minio-operator microk8s-user-1 -o jsonpath='{.data.CONSOLE_ACCESS_KEY}' | base64 -d)
export SECRET_KEY=$(kubectl get secret -n minio-operator microk8s-user-1 -o jsonpath='{.data.CONSOLE_SECRET_KEY}' | base64 -d)
export S3_ENDPOINT=$(kubectl get service minio -n minio-operator -o jsonpath='{.spec.clusterIP}')
```

## Initial setup

Let's create a fresh Juju model for some experiments with the Charmed Apache Kyuubi K8s charm.

```bash
juju add-model lakehouse
```

To create our simple, minimal data lakehouse, first, we need an object storage.
To deploy and configure [S3-integrator](https://charmhub.io/s3-integrator) to use the object storage provided by the Minion addon, run:

```shell
juju deploy s3-integrator --channel 1/stable
juju config s3-integrator bucket=lakehouse path="spark-events" endpoint=http://$S3_ENDPOINT
juju run s3-integrator/0 sync-s3-credentials access-key=$ACCESS_KEY secret-key=$SECRET_KEY
```

We will deploy the [Spark Integration Hub K8s charm](https://charmhub.io/spark-integration-hub-k8s) to manage integrations and configure service accounts on Kubernetes.

To deploy it and integrate it with the object storage integrator, use:

```shell
juju deploy spark-integration-hub-k8s --channel 3/edge --trust integration-hub
juju integrate integration-hub s3-integrator
```

We also need a database to hold our users, using the [Charmed PostgreSQL K8s charm](https://charmhub.io/postgresql-k8s).
To deploy it, run:

```shell
juju deploy postgresql-k8s --channel 14/stable --trust auth-db
```

Finally, to enable external clients to connect to our lakehouse, we need the [Data Integrator charm](https://charmhub.io/data-integrator):

```shell
juju deploy data-integrator --channel latest/stable --config database-name=test
```

We are now ready to deploy the Charmed Apache Kyuubi K8s charm, and integrate it with the previous charms.

```shell
juju deploy kyuubi-k8s --channel 3.4/edge --trust --config expose-external=loadbalancer
juju integrate kyuubi-k8s integration-hub 
juju integrate kyuubi-k8s:auth-db auth-db
juju integrate kyuubi-k8s data-integrator
```

Check the list of charms that have been deployed and their statuses:

```shell
watch -c juju status --relations --color
```

Wait until the status to be active for each charm:

```text
Model              Controller  Cloud/Region        Version  SLA          Timestamp
lakehouse          microk8s    microk8s/localhost  3.6.8    unsupported  16:43:19+02:00

App                       Version  Status  Scale  Charm                      Channel        Rev  Address         Exposed  Message
auth-db                   14.15    active      1  postgresql-k8s             14/stable      495  10.152.183.19   no
data-integrator                    active      1  data-integrator            latest/stable  181  10.152.183.94   no
integration-hub                    active      1  spark-integration-hub-k8s  latest/edge     66  10.152.183.220  no
kyuubi-k8s                1.10     active      1  kyuubi-k8s                 3.4/edge       109  10.152.183.84   no
s3-integrator                      active      1  s3-integrator              1/stable       146  10.152.183.103  no

Unit                         Workload  Agent  Address       Ports  Message
auth-db/0*                   active    idle   10.1.111.95          Primary
data-integrator/0*           active    idle   10.1.111.66
integration-hub/0*           active    idle   10.1.111.101
kyuubi-k8s/0                 active    idle   10.1.111.80
s3-integrator/0*             active    idle   10.1.111.77
```

## Access Charmed Apache Kyuubi K8s

Get the JDBC endpoint and its credentials with the following command:

```shell
juju run data-integrator/0 get-credentials
```

The resulted output should look like the following:

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

Install the [spark-client snap](https://snapcraft.io/spark-client) using:

```shell
sudo snap install spark-client --channel=3.4/edge
```

Use the `spark-client.beeline` command to access the endpoint with a JDBC-compliant client `beeline`:

```shell
spark-client.beeline -u "jdbc:hive2://10.64.140.43:10009/" -n relation_id_15 -p 31rwWzk8wpnhoZvU
```

The client should welcome you with a prompt where you can run SQL queries.

## Enable encryption with TLS

[Transport Layer Security (TLS)](https://en.wikipedia.org/wiki/Transport_Layer_Security) is a protocol used to encrypt data exchanged between two applications.
Essentially, it secures data transmitted over a network.

Typically, enabling TLS between a highly available database and client/server applications requires a high level of expertise.

TLS is enabled by integrating Charmed Apache Kyuubi K8s with the [Self-signed certificates charm](https://charmhub.io/self-signed-certificates).
This charm centralises TLS certificate management consistently and handles operations like providing, requesting, and renewing TLS certificates.

[note]
We recommend to avoid using self-signed TLS certificates for production environments.
Please refer to the [X.509 certificates post](https://charmhub.io/topics/security-with-x-509-certificates) for an overview of the TLS certificates Providers charms and some guidance on how to choose the right charm for your use case.
[/note]

Before enabling TLS on Charmed Apache Kyuubi K8s, deploy the `self-signed-certificates` charm to use as a certificate provider:

```shell
juju deploy self-signed-certificates --config ca-common-name="Tutorial CA"
```

Wait for the charm settle into an `active/idle` state, as shown by the `juju status`.

To enable TLS on Charmed Apache Kyuubi K8s, integrate the `kyuubi-k8s` charm with the `self-signed-certificates` charm:

```shell
juju integrate kyuubi-k8s self-signed-certificates
```

After the charms settle into `active/idle` states, the Charmed Apache Kyuubi K8s endpoint should now accept encrypted traffic.
Requesting the credentials again should now display the certificate to use:

```shell
juju run data-integrator/0 get-credentials
```

The resulted output should look like the following:

```
kyuubi:
  data: '{"database": "test", "external-node-connectivity": "true", "provided-secrets":
    "[\"mtls-cert\"]", "requested-secrets": "[\"username\", \"password\", \"tls\",
    \"tls-ca\", \"uris\", \"read-only-uris\"]"}'
  database: test
  endpoints: 10.64.140.43:10009
  password: 31rwWzk8wpnhoZvU
  tls: "True"
  tls-ca: |-
    -----BEGIN CERTIFICATE-----
    MIIDMTCCAhmgAwIBAgIUTM5oAAEAuCDfu/gmUnbZ0ei5ZSUwDQYJKoZIhvcNAQEL
    BQAwHjELMAkGA1UEBhMCVVMxDzANBgNVBAMMBmt5dXViaTAeFw0yNTA3MTcxNDM4
    NDRaFw0yNjA3MTcxNDM4NDRaMB4xCzAJBgNVBAYTAlVTMQ8wDQYDVQQDDAZreXV1
    YmkwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDcpvObLJIhUhKaNHbP
    Ju4+XDjHRk6hMJhNdOo76mQHnbJR0c3ZlN8XSdZJ3ekgJOtUe4VY9stVZMZI3LGb
    5/CcxSYZ8oYeWaQ06ST3v7bwZvyJMoInSRMYzLnCIzzXDSVajfLO9bqDKBhw7sPq
    cW5j+FYhLlvqDhU1wXgwwf5KfhIpN70PQnBh1UhdYryU0Qg11caf4N8s+6TN39qu
    hWewhAtADlWrbba/s34yHDSNxl1VVO3cxPmFmYp0UvraecEOsbhRhoX7ZfUlxF+t
    OVjiB/LwWulDgTTFwOPEBku1Zqwuq1Bgl+VD6wGRC2uRsPy2lekDDfi4lDmBREdN
    V6hvAgMBAAGjZzBlMB8GA1UdDgQYBBYEFC5E5p+5CDMi8lwiDZKG4RHRNxVYMCEG
    A1UdIwQaMBiAFgQULkTmn7kIMyLyXCINkobhEdE3FVgwDgYDVR0PAQH/BAQDAgKk
    MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAEZCONjNJw22Fox/
    7YCRMgb8TTLjybl5aFnpISVe+DbDiceBHrwcP+gJxHTh6cWs8tBrqi2v9ghcYo7S
    Ux7MnRzc4qQTSravR+07guGDeQjaSNk6FX2I5J8shrUD4167ZbPDMoYmcawr4wAZ
    NpIeRGN8IkezA5nMCY0iSrBsrpMYUepDmIPWck8MvrgPGjrR+hZSBq3EJc5J91Os
    QLWGr1RlSjFOfsP8s8n0dkC2UqXmOBN7NZogizGS2mbQvLAg0dSOvueaJsh8dPBU
    eN0aIQcZSPwCK/6iPokfO/afCYZIEmr5LBs81i5B8bQXqnxpltmcNbOQICfqA9XK
    m/BZ6OU=
    -----END CERTIFICATE-----
  uris: jdbc:hive2://10.64.140.43:10009/
  username: relation_id_15
  version: 1.10.2
ok: "True"
```

This can be tested by requesting the server certificate using `openssl` on the endpoint returned above:

```shell
openssl s_client -showcerts -connect $(juju run data-integrator/0 get-credentials | yq ".kyuubi.endpoints") < /dev/null
```

To connect to Charmed Apache Kyuubi K8s using the spark-client's bundled `beeline` client, import the certificate in the spark-client snap:

```shell
juju run data-integrator/0 get-credentials | yq ".kyuubi.tls-ca" > cert.pem
spark-client.import-certificate tutorial-cert cert.pem
```

Then, add `;ssl=true` to the JDBC endpoint you got from the data-integrator charm.

```shell
spark-client.beeline -u "jdbc:hive2://10.64.140.43:10009/;ssl=true" -n relation_id_15 -p 31rwWzk8wpnhoZvU
```

The client should welcome you once again with a prompt where you can run SQL queries.
Congratulations! You are now connected to Charmed Apache Kyuubi K8s using TLS.
