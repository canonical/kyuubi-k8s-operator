# How to enable encryption

The Charmed Apache Kyuubi K8s charm implements the Requirer side of the [`tls-certificates/v4`](https://charmhub.io/tls-certificates-interface/libraries/tls_certificates) charm relation.
Therefore, any charm implementing the Provider side could be used.
To enable encryption, you should first deploy a TLS certificates Provider charm.

## Deploy a TLS Provider charm

One possible option, suitable for testing, could be to use the `self-signed-certificates` charm..

To deploy a `self-signed-certificates` charm:

```shell
juju deploy self-signed-certificates --channel=1/stable
```

Add necessary configuration parameters:

```shell
juju config self-signed-certificates ca-common-name="Test CA"
```

[note]
We recommend to avoid using self-signed TLS certificates for production environments.
Please refer to the [X.509 certificates post](https://charmhub.io/topics/security-with-x-509-certificates) for an overview of the TLS certificates Providers charms and some guidance on how to choose the right charm for your use case.
[/note]

## Relate the charms

```
juju integrate <tls-certificates> kyuubi-k8s
```

where `<tls-certificates>` is the name of the TLS certificate provider charm deployed.

To disable TLS remove the relation:

```shell
juju remove-relation <tls-certificates> kyuubi-k8s
```

## Manage keys

Updates to private keys for certificate signing requests (CSR) can be made via the `tls-client-private-key` configuration option.
If this configuration option is not provided, the charm will generate a new private key and use it instead.

To generate a shared internal key:

```shell
openssl genrsa -out internal-key.pem 3072
```

Create a new juju secret using the content of the shared key file:

```shell
juju add-secret kyuubi-tls-secret private-key#file=internal-key.pem
```

The previous command above returns a secret id, e.g. `secret:d1seounmp25c76bq4ha0`.
To grant the application access to the secret, run:

```shell
juju grant-secret kyuubi-tls-secret kyuubi-k8s
```

Finally, configure the application to use the secret using the secret id from above

```shell
juju config kyuubi-k8s tls-client-private-key=secret:d1seounmp25c76bq4ha0
```

To rotate a private key, update the associated secret:

```shell
juju update-secret kyuubi-tls-secret private-key#file=new-internal-key.pem
```

> See also: `juju update-secret` command [reference](https://documentation.ubuntu.com/juju/3.6/reference/juju-cli/list-of-juju-cli-commands/update-secret/).

## Retrieve the certificate chain

To retrieve the certificate in use, use the data-integrator charm:

```shell
juju run data-integrator/0 get-credentials | yq ".kyuubi.tls-ca"
```
