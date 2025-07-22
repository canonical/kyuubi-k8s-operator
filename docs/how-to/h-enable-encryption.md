# How to enable encryption

The Charmed Apache Kyuubi K8s charm implements the Requirer side of the [`tls-certificates/v4`](https://charmhub.io/tls-certificates-interface/libraries/tls_certificates) charm relation.
Therefore, any charm implementing the Provider side could be used.
To enable encryption, you should first deploy a TLS certificates Provider charm.

## Deploy a TLS Provider charm

One possible option, suitable for testing, could be to use the `self-signed-certificates` charm..

To deploy a `self-signed-certificates` charm:

```shell
# deploy the TLS charm
juju deploy self-signed-certificates --channel=1/stable
# add the necessary configurations for TLS
juju config self-signed-certificates ca-common-name="Test CA"
```

[note]
This setup is not recommended for production clusters.
Please refer to [this post](https://charmhub.io/topics/security-with-x-509-certificates) for an overview of the TLS certificates Providers charms and some guidance on how to choose the right charm for your use case.
[/note]

## Relate the charms

```
juju relate <tls-certificates> kyuubi-k8s
```

where `<tls-certificates>` is the name of the TLS certificate provider charm deployed.

To disable TLS remove the relation:

```shell
juju remove-relation <tls-certificates> kyuubi-k8s
```

## Manage keys

Updates to private keys for certificate signing requests (CSR) can be made via the `tls-client-private-key` configuration option.
If this configuration option is not provided, the charm will generate a new private key and use it instead.

```shell
# generate shared internal key
openssl genrsa -out internal-key.pem 3072

# create a new juju secret 
juju add-secret kyuubi-tls-secret private-key#file=internal-key.pem
# The command above returns the secret id e.g. secret:d1seounmp25c76bq4ha0

# grant secret access to the app, and configure the app to use the secret
juju grant-secret kyuubi-tls-secret kyuubi-k8s
juju config kyuubi-k8s tls-client-private-key=secret:d1seounmp25c76bq4ha0
```

Private keys may be rotated by updating the secret using `juju update-secret`.

## Retrieve the certificate chain

The data-integrator charm can be used to retrieve the certificate in use, through the `kyuubi_client` interface.

```shell
juju run data-integrator/0 get-credentials | yq ".kyuubi.tls-ca"
```
