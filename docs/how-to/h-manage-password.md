# How to manage passwords

Charmed Apache Kyuubi K8s uses [Juju secrets](https://documentation.ubuntu.com/juju/latest/reference/secret/#secret) to manage passwords.

```{seealso}
> See also: [Juju | How to manage secrets](https://documentation.ubuntu.com/juju/latest/howto/manage-secrets/#manage-secrets)
```

## Create a password

Create a secret in Juju containing one or more user passwords:

```text
juju add-secret <secret_name> admin=<password>
```

The above outputs a secret URI, which you need for configuring `system-users` configuration parameter.
Without a valid secret granted to the application, the admin user uses an automatically created password.

To grant the secret to the `kyuubi-k8s` charm:

```text
juju grant-secret <secret_name> kyuubi-k8s
```

## Configure the system-users

To set the `system-users` configuration option to the secret URI:

```text
juju config charm-app system-users=<secret_URI>
```

When the `system-users` configuration option is set, the charm:

* Uses the specified secret instead of the auto generated one.
* Updates the passwords of the internal `system-users` in its user database.

If the configuration option is **not** specified, the charm automatically generates passwords for the internal system-users and store them in a secret.

To retrieve the password of an internal system-user, run the `juju show-secret` command with the respective secret URI.

## Update a secret

To update an existing secret:

```text
juju update-secret <secret_name> admin=<new_password>
```
