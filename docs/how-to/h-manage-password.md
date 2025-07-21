# How to manage passwords

Charmed Kyuubi uses [Juju secrets](https://documentation.ubuntu.com/juju/latest/reference/secret/#secret) to manage passwords.

```{seealso}
[Juju | How to manage secrets](https://documentation.ubuntu.com/juju/latest/howto/manage-secrets/#manage-secrets)
```

## Create a secret

To create a secret in Juju containing one or more user passwords:

```text
juju add-secret <secret_name> admin=<password>
```

The command above will output a secret URI, which you'll need for configuring `system-users`.

Admin user that was not included in the secret will use an automatically created password.

To grant the secret to the `kyuubi-k8s` charm:

```text
juju grant-secret <secret_name> kyuubi-k8s
```

## Configure `system-users`

To set the `system-users` config option to the secret URI:

```text
juju config charm-app system-users=<secret_URI>
```

When the `system-users` config option is set, the charm will:
* Use the content of the secret specified by the `system-users` config option instead of the one generated.
* Update the passwords of the internal `system-users` in its user database.

If the config option is **not** specified, the charm will automatically generate passwords for the internal system-users and store them in a secret.

To retrieve the password of an internal system-user, run the `juju show-secret` command with the respective secret URI.

## Update a secret

To update an existing secret:

```text
juju update-secret <secret_name> admin=<new_password>
```
