# How to integrate with another application

This guide shows how to integrate Charmed Apache Kyuubi K8s with both charmed and non-charmed applications.

For developer information about how to integrate your own charmed application with Charmed Kyuubi, see []().

## Integrate with a charmed application

Integrations with charmed applications are supported via the modern `kyuubi_client` interface.

[note]
You can see which existing charms are compatible with Kyuubi in the [Integrations](https://charmhub.io/kyuubi-k8s/integrations) tab on Charmhub.
[/note]

### The kyuubi_client interface

To integrate, run

```shell
juju integrate kyuubi-k8s:database <charm>
```

To remove the integration, run

```text
juju remove-relation kyuubi-k8s <charm>
```

## Integrate with a non-charmed application

To integrate with an application outside of Juju, use the [`data-integrator` charm](https://charmhub.io/data-integrator) to create the required credentials and endpoints.

Deploy `data-integrator`:

```text
juju deploy data-integrator --config database-name=<name>
```

Integrate with Kyuubi K8s:

```text
juju integrate data-integrator kyuubi-k8s
```

Use the `get-credentials` action to retrieve credentials from `data-integrator`:

```text
juju run data-integrator/leader get-credentials
```

## Rotate application passwords

To rotate the passwords of users created for integrated applications, the integration should be removed and created again. This process will generate a new user and password for the application.

```text
juju remove-relation <charm> kyuubi-k8s
juju integrate <charm> kyuubi-k8s
```

For a non-charmed application, the `data-integrator` is the `<charm>`.
