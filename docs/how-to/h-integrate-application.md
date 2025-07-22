# How to integrate with another application

[Integrations](https://juju.is/docs/juju/relation), also known as “relations” are connections between two applications with compatible endpoints. These connections simplify the creation and management of users, passwords, and other shared data.

This guide shows how to integrate Charmed Apache Kyuubi with both charmed and non-charmed applications.

For developer information about how to integrate your own charmed application with Charmed Kyuubi, see []().

## Integrate with a charmed application

Integrations with charmed applications are supported via the modern `kyuubi_client` interface.
```{note}
You can see which existing charms are compatible with Kyuubi in the [Integrations](https://charmhub.io/kyuubi-k8s/integrations) tab on Charmhub.
```

### `kyuubi_client` interface

To integrate, run
```text
juju integrate kyuubi-k8s:database <charm>
```

To remove the integration, run
```text
juju remove-relation kyuubi-k8s <charm>
```

## Integrate with a non-charmed application

To integrate with an application outside of Juju, you must use the [`data-integrator` charm](https://charmhub.io/data-integrator) to create the required credentials and endpoints.

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

In the case of connecting with a non-charmed application, `<charm>` would be `data-integrator`.
