# How to use an external metastore

By default, Apache Kyuubi uses an embedded database to manage the metadata of persistent relational entities.
However, this database is limited to a single unit and is not persisted should the pod be rescheduled.

In a production environment, we recommend deploying an external metastore shared by all Charmed Apache Kyuubi K8s units, that can be backed up and restored as well.

The Charmed Apache Kyuubi K8s charm provides a `metastore-db` integration through the `postgresql_client` interface.
To use it, deploy a Charmed PostgreSQL K8s charm:

```shell
juju deploy postgresql-k8s --channel 14/stable --trust
```

Then, integrate it with the Charmed Apache Kyuubi K8s charm on the `metastore-db` relation:

```shell
juju integrate kyuubi-k8s:metastore-db postgresql-k8s
```

```{note}
We must specify the relation name because the Charmed Apache Kyuubi K8s charm offers two different relations under the `postgresql_client` interface.
```

Once the two charms settles in `active/idle`, the metastore is configured for all deployed units.
A guide on how to backup and restore the metastore can be find [here](#TODO).

To stop using the external metastore, remove the integration:

```shell
juju remove-relation kyuubi-k8s:metastore-db postgresql-k8s
```
