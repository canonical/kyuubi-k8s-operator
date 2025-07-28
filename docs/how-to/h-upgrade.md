# How to upgrade

Charmed Apache Kyuubi K8s can perform in-place upgrades to update the charm and the workload to a newer version.

[note]
This charm does not support in-place upgrades for major version changes, nor for a Spark support track change (e.g. upgrading from the channel `3.4` to `3.5`).
[/note]

[note]
A one-unit deployment presents some limitations compared to a multi units deployment.
For instance, it is not possible to rollback an upgrade in progress.
Therefore, it is not supported.
[/note]

## Step 1: Pre-upgrade checks

Here are some key topics to consider before upgrading: concurrent operations, data backup, rollback preparation, and automated checks.

### Concurrency with other operations

Concurrency with other operations is not supported.
Therefore, we recommend to not perform any other extraordinary operation on Charmed Apache Kyuubi K8s while upgrading.
Those operations include (but not limited to) the following:

- Adding or removing units
- Creating, upgrading or destroying relations
- Changing the workload configuration

In short, the cluster must be as stable as possible before upgrading.

### Backups

Make sure to have a backup of your data when running any kind of upgrade.

A guide on how to backup and restore the metastore and the user databases can be find [here](#TODO).

### Rollback preparations

[note]
This step is only valid when deploying from [Charmhub](https://charmhub.io/).

If a [local charm](https://juju.is/docs/sdk/deploy-a-charm) is deployed (revision is small, e.g. 0-10), make sure the proper/current local revision of the `.charm` file is available BEFORE going further. You might need it for a rollback.
[/note]

We recommend recording the revision of the running application as a safety measure for a rollback action.
To get the currently in use revision for Charmed Apache Kyuubi K8s, use:

```shell
juju status --format yaml | yq ".applications.<application-name>.charm-rev"
```

where `<application-name>` is the name of the Charmed Apache Kyuubi K8s application in your model.
Store it safely to use in case of a rollback.

### Automated checks

Before running the [juju refresh](https://juju.is/docs/juju/juju-refresh) command, it is necessary to run the `pre-refresh-check` action against the leader unit:

```shell
juju run kyuubi-k8s/leader pre-refresh-check
```

Make sure there are no errors in the resulted output.

## Step 2: Upgrade and check first unit

Use the `juju refresh` command to trigger the charm upgrade process, for example:

```shell
juju refresh kyuubi-k8s --revision=104
```

After some time, the first unit finishes its refresh.
Its status can be of two different kinds:

- if it is `active/idle`, you may proceed to Step 3.
- if it shows a blocked status, read below.

The charm has a number of built-in checks that are ran post refresh, such as checking that the OCI resource is the one defined by the charm or making sure you don't upgrade to an incompatible revision (like updating to a revision built for Apache Spark 3.5 if your original deployment uses Apache Spark 3.4).

If your in-progress upgrade violates one or more of these checks, the first unit to refresh displays a blocked status with a "Refresh incompatible" or "Incorrect OCI resource" message.
This usually indicates that you need to rollback, but if you consider the risks acceptable (like if you purposefully overwrote the OCI resource to use a custom one), you can force the charm to start the workload anyway on the blocked unit using the following:

```shell
juju run kyuubi-k8s/<refreshed-unit-number> force-refresh-start <parameters>
```

where `<refreshed-unit-number>` should be the highest ordinal one ("2" for a three units deployment) and `<parameters>` one or more action parameter set to false (for instance `check-workload-container=false` if you purposefully overwrite the OCI resource).
The first unit to refresh must end up in `active/idle` state before moving on with the next step.
Make sure that you test that it is properly functioning as well.

## Step 3: Upgrade the rest of the units

By default, the `pause_unit_after_refresh` configuration option is set to `"first"`, meaning that a manual confirmation is needed before upgrading the rest of the units.
We recommend keeping it, as it provides the safest way to rollback in case of an issue while minimizing the amount of manual operations.

To resume the upgrade process with the rest of the units, run the following

```shell
juju run kyuubi-k8s/leader resume-refresh
```

All units will be refreshed (i.e. receive new charm content), and the upgrade will execute one unit at a time.

## Step 4: (Optional) Rollback in case of failure

Should a failure arise at any point after the `juju refresh` command is run, the application status should display the command to run to rollback the changes, for instance:

```shell
juju refresh kyuubi-k8s --revision 103 --resource kyuubi-image=ghcr.io/canonical/charmed-spark-kyuubi@sha256:153eaf8be341dcea2c91277cbc2a69a1c9c48d3f7847151898ab2e5a81753ec5
```

This command can also be found in the logs using `juju debug-log`.

The procedure is similar to an upgrade: the highest ordinal unit is the first to rollback, then after a manual confirmation the rest can proceed.
