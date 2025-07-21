# How to perform a minor upgrade on the charm

Charmed Apache Kyuubi K8s charm can perform in-place upgrades to update the charm and the workload to a newer version.

```{caution}
This charm does not support in-place upgrades for major version changes, nor for a Spark support track change (e.g. upgrading from the channel `3.4` to `3.5`).
```

```{caution}
Like other K8s charms, this guide is only valid for multi units deployments.
This is because it is not possible to prevent refresh of the highest number unit and after the first and only unit refreshes the application's refresh has completed.
```

## Step 1: Pre-upgrade checks

Here are some key topics to consider before upgrading.

### Concurrency with other operations

Concurrency with other operations is not supported.
Therefore, we recommend to not perform any other extraordinary operation on Charmed Apache Kyuubi K8s while upgrading.
Those operations include (but not limited to) the following:

- Adding or removing units
- Creating, upgrading or destroying relations
- Changing the workload configuration

In summary, the cluster must be as stable as possible before upgrading.

### Backups

Make sure to have a backup of your data when running any kind of upgrade.

A guide on how to backup and restore the metastore and the user databases can be find [here](#TODO).

### Collect

```{note}
This step is only valid when deploying from [charmhub](https://charmhub.io/). 

If a [local charm](https://juju.is/docs/sdk/deploy-a-charm) is deployed (revision is small, e.g. 0-10), make sure the proper/current local revision of the `.charm` file is available BEFORE going further. You might need it for a rollback.
```

We recommend recording the revision of the running application as a safety measure for a rollback action.
To accomplish this, run the `juju status` command and look for the deployed Charmed Apache Kyuubi K8s revision in the command output, e.g.:

```text
juju status
Model              Controller  Cloud/Region        Version  SLA          Timestamp
jubilant-7db2fec3  microk8s    microk8s/localhost  3.6.8    unsupported  16:43:19+02:00

App                       Version  Status  Scale  Charm                      Channel        Rev  Address         Exposed  Message
kyuubi-k8s                1.10     active      3  kyuubi-k8s                 latest/edge    103  10.152.183.84   no
...

Unit                         Workload  Agent  Address       Ports  Message
kyuubi-k8s/0                 active    idle   10.1.111.80
kyuubi-k8s/1                 active    idle   10.1.111.78
kyuubi-k8s/2*                active    idle   10.1.111.98
...
```

In this example, the current revision is `103`.
Store it safely to use in case of a rollback.

### Automated checks

Before running the [juju refresh](https://juju.is/docs/juju/juju-refresh) command, it is necessary to run the `pre-refresh-check` action against the leader unit:

```shell
juju run kyuubi-k8s/leader pre-refresh-check
```

Make sure there are no errors in the result output.

## Step 2: Upgrade and check first unit

Use the `juju refresh` command to trigger the charm upgrade process.

Example with specific revision:

```shell
juju refresh kyuubi-k8s --revision=104
```

After the first unit finishes its refresh, its status should be `active/idle`.
It could however show a "Refresh incompatible" or "Incorrect OCI resource" depending on the channel, revision or OCI resource passed in the `juju refresh` command.
This usually indicates that you need to rollback, but if you consider the risks acceptable (like if you purposefully overwrote the OCI resource to use a custom one), you can force the charm to start the workload using the following, where `<refreshed-unit-number>` should be the highest ordinal one ("2" for a three units deployment) and `<parameters>` one or more action parameter set to false (for instance `check-workload-container=false` if you purposefully overwrite the OCI resource).

```shell
juju run kyuubi-k8s/<refreshed-unit-number> force-refresh-start <parameters>
```

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

## Step 4: Rollback in case of failure

Should a failure arise at any point after the `juju refresh` command is run, the application status should display the command to run to rollback the changes.
This command can also be found in the logs using `juju debug-log`.

The procedure is similar to an upgrade: the highest ordinal unit is the first to rollback, then after a manual confirmation the rest can proceed.
