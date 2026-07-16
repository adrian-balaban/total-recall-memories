---
title: Apache Flink 2.3 docs — Deployment (overview/java-compatibility/standalone overview+working-directory+docker+kubernetes)
tags: [org, flink, flink-2.3, docs, deployment, overview, java-compatibility, standalone, working-directory, docker, kubernetes, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:51:59.700Z'
updated: '2026-07-08T04:51:59.700Z'
importanceScore: 1
---

## Executive Summary

**Executive summary:** Apache Flink 2.3 docs condensation for the deployment section (batch 1 of ~5): deployment overview (cluster building blocks, Application vs Session mode, vendor solutions), Java compatibility (11/17/21), standalone resource provider (overview, working directory, Docker setup, Kubernetes setup). **Why kept:** Part of the standing request to ingest all Flink 2.3 EN docs subpages into org memories; deployment knowledge is needed to operate Flink clusters. Prose near-verbatim; code blocks condensed to `[code: <first-line> ... (N chars)]`, tables to `[table: N rows]`. Source URLs preserved per page.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/overview/

# Deployment

Flink is a versatile framework, supporting many different deployment scenarios in a mix and match fashion.

Below, we briefly explain the building blocks of a Flink cluster, their purpose and available implementations. If you just want to start Flink locally, we recommend setting up a Standalone Cluster.

## Overview and Reference Architecture

The figure below shows the building blocks of every Flink cluster. There is always somewhere a client running. It takes the code of the Flink applications, transforms it into a JobGraph and submits it to the JobManager.

The JobManager distributes the work onto the TaskManagers, where the actual operators (such as sources, transformations and sinks) are running.

When deploying Flink, there are often multiple options available for each building block. We have listed them in the table below the figure.

[table: 10 rows]

### Repeatable Resource Cleanup

Once a job has reached a globally terminal state of either finished, failed or cancelled, the external component resources associated with the job are then cleaned up. In the event of a failure when cleaning up a resource, Flink will attempt to retry the cleanup. You can configure the retry strategy used. Reaching the maximum number of retries without succeeding will leave the job in a dirty state. Its artifacts would need to be cleaned up manually (see the High Availability Services / JobResultStore section for further details). Restarting the very same job (i.e. using the same job ID) will result in the cleanup being restarted without running the job again.

There is currently an issue with the cleanup of CompletedCheckpoints that failed to be deleted while subsuming them as part of the usual CompletedCheckpoint management. These artifacts are not covered by the repeatable cleanup, i.e. they have to be deleted manually, still. This is covered by FLINK-26606.

The application resource cleanup is similar (see the High Availability Services / ApplicationResultStore section for further details).

## Deployment Modes

Flink can execute applications in two modes:
- Application Mode,
- Session Mode.

The above modes differ in:
- the cluster lifecycle and resource isolation guarantees
- whether the application's main() method is executed on the client or on the cluster.

### Application Mode

If the application's main() method is executed on the client side, this process includes downloading the application's dependencies locally, executing the main() to extract a representation of the application that Flink's runtime can understand (i.e. the JobGraph) and ship the dependencies and the JobGraph(s) to the cluster. This makes the Client a heavy resource consumer as it may need substantial network bandwidth to download dependencies and ship binaries to the cluster, and CPU cycles to execute the main(). This problem can be more pronounced when the Client is shared across users.

Building on this observation, the Application Mode creates a cluster per submitted application, and the main() method of the application is executed by the JobManager. Creating a cluster per application can be seen as creating a session cluster shared only among the jobs of a particular application, and turning down when the application finishes. With this architecture, the Application Mode provides the application granularity resource isolation and load balancing guarantees.

The Application Mode builds on an assumption that the user jars are already available on the classpath (usrlib folder) of all Flink components that needs access to it (JobManager, TaskManager). In other words, your application comes bundled with the Flink distribution. This allows the application mode to speed up the deployment / recovery process, by not having to distribute the user jars to the Flink components via RPC as the other deployment modes do.

The application mode assumes that the user jars are bundled with the Flink distribution.

Executing the main() method on the cluster may have other implications for your code, such as any paths you register in your environment using the registerCachedFile() must be accessible by the JobManager of your application.

The Application Mode allows the submission of applications consisting of multiple jobs. The order of job execution is not affected by the deployment mode but by the call used to launch the job. Using execute(), which is blocking, establishes an order and it will lead to the execution of the "next" job being postponed until "this" job finishes. Using executeAsync(), which is non-blocking, will lead to the "next" job starting before "this" job finishes.

The Application Mode allows for multi-job applications (by calling execute() or executeAsync() multiple times in the main() method) but High-Availability is limited in these cases. High-Availability in Application Mode is only supported for applications with a single streaming job or multiple batch jobs. For more details, see FLIP-560.

Additionally, when any of multiple running jobs in Application Mode (submitted for example using executeAsync()) gets cancelled, all jobs will be stopped and the JobManager will shut down by default. This behavior can be configured through the execution.terminate-application-on-any-job-terminated-exceptionally option. Regular job completions (by the sources shutting down) are supported.

### Session Mode

Session mode assumes an already running cluster and uses the resources of that cluster to execute any submitted application. Applications executed in the same (session) cluster use, and consequently compete for, the same resources. This has the advantage that you do not pay the resource overhead of spinning up a full cluster for every submitted job. But, if one of the jobs misbehaves or brings down a TaskManager, then all jobs running on that TaskManager will be affected by the failure. This, apart from a negative impact on the job that caused the failure, implies a potential massive recovery process with all the restarting jobs accessing the filesystem concurrently and making it unavailable to other services. Additionally, having a single cluster running multiple jobs implies more load for the JobManager, who is responsible for the book-keeping of all the jobs in the cluster.

In Session Mode, the application's main() method can be executed either on the client or on the cluster. When submitting applications via Command-Line Interface (CLI) or the SQL Client, the main() method is executed on the client. However, when submitting applications via the REST API /jars/:jarid/run-application, the main() method is executed on the cluster. This provides the same benefits as Application Mode in terms of resource usage and network bandwidth for the client, while still maintaining the shared cluster resource model of Session Mode.

### Summary

In Session Mode, the cluster lifecycle is independent of that of any application running on the cluster and the resources are shared across all applications. The application's main() method can be executed either on the client or on the cluster. Application Mode creates a session cluster per application and executes the application's main() method on the cluster. It thus comes with better resource isolation as the resources are only used by the job(s) launched from a single main() method. This comes at the price of spinning up a dedicated cluster for each application.

## Vendor Solutions

A number of vendors offer managed or fully hosted Flink solutions. None of these vendors are officially supported or endorsed by the Apache Flink PMC. Please refer to vendor maintained documentation on how to use these products.

- AliCloud Realtime Compute — AliCloud
- Amazon EMR — AWS
- Amazon Managed Service for Apache Flink — AWS
- Cloudera Stream Processing — AWS/Azure/Google/On-Premises
- Confluent Cloud and Platform — AWS/Azure/Google/On-Premises
- Huawei Cloud Stream Service — Huawei
- Ververica's Unified Streaming Data Platform (Managed Service / BYOC / Self-Managed) — AliCloud/AWS/Azure/Google/On-Premises

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/java_compatibility/

# Java compatibility

This page lists which Java versions Flink supports and what limitations apply (if any).

## Java 11

Support for Java 11 was added in 1.10.0.

### Untested Flink features
The following Flink features have not been tested with Java 11:
- Hive connector
- Hbase 1.x connector

### Untested language features
- Modularized user jars have not been tested.

## Java 17

We use Java 17 by default in Flink 2.0.0 and is the recommended Java version to run Flink on. This is the default version for docker images.

Support for Java Records was added in Flink 1.19 (FLINK-32380). Java records are handled as POJO types and serialized via their canonical constructor.

### Untested Flink features
These Flink features have not been tested with Java 17:
- Hive connector
- Hbase 1.x connector

## Java 21

Experimental support for Java 21 was added in 2.0.0. (FLINK-33163)

### Untested Flink features
These Flink features have not been tested with Java 21:
- Hive connector
- Hbase 1.x connector

### JDK modularization

Starting with Java 16 Java applications have to fully cooperate with the JDK modularization, also known as Project Jigsaw. This means that access to JDK classes/internal must be explicitly allowed by the application when it is started, on a per-module basis, in the form of –add-opens/–add-exports JVM arguments.

Since Flink uses reflection for serializing user-defined functions and data (via Kryo), this means that if your UDFs or data types use JDK classes you may have to allow access to these JDK classes.

These should be configured via the env.java.opts.all option. In the default configuration in the Flink distribution this option is configured such that Flink itself works on Java 17. The list of configured arguments must not be shortened, but only extended.

### Known issues
- Mandatory Kryo dependency upgrade, FLIP-371.
- SIGSEGV in C2 Compiler thread: Early Java 17 builds are affected by a bug where the JVM can fail abruptly. Update your Java 17 installation to resolve the issue. See JDK-8277529 for details.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/standalone/overview/

# Standalone

## Getting Started

This Getting Started section guides you through the local setup (on one machine, but in separate processes) of a Flink cluster. This can easily be expanded to set up a distributed standalone cluster, which we describe in the reference section.

### Introduction

The standalone mode is the most barebone way of deploying Flink: The Flink services described in the deployment overview are just launched as processes on the operating system. Unlike deploying Flink with a resource provider such as Kubernetes or YARN, you have to take care of restarting failed processes, or allocation and de-allocation of resources during operation.

In the additional subpages of the standalone mode resource provider, we describe additional deployment methods which are based on the standalone mode: Deployment in Docker containers, and on Kubernetes.

### Preparation

Flink runs on all UNIX-like environments, e.g. Linux, Mac OS X, and Cygwin (for Windows). Before you start to setup the system, make sure your system fulfils the following requirements.
- Java 1.8.x or higher installed,
- Downloaded a recent Flink distribution from the download page and unpacked it.

### Starting a Standalone Cluster (Session Mode)

`[code: # we assume to be in the root directory of the unzipped Flink distribu ... (336 chars)]`

In step (1), we've started 2 processes: A JVM for the JobManager, and a JVM for the TaskManager. The JobManager is serving the web interface accessible at localhost:8081. In step (3), we are starting a Flink Client (a short-lived JVM process) that submits an application to the JobManager.

## Deployment Modes

### Application Mode

To start a Flink JobManager with an embedded application, we use the bin/standalone-job.sh script. We demonstrate this mode by locally starting the TopSpeedWindowing.jar example, running on a single TaskManager.

The application jar file needs to be available in the classpath. The easiest approach to achieve that is putting the jar into the lib/ folder:
`[code: $ cp ./examples/streaming/TopSpeedWindowing.jar lib/ ... (52 chars)]`

Then, we can launch the JobManager:
`[code: $ ./bin/standalone-job.sh start --job-classname org.apache.flink.strea ... (111 chars)]`

The web interface is now available at localhost:8081.

Another approach would be to use the artifact fetching mechanism via the --jars option:
`[code: $ ./bin/standalone-job.sh start -D user.artifacts.base-dir=/tmp/flink- ... (125 chars)]`

However, the application won't be able to start, because there are no TaskManagers running yet:
`[code: $ ./bin/taskmanager.sh start ... (28 chars)]`

Note You can start multiple TaskManagers, if your application needs more resources.

Stopping the services is also supported via the scripts. Call them multiple times if you want to stop multiple instances, or use stop-all:
`[code: $ ./bin/taskmanager.sh stop ... (58 chars)]`

### Session Mode

Local deployment in Session Mode has already been described in the introduction above.

## Standalone Cluster Reference

### Configuration

All available configuration options are listed on the configuration page, in particular the Basic Setup section contains good advise on configuring the ports, memory, parallelism etc.

The following scripts also allow configuration parameters to be set via dynamic properties:
- jobmanager.sh
- standalone-job.sh
- taskmanager.sh
- historyserver.sh

Example:
`[code: $ ./bin/jobmanager.sh start -D jobmanager.rpc.address=localhost -D res ... (81 chars)]`

Options set via dynamic properties overwrite the options from Flink configuration file.

### Debugging

If Flink is behaving unexpectedly, we recommend looking at Flink's log files as a starting point for further investigations.

The log files are located in the logs/ directory. There's a .log file for each Flink service running on this machine. In the default configuration, log files are rotated on each start of a Flink service – older runs of a service will have a number suffixed to the log file.

Alternatively, logs are available from the Flink web frontend (both for the JobManager and each TaskManager).

By default, Flink is logging on the "INFO" log level, which provides basic information for all obvious issues. For cases where Flink seems to behave wrongly, reducing the log level to "DEBUG" is advised. The logging level is controlled via the conf/log4.properties file. Setting rootLogger.level = DEBUG will bootstrap Flink on the DEBUG log level.

### Component Management Scripts

#### Starting and Stopping a cluster
bin/start-cluster.sh and bin/stop-cluster.sh rely on conf/masters and conf/workers to determine the number of cluster component instances.

If password-less SSH access to the listed machines is configured, and they share the same directory structure, the scripts also support starting and stopping instances remotely.

##### Example 1: Start a cluster with 2 TaskManagers locally
conf/masters: `localhost`; conf/workers: `localhost` x2

##### Example 2: Start a distributed cluster JobManagers
This assumes a cluster with 4 machines (master1, worker1, worker2, worker3), which all can reach each other over the network. conf/masters: `master1`; conf/workers: `worker1 worker2 worker3`. Note that the configuration key jobmanager.rpc.address needs to be set to master1 for this to work.

#### Starting and Stopping Flink Components
The bin/jobmanager.sh and bin/taskmanager.sh scripts support starting the respective daemon in the background (using the start argument), or in the foreground (using start-foreground). In the foreground mode, the logs are printed to standard out. This mode is useful for deployment scenarios where another process is controlling the Flink daemon (e.g. Docker).

The scripts can be called multiple times, for example if multiple TaskManagers are needed. The instances are tracked by the scripts, and can be stopped one-by-one (using stop) or all together (using stop-all).

#### Windows Cygwin Users
If installing from the git repo using the Windows git shell, Cygwin can produce a line-ending error. The solution is to adjust the Cygwin settings: start a Cygwin shell, find home dir (`cd; pwd`), append `export SHELLOPTS` to `.bash_profile`, save and open a new bash shell.

### Setting up High-Availability

In order to enable HA for a standalone cluster, you have to use the ZooKeeper HA services. Additionally, you have to configure your cluster to start multiple JobManagers.

masters file: contains all hosts on which JobManagers are started, and the ports to which the web UI binds: `master1:webUIPort1`

By default, the JobManager will pick a random port for inter process communication. You can change this via the high-availability.jobmanager.port key. This key accepts single ports (e.g. 50010), ranges (50000-50025), or a combination of both (50010,50011,50020-50025,50050-50075).

#### Example: Standalone HA Cluster with 2 JobManagers
- Configure HA mode and ZooKeeper quorum in Flink config: `[code: high-availability.type: zookeeper ... (261 chars)]`
- Configure masters in conf/masters: `localhost:8081` x2
- Configure ZooKeeper server in conf/zoo.cfg: `server.0=localhost:2888:3888`
- Start ZooKeeper quorum: `$ ./bin/start-zookeeper-quorum.sh`
- Start an HA-cluster: `$ ./bin/start-cluster.sh`
- Stop ZooKeeper quorum and cluster: `$ ./bin/stop-cluster.sh`

### User jars & Classpath

In Standalone mode, the following jars will be recognized as user-jars and included into user classpath:
- Session Mode: The JAR file specified in startup command.
- Application Mode: The JAR file specified in startup command and all JAR files in Flink's usrlib folder.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/standalone/working_directory/

# Working Directory

Flink supports to configure a working directory (FLIP-198) for Flink processes (JobManager and TaskManager). The working directory is used by the processes to store information that can be recovered upon a process restart. The requirement for this to work is that the process is started with the same identity and has access to the volume on which the working directory is stored.

## Configuring the Working Directory

The working directories for the Flink processes are:
- JobManager working directory: <WORKING_DIR_BASE>/jm_<JM_RESOURCE_ID>
- TaskManager working directory: <WORKING_DIR_BASE>/tm_<TM_RESOURCE_ID>

with <WORKING_DIR_BASE> being the working directory base, <JM_RESOURCE_ID> being the resource id of the JobManager process and <TM_RESOURCE_ID> being the resource id of the TaskManager process.

The <WORKING_DIR_BASE> can be configured by process.working-dir. It needs to point to a local directory. If not explicitly configured, then it defaults to a randomly picked directory from io.tmp.dirs.

It is also possible to configure a JobManager and TaskManager specific <WORKING_DIR_BASE> via process.jobmanager.working-dir and process.taskmanager.working-dir respectively.

The JobManager resource id can be configured via jobmanager.resource-id. If not explicitly configured, then it will be a random UUID. Similarly, the TaskManager resource id can be configured via taskmanager.resource-id. If not explicitly configured, then it will be a random value containing the host and port of the running process.

## Artifacts Stored in the Working Directory

Flink processes will use the working directory to store the following artifacts:
- Blobs stored by the BlobServer and BlobCache
- Local state if state.backend.local-recovery is enabled
- RocksDB's working directory

## Local Recovery Across Process Restarts

The working directory can be used to enable local recovery across process restarts (FLIP-201). This means that Flink does not have to recover state information from remote storage.

In order to use this feature, local recovery has to be enabled via state.backend.local-recovery. Moreover, the TaskManager processes need to get a deterministic resource id assigned via taskmanager.resource-id. Last but not least, a failed TaskManager process needs to be restarted with the same working directory.

`[code: process.working-dir: /path/to/working/dir/base ... (170 chars)]`

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/standalone/docker/

# Docker Setup

## Getting Started

This Getting Started section guides you through the local setup (on one machine, but in separate containers) of a Flink cluster using Docker containers.

### Introduction

Docker is a popular container runtime. There are official Docker images for Apache Flink available on Docker Hub. You can use the Docker images to deploy a Session or Application cluster on Docker. This page focuses on the setup of Flink on Docker and Docker Compose.

### Starting a Session Cluster on Docker

A Flink Session cluster can be used to run multiple jobs. Each job needs to be submitted to the cluster after the cluster has been deployed. To deploy a Flink Session cluster with Docker, you need to start a JobManager container. To enable communication between the containers, we first set a required Flink configuration property and create a network:

`[code: $ FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" ... (93 chars)]`

Then we launch the JobManager: `[code: $ docker run \ ... (194 chars)]`
and one or more TaskManager containers: `[code: $ docker run \ ... (170 chars)]`

The web interface is now available at localhost:8081. Submission of a job: `$ ./bin/flink run ./examples/streaming/TopSpeedWindowing.jar`. To shut down, terminate the processes or use docker ps/docker stop.

## Deployment Modes

The Flink image contains a regular Flink distribution with its default configuration and a standard entry point script. You can run its entry point as: JobManager for a Session cluster; JobManager for an Application cluster; TaskManager for any cluster. This allows you to deploy a standalone cluster (Session or Application Mode) in any containerised environment, for example: manually in a local Docker setup, in a Kubernetes cluster, with Docker Compose. Note: native Kubernetes also runs the same image by default and deploys TaskManagers on demand.

We recommend using Docker Compose for deploying Flink in Session Mode to ease system configuration.

### Application Mode

A Flink Application cluster is a dedicated cluster which runs a single job. In this case, you deploy the cluster with the job as one step, thus, there is no extra job submission needed. The job artifacts are included into the class path of Flink's JVM process within the container and consist of: your job jar, and all other necessary dependencies or resources, not included into Flink.

To deploy a cluster for a single job with Docker, you need to:
- make job artifacts available locally in all containers under /opt/flink/usrlib, or pass a list of jars via the --jars argument
- start a JobManager container in the Application cluster mode
- start the required number of TaskManager containers.

To make the job artifacts available locally in the container, you can:
- mount a volume (or volumes) with the artifacts to /opt/flink/usrlib: `[code: $ FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" ... (902 chars)]`
- extend the Flink image by writing a custom Dockerfile, build it and use it: `[code: FROM flink ... (138 chars)]`, `[code: $ docker build --tag flink_with_job_artifacts . ... (314 chars)]`
- pass jar path by jars argument when you start the JobManager: `[code: $ FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" ... (555 chars)]`

The standalone-job argument starts a JobManager container in the Application Mode.

#### JobManager additional command line arguments
- --job-classname <job class name> (optional): Class name of the job to run. By default, Flink scans its class path for a JAR with a Main-Class or program-class manifest entry and chooses it as the job class. Required in case no or more than one JAR with such a manifest entry is available.
- --job-id <job id> (optional): Manually set a Flink job ID (default: 00000000000000000000000000000000)
- --fromSavepoint /path/to/savepoint (optional): Restore from a savepoint. /path/to/savepoint needs to be accessible in all Docker containers (e.g. on a DFS or mounted volume or added to the image).
- --allowNonRestoredState (optional): Skip broken savepoint state.
- --jars (optional): the paths of the job jar and any additional artifact(s) separated by commas. Flink will fetch these during job deployment (e.g. --jars s3://my-bucket/my-flink-job.jar, --jars s3://my-bucket/my-flink-job.jar,s3://my-bucket/my-flink-udf.jar).

If the main function of the user job main class accepts arguments, you can also pass them at the end of the docker run command.

### Session Mode

Local deployment in the Session Mode has already been described in the Getting Started section above.

## Flink Docker Images

### Image Hosting
Two distribution channels: Official Flink images on Docker Hub (reviewed and build by Docker); Flink images on Docker Hub apache/flink (managed by the Flink developers). We recommend the official images. Launching an image named flink:latest will pull the latest from Docker Hub. To use apache/flink, replace flink by apache/flink.

### Image Tags
The Flink Docker repository is hosted on Docker Hub and serves images of Flink version 1.2.1 and later. Images for each supported combination of Flink and Scala versions are available, with tag aliases:
- flink:latest → flink:<latest-flink>-scala_<latest-scala>
- flink:1.11 → flink:1.11.<latest-flink-1.11>-scala_2.12

Note: It is recommended to always use an explicit version tag specifying both Flink and Scala versions (e.g. flink:1.11-scala_2.12) to avoid class conflicts. Note: Prior to Flink 1.5, Hadoop dependencies were always bundled; tags like -hadoop28. Beginning with Flink 1.5, image tags that omit the Hadoop version correspond to Hadoop-free releases.

## Flink with Docker Compose

Docker Compose is a way to run a group of Docker containers locally.

### General
- Create the docker-compose.yaml file.
- Launch a cluster in the foreground (use -d for background): `$ docker compose up`
- Scale the cluster up or down to N TaskManagers: `$ docker compose scale taskmanager=<N>`
- Access the JobManager container: `$ docker exec -it $(docker ps --filter name=jobmanager --format={{.ID}}) /bin/sh`
- Kill the cluster: `$ docker compose down`
- Access Web UI at http://localhost:8081.

### Application Mode
In application mode you start a Flink cluster dedicated to run only the Flink Jobs bundled with the images. You need to build a dedicated Flink Image per application. docker-compose.yml: `[code: version: "2.2" ... (871 chars)]`

### Session Mode
In Session Mode you use docker compose to spin up a long-running Flink Cluster to which you can then submit Jobs. docker-compose.yml: `[code: version: "2.2" ... (491 chars)]`

### Flink SQL Client with Session Cluster
docker-compose.yml: `[code: version: "2.2" ... (742 chars)]`. Start the SQL Client: `docker compose run sql-client`. Note that all required dependencies (e.g. for connectors) need to be available in the cluster as well as the client. For the SQL Kafka Connector, build a custom image: `[code: FROM flink:2.3.0-scala_2.12 ... (252 chars)]` and reference the build command for jobmanager, taskmanager and sql-client services. SQL Commands like ADD JAR will not work for JARs located on the host machine as they only work with the local filesystem (Docker's overlay filesystem).

## Using Flink Python on Docker
`[code: FROM flink:2.3.0 ... (253 chars)]`. Build: `$ docker build --tag pyflink:latest .`

## Configuring Flink on Docker

### Via dynamic properties
`[code: $ docker run flink:2.3.0-scala_2.12 \ ... (204 chars)]`. Options set via dynamic properties overwrite the options from Flink configuration file.

### Via Environment Variables
Set the environment variable FLINK_PROPERTIES: `[code: $ FLINK_PROPERTIES="jobmanager.rpc.address: host ... (225 chars)]`. The jobmanager.rpc.address option must be configured, others optional. FLINK_PROPERTIES contains a list of Flink cluster configuration options separated by new line. FLINK_PROPERTIES takes precedence over configurations in Flink configuration file.

### Via Flink configuration file
Configuration files located in /opt/flink/conf. To provide a custom location, mount a volume to /opt/flink/conf or add to a custom image. The mounted volume must contain all necessary configuration files. The Flink configuration file must have write permission so the Docker entry point script can modify it.

### Using Filesystem Plugins
Plugins must be copied to the correct location in the Flink installation. To enable plugins provided with Flink (in opt/), pass ENABLE_BUILT_IN_PLUGINS — a list of plugin jar file names separated by ; (e.g. flink-s3-fs-hadoop-2.3.0.jar). `[code: $ docker run \ ... (165 chars)]`

### Switching the Memory Allocator
Flink introduced jemalloc as default memory allocator to resolve memory fragmentation (FLINK-19125). Switch back to glibc by setting DISABLE_JEMALLOC=true. For glibc users, set MALLOC_ARENA_MAX to avoid unlimited memory growth (especially during savepoints/full checkpoints with RocksDBStateBackend).

### Further Customization
- install custom software (e.g. python)
- enable (symlink) optional libraries or plugins from /opt/flink/opt into /opt/flink/lib or /opt/flink/plugins
- add other libraries to /opt/flink/lib (e.g. Hadoop)
- add other plugins to /opt/flink/plugins

You can: override the container entry point with a custom bootstrap script (calling /docker-entrypoint.sh at the end); or extend the Flink image via a custom Dockerfile.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/standalone/kubernetes/

# Kubernetes Setup

## Getting Started

This Getting Started guide describes how to deploy a Session cluster on Kubernetes.

### Introduction

This page describes deploying a standalone Flink cluster on top of Kubernetes, using Flink's standalone deployment. We generally recommend new users to deploy Flink on Kubernetes using native Kubernetes deployments.

Apache Flink also provides a Kubernetes operator for managing Flink clusters on Kubernetes. It supports both standalone and native deployment mode and greatly simplifies deployment, configuration and the life cycle management of Flink resources on Kubernetes.

### Preparation

This guide expects a Kubernetes environment to be present. Verify with `kubectl get nodes`. For local Kubernetes, use MiniKube. Note: If using MiniKube, execute `minikube ssh 'sudo ip link set docker0 promisc on'` before deploying, otherwise Flink components can't reference themselves through a Kubernetes service.

### Starting a Kubernetes Cluster (Session Mode)

A Flink Session cluster is executed as a long-running Kubernetes Deployment. A Flink Session cluster deployment in Kubernetes has at least three components:
- a Deployment which runs a JobManager
- a Deployment for a pool of TaskManagers
- a Service exposing the JobManager's REST and UI ports

Create the respective components with kubectl using the common resource definitions. Then set up a port forward to access the Flink UI and submit jobs:
- `kubectl port-forward ${flink-jobmanager-pod} 8081:8081`
- Navigate to http://localhost:8081
- Submit jobs: `$ ./bin/flink run -m localhost:8081 ./examples/streaming/TopSpeedWindowing.jar`

Tear down: `$ kubectl delete -f jobmanager-service.yaml ...` etc.

## Deployment Modes

### Application Mode

A Flink Application cluster is a dedicated cluster which runs a single application, available at deployment time. A basic deployment has three components: an Application which runs a JobManager; a Deployment for a pool of TaskManagers; a Service exposing the JobManager's REST and UI ports.

The args attribute in jobmanager-application-non-ha.yaml has to specify the main class of the user job. Job artifacts could be provided by:
- the job-artifacts-volume in the resource definition examples (mounted as a local directory; minikube assumed)
- a custom image which already contains the artifacts
- the --jars option (stored locally, on remote DFS, or accessible via HTTP(S) endpoint)

Launch: `$ kubectl create -f jobmanager-application-non-ha.yaml`. Terminate: `$ kubectl delete -f taskmanager-job-deployment.yaml` etc.

### Session Mode

Deployment of a Session cluster is explained in the Getting Started guide at the top of this page.

## Flink on Standalone Kubernetes Reference

### Configuration

All configuration options are listed on the configuration page. Configuration options can be added to the Flink configuration file section of the flink-configuration-configmap.yaml config map.

### Accessing Flink in Kubernetes
- kubectl proxy: Run `kubectl proxy`; navigate to http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:webui/proxy.
- kubectl port-forward: `kubectl port-forward ${flink-jobmanager-pod} 8081:8081`; navigate to http://localhost:8081; submit jobs `$ ./bin/flink run -m localhost:8081 ...`.
- Create a NodePort service on the rest service of jobmanager: `kubectl create -f jobmanager-rest-service.yaml`; `kubectl get svc flink-jobmanager-rest` to know the node-port; navigate to http://<public-node-ip>:<node-port>. With minikube, get public ip via `minikube ip`.

### Debugging and Log Access

Check Flink's log files. Use `kubectl get pods` to see all running pods (three pods for the quickstart). Access logs via `kubectl logs flink-jobmanager-589967dcfc-m49xv`.

### High-Availability with Standalone Kubernetes

#### Kubernetes High-Availability Services
Session Mode and Application Mode clusters support using the Kubernetes high availability service. Add config options to flink-configuration-configmap.yaml. Note: The filesystem corresponding to the scheme of your configured HA storage directory must be available to the runtime. `[code: apiVersion: v1 ... (343 chars)]`. Start JobManager and TaskManager pods with a service account having permissions to create, edit, delete ConfigMaps. When HA is enabled, Flink uses its own HA-services for service discovery; JobManager pods should be started with their IP address instead of a Kubernetes service as jobmanager.rpc.address.

#### Standby JobManagers
Usually a single JobManager pod suffices because Kubernetes restarts crashed pods. For faster recovery, configure replicas in jobmanager-session-deployment-ha.yaml or parallelism in jobmanager-application-ha.yaml > 1 to start standby JobManagers.

### Using Standalone Kubernetes with Reactive Mode

Reactive Mode runs Flink where the Application Cluster always adjusts job parallelism to available resources. With Kubernetes, the replica count of the TaskManager deployment determines available resources. Increasing replicas scales up, reducing triggers scale down. Can be automated with a Horizontal Pod Autoscaler.

To use Reactive Mode on Kubernetes, follow the Application Cluster steps but use flink-reactive-mode-configuration-configmap.yaml instead (contains scheduler-mode: reactive). Scale by changing the replica count in the flink-taskmanager deployment.

### Enabling Local Recovery Across Pod Restarts

Leverage Flink's working directory feature with local recovery. If the working directory resides on a persistent volume remounted to a restarted TaskManager pod, Flink recovers state locally. With StatefulSet, Kubernetes maps a pod to a persistent volume. Deploy TaskManagers as a StatefulSet to configure a volume claim template, and configure a deterministic taskmanager.resource-id (a suitable value is the pod name, exposed via environment variables).

## Appendix

### Common cluster resource definitions
- flink-configuration-configmap.yaml: `[code: apiVersion: v1 ... (2364 chars)]`
- flink-reactive-mode-configuration-configmap.yaml: `[code: apiVersion: v1 ... (2435 chars)]`
- jobmanager-service.yaml (optional, non-HA mode only): `[code: apiVersion: v1 ... (246 chars)]`
- jobmanager-rest-service.yaml (optional, exposes jobmanager rest port as public node port): `[code: apiVersion: v1 ... (224 chars)]`

### Session cluster resource definitions
- jobmanager-session-deployment-non-ha.yaml: `[code: apiVersion: apps/v1 ... (1162 chars)]`
- jobmanager-session-deployment-ha.yaml: `[code: apiVersion: apps/v1 ... (1640 chars)]`
- taskmanager-session-deployment.yaml: `[code: apiVersion: apps/v1 ... (1058 chars)]`

### Application cluster resource definitions
- jobmanager-application-non-ha.yaml: `[code: apiVersion: batch/v1 ... (1630 chars)]`
- jobmanager-application-ha.yaml: `[code: apiVersion: batch/v1 ... (2134 chars)]`
- taskmanager-job-deployment.yaml: `[code: apiVersion: apps/v1 ... (1244 chars)]`

### Local Recovery Enabled TaskManager StatefulSet
`[code: apiVersion: v1 ... (1975 chars)]`