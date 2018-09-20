# Sherlock: Anomaly Detector

[![Build Status](https://travis-ci.org/yahoo/sherlock.svg?branch=master)](https://travis-ci.org/yahoo/sherlock)
[![Coverage Status](https://coveralls.io/repos/github/yahoo/sherlock/badge.svg?branch=master)](https://coveralls.io/github/yahoo/sherlock?branch=master)
[![GPL 3.0](https://img.shields.io/badge/license-GPL%203.0-blue.svg?style=flat)](LICENSE)

## Table of Contents

  * [Introduction](#introduction-to-sherlock)
  * [Components](#components)
  * [Detailed Description](#detailed-description)
  * [How to build](#how-to-build)
      * [Build JAR](#build-jar)
      * [How to run](#how-to-run)
      * [CLI args usage](#cli-args-usage)
  * [Committers](#committers)
  * [Contributors](#contributors)
  * [License](#license)

## Introduction to Sherlock

Sherlock is an anomaly detection service built on top of [Druid](http://druid.io/). It leverages [EGADS (Extensible Generic Anomaly Detection System)](https://github.com/yahoo/egads) to detect anomalies in time-series data. Users can schedule jobs on an hourly, daily, weekly, or monthly basis, view anomaly reports from Sherlock's interface, or receive them via email.

## Components

1. [Timeseries Generation](#timeseries-generation)
2. [EGADS Anomaly Detection](#egads-anomaly-detection)
3. [Redis database](#redis-database)
4. [UI in Spark Java](#ui-in-spark-java)

## Detailed Description

### Timeseries Generation
    
Timeseries generation is the first phase of Sherlock's anomaly detection. The user inputs a full Druid JSON query with a metric name and group-by dimensions. Sherlock validates the query, adjusts the time interaval and granularity based on the EGADS config, and makes a call to Druid. Druid responds with an array of time-series, which are parsed into EGADS time-series.
    
#### Sample Druid Query:
```json
{
  "metric": "metric(metric1/metric2)", 
  "aggregations": [
    {
      "filter": {
        "fields": [
          {
            "type": "selector", 
            "dimension": "dim1", 
            "value": "value1"
          }
        ], 
        "type": "or"
      }, 
      "aggregator": {
        "fieldName": "metric2", 
        "type": "longSum", 
        "name": "metric2"
      }, 
      "type": "filtered"
    }
  ], 
  "dimension": "groupByDimension", 
  "intervals": "2017-09-10T00:00:01+00:00/2017-10-12T00:00:01+00:00", 
  "dataSource": "source1", 
  "granularity": {
    "timeZone": "UTC", 
    "type": "period", 
    "period": "P1D"
  }, 
  "threshold": 50, 
  "postAggregations": [
    {
      "fields": [
        {
          "fieldName": "metric1", 
          "type": "fieldAccess", 
          "name": "metric1"
        }
      ], 
      "type": "arithmetic", 
      "name": "metric(metric1/metric2)", 
      "fn": "/"
    }
  ], 
  "queryType": "topN"
}
```
#### Sample Druid Response:
```json
[ {
  "timestamp" : "2017-10-11T00:00:00.000Z",
  "result" : [ {
    "groupByDimension" : "dim1",
    "metric(metric1/metric2)" : 8,
    "metric1" : 128,
    "metric2" : 16
  }, {
    "groupByDimension" : "dim2",
    "metric(metric1/metric2)" : 4.5,
    "metric1" : 42,
    "metric2" : 9.33
  } ]
}, {
  "timestamp" : "2017-10-12T00:00:00.000Z",
  "result" : [ {
    "groupByDimension" : "dim1",
    "metric(metric1/metric2)" : 9,
    "metric1" : 180,
    "metric2" : 20
  }, {
    "groupByDimension" : "dim2",
    "metric(metric1/metric2)" : 5.5,
    "metric1" : 95,
    "metric2" : 17.27
  } ]
} ]
```
### EGADS Anomaly Detection

Sherlock calls the user-configured EGADS API for each generated time-series, generates anomaly reports from the response, and stores these reports in a database. Users may also elect to receive anomaly reports by email.
    
### Redis Database

Sherlock uses a Redis backend [Redis](https://redis.io/) to store job metadata, generated anomaly reports, among other information, and as a persistent job queue. Keys related to Reports have retention policy. Hourly job reports have retention of 14 days and daily/weekly/monthly job reports have 1 year of retention.   
    
### Sherlock UI

Sherlock's user interface is built with [Spark](http://sparkjava.com/). The UI enables users to submit instant anomaly analyses, create and launch detection jobs, view anomalies on a heatmap, and on a graph.

## Building Sherlock

A `Makefile` is provided with all build targets.

### Building the JAR

```bash
make jar
```

This creates `sherlock.jar` in the `target/` directory.

### How to run
Sherlock is run through the commandline with config arguments.

```bash
java -Dlog4j.configuration=file:${path_to_log4j}/log4j.properties \
      -jar ${path_to_jar}/sherlock.jar \
      --version $(VERSION) \
      --project-name $(PROJECT_NAME) \
      --port $(PORT) \
      --enable-email \
      --failure-email $(FAILURE_EMAIL) \
      --from-mail $(FROM_MAIL) \
      --reply-to $(REPLY_TO) \
      --smtp-host $(SMTP_HOST) \
      --interval-minutes $(INTERVAL_MINUTES) \
      --interval-hours $(INTERVAL_HOURS) \
      --interval-days $(INTERVAL_DAYS) \
      --interval-weeks $(INTERVAL_WEEKS) \
      --interval-months $(INTERVAL_MONTHS) \
      --egads-config-filename $(EGADS_CONFIG_FILENAME) \
      --redis-host $(REDIS_HOSTNAME) \
      --redis-port $(REDIS_PORT) \
      --execution-delay $(EXECUTION_DELAY) \
      --timeseries-completeness $(TIMESERIES_COMPLETENESS)
```

### CLI args usage

| args                      | required            | default     | description                                         |
|-------------------------  |---------------------|-------------|-------------------------------------------------    |
| --help                    |    -                | `false`     | [help](#help)                                       |
| --config                  |    -                | `null`      | [config](#config)                                   |
| --version                 |    -                | `v0.0.0`    | [version](#version)                                 |
| --egads-config-filename   |    -                | `provided`  | [egads-config-filename](#egads-config-filename)     |
| --port                    |    -                | `4080`      | [port](#port)                                       |
| --interval-minutes        |    -                | `180`        | [interval-minutes](#interval-minutes)                   |
| --interval-hours          |    -                | `672`       | [interval-hours](#interval-hours)                   |
| --interval-days           |    -                | `28`        | [interval-days](#interval-days)                     |
| --interval-weeks          |    -                | `12`        | [interval-weeks](#interval-weeks)                   |
| --interval-months         |    -                | `6`         | [interval-months](#interval-months)                 |
| --enable-email            |    -                | `false`     | [enable-email](#enable-email)                       |
| --from-mail               | if email `enabled`  |             | [from-mail](#from-mail)                             |
| --reply-to                | if email `enabled`  |             | [reply-to](#reply-to)                               |
| --smtp-host               | if email `enabled`  |             | [smtp-host](#smtp-host)                             |
| --smtp-port               |    -                | `25`        | [smtp-port](#smtp-port)                             |
| --failure-email           | if email `enabled`  |             | [failure-email](#failure-email)                     |
| --execution-delay         |    -                | `30`        | [execution-delay](#execution-delay)                 |
| --valid-domains           |    -                | `null`      | [valid-domains](#valid-domains)                     |
| --redis-host              |    -                | `127.0.0.1` | [redis-host](#redis-host)                           |
| --redis-port              |    -                | `6379`      | [redis-port](#redis-port)                           |
| --redis-ssl               |    -                | `false`     | [redis-ssl](#redis-ssl)                             |
| --redis-timeout           |    -                | `5000`      | [redis-timeout](#redis-timeout)                     |
| --redis-password          |    -                |  -          | [redis-password](#redis-password)                   |
| --redis-clustered         |    -                | `false`     | [redis-clustered](#redis-clustered)                 |
| --project-name            |    -                |  -          | [project-name](#project-name)                       |
| --external-file-path      |    -                |  -          | [external-file-path](#external-file-path)           |
| --debug-mode              |    -                | `false`     | [debug-mode](#debug-mode)                           |
| --timeseries-completeness |    -                | `60`        | [timeseries-completeness](#timeseries-completeness) |
| --http-client-timeout     |    -                | `20000`     | [http-client-timeout](#http-client-timeout)         |

#### help
Prints commandline argument help message.
#### config
Path to a Sherlock configuration file, where the above configuration may be specified. Config arguments in the file override commandline arguments.
#### version
Version of `sherlock.jar` to display on the UI 
#### egads-config-filename
Path to a custom EGADS configuration file. If none is specified, the default configuration is used.
#### port
Port on which to host the Spark application.
#### interval-minutes
Number of historic data points to use for detection on time-series every minute.
#### interval-hours
Number of historic data points to use for detection on hourly time-series.
#### interval-days
Number of historic data points to use for detection on daily time-series.
#### interval-weeks
Number of historic data points to use for detection on weekly time-series.
#### interval-months
Number of historic data points to use for detection on monthly time-series.
#### enable-email
Enable the email service. This enables users to receive email anomaly report notifications.
#### from-mail
The handle's `FROM` email displayed to email recipients.
#### reply-to
The handle's `REPLY TO` email where replies will be sent.
#### smtp-host
The email service's `SMTP HOST`.
#### smtp-port
The email service's `SMTP PORT`. The default value is `25`.
#### failure-email
A dedicated email which may be set to receive job failure notifications.
#### execution-delay
Sherlock periodically pings Redis to check scheduled jobs. This sets the ping delay in seconds. Jobs are scheduled with a precision of one minute.
#### valid-domains
A comma-separated list of valid domains to receive emails, e.g. 'yahoo,gmail,hotmail'. If specified, Sherlock will restrict who may receive emails.
#### redis-host
The Redis backend hostname.
#### redis-port
The Redis backend port.
#### redis-ssl
Whether Sherlock should connect to Redis via SSL.
#### redis-timeout
The Redis connection timeout.
#### redis-password
The password to use when authenticating to Redis.
#### redis-clustered
Whether the Redis backend is a cluster.
#### project-name
Name of the project to display on UI.
#### external-file-path
Specify the path to external files for Spark framework via this argument.
#### debug-mode
Debug mode enables debug routes. Ex. '/DatabaseJson' (shows redis data as json dump). Look at `com.yahoo.sherlock.App` for more details. 
#### timeseries-completeness
This defines minimum fraction of datapoints needed in the timeseries to consider it as a valid timeseries o/w sherlock ignores such timeseries. (default value 60 i.e. 0.6 in fraction)
#### http-client-timeout
HttpClient timeout can be configured using this(in millis). (default value 20000)

## Committers

Jigar Patel, [jigsdevbox@gmail.com](mailto:jigsdevbox@gmail.com)

Jeff Niu, [me@jeffniu.com](mailto:me@jeffniu.com)

## Contributors

Josh Walters, [josh@joshwalters.com](mailto:josh@joshwalters.com)

## License

Code licensed under the [GPL v3 License](https://www.gnu.org/licenses/quick-guide-gplv3.en.html). See LICENSE file for terms.
