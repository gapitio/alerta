Alerta API — Gapit fork
=======================

[![Tests](https://github.com/gapitio/alerta/actions/workflows/tests.yml/badge.svg)](https://github.com/gapitio/alerta/actions/workflows/tests.yml)

> **This repository is a fork of [alerta/alerta](https://github.com/alerta/alerta).**
> It is maintained by [Gapit](https://github.com/gapitio) and tracks upstream
> release **9.0.3**, with a set of additional features focused on notifications,
> on-call handling, escalation and alarm management. Credit for the original
> Alerta server goes to Nick Satterly and the upstream contributors.

Docker images are published to `ghcr.io/gapitio/alerta-api` on every tag.

What this fork adds
-------------------

### Notifications

*   **Notification rules** (`/notificationrules`) – send notifications when alerts
    match a rule. Rules can match on environment, service, resource, event, tags
    (with `all`/`any` and negation `!`), excluded tags, severity/status
    **triggers** (each with its own message text), users, notification groups,
    time windows and days. Rules can be muted/deactivated and given a
    **reactivate date**, and have a custom name and searchable fields. The
    alerts a rule would trigger on can be listed via
    `/notificationrules/<id>/alerts`.
*   **Notification channels** (`/notificationchannels`) – the delivery backends a
    rule sends through. Supported channel types:
    *   Twilio SMS and voice call (auth token or API key)
    *   SendGrid e-mail (with subject field)
    *   SMTP e-mail
    *   Link Mobility (XML)
    *   MyLink SMS

    Channel credentials are stored encrypted with Fernet using the
    `NOTIFICATION_KEY` setting (`/notificationchannels/keygen` generates a key).
    Channels can be tested with `/notificationchannels/<id>/test`.
*   **Notification groups** (`/notificationgroups`) – reusable groups of users,
    phone numbers and e-mail addresses that rules can send to.
*   **Notification delay** (`/notificationdelay`) – postpone notifications so
    alerts that clear quickly do not page anyone.
*   **Notification history** (`/notificationhistory`) – log of every sent
    notification with receivers and sent status, filterable, plus per-rule
    history at `/notificationrules/<id>/history`.
*   **Notification sends** (`/notificationsends`) – send custom SMS/e-mail
    messages to users and notification groups on demand.
*   Implemented as the `notification_rule` plugin, which is enabled by default.

### On-call and escalation

*   **On-call schedules** (`/oncalls`) – define which users/groups are on call
    for date ranges and times, with daily/weekly/monthly repeat patterns and
    time offsets; notification rules
    send to whoever is currently on call.
*   **Escalation rules** (`/escalationrules`, `/escalate`) – automatically
    escalate alerts that match a rule (environment, service, resource, event,
    tags, triggers, days/times) and remain unhandled for a given time.

### Alerts and alarm models

*   **`ALERTA_ISA_18_2` alarm model** – a combination of the Alerta and ISA 18.2
    alarm models (open, unack, close actions, etc.). Only severities defined by
    the active alarm model are accepted.
*   **Notes plugin** (`notes`) – removes notes from an alert when it returns to
    normal severity.
*   **Bulk operations** (`/_bulk/alerts/...`) – bulk post, delete, action, tag,
    untag and attribute updates of alerts.
*   **Filter tabs** (`/filtertabs`) – saved filters for the alert and history
    views, with alert counts per tab.
*   **Improved filtering and search** – filter per alert column, wildcards, `!`
    negation, multiple values for attribute columns, new tagging structure.
*   **History improvements** – history entries keep the `alert_id` they belong
    to, better pagination and environment-based filtering.
*   **Return keys** – clients can request only specific alert fields to reduce
    response size, plus a page size of `all` in the alert search view.
*   **Scoped alert actions** – permissions/scopes per alert action.
*   **Shelved/acked timeouts** replace the generic alert timeout. Correlation of
    events and the `group`, `repeat`, `trendIndication` and `type` alert fields
    have been removed; duplicate and correlated alerts update all fields.
*   **Reports** – CSV export of top offenders, standing and flapping alerts.
*   **Export of manual setup** configuration.

### Heartbeats

*   Proxies to an external heartbeat service (`/heartbeats`, `/heartbeat/<id>`)
    configured with `HEARTBEAT_URL`, `HEARTBEAT_KEY` and `HEARTBEAT_VERIFY`.
    `Heartbeats` is included in `ALLOWED_ENVIRONMENTS` by default.

### Authentication and users

*   OIDC scopes (`OIDC_SCOPES`), OIDC roles stored on saved users and
    `OIDC_VERIFY_SSL`.
*   `LDAP_ALLOW_EMAIL_EDIT` setting; HTTP 422 when an LDAP user is missing a
    required attribute.
*   Users are referenced by e-mail/ID consistently across notification rules,
    groups and on-call, and phone numbers are returned alongside e-mails.

### Configuration and build

*   New settings: `NOTIFICATION_KEY`, `HIDDEN_PAGES` (hide pages in the web UI
    navigation bar), `LDAP_ALLOW_EMAIL_EDIT`, `OIDC_SCOPES`, `OIDC_VERIFY_SSL`,
    `HEARTBEAT_*`.
*   Changed defaults: `SORT_LIST_BY = ['-severity', 'lastReceiveTime']`,
    `SHELVE_TIMEOUT = 0`, `notification_rule` added to `PLUGINS`; the
    housekeeping `DELETE_EXPIRED_AFTER`/`DELETE_INFO_AFTER` settings were removed.
*   The Gapit version is exposed in the `/config` endpoint.
*   Builds and tests target Python 3.14 and PostgreSQL 18; `pkg_resources`
    replaced by `importlib`; the Twilio and SendGrid SDKs replaced by plain HTTP
    requests. No PyPI release — install from source or use the Docker image.

See the [git history](https://github.com/gapitio/alerta/commits/master) and
[pull requests](https://github.com/gapitio/alerta/pulls?q=is%3Apr) for details.
([CHANGELOG.md](CHANGELOG.md) only covers upstream releases.)

----

About Alerta
------------

The Alerta monitoring tool was developed with the following aims in mind:

*   distributed and de-coupled so that it is **SCALABLE**
*   minimal **CONFIGURATION** that easily accepts alerts from any source
*   quick at-a-glance **VISUALISATION** with drill-down to detail

![webui](/docs/images/alerta-webui-v7.jpg?raw=true)

----

Requirements
------------

Release 9 only supports Python 3.9 or higher.

The only mandatory dependency is MongoDB or PostgreSQL. Everything else is optional.

- Postgres version 13 or better
- MongoDB version 6.0 or better

Installation
------------

To install MongoDB on Debian/Ubuntu run:

    $ sudo apt-get install -y mongodb-org
    $ mongod

To install MongoDB on CentOS/RHEL run:

    $ sudo yum install -y mongodb
    $ mongod

To install the Alerta server and client run:

    $ pip install alerta-server alerta
    $ alertad run

To install the web console run:

    $ wget https://github.com/alerta/alerta-webui/releases/latest/download/alerta-webui.tar.gz
    $ tar zxvf alerta-webui.tar.gz
    $ cd dist
    $ python3 -m http.server 8000

    >> browse to http://localhost:8000

### Docker
Alerta and MongoDB can also run using Docker containers, see [alerta/docker-alerta](https://github.com/alerta/docker-alerta).

Configuration
-------------

To configure the ``alertad`` server override the default settings in ``/etc/alertad.conf``
or using ``ALERTA_SVR_CONF_FILE`` environment variable::

    $ ALERTA_SVR_CONF_FILE=~/.alertad.conf
    $ echo "DEBUG=True" > $ALERTA_SVR_CONF_FILE

Documentation
-------------

More information on configuration and other aspects of alerta can be found
at <http://docs.alerta.io>

Development
-----------

To run in development mode, listening on port 5000:

    $ export FLASK_APP=alerta FLASK_DEBUG=1
    $ pip install -e .
    $ flask run

To run in development mode, listening on port 8080, using Postgres and
reporting errors to [Sentry](https://sentry.io):

    $ export FLASK_APP=alerta FLASK_DEBUG=1
    $ export DATABASE_URL=postgres://localhost:5432/alerta5
    $ export SENTRY_DSN=https://8b56098250544fb78b9578d8af2a7e13:fa9d628da9c4459c922293db72a3203f@sentry.io/153768
    $ pip install -e .[postgres]
    $ flask run --debugger --port 8080 --with-threads --reload

Troubleshooting
---------------

Enable debug log output by setting `DEBUG=True` in the API server
configuration:

```
DEBUG=True

LOG_HANDLERS = ['console','file']
LOG_FORMAT = 'verbose'
LOG_FILE = '$HOME/alertad.log'
```

It can also be helpful to check the web browser developer console for
JavaScript logging, network problems and API error responses.

Tests
-----

To run the *all* the tests there must be a local Postgres
and MongoDB database running. Then run:

    $ TOXENV=ALL make test

To just run the Postgres or MongoDB tests run:

    $ TOXENV=postgres make test
    $ TOXENV=mongodb make test

To run a single test run something like:

    $ TOXENV="mongodb -- tests/test_search.py::QueryParserTestCase::test_boolean_operators" make test
    $ TOXENV="postgres -- tests/test_queryparser.py::PostgresQueryTestCase::test_boolean_operators" make test

Cloud Deployment
----------------

Alerta can be deployed to the cloud easily using Heroku <https://github.com/alerta/heroku-api-alerta>,
AWS EC2 <https://github.com/alerta/alerta-cloudformation>, or Google Cloud Platform
<https://github.com/alerta/gcloud-api-alerta>

License
-------

    Alerta monitoring system and console
    Copyright 2012-2023 Nick Satterly

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
