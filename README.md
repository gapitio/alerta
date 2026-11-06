Alerta Release 9.1
==================

[![Actions Status](https://github.com/alerta/alerta/workflows/CI%20Tests/badge.svg)](https://github.com/alerta/alerta/actions)
[![Slack chat](https://img.shields.io/badge/chat-on%20slack-blue?logo=slack)](https://slack.alerta.dev)
[![Coverage Status](https://coveralls.io/repos/github/alerta/alerta/badge.svg?branch=master)](https://coveralls.io/github/alerta/alerta?branch=master)
[![Docker Pulls](https://img.shields.io/docker/pulls/alerta/alerta-web.svg)](https://hub.docker.com/r/alerta/alerta-web)

The Alerta monitoring tool was developed with the following aims in mind:

*   distributed and de-coupled so that it is **SCALABLE**
*   minimal **CONFIGURATION** that easily accepts alerts from any source
*   quick at-a-glance **VISUALISATION** with drill-down to detail

![webui](/docs/images/alerta-webui-v7.jpg?raw=true)

----

Requirements
------------

Release 9 only supports Python 3.11 or higher.

The only mandatory dependency is PostgreSQL version 13 or better. Everything else is optional.

Installation
------------

To install PostgreSQL on Debian/Ubuntu run:

    $ sudo apt-get install -y postgresql libpq-dev

To install PostgreSQL on CentOS/RHEL run:

    $ sudo yum install -y postgresql-server postgresql-devel
    $ sudo postgresql-setup --initdb
    $ sudo systemctl start postgresql

The `libpq-dev` / `postgresql-devel` packages are needed to build `psycopg2`.

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
Alerta and PostgreSQL can also run using Docker containers, see [alerta/docker-alerta](https://github.com/alerta/docker-alerta).

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

To run in development mode, listening on port 8080, using a specific Postgres database:

    $ export FLASK_APP=alerta FLASK_DEBUG=1
    $ export DATABASE_URL=postgres://localhost:5432/alerta5
    $ pip install -e .
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

To run the tests there must be a local Postgres database running. Then run:

    $ make test

To run a single test run something like:

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
