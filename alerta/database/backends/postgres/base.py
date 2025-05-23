import threading
import time
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta
from uuid import uuid4

import psycopg2
from flask import current_app
from psycopg2.extensions import AsIs, adapt, register_adapter
from psycopg2.extras import Json, NamedTupleCursor, register_composite

from alerta.app import alarm_model
from alerta.database.base import Database
from alerta.exceptions import NoCustomerMatch
from alerta.models.enums import ADMIN_SCOPES
from alerta.models.heartbeat import HeartbeatStatus
from alerta.utils.format import DateTime
from alerta.utils.response import absolute_url

from .utils import Query

MAX_RETRIES = 5


class HistoryAdapter:
    def __init__(self, history):
        self.history = history
        self.conn = None

    def prepare(self, conn):
        self.conn = conn

    def getquoted(self):
        def quoted(o):
            a = adapt(o)
            if hasattr(a, 'prepare'):
                a.prepare(self.conn)
            return a.getquoted().decode('utf-8')

        return '({}, {}, {}, {}, {}, {}, {}, {}::timestamp, {}, {})::history'.format(
            quoted(self.history.id),
            quoted(self.history.event),
            quoted(self.history.severity),
            quoted(self.history.status),
            quoted(self.history.value),
            quoted(self.history.text),
            quoted(self.history.change_type),
            quoted(self.history.update_time),
            quoted(self.history.user),
            quoted(self.history.timeout)
        )

    def __str__(self):
        return str(self.getquoted())


class NotificationTriggersAdapter:
    def __init__(self, notification_triggers) -> None:
        self.triggers = notification_triggers
        self.conn = None

    def prepare(self, conn):
        self.conn = conn

    def getquoted(self):
        def quoted(o):
            a = adapt(o)
            if hasattr(a, 'prepare'):
                a.prepare(self.conn)
            return a.getquoted().decode('utf-8')

        return f'({quoted(self.triggers.from_severity)},{quoted(self.triggers.to_severity)}, {quoted(self.triggers.status)},{quoted(self.triggers.text)})::notification_triggers'


class AdvancedTagsAdapter:
    def __init__(self, tags) -> None:
        self.tags = tags
        self.conn = None

    def prepare(self, conn):
        self.conn = conn

    def getquoted(self):
        def quoted(o):
            a = adapt(o)
            if hasattr(a, 'prepare'):
                a.prepare(self.conn)
            return a.getquoted().decode('utf-8')

        return f'({quoted(self.tags.all)},{quoted(self.tags.any)})::advanced_tags'


Record = namedtuple('Record', [
    'id', 'resource', 'event', 'environment', 'severity', 'status', 'service',
    'group', 'value', 'text', 'tags', 'attributes', 'origin', 'update_time',
    'user', 'timeout', 'type', 'customer',
])


class Backend(Database):

    def create_engine(self, app, uri, dbname=None, schema='public', raise_on_error=True):
        self.uri = uri
        self.dbname = dbname
        self.schema = schema

        lock = threading.Lock()
        with lock:
            conn = self.connect()

            with app.open_resource('sql/schema.sql') as f:
                try:
                    conn.cursor().execute(f.read())
                    conn.commit()
                except Exception as e:
                    if raise_on_error:
                        raise
                    app.logger.warning(e)

        register_adapter(dict, Json)
        register_adapter(datetime, self._adapt_datetime)
        register_composite(
            schema + '.history' if schema else 'history',
            conn,
            globally=True
        )
        register_composite('notification_triggers', conn, globally=True)
        register_composite('advanced_tags', conn, globally=True)
        from alerta.models.alert import History
        from alerta.models.notification_rule import (AdvancedTags,
                                                     NotificationTriggers)
        register_adapter(History, HistoryAdapter)
        register_adapter(NotificationTriggers, NotificationTriggersAdapter)
        register_adapter(AdvancedTags, AdvancedTagsAdapter)

    def connect(self):
        retry = 0
        while True:
            try:
                conn = psycopg2.connect(
                    dsn=self.uri,
                    dbname=self.dbname,
                    cursor_factory=NamedTupleCursor
                )

                conn.set_client_encoding('UTF8')
                break
            except Exception as e:
                print(e)  # FIXME - should log this error instead of printing, but current_app is unavailable here
                retry += 1
                if retry > MAX_RETRIES:
                    conn = None
                    break
                else:
                    backoff = 2 ** retry
                    print(f'Retry attempt {retry}/{MAX_RETRIES} (wait={backoff}s)...')
                    time.sleep(backoff)

        if conn:
            conn.cursor().execute('SET search_path TO {}'.format(self.schema))
            conn.commit()
            return conn
        else:
            raise RuntimeError(f'Database connect error. Failed to connect after {MAX_RETRIES} retries.')

    @staticmethod
    def _adapt_datetime(dt):
        return AsIs(f'{adapt(DateTime.iso8601(dt))}')

    @property
    def name(self):
        cursor = self.get_db().cursor()
        cursor.execute('SELECT current_database()')
        return cursor.fetchone()[0]

    @property
    def version(self):
        cursor = self.get_db().cursor()
        cursor.execute('SHOW server_version')
        return cursor.fetchone()[0]

    @property
    def is_alive(self):
        cursor = self.get_db().cursor()
        cursor.execute('SELECT true')
        return cursor.fetchone()

    def close(self, db):
        db.close()

    def destroy(self):
        conn = self.connect()
        cursor = conn.cursor()
        for table in ['alerts', 'blackouts', 'escalation_rules', 'notification_rules_history', 'notification_rules', 'notification_history', 'notification_channels', 'on_calls', 'customers', 'groups', 'heartbeats', 'keys', 'metrics', 'perms', 'notification_sends', 'users', 'delayed_notifications']:
            cursor.execute(f'DROP TABLE IF EXISTS {table}')
        conn.commit()
        conn.close()

    # ALERTS

    def get_severity(self, alert):
        select = """
            SELECT severity FROM alerts
             WHERE environment=%(environment)s AND resource=%(resource)s
               AND ((event=%(event)s AND severity!=%(severity)s)
                OR (event!=%(event)s AND %(event)s=ANY(correlate)))
               AND {customer}
            """.format(customer='customer=%(customer)s' if alert.customer else 'customer IS NULL')
        return self._fetchone(select, vars(alert)).severity

    def get_status(self, alert):
        select = """
            SELECT status FROM alerts
             WHERE environment=%(environment)s AND resource=%(resource)s
              AND (event=%(event)s OR %(event)s=ANY(correlate))
              AND {customer}
            """.format(customer='customer=%(customer)s' if alert.customer else 'customer IS NULL')
        return self._fetchone(select, vars(alert)).status

    def is_duplicate(self, alert):
        select = """
            SELECT * FROM alerts
             WHERE environment=%(environment)s
               AND resource=%(resource)s
               AND event=%(event)s
               AND severity=%(severity)s
               AND {customer}
            """.format(customer='customer=%(customer)s' if alert.customer else 'customer IS NULL')
        return self._fetchone(select, vars(alert))

    def is_correlated(self, alert):
        select = """
            SELECT * FROM alerts
             WHERE environment=%(environment)s AND resource=%(resource)s
               AND ((event=%(event)s AND severity!=%(severity)s)
                OR (event!=%(event)s AND %(event)s=ANY(correlate)))
               AND {customer}
        """.format(customer='customer=%(customer)s' if alert.customer else 'customer IS NULL')
        return self._fetchone(select, vars(alert))

    def is_flapping(self, alert, window=1800, count=2):
        """
        Return true if alert severity has changed more than X times in Y seconds
        """
        select = """
            SELECT COUNT(*)
              FROM alerts, unnest(history) h
             WHERE environment=%(environment)s
               AND resource=%(resource)s
               AND h.event=%(event)s
               AND h.update_time > (NOW() at time zone 'utc' - INTERVAL '{window} seconds')
               AND h.type='severity'
               AND {customer}
        """.format(window=window, customer='customer=%(customer)s' if alert.customer else 'customer IS NULL')
        return self._fetchone(select, vars(alert)).count > count

    def dedup_alert(self, alert, history):
        """
        Update alert status, service, value, text, timeout and rawData, increment duplicate count and set
        repeat=True, and keep track of last receive id and time but don't append to history unless status changes.
        """
        alert.history = history
        update = """
            UPDATE alerts
               SET status=%(status)s, service=%(service)s, value=%(value)s, text=%(text)s,
                   timeout=%(timeout)s, raw_data=%(raw_data)s, repeat=%(repeat)s,
                   last_receive_id=%(last_receive_id)s, last_receive_time=%(last_receive_time)s,
                   tags=ARRAY(SELECT DISTINCT UNNEST(tags || %(tags)s)), attributes=attributes || %(attributes)s,
                   duplicate_count=duplicate_count + 1, {update_time}, history=(%(history)s || history)[1:{limit}]
             WHERE environment=%(environment)s
               AND resource=%(resource)s
               AND event=%(event)s
               AND severity=%(severity)s
               AND {customer}
         RETURNING *
        """.format(
            limit=current_app.config['HISTORY_LIMIT'],
            update_time='update_time=%(update_time)s' if alert.update_time else 'update_time=update_time',
            customer='customer=%(customer)s' if alert.customer else 'customer IS NULL'
        )
        return self._updateone(update, vars(alert), returning=True)

    def correlate_alert(self, alert, history):
        alert.history = history
        update = """
            UPDATE alerts
               SET event=%(event)s, severity=%(severity)s, status=%(status)s, service=%(service)s, value=%(value)s,
                   text=%(text)s, create_time=%(create_time)s, timeout=%(timeout)s, raw_data=%(raw_data)s,
                   duplicate_count=%(duplicate_count)s, repeat=%(repeat)s, previous_severity=%(previous_severity)s,
                   trend_indication=%(trend_indication)s, receive_time=%(receive_time)s, last_receive_id=%(last_receive_id)s,
                   last_receive_time=%(last_receive_time)s, tags=ARRAY(SELECT DISTINCT UNNEST(tags || %(tags)s)),
                   attributes=attributes || %(attributes)s, {update_time}, history=(%(history)s || history)[1:{limit}]
             WHERE environment=%(environment)s
               AND resource=%(resource)s
               AND ((event=%(event)s AND severity!=%(severity)s) OR (event!=%(event)s AND %(event)s=ANY(correlate)))
               AND {customer}
         RETURNING *
        """.format(
            limit=current_app.config['HISTORY_LIMIT'],
            update_time='update_time=%(update_time)s' if alert.update_time else 'update_time=update_time',
            customer='customer=%(customer)s' if alert.customer else 'customer IS NULL'
        )
        return self._updateone(update, vars(alert), returning=True)

    def create_alert(self, alert):
        insert = """
            INSERT INTO alerts (id, resource, event, environment, severity, correlate, status, service, "group",
                value, text, tags, attributes, origin, type, create_time, timeout, raw_data, customer,
                duplicate_count, repeat, previous_severity, trend_indication, receive_time, last_receive_id,
                last_receive_time, update_time, history)
            VALUES (%(id)s, %(resource)s, %(event)s, %(environment)s, %(severity)s, %(correlate)s, %(status)s,
                %(service)s, %(group)s, %(value)s, %(text)s, %(tags)s, %(attributes)s, %(origin)s,
                %(event_type)s, %(create_time)s, %(timeout)s, %(raw_data)s, %(customer)s, %(duplicate_count)s,
                %(repeat)s, %(previous_severity)s, %(trend_indication)s, %(receive_time)s, %(last_receive_id)s,
                %(last_receive_time)s, %(update_time)s, %(history)s::history[])
            RETURNING *
        """
        return self._insert(insert, vars(alert))

    def set_alert(self, id, severity, status, tags, attributes, timeout, previous_severity, update_time, history=None):
        update = """
            UPDATE alerts
               SET severity=%(severity)s, status=%(status)s, tags=ARRAY(SELECT DISTINCT UNNEST(tags || %(tags)s)),
                   attributes=%(attributes)s, timeout=%(timeout)s, previous_severity=%(previous_severity)s,
                   update_time=%(update_time)s, history=(%(change)s || history)[1:{limit}]
             WHERE id=%(id)s OR id LIKE %(like_id)s
         RETURNING *
        """.format(limit=current_app.config['HISTORY_LIMIT'])
        return self._updateone(update, {'id': id, 'like_id': id + '%', 'severity': severity, 'status': status,
                                        'tags': tags, 'attributes': attributes, 'timeout': timeout,
                                        'previous_severity': previous_severity, 'update_time': update_time,
                                        'change': history}, returning=True)

    def get_alert(self, id, customers=None):
        select = """
            SELECT * FROM alerts
             WHERE (id ~* (%(id)s) OR last_receive_id ~* (%(id)s))
               AND {customer}
        """.format(customer='customer=ANY(%(customers)s)' if customers else '1=1')
        return self._fetchone(select, {'id': '^' + id, 'customers': customers})

    # STATUS, TAGS, ATTRIBUTES

    def set_status(self, id, status, timeout, update_time, history=None):
        update = """
            UPDATE alerts
            SET status=%(status)s, timeout=%(timeout)s, update_time=%(update_time)s, history=(%(change)s || history)[1:{limit}]
            WHERE id=%(id)s OR id LIKE %(like_id)s
            RETURNING *
        """.format(limit=current_app.config['HISTORY_LIMIT'])
        return self._updateone(update, {'id': id, 'like_id': id + '%', 'status': status, 'timeout': timeout, 'update_time': update_time, 'change': history}, returning=True)

    def tag_alert(self, id, tags):
        update = """
            UPDATE alerts
            SET tags=ARRAY(SELECT DISTINCT UNNEST(tags || %(tags)s))
            WHERE id=%(id)s OR id LIKE %(like_id)s
            RETURNING *
        """
        return self._updateone(update, {'id': id, 'like_id': id + '%', 'tags': tags}, returning=True)

    def untag_alert(self, id, tags):
        update = """
            UPDATE alerts
            SET tags=(select array_agg(t) FROM unnest(tags) AS t WHERE NOT t=ANY(%(tags)s) )
            WHERE id=%(id)s OR id LIKE %(like_id)s
            RETURNING *
        """
        return self._updateone(update, {'id': id, 'like_id': id + '%', 'tags': tags}, returning=True)

    def update_tags(self, id, tags):
        update = """
            UPDATE alerts
            SET tags=%(tags)s
            WHERE id=%(id)s OR id LIKE %(like_id)s
            RETURNING *
        """
        return self._updateone(update, {'id': id, 'like_id': id + '%', 'tags': tags}, returning=True)

    def update_attributes(self, id, old_attrs, new_attrs):
        old_attrs.update(new_attrs)
        attrs = {k: v for k, v in old_attrs.items() if v is not None}

        update = """
            UPDATE alerts
            SET attributes=%(attrs)s
            WHERE id=%(id)s OR id LIKE %(like_id)s
            RETURNING attributes
        """
        return self._updateone(update, {'id': id, 'like_id': id + '%', 'attrs': attrs}, returning=True).attributes

    def delete_alert(self, id):
        delete = """
            DELETE FROM alerts
            WHERE id=%(id)s OR id LIKE %(like_id)s
            RETURNING id
        """
        return self._deleteone(delete, {'id': id, 'like_id': id + '%'}, returning=True)

    # BULK

    def tag_alerts(self, query=None, tags=None):
        query = query or Query()
        update = f"""
            UPDATE alerts
            SET tags=ARRAY(SELECT DISTINCT UNNEST(tags || %(_tags)s))
            WHERE {query.where}
            RETURNING id
        """
        return [row[0] for row in self._updateall(update, {**query.vars, **{'_tags': tags}}, returning=True)]

    def untag_alerts(self, query=None, tags=None):
        query = query or Query()
        update = """
            UPDATE alerts
            SET tags=(select array_agg(t) FROM unnest(tags) AS t WHERE NOT t=ANY(%(_tags)s) )
            WHERE {where}
            RETURNING id
        """.format(where=query.where)
        return [row[0] for row in self._updateall(update, {**query.vars, **{'_tags': tags}}, returning=True)]

    def update_attributes_by_query(self, query=None, attributes=None):
        update = f"""
            UPDATE alerts
            SET attributes=attributes || %(_attributes)s
            WHERE {query.where}
            RETURNING id
        """
        return [row[0] for row in self._updateall(update, {**query.vars, **{'_attributes': attributes}}, returning=True)]

    def delete_alerts(self, query=None):
        query = query or Query()
        delete = f"""
            DELETE FROM alerts
            WHERE {query.where}
            RETURNING id
        """
        return [row[0] for row in self._deleteall(delete, query.vars, returning=True)]

    # SEARCH & HISTORY

    def add_history(self, id, history):
        update = """
            UPDATE alerts
               SET history=(%(history)s || history)[1:{limit}]
             WHERE id=%(id)s OR id LIKE %(like_id)s
         RETURNING *
        """.format(limit=current_app.config['HISTORY_LIMIT'])
        return self._updateone(update, {'id': id, 'like_id': id + '%', 'history': history}, returning=True)

    def get_alerts(self, query=None, raw_data=False, history=False, page=None, page_size=None):
        query = query or Query()
        if raw_data and history:
            select = '*'
        else:
            select = (
                'id, resource, event, environment, severity, correlate, status, service, "group", value, "text",'
                + 'tags, attributes, origin, type, create_time, timeout, {raw_data}, customer, duplicate_count, repeat,'
                + 'previous_severity, trend_indication, receive_time, last_receive_id, last_receive_time, update_time,'
                + '{history}'
            ).format(
                raw_data='raw_data' if raw_data else 'NULL as raw_data',
                history='history' if history else 'array[]::history[] as history'
            )

        join = ''
        if 's.code' in query.sort:
            join += 'JOIN (VALUES {}) AS s(sev, code) ON alerts.severity = s.sev '.format(
                ', '.join((f"('{k}', {v})" for k, v in alarm_model.Severity.items()))
            )
        if 'st.state' in query.sort:
            join += 'JOIN (VALUES {}) AS st(sts, state) ON alerts.status = st.sts '.format(
                ', '.join((f"('{k}', '{v}')" for k, v in alarm_model.Status.items()))
            )
        select = f"""
            SELECT {select}
              FROM alerts {join}
             WHERE {query.where}
          ORDER BY {query.sort or 'last_receive_time'}
        """
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_escalate(self):
        select = """
            SELECT id, resource, event, environment, severity, correlate, status, service, "group",
                value, text, tags, attributes, origin, type, create_time, timeout, raw_data, customer,
                duplicate_count, repeat, previous_severity, trend_indication, receive_time, last_receive_id,
                last_receive_time, update_time, history
            FROM alerts
            WHERE status='open' AND last_receive_time < %(etime)s
        """
        return self._fetchall(select, {'etime': datetime.utcnow() - timedelta(minutes=current_app.config['ESCALATE_TIME'])}, limit=1000)

    def get_alert_history(self, alert, page=None, page_size=None):
        select = """
            SELECT resource, environment, service, "group", tags, attributes, origin, customer, h.*
              FROM alerts, unnest(history[1:{limit}]) h
             WHERE environment=%(environment)s AND resource=%(resource)s
               AND (h.event=%(event)s OR %(event)s=ANY(correlate))
               AND {customer}
          ORDER BY update_time DESC
            """.format(
            customer='customer=%(customer)s' if alert.customer else 'customer IS NULL',
            limit=current_app.config['HISTORY_LIMIT']
        )
        return [
            Record(
                id=h.id,
                resource=h.resource,
                event=h.event,
                environment=h.environment,
                severity=h.severity,
                status=h.status,
                service=h.service,
                group=h.group,
                value=h.value,
                text=h.text,
                tags=h.tags,
                attributes=h.attributes,
                origin=h.origin,
                update_time=h.update_time,
                user=getattr(h, 'user', None),
                timeout=getattr(h, 'timeout', None),
                type=h.type,
                customer=h.customer
            ) for h in self._fetchall(select, vars(alert), limit=page_size, offset=(page - 1) * page_size)
        ]

    def get_history(self, query=None, page=None, page_size=None):
        query = query or Query()
        if 'id' in query.vars:
            select = """
                SELECT a.id
                  FROM alerts a, unnest(history[1:{limit}]) h
                 WHERE h.id LIKE %(id)s
            """.format(limit=current_app.config['HISTORY_LIMIT'])
            query.vars['id'] = self._fetchone(select, query.vars)

        select = """
            SELECT resource, environment, service, "group", tags, attributes, origin, customer, history, h.*
              FROM alerts, unnest(history[1:{limit}]) h
             WHERE {where}
          ORDER BY update_time DESC
        """.format(where=query.where, limit=current_app.config['HISTORY_LIMIT'])

        return [
            Record(
                id=h.id,
                resource=h.resource,
                event=h.event,
                environment=h.environment,
                severity=h.severity,
                status=h.status,
                service=h.service,
                group=h.group,
                value=h.value,
                text=h.text,
                tags=h.tags,
                attributes=h.attributes,
                origin=h.origin,
                update_time=h.update_time,
                user=getattr(h, 'user', None),
                timeout=getattr(h, 'timeout', None),
                type=h.type,
                customer=h.customer
            ) for h in self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)
        ]

    def get_history_count(self, query=None):
        query = query or Query()
        select = f"SELECT count(*) FROM alerts, unnest(history[1:{current_app.config['HISTORY_LIMIT']}]) h WHERE {query.where}"
        return self._fetchone(select, query.vars)

    def get_history_environment_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT environment, count(1) FROM alerts, unnest(history[1:{current_app.config['HISTORY_LIMIT']}]) h
            WHERE {query.where}
            GROUP BY environment
        """
        return self._fetchall(select, query.vars)

    # COUNTS

    def get_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT COUNT(1) FROM alerts
             WHERE {query.where}
        """
        return self._fetchone(select, query.vars).count

    def get_counts(self, query=None, group=None):
        query = query or Query()
        if group is None:
            raise ValueError('Must define a group')
        select = """
            SELECT {group}, COUNT(*) FROM alerts
             WHERE {where}
            GROUP BY {group}
        """.format(where=query.where, group=group)
        return {s['group']: s.count for s in self._fetchall(select, query.vars)}

    def get_counts_by_severity(self, query=None):
        query = query or Query()
        select = f"""
            SELECT severity, COUNT(*) FROM alerts
             WHERE {query.where}
            GROUP BY severity
        """
        return {s.severity: s.count for s in self._fetchall(select, query.vars)}

    def get_counts_by_status(self, query=None):
        query = query or Query()
        select = f"""
            SELECT status, COUNT(*) FROM alerts
            WHERE {query.where}
            GROUP BY status
        """
        return {s.status: s.count for s in self._fetchall(select, query.vars)}

    def get_topn_count(self, query=None, topn=100):
        query = query or Query()
        group = 'event'
        if query and query.group:
            group = query.group[0]

        select = """
            SELECT {group}, COUNT(1) as count, SUM(duplicate_count) AS duplicate_count,
                   array_agg(DISTINCT environment) AS environments, array_agg(DISTINCT svc) AS services,
                   array_agg(DISTINCT ARRAY[id, resource]) AS resources
              FROM alerts, UNNEST (service) svc
             WHERE {where}
          GROUP BY {group}
          ORDER BY count DESC
        """.format(where=query.where, group=group)
        return [
            {
                'count': t.count,
                'duplicateCount': t.duplicate_count,
                'environments': t.environments,
                'services': t.services,
                group: getattr(t, group),
                'resources': [{'id': r[0], 'resource': r[1], 'href': absolute_url(f'/alert/{r[0]}')} for r in t.resources]
            } for t in self._fetchall(select, query.vars, limit=topn)
        ]

    def get_topn_flapping(self, query=None, topn=100):
        query = query or Query()
        group = 'event'
        if query and query.group:
            group = query.group[0]
        select = """
            WITH topn AS (SELECT * FROM alerts WHERE {where})
            SELECT topn.{group}, COUNT(1) as count, SUM(duplicate_count) AS duplicate_count,
                   array_agg(DISTINCT environment) AS environments, array_agg(DISTINCT svc) AS services,
                   array_agg(DISTINCT ARRAY[topn.id, resource]) AS resources
              FROM topn, UNNEST (service) svc, UNNEST (history) hist
             WHERE hist.type='severity'
          GROUP BY topn.{group}
          ORDER BY count DESC
        """.format(where=query.where, group=group)
        return [
            {
                'count': t.count,
                'duplicateCount': t.duplicate_count,
                'environments': t.environments,
                'services': t.services,
                group: getattr(t, group),
                'resources': [{'id': r[0], 'resource': r[1], 'href': absolute_url(f'/alert/{r[0]}')} for r in t.resources]
            } for t in self._fetchall(select, query.vars, limit=topn)
        ]

    def get_topn_standing(self, query=None, topn=100):
        query = query or Query()
        group = 'event'
        if query and query.group:
            group = query.group[0]
        select = """
            WITH topn AS (SELECT * FROM alerts WHERE {where})
            SELECT topn.{group}, COUNT(1) as count, SUM(duplicate_count) AS duplicate_count,
                   SUM(last_receive_time - create_time) as life_time,
                   array_agg(DISTINCT environment) AS environments, array_agg(DISTINCT svc) AS services,
                   array_agg(DISTINCT ARRAY[topn.id, resource]) AS resources
              FROM topn, UNNEST (service) svc, UNNEST (history) hist
             WHERE hist.type='severity'
          GROUP BY topn.{group}
          ORDER BY life_time DESC
        """.format(where=query.where, group=group)
        return [
            {
                'count': t.count,
                'duplicateCount': t.duplicate_count,
                'environments': t.environments,
                'services': t.services,
                group: getattr(t, group),
                'resources': [{'id': r[0], 'resource': r[1], 'href': absolute_url(f'/alert/{r[0]}')} for r in t.resources]
            } for t in self._fetchall(select, query.vars, limit=topn)
        ]

    # ENVIRONMENTS

    def get_environments(self, query=None, topn=1000):
        query = query or Query()
        select = f"""
            SELECT environment, severity, status, count(1) FROM alerts
            WHERE {query.where}
            GROUP BY environment, CUBE(severity, status)
        """
        result = self._fetchall(select, query.vars, limit=topn)

        severity_count = defaultdict(list)
        status_count = defaultdict(list)
        total_count = defaultdict(int)

        for row in result:
            if row.severity and not row.status:
                severity_count[row.environment].append((row.severity, row.count))
            if not row.severity and row.status:
                status_count[row.environment].append((row.status, row.count))
            if not row.severity and not row.status:
                total_count[row.environment] = row.count

        select = """SELECT DISTINCT environment FROM alerts"""
        environments = self._fetchall(select, {})
        return [
            {
                'environment': e.environment,
                'severityCounts': dict(severity_count[e.environment]),
                'statusCounts': dict(status_count[e.environment]),
                'count': total_count[e.environment]
            } for e in environments]

    # SERVICES

    def get_services(self, query=None, topn=1000):
        query = query or Query()
        select = """
            SELECT environment, svc, severity, status, count(1) FROM alerts, UNNEST(service) svc
            WHERE {where}
            GROUP BY environment, svc, CUBE(severity, status)
        """.format(where=query.where)
        result = self._fetchall(select, query.vars, limit=topn)

        severity_count = defaultdict(list)
        status_count = defaultdict(list)
        total_count = defaultdict(int)

        for row in result:
            if row.severity and not row.status:
                severity_count[(row.environment, row.svc)].append((row.severity, row.count))
            if not row.severity and row.status:
                status_count[(row.environment, row.svc)].append((row.status, row.count))
            if not row.severity and not row.status:
                total_count[(row.environment, row.svc)] = row.count

        select = """SELECT DISTINCT environment, svc FROM alerts, UNNEST(service) svc"""
        services = self._fetchall(select, {})
        return [
            {
                'environment': s.environment,
                'service': s.svc,
                'severityCounts': dict(severity_count[(s.environment, s.svc)]),
                'statusCounts': dict(status_count[(s.environment, s.svc)]),
                'count': total_count[(s.environment, s.svc)]
            } for s in services]

    # ALERT GROUPS

    def get_alert_groups(self, query=None, topn=1000):
        query = query or Query()
        select = f"""
            SELECT environment, "group", count(1) FROM alerts
            WHERE {query.where}
            GROUP BY environment, "group"
        """
        return [
            {
                'environment': g.environment,
                'group': g.group,
                'count': g.count
            } for g in self._fetchall(select, query.vars, limit=topn)]

    # ALERT TAGS

    def get_alert_tags(self, query=None, topn=1000):
        query = query or Query()
        select = """
            SELECT environment, tag, count(1) FROM alerts, UNNEST(tags) tag
            WHERE {where}
            GROUP BY environment, tag
        """.format(where=query.where)
        return [{'environment': t.environment, 'tag': t.tag, 'count': t.count} for t in self._fetchall(select, query.vars, limit=topn)]

    # BLACKOUTS

    def create_blackout(self, blackout):
        insert = """
            INSERT INTO blackouts (id, priority, environment, service, resource, event,
                "group", tags, origin, customer, start_time, end_time,
                duration, "user", create_time, text)
            VALUES (%(id)s, %(priority)s, %(environment)s, %(service)s, %(resource)s, %(event)s,
                %(group)s, %(tags)s, %(origin)s, %(customer)s, %(start_time)s, %(end_time)s,
                %(duration)s, %(user)s, %(create_time)s, %(text)s)
            RETURNING *, duration AS remaining
        """
        return self._insert(insert, vars(blackout))

    def get_blackout(self, id, customers=None):
        select = """
            SELECT *, GREATEST(EXTRACT(EPOCH FROM (end_time - GREATEST(start_time, NOW() at time zone 'utc'))), 0) AS remaining
            FROM blackouts
            WHERE id=%(id)s
              AND {customer}
        """.format(customer='customer=ANY(%(customers)s)' if customers else '1=1')
        return self._fetchone(select, {'id': id, 'customers': customers})

    def get_blackouts(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT *, GREATEST(EXTRACT(EPOCH FROM (end_time - GREATEST(start_time, NOW() at time zone 'utc'))), 0) AS remaining
              FROM blackouts
             WHERE {where}
          ORDER BY {order}
        """.format(where=query.where, order=query.sort)
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_blackouts_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT COUNT(1) FROM blackouts
             WHERE {query.where}
        """
        return self._fetchone(select, query.vars).count

    def is_blackout_period(self, alert):
        select = """
            SELECT *
            FROM blackouts
            WHERE start_time <= %(create_time)s AND end_time > %(create_time)s
              AND environment=%(environment)s
              AND (
                 ( resource IS NULL AND service='{}' AND event IS NULL AND "group" IS NULL AND tags='{}' AND origin IS NULL )
              OR ( resource IS NULL AND service='{}' AND event IS NULL AND "group" IS NULL AND tags='{}' AND origin=%(origin)s )
              OR ( resource IS NULL AND service='{}' AND event IS NULL AND "group" IS NULL AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource IS NULL AND service='{}' AND event IS NULL AND "group" IS NULL AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource IS NULL AND service='{}' AND event IS NULL AND "group"=%(group)s AND tags='{}' AND origin IS NULL )
              OR ( resource IS NULL AND service='{}' AND event IS NULL AND "group"=%(group)s AND tags='{}' AND origin=%(origin)s )
              OR ( resource IS NULL AND service='{}' AND event IS NULL AND "group"=%(group)s AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource IS NULL AND service='{}' AND event IS NULL AND "group"=%(group)s AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource IS NULL AND service='{}' AND event=%(event)s AND "group" IS NULL AND tags='{}' AND origin IS NULL )
              OR ( resource IS NULL AND service='{}' AND event=%(event)s AND "group" IS NULL AND tags='{}' AND origin=%(origin)s )
              OR ( resource IS NULL AND service='{}' AND event=%(event)s AND "group" IS NULL AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource IS NULL AND service='{}' AND event=%(event)s AND "group" IS NULL AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource IS NULL AND service='{}' AND event=%(event)s AND "group"=%(group)s AND tags='{}' AND origin IS NULL )
              OR ( resource IS NULL AND service='{}' AND event=%(event)s AND "group"=%(group)s AND tags='{}' AND origin=%(origin)s )
              OR ( resource IS NULL AND service='{}' AND event=%(event)s AND "group"=%(group)s AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource IS NULL AND service='{}' AND event=%(event)s AND "group"=%(group)s AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource IS NULL AND service <@ %(service)s AND event IS NULL AND "group" IS NULL AND tags='{}' AND origin IS NULL )
              OR ( resource IS NULL AND service <@ %(service)s AND event IS NULL AND "group" IS NULL AND tags='{}' AND origin=%(origin)s )
              OR ( resource IS NULL AND service <@ %(service)s AND event IS NULL AND "group" IS NULL AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource IS NULL AND service <@ %(service)s AND event IS NULL AND "group" IS NULL AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource IS NULL AND service <@ %(service)s AND event IS NULL AND "group"=%(group)s AND tags='{}' AND origin IS NULL )
              OR ( resource IS NULL AND service <@ %(service)s AND event IS NULL AND "group"=%(group)s AND tags='{}' AND origin=%(origin)s )
              OR ( resource IS NULL AND service <@ %(service)s AND event IS NULL AND "group"=%(group)s AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource IS NULL AND service <@ %(service)s AND event IS NULL AND "group"=%(group)s AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource IS NULL AND service <@ %(service)s AND event=%(event)s AND "group" IS NULL AND tags='{}' AND origin IS NULL )
              OR ( resource IS NULL AND service <@ %(service)s AND event=%(event)s AND "group" IS NULL AND tags='{}' AND origin=%(origin)s )
              OR ( resource IS NULL AND service <@ %(service)s AND event=%(event)s AND "group" IS NULL AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource IS NULL AND service <@ %(service)s AND event=%(event)s AND "group" IS NULL AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource IS NULL AND service <@ %(service)s AND event=%(event)s AND "group"=%(group)s AND tags='{}' AND origin IS NULL )
              OR ( resource IS NULL AND service <@ %(service)s AND event=%(event)s AND "group"=%(group)s AND tags='{}' AND origin=%(origin)s )
              OR ( resource IS NULL AND service <@ %(service)s AND event=%(event)s AND "group"=%(group)s AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource IS NULL AND service <@ %(service)s AND event=%(event)s AND "group"=%(group)s AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service='{}' AND event IS NULL AND "group" IS NULL AND tags='{}' AND origin IS NULL )
              OR ( resource=%(resource)s AND service='{}' AND event IS NULL AND "group" IS NULL AND tags='{}' AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service='{}' AND event IS NULL AND "group" IS NULL AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource=%(resource)s AND service='{}' AND event IS NULL AND "group" IS NULL AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service='{}' AND event IS NULL AND "group"=%(group)s AND tags='{}' AND origin IS NULL )
              OR ( resource=%(resource)s AND service='{}' AND event IS NULL AND "group"=%(group)s AND tags='{}' AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service='{}' AND event IS NULL AND "group"=%(group)s AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource=%(resource)s AND service='{}' AND event IS NULL AND "group"=%(group)s AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service='{}' AND event=%(event)s AND "group" IS NULL AND tags='{}' AND origin IS NULL )
              OR ( resource=%(resource)s AND service='{}' AND event=%(event)s AND "group" IS NULL AND tags='{}' AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service='{}' AND event=%(event)s AND "group" IS NULL AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource=%(resource)s AND service='{}' AND event=%(event)s AND "group" IS NULL AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service='{}' AND event=%(event)s AND "group"=%(group)s AND tags='{}' AND origin IS NULL )
              OR ( resource=%(resource)s AND service='{}' AND event=%(event)s AND "group"=%(group)s AND tags='{}' AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service='{}' AND event=%(event)s AND "group"=%(group)s AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource=%(resource)s AND service='{}' AND event=%(event)s AND "group"=%(group)s AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event IS NULL AND "group" IS NULL AND tags='{}' AND origin IS NULL )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event IS NULL AND "group" IS NULL AND tags='{}' AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event IS NULL AND "group" IS NULL AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event IS NULL AND "group" IS NULL AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event IS NULL AND "group"=%(group)s AND tags='{}' AND origin IS NULL )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event IS NULL AND "group"=%(group)s AND tags='{}' AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event IS NULL AND "group"=%(group)s AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event IS NULL AND "group"=%(group)s AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event=%(event)s AND "group" IS NULL AND tags='{}' AND origin IS NULL )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event=%(event)s AND "group" IS NULL AND tags='{}' AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event=%(event)s AND "group" IS NULL AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event=%(event)s AND "group" IS NULL AND tags <@ %(tags)s AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event=%(event)s AND "group"=%(group)s AND tags='{}' AND origin IS NULL )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event=%(event)s AND "group"=%(group)s AND tags='{}' AND origin=%(origin)s )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event=%(event)s AND "group"=%(group)s AND tags <@ %(tags)s AND origin IS NULL )
              OR ( resource=%(resource)s AND service <@ %(service)s AND event=%(event)s AND "group"=%(group)s AND tags <@ %(tags)s AND origin=%(origin)s )
                 )
        """
        if current_app.config['CUSTOMER_VIEWS']:
            select += ' AND (customer IS NULL OR customer=%(customer)s)'
        if self._fetchone(select, vars(alert)):
            return True
        return False

    def update_blackout(self, id, **kwargs):
        update = """
            UPDATE blackouts
            SET
        """
        if kwargs.get('environment') is not None:
            update += 'environment=%(environment)s, '
        if 'service' in kwargs:
            update += 'service=%(service)s, '
        if 'resource' in kwargs:
            update += 'resource=%(resource)s, '
        if 'event' in kwargs:
            update += 'event=%(event)s, '
        if 'group' in kwargs:
            update += '"group"=%(group)s, '
        if 'tags' in kwargs:
            update += 'tags=%(tags)s, '
        if 'origin' in kwargs:
            update += 'origin=%(origin)s, '
        if 'customer' in kwargs:
            update += 'customer=%(customer)s, '
        if kwargs.get('startTime') is not None:
            update += 'start_time=%(startTime)s, '
        if kwargs.get('endTime') is not None:
            update += 'end_time=%(endTime)s, '
        if 'duration' in kwargs:
            update += 'duration=%(duration)s, '
        if 'text' in kwargs:
            update += 'text=%(text)s, '
        update += """
            "user"=COALESCE(%(user)s, "user")
            WHERE id=%(id)s
            RETURNING *, GREATEST(EXTRACT(EPOCH FROM (end_time - GREATEST(start_time, NOW() at time zone 'utc'))), 0) AS remaining
        """
        kwargs['id'] = id
        kwargs['user'] = kwargs.get('user')
        return self._updateone(update, kwargs, returning=True)

    def delete_blackout(self, id):
        delete = """
            DELETE FROM blackouts
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    # NOTIFICATION CHANNELS

    def create_notification_channel(self, notification_channel):
        insert = """
            INSERT INTO notification_channels (id, type, api_token, api_sid, sender, customer, host, platform_id, platform_partner_id, verify)
            VALUES (%(id)s, %(type)s, %(api_token)s, %(api_sid)s, %(sender)s, %(customer)s, %(host)s, %(platform_id)s, %(platform_partner_id)s, %(verify)s)
            RETURNING *
        """
        return self._insert(insert, vars(notification_channel))

    def get_notification_channel(self, id, customers=None):
        select = """
            SELECT * FROM notification_channels
            WHERE id=%(id)s
              AND {customer}
        """.format(
            customer='customer=ANY(%(customers)s)' if customers else '1=1'
        )
        return self._fetchone(select, {'id': id, 'customers': customers})

    def get_notification_channels(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT * FROM notification_channels
             WHERE {where}
          ORDER BY {order}
        """.format(
            where=query.where, order=query.sort
        )
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_notification_channels_count(self, query=None):
        query = query or Query()
        select = """
            SELECT COUNT(1) FROM notification_channels
             WHERE {where}
        """.format(
            where=query.where
        )
        return self._fetchone(select, query.vars).count

    def update_notification_channel(self, id, **kwargs):
        update = """
            UPDATE notification_channels
            SET
        """
        if kwargs.get('type') is not None:
            update += 'type=%(type)s, '
        if 'apiToken' in kwargs:
            update += 'api_token=%(apiToken)s, '
        if 'apiSid' in kwargs:
            update += 'api_sid=%(apiSid)s, '
        if 'customer' in kwargs:
            update += 'customer=%(customer)s, '
        if 'sender' in kwargs:
            update += 'sender=%(sender)s, '
        if 'host' in kwargs:
            update += 'host=%(host)s, '
        if 'platformId' in kwargs:
            update += 'platform_id=%(platformId)s, '
        if 'platformPartnerId' in kwargs:
            update += 'platform_partner_id=%(platformPartnerId)s, '
        if 'verify' in kwargs:
            update += 'verify=%(verify)s, '
        if 'bearer' in kwargs:
            update += 'bearer=%(bearer)s, '
        if 'bearer_timeout' in kwargs:
            update += 'bearer_timeout=%(bearer_timeout)s, '
        update = update[0:-2]
        update += """
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        kwargs['user'] = kwargs.get('user')
        return self._updateone(update, kwargs, returning=True)

    def delete_notification_channel(self, id):
        delete = """
            DELETE FROM notification_channels
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    # DELAYED NOTIFICATIONS

    def create_delayed_notification(self, delayed_notification):
        insert = """
            INSERT INTO delayed_notifications (id, alert_id, notification_rule_id, delay_time)
            VALUES (%(id)s, %(alert_id)s, %(notification_rule_id)s, %(delay_time)s )
            RETURNING *
        """
        return self._insert(insert, vars(delayed_notification))

    def get_delayed_notifications(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT * FROM delayed_notifications
             WHERE {where}
          ORDER BY {order}
        """.format(
            where=query.where, order=query.sort
        )
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_delayed_notification(self, id):
        select = """
            SELECT * FROM delayed_notifications
            WHERE id=%(id)s
        """
        return self._fetchone(select, {'id': id})

    def get_delayed_notifications_firing(self, time):
        select = """
            SELECT * FROM delayed_notifications
            WHERE delay_time < %s
        """
        return self._fetchall(select, (time,))

    def get_delayed_notifications_count(self, query=None):
        query = query or Query()
        select = """
            SELECT COUNT(1) FROM delayed_notifications
             WHERE {where}
        """.format(
            where=query.where
        )
        return self._fetchone(select, query.vars).count

    def delete_delayed_notifications_alert(self, alert_id):
        delete = """
            DELETE FROM delayed_notifications
            WHERE alert_id=%s
            RETURNING id
        """
        return self._deleteall(delete, (alert_id,), returning=True)

    def delete_delayed_notification(self, id):
        delete = """
            DELETE FROM delayed_notifications
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    # NOTIFICATION RULES

    def create_notification_rule(self, notification_rule):
        insert = """
            INSERT INTO notification_rules (id, name, active, priority, environment, service, resource, event, "group", tags, reactivate, excluded_tags, delay_time,
                customer, "user", create_time, start_time, end_time, days, receivers, user_ids, group_ids, use_oncall, text, channel_id, triggers)
            VALUES (%(id)s, %(name)s, %(active)s, %(priority)s, %(environment)s, %(service)s, %(resource)s, %(event)s, %(group)s, %(tags)s, %(reactivate)s, %(excluded_tags)s, %(delay_time)s,
                %(customer)s, %(user)s, %(create_time)s, %(start_time)s, %(end_time)s, %(days)s, %(receivers)s, %(user_ids)s, %(group_ids)s, %(use_oncall)s, %(text)s, %(channel_id)s, %(triggers)s::notification_triggers[] )
            RETURNING *
        """
        return self._insert(insert, vars(notification_rule))

    def get_notification_rule(self, id, customers=None):
        select = """
            SELECT * FROM notification_rules
            WHERE id=%(id)s
              AND {customer}
        """.format(
            customer='customer=ANY(%(customers)s)' if customers else '1=1'
        )
        return self._fetchone(select, {'id': id, 'customers': customers})

    def get_notification_rules(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT * FROM notification_rules
             WHERE {where}
          ORDER BY {order}
        """.format(
            where=query.where, order=query.sort
        )
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_notification_rules_count(self, query=None):
        query = query or Query()
        select = """
            SELECT COUNT(1) FROM notification_rules
             WHERE {where}
        """.format(
            where=query.where
        )
        return self._fetchone(select, query.vars).count

    def get_notification_rules_active(self, alert):
        select = """
            WITH alert_triggers AS (
                SELECT * from (select *, generate_subscripts(triggers,1) as s
                FROM notification_rules) as foo
                WHERE (start_time IS NULL OR start_time <= %(time)s) AND (end_time IS NULL OR end_time > %(time)s)
                AND (days='{}' OR ARRAY[%(day)s] <@ days)
                AND environment=%(environment)s
                AND (
                        (triggers[s].from_severity='{}' OR triggers[s].from_severity IS NULL OR ARRAY[%(previous_severity)s] <@ triggers[s].from_severity)
                        AND (triggers[s].to_severity='{}' OR triggers[s].to_severity IS NULL OR ARRAY[%(severity)s] <@ triggers[s].to_severity)
                        AND (triggers[s].status='{}' OR triggers[s].status IS NULL OR ARRAY[%(status)s] <@ triggers[s].status)
                    )
                AND (resource IS NULL OR resource=%(resource)s)
                AND (service='{}' OR service <@ %(service)s)
                AND (event IS NULL OR event=%(event)s)
                AND ("group" IS NULL OR "group"=%(group)s)
                AND active=true
            ), alert_tags AS (
                SELECT * from (select *, generate_subscripts(tags,1) as t from alert_triggers) as foo
                WHERE (tags[t].all='{}' OR tags[t].all <@ %(tags)s)
                    AND (tags[t].any='{}' OR tags[t].any && %(tags)s)
            ), alert_excluded AS (
                SELECT id FROM (select * FROM alert_tags, unnest(excluded_tags) as t("e_all","e_any")) as foo
                WHERE NOT ("e_all"='{}' AND "e_any"='{}')
                AND (
                    ("e_all"='{}' AND %(tags)s && "e_any")
                    OR ("e_any"='{}' AND %(tags)s @> "e_all")
                    OR (%(tags)s @> "e_all" AND %(tags)s && "e_any")
                )
            )

            SELECT * from alert_tags
            WHERE alert_tags.id NOT IN (SELECT DISTINCT alert_excluded.id FROM alert_excluded)
        """
        if current_app.config['CUSTOMER_VIEWS']:
            select += ' AND (customer IS NULL OR customer=%(customer)s)'
        return self._fetchall(select, vars(alert))

    def get_notification_rules_reactivate(self, time):
        select = """
            SELECT * FROM notification_rules
            WHERE active = false
            AND reactivate IS NOT NULL
            AND reactivate < %(time)s
        """
        return self._fetchall(select, {'time': time})

    def get_notification_rules_active_status(self, alert, status):
        select = """
            WITH alert_triggers AS (
                SELECT * from (select *, generate_subscripts(triggers,1) as s
                FROM notification_rules) as foo
                WHERE (start_time IS NULL OR start_time <= %(time)s) AND (end_time IS NULL OR end_time > %(time)s)
                    AND (days='{}' OR ARRAY[%(day)s] <@ days)
                    AND environment=%(environment)s
                    AND (
                        (triggers[s].from_severity='{}' OR ARRAY[%(previous_severity)s] <@ triggers[s].from_severity)
                        AND (triggers[s].to_severity='{}' OR ARRAY[%(severity)s] <@ triggers[s].to_severity)
                        AND (ARRAY[%(status)s] <@ triggers[s].status)
                    )
                    AND (resource IS NULL OR resource=%(resource)s)
                    AND (service='{}' OR service <@ %(service)s)
                    AND (event IS NULL OR event=%(event)s)
                    AND ("group" IS NULL OR "group"=%(group)s)
                    AND active=true
            ), alert_tags AS (
                SELECT * from (select *, generate_subscripts(tags,1) as t from alert_triggers) as foo
                WHERE (tags[t].all='{}' OR tags[t].all <@ %(tags)s)
                    AND (tags[t].any='{}' OR tags[t].any && %(tags)s)
            ), alert_excluded AS (
                SELECT id FROM (select * FROM alert_tags, unnest(excluded_tags) as t("e_all","e_any")) as foo
                WHERE NOT ("e_all"='{}' AND "e_any"='{}')
                AND (
                    ("e_all"='{}' AND %(tags)s && "e_any")
                    OR ("e_any"='{}' AND %(tags)s @> "e_all")
                    OR (%(tags)s @> "e_all" AND %(tags)s && "e_any")
                )
            )

            SELECT * from alert_tags
            WHERE alert_tags.id NOT IN (SELECT DISTINCT alert_excluded.id FROM alert_excluded)
        """
        if current_app.config['CUSTOMER_VIEWS']:
            select += ' AND (customer IS NULL OR customer=%(customer)s)'
        return self._fetchall(select, {**vars(alert), 'status': status})

    def create_notification_rule_history(self, update_type: str, notification_rule):
        insert = """
            INSERT INTO notification_rules_history (rule_id, "user", type, create_time, rule_data)
            VALUES (%(id)s, %(user)s, %(type)s, %(create_time)s, %(data)s)
            returning *
        """
        data = (notification_rule.serialize)
        data['reactivate'] = data['reactivate'].isoformat() if data.get('reactivate') is not None else None
        data['createTime'] = data['createTime'].isoformat() if data.get('createTime') is not None else None
        data['delayTime'] = str(data['delayTime']) if data.get('delayTime') is not None else None
        return self._insert(insert, {'data': data, 'id': notification_rule.id, 'user': notification_rule.user, 'type': update_type, 'create_time': datetime.utcnow()})

    def get_notification_rule_history(self, rule_id: str, page, page_size):
        select = """
            SELECT * FROM notification_rules_history
            WHERE rule_id=%(id)s
            ORDER BY create_time
        """
        return self._fetchall(select, {'id': rule_id}, limit=page_size, offset=(page - 1) * page_size)

    def get_notification_rule_history_count(self, rule_id: str):
        select = """
            SELECT COUNT(1) FROM notification_rules_history
            WHERE rule_id=%(id)s
        """
        return self._fetchone(select, {'id': rule_id}).count

    def update_notification_rule(self, id, **kwargs):
        update = """
            UPDATE notification_rules
            SET
        """
        if kwargs.get('environment') is not None:
            update += 'environment=%(environment)s, '
        if 'name' in kwargs:
            update += 'name=%(name)s, '
        if 'service' in kwargs:
            update += 'service=%(service)s, '
        if 'resource' in kwargs:
            update += 'resource=%(resource)s, ' if kwargs['resource'] != '' else 'resource=NULL, '
        if 'event' in kwargs:
            update += 'event=%(event)s, ' if kwargs['event'] != '' else 'event=NULL, '
        if 'group' in kwargs:
            update += '"group"=%(group)s, ' if kwargs['group'] != '' else '"group"=NULL, '
        if kwargs.get('tags') is not None:
            update += 'tags=%(tags)s::advanced_tags[], '
        if 'excludedTags' in kwargs:
            update += 'excluded_tags=%(excludedTags)s::advanced_tags[], '
        if 'customer' in kwargs:
            update += 'customer=%(customer)s, '
        if 'startTime' in kwargs:
            update += 'start_time=%(startTime)s, '
        if 'endTime' in kwargs:
            update += 'end_time=%(endTime)s, '
        if 'delayTime' in kwargs:
            update += 'delay_time=%(delayTime)s, '
        if 'days' in kwargs:
            update += 'days=%(days)s, '
        if 'status' in kwargs:
            update += 'status=%(status)s, '
        if 'receivers' in kwargs:
            update += 'receivers=%(receivers)s, '
        if 'useOnCall' in kwargs:
            update += 'use_oncall=%(useOnCall)s, '
        if kwargs.get('severity') is not None:
            update += 'severity=%(severity)s, '
        if kwargs.get('triggers') is not None:
            update += 'triggers=%(triggers)s::notification_triggers[], '
        if 'text' in kwargs:
            update += 'text=%(text)s, '
        if 'channelId' in kwargs:
            update += 'channel_id=%(channelId)s,'
        if 'active' in kwargs:
            update += 'active=%(active)s,'
        if 'reactivate' in kwargs:
            update += 'reactivate=%(reactivate)s,'
        if 'userIds' in kwargs:
            update += 'user_ids=%(userIds)s, '
        if 'groupIds' in kwargs:
            update += 'group_ids=%(groupIds)s, '
        update += """
            "user"=COALESCE(%(user)s, "user")
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        kwargs['user'] = kwargs.get('user')
        return self._updateone(update, kwargs, returning=True)

    def delete_notification_rule(self, id):
        delete = """
            DELETE FROM notification_rules
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

# NOTIFICATION GROUPS
    def create_notification_group(self, notification_group):
        insert = """
            INSERT INTO notification_groups (id, name, users, phone_numbers, mails)
            VALUES (%(id)s, %(name)s, %(users)s, %(phone_numbers)s, %(mails)s)
            RETURNING *
        """
        return self._insert(insert, vars(notification_group))

    def get_notification_group(self, id):
        select = """
            SELECT * FROM notification_groups
            WHERE id=%(id)s
        """
        return self._fetchone(select, {'id': id})

    def get_notification_groups(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT * FROM notification_groups
             WHERE {where}
          ORDER BY {order}
        """.format(
            where=query.where, order=query.sort
        )
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_notification_group_users(self, id):
        select = """
            SELECT u.id, u.login, u.email, u.name, u.status
            FROM (SELECT id, UNNEST(users) as uid FROM notification_groups) g
            INNER JOIN users u on g.uid = u.id
            WHERE g.id = %s
        """
        return self._fetchall(select, (id,))

    def get_notification_groups_count(self, query=None):
        query = query or Query()
        select = """
            SELECT COUNT(1) FROM notification_groups
             WHERE {where}
        """.format(
            where=query.where
        )
        return self._fetchone(select, query.vars).count

    def update_notification_group(self, id, **kwargs):
        update = """
            UPDATE notification_groups
            SET
        """
        if 'name' in kwargs:
            update += 'name=%(name)s, '
        if 'users' in kwargs:
            update += 'users=%(users)s, '
        if 'phoneNumbers' in kwargs:
            update += 'phone_numbers=%(phoneNumbers)s, '
        if 'mails' in kwargs:
            update += 'mails=%(mails)s, '
        update = update[0:-2]
        update += """
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        return self._updateone(update, kwargs, returning=True)

    def delete_notification_group(self, id):
        delete = """
            DELETE FROM notification_groups
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

# NOTIFICATION SEND
    def get_notification_sends(self):
        select_users = """
            SELECT name, email FROM users
            WHERE email NOT IN (select user_email from notification_sends where user_email is not null)
        """
        users = self._fetchall(select_users, [])
        if len(users):
            insert_users = 'INSERT INTO notification_sends (id, user_name, user_email, mail, sms) VALUES'
            users_data = {}
            for user in users:
                index = users.index(user)
                insert_users += f'(%(email_{index})s, %(name_{index})s, %(email_{index})s, false, false),'
                users_data = {**users_data, **{f'email_{index}': user.email, f'name_{index}': user.name, f'id_{index}': str(uuid4())}}
            insert_users = insert_users[:-1]
            self._insert(insert_users, users_data)

        insert_groups = """
            INSERT INTO notification_sends (id, group_name, mail, sms)
            SELECT id, name, false, false from notification_groups
            ON CONFLICT DO NOTHING
        """
        try:
            self._insert(insert_groups, [])
        except psycopg2.ProgrammingError:
            pass
        select = 'SELECT * FROM notification_sends'
        return self._fetchall(select, [])

    def update_notification_send(self, id, **kwargs):
        update = """
            UPDATE notification_sends
            SET
        """
        if 'mail' in kwargs:
            update += 'mail=%(mail)s, '
        if 'sms' in kwargs:
            update += 'sms=%(sms)s, '
        update = update[0:-2]
        update += """
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        return self._updateone(update, kwargs, returning=True)

# NOTIFICATION SENT

    def create_notification_history(self, notification_history):
        insert = """
            INSERT INTO notification_history (id, sent, message, channel, rule, alert, receiver, sender, sent_time, error)
            VALUES (%(id)s, %(sent)s, %(message)s, %(channel)s, %(rule)s, %(alert)s, %(receiver)s, %(sender)s, %(sent_time)s, %(error)s)
            RETURNING *
        """
        return self._insert(insert, vars(notification_history))

    def get_notification_history(self, id):
        select = """
            SELECT * FROM notification_history
            WHERE id=%(id)s
        """
        return self._fetchone(select, {'id': id})

    def get_notifications_history(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT * FROM notification_history
             WHERE {where}
          ORDER BY sent_time desc
        """.format(
            where=query.where
        )
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_notifications_history_count(self, query=None):
        query = query or Query()
        select = """
            SELECT COUNT(1) FROM notification_history
             WHERE {where}
        """.format(
            where=query.where
        )
        return self._fetchone(select, query.vars).count

    def confirm_notification_history(self, id):
        update = """
            UPDATE notification_history
            SET confirmed=true, confirmed_time=%(time)s
            WHERE id=%(id)s
            RETURNING *
        """
        return self._updateone(update, {'id': id, 'time': datetime.utcnow()}, returning=True)

# ESCALATION RULES

    def create_escalation_rule(self, escalation_rule):
        insert = """
            INSERT INTO escalation_rules (id, active, "time", priority, environment, service, resource, event, "group", tags,
                customer, "user", create_time, start_time, end_time, days, triggers, excluded_tags)
            VALUES (%(id)s, %(active)s, %(time)s, %(priority)s, %(environment)s, %(service)s, %(resource)s, %(event)s, %(group)s, %(tags)s,
                %(customer)s, %(user)s, %(create_time)s, %(start_time)s, %(end_time)s, %(days)s, %(triggers)s::notification_triggers[], %(excluded_tags)s )
            RETURNING *
        """
        test = self._insert(insert, vars(escalation_rule))
        return test

    def get_escalation_rule(self, id, customers=None):
        select = """
            SELECT * FROM escalation_rules
            WHERE id=%(id)s
              AND {customer}
        """.format(
            customer='customer=ANY(%(customers)s)' if customers else '1=1'
        )
        return self._fetchone(select, {'id': id, 'customers': customers})

    def get_escalation_rules(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT * FROM escalation_rules
             WHERE {where}
          ORDER BY {order}
        """.format(
            where=query.where, order=query.sort
        )
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_escalation_rules_count(self, query=None):
        query = query or Query()
        select = """
            SELECT COUNT(1) FROM escalation_rules
             WHERE {where}
        """.format(
            where=query.where
        )
        return self._fetchone(select, query.vars).count

    def get_escalation_alerts(self):
        select = """
            WITH alert_triggers as (
                SELECT DISTINCT a.*
                FROM public.alerts as a, public.escalation_rules as e, generate_subscripts(e.triggers,1) as s
                WHERE e.active
                    AND a.status = 'open'
                    AND (%(now)s - a.last_receive_time > e.time)
                    AND a.environment=e.environment
                    AND (e.resource IS NULL OR e.resource=a.resource OR e.resource = '')
                    AND (e.service='{}' OR e.service <@ a.service)
                    AND (e.event IS NULL OR e.event=a.event or e.event = '')
                    AND (e.group IS NULL OR e.group=a.group)
                    AND (((e.triggers[s].from_severity='{}' OR ARRAY[a.previous_severity] <@ e.triggers[s].from_severity) AND (e.triggers[s].to_severity='{}' OR ARRAY[a.severity] <@ e.triggers[s].to_severity)))
            ),
            alert_tags AS (
                SELECT DISTINCT a.* from alert_triggers as a, public.escalation_rules as e, generate_subscripts(e.tags,1) as t
                WHERE (e.tags[t].all='{}' OR e.tags[t].all <@ a.tags)
                    AND (e.tags[t].any='{}' OR e.tags[t].any && a.tags)
            ), alert_excluded AS (
                SELECT a.id FROM alert_tags as a, public.escalation_rules as e, unnest(e.excluded_tags) as t("e_all","e_any")
                WHERE NOT ("e_all"='{}' AND "e_any"='{}')
                AND (
                    ("e_all"='{}' AND a.tags && "e_any")
                    OR ("e_any"='{}' AND a.tags @> "e_all")
                    OR (a.tags @> "e_all" AND a.tags && "e_any")
                )
            )

            SELECT * from alert_tags
            WHERE alert_tags.id NOT IN (SELECT DISTINCT alert_excluded.id FROM alert_excluded)
        """
        return self._fetchall(select, {'now': datetime.utcnow()}, limit='ALL')

    def update_escalation_rule(self, id, **kwargs):
        update = """
            UPDATE escalation_rules
            SET
        """
        if kwargs.get('environment') is not None:
            update += 'environment=%(environment)s, '
        if 'service' in kwargs:
            update += 'service=%(service)s, '
        if 'time' in kwargs:
            update += '"time"=%(time)s, '
        if 'resource' in kwargs:
            update += 'resource=%(resource)s, '
        if 'event' in kwargs:
            update += 'event=%(event)s, '
        if 'group' in kwargs:
            update += '"group"=%(group)s, '
        if 'tags' in kwargs:
            update += 'tags=%(tags)s::advanced_tags[], '
        if 'excludedTags' in kwargs:
            update += 'excluded_tags=%(excludedTags)s::advanced_tags[], '
        if 'customer' in kwargs:
            update += 'customer=%(customer)s, '
        if 'startTime' in kwargs:
            update += 'start_time=%(startTime)s, '
        if 'endTime' in kwargs:
            update += 'end_time=%(endTime)s, '
        if 'days' in kwargs:
            update += 'days=%(days)s, '
        if kwargs.get('severity') is not None:
            update += 'severity=%(severity)s, '
        if kwargs.get('triggers') is not None:
            update += 'triggers=%(triggers)s::notification_triggers[], '
        if 'active' in kwargs:
            update += 'active=%(active)s,'
        update += """
            "user"=COALESCE(%(user)s, "user")
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        kwargs['user'] = kwargs.get('user')
        return self._updateone(update, kwargs, returning=True)

    def delete_escalation_rule(self, id):
        delete = """
            DELETE FROM escalation_rules
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    # ON CALLS

    def create_on_call(self, on_call):
        insert = """
            INSERT INTO on_calls (id, user_ids, group_ids, "start_date", end_date, start_time, end_time, "user", customer,
                repeat_type, repeat_days, repeat_weeks, repeat_months)
            VALUES (%(id)s, %(user_ids)s, %(group_ids)s, %(start_date)s, %(end_date)s, %(start_time)s, %(end_time)s, %(user)s, %(customer)s,
                %(repeat_type)s, %(repeat_days)s, %(repeat_weeks)s, %(repeat_months)s)
            RETURNING *
        """
        return self._insert(insert, vars(on_call))

    def get_on_call(self, id, customers=None):
        select = """
            SELECT * FROM on_calls
            WHERE id=%(id)s
              AND {customer}
        """.format(
            customer='customer=ANY(%(customers)s)' if customers else '1=1'
        )
        return self._fetchone(select, {'id': id, 'customers': customers})

    def get_on_calls(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT * FROM on_calls
             WHERE {where}
          ORDER BY {order}
        """.format(
            where=query.where, order=query.sort
        )
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size if page else None)

    def get_on_calls_count(self, query=None):
        query = query or Query()
        select = """
            SELECT COUNT(1) FROM on_calls
             WHERE {where}
        """.format(
            where=query.where
        )
        return self._fetchone(select, query.vars).count

    def get_on_calls_active(self, alert):
        date_data = {}
        date_data['date'] = alert.create_time.date()
        date_data['time'] = alert.create_time.time()
        date_data['day'] = alert.create_time.strftime('%a')
        _year, date_data['week'], _day_number = alert.create_time.isocalendar()
        date_data['month'] = alert.create_time.strftime('%b')
        select = """
            SELECT *
            FROM on_calls
            WHERE ((start_time IS NULL OR start_time <= %(time)s) AND (end_time IS NULL OR end_time > %(time)s))
            AND (
              (start_date = %(date)s) OR (start_date < %(date)s AND end_date >= %(date)s)
              OR (
                repeat_type = 'list'
                AND (repeat_days IS NULL OR repeat_days='{}' OR ARRAY[%(day)s] <@ repeat_days)
                AND (repeat_weeks IS NULL OR repeat_weeks='{}' OR ARRAY[%(week)s] <@ repeat_weeks)
                AND (repeat_months IS NULL OR repeat_months='{}' OR ARRAY[%(month)s] <@ repeat_months)
              )
            )

        """
        if current_app.config['CUSTOMER_VIEWS']:
            select += ' AND (customer IS NULL OR customer=%(customer)s)'
        return self._fetchall(select, {**vars(alert), **date_data})

    def update_on_call(self, id, **kwargs):
        update = """
            UPDATE on_calls
            SET
        """
        if 'userIds' in kwargs:
            update += 'user_ids=%(userIds)s, '
        if 'groupIds' in kwargs:
            update += 'group_ids=%(groupIds)s, '
        if 'startDate' in kwargs:
            update += 'start_date=%(startDate)s, '
        if 'endDate' in kwargs:
            update += '"end_date"=%(endDate)s, '
        if 'startTime' in kwargs:
            update += 'start_time=%(startTime)s, '
        if 'endTime' in kwargs:
            update += 'end_time=%(endTime)s, '
        if 'fullDay' in kwargs:
            update += 'full_day=%(fullDay)s, '
        if 'repeatType' in kwargs:
            update += 'repeat_type=%(repeatType)s, '
        if 'repeatDays' in kwargs:
            update += 'repeat_days=%(repeatDays)s, '
        if 'repeatWeeks' in kwargs:
            update += 'repeat_weeks=%(repeatWeeks)s, '
        if 'repeatMonths' in kwargs:
            update += 'repeat_months=%(repeatMonths)s,'

        update += """
            "user"=COALESCE(%(user)s, "user")
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        kwargs['user'] = kwargs.get('user')
        return self._updateone(update, kwargs, returning=True)

    def delete_on_call(self, id):
        delete = """
            DELETE FROM on_calls
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    # HEARTBEATS

    def upsert_heartbeat(self, heartbeat):
        upsert = """
            INSERT INTO heartbeats (id, origin, tags, attributes, type, create_time, timeout, receive_time, customer)
            VALUES (%(id)s, %(origin)s, %(tags)s, %(attributes)s, %(event_type)s, %(create_time)s, %(timeout)s, %(receive_time)s, %(customer)s)
            ON CONFLICT (origin, COALESCE(customer, '')) DO UPDATE
                SET tags=%(tags)s, attributes=%(attributes)s, create_time=%(create_time)s, timeout=%(timeout)s, receive_time=%(receive_time)s
            RETURNING *,
                   EXTRACT(EPOCH FROM (receive_time - create_time)) AS latency,
                   EXTRACT(EPOCH FROM (NOW() - receive_time)) AS since
        """
        return self._upsert(upsert, vars(heartbeat))

    def get_heartbeat(self, id, customers=None):
        select = """
            SELECT *,
                   EXTRACT(EPOCH FROM (receive_time - create_time)) AS latency,
                   EXTRACT(EPOCH FROM (NOW() - receive_time)) AS since
              FROM heartbeats
             WHERE (id=%(id)s OR id LIKE %(like_id)s)
               AND {customer}
        """.format(customer='customer=%(customers)s' if customers else '1=1')
        return self._fetchone(select, {'id': id, 'like_id': id + '%', 'customers': customers})

    def get_heartbeats(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT *,
                   EXTRACT(EPOCH FROM (receive_time - create_time)) AS latency,
                   EXTRACT(EPOCH FROM (NOW() - receive_time)) AS since
              FROM heartbeats
             WHERE {where}
          ORDER BY {order}
        """.format(where=query.where, order=query.sort)
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_heartbeats_by_status(self, status=None, query=None, page=None, page_size=None):
        status = status or list()
        query = query or Query()

        swhere = ''
        if status:
            q = list()
            if HeartbeatStatus.OK in status:
                q.append(
                    """
                    (EXTRACT(EPOCH FROM (NOW() at time zone 'utc' - receive_time)) <= timeout
                    AND EXTRACT(EPOCH FROM (receive_time - create_time)) * 1000 <= {max_latency})
                    """.format(max_latency=current_app.config['HEARTBEAT_MAX_LATENCY']))
            if HeartbeatStatus.Expired in status:
                q.append("(EXTRACT(EPOCH FROM (NOW() at time zone 'utc' - receive_time)) > timeout)")
            if HeartbeatStatus.Slow in status:
                q.append(
                    """
                    (EXTRACT(EPOCH FROM (NOW() at time zone 'utc' - receive_time)) <= timeout
                    AND EXTRACT(EPOCH FROM (receive_time - create_time)) * 1000 > {max_latency})
                    """.format(max_latency=current_app.config['HEARTBEAT_MAX_LATENCY']))
            if q:
                swhere = 'AND (' + ' OR '.join(q) + ')'

        select = """
            SELECT *,
                   EXTRACT(EPOCH FROM (receive_time - create_time)) AS latency,
                   EXTRACT(EPOCH FROM (NOW() - receive_time)) AS since
              FROM heartbeats
             WHERE {where}
             {swhere}
          ORDER BY {order}
        """.format(where=query.where, swhere=swhere, order=query.sort)
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_heartbeats_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT COUNT(1) FROM heartbeats
             WHERE {query.where}
        """
        return self._fetchone(select, query.vars).count

    def delete_heartbeat(self, id):
        delete = """
            DELETE FROM heartbeats
            WHERE id=%(id)s OR id LIKE %(like_id)s
            RETURNING id
        """
        return self._deleteone(delete, {'id': id, 'like_id': id + '%'}, returning=True)

    # API KEYS

    def create_key(self, key):
        insert = """
            INSERT INTO keys (id, key, "user", scopes, text, expire_time, "count", last_used_time, customer)
            VALUES (%(id)s, %(key)s, %(user)s, %(scopes)s, %(text)s, %(expire_time)s, %(count)s, %(last_used_time)s, %(customer)s)
            RETURNING *
        """
        return self._insert(insert, vars(key))

    def get_key(self, key, user=None):
        select = f"""
            SELECT * FROM keys
             WHERE (id=%(key)s OR key=%(key)s)
               AND {'"user"=%(user)s' if user else '1=1'}
        """
        return self._fetchone(select, {'key': key, 'user': user})

    def get_keys(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = f"""
            SELECT * FROM keys
             WHERE {query.where}
          ORDER BY {query.sort}
        """
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_keys_by_user(self, user):
        select = """
            SELECT * FROM keys
             WHERE "user"=%s
        """
        return self._fetchall(select, (user,))

    def get_keys_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT COUNT(1) FROM keys
             WHERE {query.where}
        """
        return self._fetchone(select, query.vars).count

    def update_key(self, key, **kwargs):
        update = """
            UPDATE keys
            SET
        """
        if 'user' in kwargs:
            update += '"user"=%(user)s, '
        if 'scopes' in kwargs:
            update += 'scopes=%(scopes)s, '
        if 'text' in kwargs:
            update += 'text=%(text)s, '
        if 'expireTime' in kwargs:
            update += 'expire_time=%(expireTime)s, '
        if 'customer' in kwargs:
            update += 'customer=%(customer)s, '
        update += """
            id=id
            WHERE (id=%(key)s OR key=%(key)s)
            RETURNING *
        """
        kwargs['key'] = key
        return self._updateone(update, kwargs, returning=True)

    def update_key_last_used(self, key):
        update = """
            UPDATE keys
            SET last_used_time=NOW() at time zone 'utc', count=count + 1
            WHERE id=%s OR key=%s
        """
        return self._updateone(update, (key, key))

    def delete_key(self, key):
        delete = """
            DELETE FROM keys
            WHERE id=%s OR key=%s
            RETURNING key
        """
        return self._deleteone(delete, (key, key), returning=True)

    # USERS

    def create_user(self, user):
        insert = """
            INSERT INTO users (id, name, login, password, email, phone_number, country, status,
                roles, attributes, create_time, last_login, text, update_time, email_verified)
            VALUES (%(id)s, %(name)s, %(login)s, %(password)s, %(email)s, %(phone_number)s, %(country)s, %(status)s,
                %(roles)s, %(attributes)s, %(create_time)s, %(last_login)s, %(text)s, %(update_time)s, %(email_verified)s)
            RETURNING *
        """
        return self._insert(insert, vars(user))

    def get_user(self, id):
        select = """SELECT * FROM users WHERE id=%s"""
        return self._fetchone(select, (id,))

    def get_users(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = f"""
            SELECT * FROM users
             WHERE {query.where}
          ORDER BY {query.sort}
        """
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_users_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT COUNT(1) FROM users
             WHERE {query.where}
        """
        return self._fetchone(select, query.vars).count

    def get_user_by_username(self, username):
        select = """SELECT * FROM users WHERE login=%s OR email=%s"""
        return self._fetchone(select, (username, username))

    def get_user_by_email(self, email):
        select = """SELECT * FROM users WHERE email=%s"""
        return self._fetchone(select, (email,))

    def get_user_by_hash(self, hash):
        select = """SELECT * FROM users WHERE hash=%s"""
        return self._fetchone(select, (hash,))

    def update_last_login(self, id):
        update = """
            UPDATE users
            SET last_login=NOW() at time zone 'utc'
            WHERE id=%s
        """
        return self._updateone(update, (id,))

    def update_user(self, id, **kwargs):
        update = """
            UPDATE users
            SET
        """
        if kwargs.get('name', None) is not None:
            update += 'name=%(name)s, '
        if kwargs.get('login', None) is not None:
            update += 'login=%(login)s, '
        if kwargs.get('password', None) is not None:
            update += 'password=%(password)s, '
        if kwargs.get('email', None) is not None:
            update += 'email=%(email)s, '
        if kwargs.get('phoneNumber', None) is not None:
            update += 'phone_number=%(phoneNumber)s, '
        if kwargs.get('country', None) is not None:
            update += 'country=%(country)s, '
        if kwargs.get('status', None) is not None:
            update += 'status=%(status)s, '
        if kwargs.get('roles', None) is not None:
            update += 'roles=%(roles)s, '
        if kwargs.get('attributes', None) is not None:
            update += 'attributes=attributes || %(attributes)s, '
        if kwargs.get('text', None) is not None:
            update += 'text=%(text)s, '
        if kwargs.get('email_verified', None) is not None:
            update += 'email_verified=%(email_verified)s, '
        update += """
            update_time=NOW() at time zone 'utc'
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        return self._updateone(update, kwargs, returning=True)

    def update_user_attributes(self, id, old_attrs, new_attrs):
        from alerta.utils.collections import merge
        merge(old_attrs, new_attrs)
        attrs = {k: v for k, v in old_attrs.items() if v is not None}
        update = """
            UPDATE users
               SET attributes=%(attrs)s, update_time=NOW() at time zone 'utc'
             WHERE id=%(id)s
            RETURNING id
        """
        return bool(self._updateone(update, {'id': id, 'attrs': attrs}, returning=True))

    def delete_user(self, id):
        delete = """
            DELETE FROM users
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    def set_email_hash(self, id, hash):
        update = """
            UPDATE users
            SET hash=%s, update_time=NOW() at time zone 'utc'
            WHERE id=%s
        """
        return self._updateone(update, (hash, id))

    # GROUPS

    def create_group(self, group):
        insert = """
            INSERT INTO groups (id, name, text)
            VALUES (%(id)s, %(name)s, %(text)s)
            RETURNING *, 0 AS count
        """
        return self._insert(insert, vars(group))

    def get_group(self, id):
        select = """SELECT *, COALESCE(CARDINALITY(users), 0) AS count FROM groups WHERE id=%s"""
        return self._fetchone(select, (id,))

    def get_groups(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = """
            SELECT *, COALESCE(CARDINALITY(users), 0) AS count FROM groups
             WHERE {where}
          ORDER BY {order}
        """.format(where=query.where, order=query.sort)
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_groups_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT COUNT(1) FROM groups
             WHERE {query.where}
        """
        return self._fetchone(select, query.vars).count

    def get_group_users(self, id):
        select = """
            SELECT u.id, u.login, u.email, u.name, u.status
              FROM (SELECT id, UNNEST(users) as uid FROM groups) g
            INNER JOIN users u on g.uid = u.id
            WHERE g.id = %s
        """
        return self._fetchall(select, (id,))

    def update_group(self, id, **kwargs):
        update = """
            UPDATE groups
            SET
        """
        if kwargs.get('name', None) is not None:
            update += 'name=%(name)s, '
        if kwargs.get('text', None) is not None:
            update += 'text=%(text)s, '
        update += """
            update_time=NOW() at time zone 'utc'
            WHERE id=%(id)s
            RETURNING *, COALESCE(CARDINALITY(users), 0) AS count
        """
        kwargs['id'] = id
        return self._updateone(update, kwargs, returning=True)

    def add_user_to_group(self, group, user):
        update = """
            UPDATE groups
            SET users=ARRAY(SELECT DISTINCT UNNEST(users || %(users)s))
            WHERE id=%(id)s
            RETURNING *
        """
        return self._updateone(update, {'id': group, 'users': [user]}, returning=True)

    def remove_user_from_group(self, group, user):
        update = """
            UPDATE groups
            SET users=(select array_agg(u) FROM unnest(users) AS u WHERE NOT u=%(user)s )
            WHERE id=%(id)s
            RETURNING *
        """
        return self._updateone(update, {'id': group, 'user': user}, returning=True)

    def delete_group(self, id):
        delete = """
            DELETE FROM groups
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    def get_groups_by_user(self, user):
        select = """
            SELECT *, COALESCE(CARDINALITY(users), 0) AS count
              FROM groups
            WHERE %s=ANY(users)
        """
        return self._fetchall(select, (user,))

    # PERMISSIONS

    def create_perm(self, perm):
        insert = """
            INSERT INTO perms (id, match, scopes)
            VALUES (%(id)s, %(match)s, %(scopes)s)
            RETURNING *
        """
        return self._insert(insert, vars(perm))

    def get_perm(self, id):
        select = """SELECT * FROM perms WHERE id=%s"""
        return self._fetchone(select, (id,))

    def get_perms(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = f"""
            SELECT * FROM perms
             WHERE {query.where}
          ORDER BY {query.sort}
        """
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_perms_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT COUNT(1) FROM perms
             WHERE {query.where}
        """
        return self._fetchone(select, query.vars).count

    def update_perm(self, id, **kwargs):
        update = """
            UPDATE perms
            SET
        """
        if 'match' in kwargs:
            update += 'match=%(match)s, '
        if 'scopes' in kwargs:
            update += 'scopes=%(scopes)s, '
        update += """
            id=%(id)s
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        return self._updateone(update, kwargs, returning=True)

    def delete_perm(self, id):
        delete = """
            DELETE FROM perms
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    def get_scopes_by_match(self, login, matches):
        if login in current_app.config['ADMIN_USERS']:
            return ADMIN_SCOPES

        scopes = list()
        for match in matches:
            if match in current_app.config['ADMIN_ROLES']:
                return ADMIN_SCOPES
            if match in current_app.config['USER_ROLES']:
                scopes.extend(current_app.config['USER_DEFAULT_SCOPES'])
            if match in current_app.config['GUEST_ROLES']:
                scopes.extend(current_app.config['GUEST_DEFAULT_SCOPES'])
            select = """SELECT scopes FROM perms WHERE match=%s"""
            response = self._fetchone(select, (match,))
            if response:
                scopes.extend(response.scopes)
        return sorted(set(scopes))

    # CUSTOMERS

    def create_customer(self, customer):
        insert = """
            INSERT INTO customers (id, match, customer)
            VALUES (%(id)s, %(match)s, %(customer)s)
            RETURNING *
        """
        return self._insert(insert, vars(customer))

    def get_customer(self, id):
        select = """SELECT * FROM customers WHERE id=%s"""
        return self._fetchone(select, (id,))

    def get_customers(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = f"""
            SELECT * FROM customers
             WHERE {query.where}
          ORDER BY {query.sort}
        """
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_customers_count(self, query=None):
        query = query or Query()
        select = f"""
            SELECT COUNT(1) FROM customers
             WHERE {query.where}
        """
        return self._fetchone(select, query.vars).count

    def update_customer(self, id, **kwargs):
        update = """
            UPDATE customers
            SET
        """
        if 'match' in kwargs:
            update += 'match=%(match)s, '
        if 'customer' in kwargs:
            update += 'customer=%(customer)s, '
        update += """
            id=%(id)s
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        return self._updateone(update, kwargs, returning=True)

    def delete_customer(self, id):
        delete = """
            DELETE FROM customers
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    def get_customers_by_match(self, login, matches):
        if login in current_app.config['ADMIN_USERS']:
            return '*'  # all customers

        customers = []
        for match in [login] + matches:
            select = """SELECT customer FROM customers WHERE match=%s"""
            response = self._fetchall(select, (match,))
            if response:
                customers.extend([r.customer for r in response])

        if customers:
            if '*' in customers:
                return '*'  # all customers
            return customers

        raise NoCustomerMatch(f"No customer lookup configured for user '{login}' or '{','.join(matches)}'")

    # NOTES

    def create_note(self, note):
        insert = """
            INSERT INTO notes (id, text, "user", attributes, type,
                create_time, update_time, alert, customer)
            VALUES (%(id)s, %(text)s, %(user)s, %(attributes)s, %(note_type)s,
                %(create_time)s, %(update_time)s, %(alert)s, %(customer)s)
            RETURNING *
        """
        return self._insert(insert, vars(note))

    def get_note(self, id):
        select = """
            SELECT * FROM notes
            WHERE id=%s
        """
        return self._fetchone(select, (id,))

    def get_notes(self, query=None, page=None, page_size=None):
        query = query or Query()
        select = f"""
            SELECT * FROM notes
             WHERE {query.where}
          ORDER BY {query.sort or 'create_time'}
        """
        return self._fetchall(select, query.vars, limit=page_size, offset=(page - 1) * page_size)

    def get_alert_notes(self, id, page=None, page_size=None):
        select = """
            SELECT * FROM notes
             WHERE alert ~* (%s)
        """
        return self._fetchall(select, (id,), limit=page_size, offset=(page - 1) * page_size)

    def get_customer_notes(self, customer, page=None, page_size=None):
        select = """
            SELECT * FROM notes
             WHERE customer=%s
        """
        return self._fetchall(select, (customer,), limit=page_size, offset=(page - 1) * page_size)

    def update_note(self, id, **kwargs):
        update = """
            UPDATE notes
            SET
        """
        if kwargs.get('text', None) is not None:
            update += 'text=%(text)s, '
        if kwargs.get('attributes', None) is not None:
            update += 'attributes=attributes || %(attributes)s, '
        update += """
            "user"=COALESCE(%(user)s, "user"),
            update_time=NOW() at time zone 'utc'
            WHERE id=%(id)s
            RETURNING *
        """
        kwargs['id'] = id
        kwargs['user'] = kwargs.get('user')
        return self._updateone(update, kwargs, returning=True)

    def delete_note(self, id):
        delete = """
            DELETE FROM notes
            WHERE id=%s
            RETURNING id
        """
        return self._deleteone(delete, (id,), returning=True)

    # METRICS

    def get_metrics(self, type=None):
        select = """SELECT * FROM metrics"""
        if type:
            select += ' WHERE type=%s'
        return self._fetchall(select, (type,))

    def set_gauge(self, gauge):
        upsert = """
            INSERT INTO metrics ("group", name, title, description, value, type)
            VALUES (%(group)s, %(name)s, %(title)s, %(description)s, %(value)s, %(type)s)
            ON CONFLICT ("group", name, type) DO UPDATE
                SET value=%(value)s
            RETURNING *
        """
        return self._upsert(upsert, vars(gauge))

    def inc_counter(self, counter):
        upsert = """
            INSERT INTO metrics ("group", name, title, description, count, type)
            VALUES (%(group)s, %(name)s, %(title)s, %(description)s, %(count)s, %(type)s)
            ON CONFLICT ("group", name, type) DO UPDATE
                SET count=metrics.count + %(count)s
            RETURNING *
        """
        return self._upsert(upsert, vars(counter))

    def update_timer(self, timer):
        upsert = """
            INSERT INTO metrics ("group", name, title, description, count, total_time, type)
            VALUES (%(group)s, %(name)s, %(title)s, %(description)s, %(count)s, %(total_time)s, %(type)s)
            ON CONFLICT ("group", name, type) DO UPDATE
                SET count=metrics.count + %(count)s, total_time=metrics.total_time + %(total_time)s
            RETURNING *
        """
        return self._upsert(upsert, vars(timer))

    # HOUSEKEEPING

    def get_expired(self, expired_threshold, info_threshold):
        # delete 'closed' or 'expired' alerts older than "expired_threshold" seconds
        # and 'informational' alerts older than "info_threshold" seconds

        if expired_threshold:
            delete = """
                DELETE FROM alerts
                 WHERE (status IN ('closed', 'expired')
                        AND last_receive_time < (NOW() at time zone 'utc' - INTERVAL '%(expired_threshold)s seconds'))
            """
            self._deleteall(delete, {'expired_threshold': expired_threshold})

        if info_threshold:
            delete = """
                DELETE FROM alerts
                 WHERE (severity=%(inform_severity)s
                        AND last_receive_time < (NOW() at time zone 'utc' - INTERVAL '%(info_threshold)s seconds'))
            """
            self._deleteall(delete, {'inform_severity': alarm_model.DEFAULT_INFORM_SEVERITY, 'info_threshold': info_threshold})

        # get list of alerts to be newly expired
        select = """
            SELECT *
              FROM alerts
             WHERE status NOT IN ('expired') AND COALESCE(timeout, {timeout})!=0
               AND (last_receive_time + INTERVAL '1 second' * timeout) < NOW() at time zone 'utc'
        """.format(timeout=current_app.config['ALERT_TIMEOUT'])

        return self._fetchall(select, {})

    def get_unshelve(self):
        # get list of alerts to be unshelved
        select = """
            SELECT DISTINCT ON (a.id) a.*
              FROM alerts a, UNNEST(history) h
             WHERE a.status='shelved'
               AND h.type='shelve'
               AND h.status='shelved'
               AND COALESCE(h.timeout, {timeout})!=0
               AND (a.update_time + INTERVAL '1 second' * h.timeout) < NOW() at time zone 'utc'
          ORDER BY a.id, a.update_time DESC
        """.format(timeout=current_app.config['SHELVE_TIMEOUT'])
        return self._fetchall(select, {})

    def get_unack(self):
        # get list of alerts to be unack'ed
        select = """
            SELECT DISTINCT ON (a.id) a.*
              FROM alerts a, UNNEST(history) h
             WHERE a.status='ack'
               AND h.type='ack'
               AND h.status='ack'
               AND COALESCE(h.timeout, {timeout})!=0
               AND (a.update_time + INTERVAL '1 second' * h.timeout) < NOW() at time zone 'utc'
          ORDER BY a.id, a.update_time DESC
        """.format(timeout=current_app.config['ACK_TIMEOUT'])
        return self._fetchall(select, {})

    # SQL HELPERS

    def _insert(self, query, vars):
        """
        Insert, with return.
        """
        cursor = self.get_db().cursor()
        self._log(cursor, query, vars)
        cursor.execute(query, vars)
        self.get_db().commit()
        return cursor.fetchone()

    def _fetchone(self, query, vars):
        """
        Return none or one row.
        """
        cursor = self.get_db().cursor()
        self._log(cursor, query, vars)
        cursor.execute(query, vars)
        return cursor.fetchone()

    def _fetchall(self, query, vars, limit=None, offset=0):
        """
        Return multiple rows.
        """
        if limit is None:
            limit = current_app.config['DEFAULT_PAGE_SIZE']
        query += f' LIMIT {limit} OFFSET {offset}'
        cursor = self.get_db().cursor()
        self._log(cursor, query, vars)
        cursor.execute(query, vars)
        return cursor.fetchall()

    def _updateone(self, query, vars, returning=False):
        """
        Update, with optional return.
        """
        cursor = self.get_db().cursor()
        self._log(cursor, query, vars)
        cursor.execute(query, vars)
        self.get_db().commit()
        return cursor.fetchone() if returning else None

    def _updateall(self, query, vars, returning=False):
        """
        Update, with optional return.
        """
        cursor = self.get_db().cursor()
        self._log(cursor, query, vars)
        cursor.execute(query, vars)
        self.get_db().commit()
        return cursor.fetchall() if returning else None

    def _upsert(self, query, vars):
        """
        Insert or update, with return.
        """
        return self._insert(query, vars)

    def _deleteone(self, query, vars, returning=False):
        """
        Delete, with optional return.
        """
        cursor = self.get_db().cursor()
        self._log(cursor, query, vars)
        cursor.execute(query, vars)
        self.get_db().commit()
        return cursor.fetchone() if returning else None

    def _deleteall(self, query, vars, returning=False):
        """
        Delete multiple rows, with optional return.
        """
        cursor = self.get_db().cursor()
        self._log(cursor, query, vars)
        cursor.execute(query, vars)
        self.get_db().commit()
        return cursor.fetchall() if returning else None

    def _log(self, cursor, query, vars):
        current_app.logger.debug('{stars}\n{query}\n{stars}'.format(
            stars='*' * 40, query=cursor.mogrify(query, vars).decode('utf-8')))
