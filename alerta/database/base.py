from importlib import import_module
from importlib.metadata import entry_points
from typing import NamedTuple
from urllib.parse import urlparse

from flask import g

# http://stackoverflow.com/questions/8544983/dynamically-mixin-a-base-class-to-an-instance-in-python


class Query(NamedTuple):
    where: str
    sort: str
    group: str


class Base:
    pass


def get_backend(app):
    db_uri = app.config['DATABASE_URL']
    backend = urlparse(db_uri).scheme

    if backend.startswith('mongodb'):
        backend = 'mongodb'
    if backend == 'postgresql':
        backend = 'postgres'
    return backend


def load_backend(backend):
    for ep in entry_points(group='alerta.database.backends'):
        if ep.name == backend:
            module_name = ep.module
            break
    else:
        module_name = f'alerta.database.backends.{backend}'

    try:
        return import_module(module_name)
    except Exception:
        raise ImportError(f'Failed to load {backend} database backend')


class Database(Base):

    def __init__(self, app=None):
        self.app = None
        if app is not None:
            self.init_db(app)

    def init_db(self, app):
        backend = get_backend(app)
        cls = load_backend(backend)
        self.__class__ = type('DatabaseImpl', (cls.Backend, Database), {})

        try:
            self.create_engine(app, uri=app.config['DATABASE_URL'], dbname=app.config['DATABASE_NAME'], schema=app.config['DATABASE_SCHEMA'],
                               raise_on_error=app.config['DATABASE_RAISE_ON_ERROR'])
        except Exception as e:
            if app.config['DATABASE_RAISE_ON_ERROR']:
                raise
            app.logger.warning(e)

        app.teardown_appcontext(self.teardown_db)

    def create_engine(self, app, uri, dbname=None, schema=None, raise_on_error=True):
        raise NotImplementedError('Database engine has no create_engine() method')

    def connect(self):
        raise NotImplementedError('Database engine has no connect() method')

    @property
    def name(self):
        raise NotImplementedError

    @property
    def version(self):
        raise NotImplementedError

    @property
    def is_alive(self):
        raise NotImplementedError

    def close(self, db):
        raise NotImplementedError('Database engine has no close() method')

    def destroy(self):
        raise NotImplementedError('Database engine has no destroy() method')

    def get_db(self):
        if 'db' not in g:
            g.db = self.connect()
        return g.db

    def teardown_db(self, exc):
        db = g.pop('db', None)
        if db is not None:
            self.close(db)

    # ALERTS

    def get_severity(self, alert):
        raise NotImplementedError

    def get_status(self, alert):
        raise NotImplementedError

    def is_duplicate(self, alert):
        raise NotImplementedError

    def is_correlated(self, alert):
        raise NotImplementedError

    def is_flapping(self, alert, window=1800, count=2):
        raise NotImplementedError

    def dedup_alert(self, alert, history):
        raise NotImplementedError

    def correlate_alert(self, alert, history):
        raise NotImplementedError

    def create_alert(self, alert):
        raise NotImplementedError

    def get_escalate(self):
        raise NotImplementedError

    def set_alert(self, id, severity, status, tags, attributes, timeout, previous_severity, update_time, history=None):
        raise NotImplementedError

    def get_alert(self, id, customers=None):
        raise NotImplementedError

    # STATUS, TAGS, ATTRIBUTES

    def set_status(self, id, status, timeout, update_time, history=None):
        raise NotImplementedError

    def tag_alert(self, id, tags):
        raise NotImplementedError

    def untag_alert(self, id, tags):
        raise NotImplementedError

    def update_tags(self, id, tags):
        raise NotImplementedError

    def update_attributes(self, id, old_attrs, new_attrs):
        raise NotImplementedError

    def add_history(self, id, history):
        raise NotImplementedError

    def delete_alert(self, id):
        raise NotImplementedError

    # BULK

    def tag_alerts(self, query=None, tags=None):
        raise NotImplementedError

    def untag_alerts(self, query=None, tags=None):
        raise NotImplementedError

    def update_attributes_by_query(self, query=None, attributes=None):
        raise NotImplementedError

    def delete_alerts(self, query=None):
        raise NotImplementedError

    # SEARCH & HISTORY

    def get_alerts(self, query=None, raw_data=False, history=False, page=None, page_size=None):
        raise NotImplementedError

    def get_alert_history(self, alert, page=None, page_size=None):
        raise NotImplementedError

    def get_history(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_history_count(self, query=None):
        raise NotImplementedError

    def get_history_environment_count(self, query=None):
        raise NotImplementedError

    # COUNTS

    def get_count(self, query=None):
        raise NotImplementedError

    def get_counts(self, query=None, group=None):
        raise NotImplementedError

    def get_counts_by_severity(self, query=None):
        raise NotImplementedError

    def get_counts_by_status(self, query=None):
        raise NotImplementedError

    def get_topn_count(self, query, group='event', topn=100):
        raise NotImplementedError

    def get_topn_flapping(self, query, group='event', topn=100):
        raise NotImplementedError

    def get_topn_standing(self, query, group='event', topn=100):
        raise NotImplementedError

    # ENVIRONMENTS

    def get_environments(self, query=None, topn=1000):
        raise NotImplementedError

    # SERVICES

    def get_services(self, query=None, topn=1000):
        raise NotImplementedError

    # ALERT GROUPS

    def get_alert_groups(self, query=None, topn=1000):
        raise NotImplementedError

    # ALERT TAGS

    def get_alert_tags(self, query=None, topn=1000):
        raise NotImplementedError

    # BLACKOUTS

    def create_blackout(self, blackout):
        raise NotImplementedError

    def get_blackout(self, id, customers=None):
        raise NotImplementedError

    def get_blackouts(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_blackouts_count(self, query=None):
        raise NotImplementedError

    def is_blackout_period(self, alert):
        raise NotImplementedError

    def update_blackout(self, id, **kwargs):
        raise NotImplementedError

    def delete_blackout(self, id):
        raise NotImplementedError

    # NOTIFICATION CHANNELS

    def create_notification_channel(self, notification_channel):
        raise NotImplementedError

    def get_notification_channel(self, id, customers=None):
        raise NotImplementedError

    def get_notification_channels(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_notification_channels_count(self, query=None):
        raise NotImplementedError

    def get_notification_channels_active(self, alert):
        raise NotImplementedError

    def update_notification_channel(self, id, **kwargs):
        raise NotImplementedError

    def delete_notification_channel(self, id):
        raise NotImplementedError

    # DELAYED NOTIFICATIONS

    def create_delayed_notification(self, delayed_notification):
        raise NotImplementedError

    def get_delayed_notifications(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_delayed_notification(self, id):
        raise NotImplementedError

    def get_delayed_notifications_firing(self, time):
        raise NotImplementedError

    def get_delayed_notifications_count(self, query=None):
        raise NotImplementedError

    def delete_delayed_notifications_alert(self, alert_id):
        raise NotImplementedError

    def delete_delayed_notification(self, id):
        raise NotImplementedError

    # NOTIFICATION RULES
    def create_notification_rule_history(self, change_type: str, notification_rule):
        raise NotImplementedError

    def get_notification_rule_history(self, rule_id: str):
        raise NotImplementedError

    def get_notification_rule_history_count(self, rule_id: str):
        raise NotImplementedError

    def create_notification_rule(self, notification_rule):
        raise NotImplementedError

    def get_notification_rule(self, id, customers=None):
        raise NotImplementedError

    def get_notification_rules(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_notification_rules_count(self, query=None):
        raise NotImplementedError

    def get_notification_rules_active(self, alert):
        raise NotImplementedError

    def get_notification_rules_active_status(self, alert):
        raise NotImplementedError

    def get_notification_rules_reactivate(self, time):
        raise NotImplementedError

    def update_notification_rule(self, id, **kwargs):
        raise NotImplementedError

    def delete_notification_rule(self, id):
        raise NotImplementedError

    # NOTIFICATION GROUPS

    def create_notification_group(self, notification_group):
        raise NotImplementedError

    def get_notification_group(self, id, customers=None):
        raise NotImplementedError

    def get_notification_groups(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_notification_group_users(self, id):
        raise NotImplementedError

    def get_notification_groups_count(self, query=None):
        raise NotImplementedError

    def update_notification_group(self, id, **kwargs):
        raise NotImplementedError

    def delete_notification_group(self, id):
        raise NotImplementedError

    # NOTIFICATION SEND
    def get_notification_sends(self):
        raise NotImplementedError

    def update_notification_send(self, id, **kwargs):
        raise NotImplementedError

    # NOTIFICATION HISTORY

    def create_notification_history(self, notification_history):
        raise NotImplementedError

    def get_notification_history(self, id, customers=None):
        raise NotImplementedError

    def get_notifications_history(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_notifications_history_count(self, query=None):
        raise NotImplementedError

    def confirm_notification_history(self, id):
        raise NotImplementedError

    # ESCALATION RULES

    def create_escalation_rule(self, escalation_rule):
        raise NotImplementedError

    def get_escalation_rule(self, id, customers=None):
        raise NotImplementedError

    def get_escalation_rules(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_escalation_rules_count(self, query=None):
        raise NotImplementedError

    def get_escalation_alerts(self, alert):
        raise NotImplementedError

    def update_escalation_rule(self, id, **kwargs):
        raise NotImplementedError

    def delete_escalation_rule(self, id):
        raise NotImplementedError

    # ON CALLS

    def create_on_call(self, on_call):
        raise NotImplementedError

    def get_on_call(self, id, customers=None):
        raise NotImplementedError

    def get_on_calls(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_on_calls_count(self, query=None):
        raise NotImplementedError

    def get_on_calls_active(self, alert):
        raise NotImplementedError

    def update_on_call(self, id, **kwargs):
        raise NotImplementedError

    def delete_on_call(self, id):
        raise NotImplementedError

    # HEARTBEATS

    def upsert_heartbeat(self, heartbeat):
        raise NotImplementedError

    def get_heartbeat(self, id, customers=None):
        raise NotImplementedError

    def get_heartbeats(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_heartbeats_by_status(self, status=None, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_heartbeats_count(self, query=None):
        raise NotImplementedError

    def delete_heartbeat(self, id):
        raise NotImplementedError

    # API KEYS

    def create_key(self, key):
        raise NotImplementedError

    def get_key(self, key, user=None):
        raise NotImplementedError

    def get_keys(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_keys_by_user(self, user):
        raise NotImplementedError

    def get_keys_count(self, query=None):
        raise NotImplementedError

    def update_key(self, key, **kwargs):
        raise NotImplementedError

    def update_key_last_used(self, key):
        raise NotImplementedError

    def delete_key(self, key):
        raise NotImplementedError

    # USERS

    def create_user(self, user):
        raise NotImplementedError

    def get_user(self, id):
        raise NotImplementedError

    def get_users(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_users_count(self, query=None):
        raise NotImplementedError

    def get_user_by_username(self, username):
        raise NotImplementedError

    def get_user_by_email(self, email):
        raise NotImplementedError

    def get_user_by_hash(self, hash):
        raise NotImplementedError

    def update_last_login(self, id):
        raise NotImplementedError

    def update_user(self, id, **kwargs):
        raise NotImplementedError

    def update_user_attributes(self, id, old_attrs, new_attrs):
        raise NotImplementedError

    def delete_user(self, id):
        raise NotImplementedError

    def set_email_hash(self, id, hash):
        raise NotImplementedError

    # GROUPS

    def create_group(self, group):
        raise NotImplementedError

    def get_group(self, id):
        raise NotImplementedError

    def get_group_users(self, id):
        raise NotImplementedError

    def get_groups(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_groups_count(self, query=None):
        raise NotImplementedError

    def update_group(self, id, **kwargs):
        raise NotImplementedError

    def add_user_to_group(self, group, user):
        raise NotImplementedError

    def remove_user_from_group(self, group, user):
        raise NotImplementedError

    def delete_group(self, id):
        raise NotImplementedError

    def get_groups_by_user(self, user):
        raise NotImplementedError

    # PERMISSIONS

    def create_perm(self, perm):
        raise NotImplementedError

    def get_perm(self, id):
        raise NotImplementedError

    def get_perms(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_perms_count(self, query=None):
        raise NotImplementedError

    def update_perm(self, id, **kwargs):
        raise NotImplementedError

    def delete_perm(self, id):
        raise NotImplementedError

    def get_scopes_by_match(self, login, matches):
        raise NotImplementedError

    # CUSTOMERS

    def create_customer(self, customer):
        raise NotImplementedError

    def get_customer(self, id):
        raise NotImplementedError

    def get_customers(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_customers_count(self, query=None):
        raise NotImplementedError

    def update_customer(self, id, **kwargs):
        raise NotImplementedError

    def delete_customer(self, id):
        raise NotImplementedError

    def get_customers_by_match(self, login, matches):
        raise NotImplementedError

    # NOTES

    def create_note(self, note):
        raise NotImplementedError

    def get_note(self, id):
        raise NotImplementedError

    def get_notes(self, query=None, page=None, page_size=None):
        raise NotImplementedError

    def get_alert_notes(self, id, page=None, page_size=None):
        raise NotImplementedError

    def get_customer_notes(self, id, page=None, page_size=None):
        raise NotImplementedError

    def update_note(self, id, **kwargs):
        raise NotImplementedError

    def delete_note(self, id):
        raise NotImplementedError

    # METRICS

    def get_metrics(self, type=None):
        raise NotImplementedError

    def set_gauge(self, gauge):
        raise NotImplementedError

    def inc_counter(self, counter):
        raise NotImplementedError

    def update_timer(self, timer):
        raise NotImplementedError

    # HOUSEKEEPING

    def get_expired(self, expired_threshold, info_threshold):
        raise NotImplementedError

    def get_unshelve(self):
        raise NotImplementedError

    def get_unack(self):
        raise NotImplementedError


class QueryBuilder(Base):

    def __init__(self, app=None):
        self.app = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        backend = get_backend(app)
        cls = load_backend(backend)

        self.__class__.alerts = type('AlertsQueryBuilder', (cls.Alerts, self.Alerts, QueryBuilder), {})
        self.__class__.blackouts = type('BlackoutsQueryBuilder', (cls.Blackouts, self.Blackouts, QueryBuilder), {})
        self.__class__.notification_channels = type('NotificationChannelsQueryBuilder', (cls.NotificationChannels, self.NotificationChannels, QueryBuilder), {})
        self.__class__.notification_delay = type('NotificationDelaysQueryBuilder', (cls.NotificationDelays, self.NotificationDelays, QueryBuilder), {})
        self.__class__.notification_rules = type('NotificationRulesQueryBuilder', (cls.NotificationRules, self.NotificationRules, QueryBuilder), {})
        self.__class__.notification_history = type('NotificationRulesHistoryBuilder', (cls.NotificationHistory, self.NotificationHistory, QueryBuilder), {})
        self.__class__.escalation_rules = type('EscalationRulesQueryBuilder', (cls.EscalationRules, self.EscalationRules, QueryBuilder), {})
        self.__class__.on_calls = type('OnCallQueryBuilder', (cls.OnCalls, self.OnCalls, QueryBuilder), {})
        self.__class__.notification_groups = type('NotificationGroupQueryBuilder', (cls.NotificationGroups, self.NotificationGroups, QueryBuilder), {})
        self.__class__.heartbeats = type('HeartbeatsQueryBuilder', (cls.Heartbeats, self.Heartbeats, QueryBuilder), {})
        self.__class__.keys = type('ApiKeysQueryBuilder', (cls.ApiKeys, self.ApiKeys, QueryBuilder), {})
        self.__class__.users = type('UsersQueryBuilder', (cls.Users, self.Users, QueryBuilder), {})
        self.__class__.groups = type('GroupsQueryBuilder', (cls.Groups, self.Groups, QueryBuilder), {})
        self.__class__.perms = type('PermissionsQueryBuilder', (cls.Permissions, self.Permissions, QueryBuilder), {})
        self.__class__.customers = type('CustomersQueryBuilder', (cls.Customers, self.Customers, QueryBuilder), {})

    class Alerts:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('AlertsQueryBuilder has no from_params() method for alerts')

    class Blackouts:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('BlackoutsQueryBuilder has no from_params() method')

    class NotificationChannels:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('NotificationChannelsQueryBuilder has no from_params() method')

    class NotificationDelays:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('NotificationChannelsQueryBuilder has no from_params() method')

    class NotificationHistory:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('NotificationChannelsQueryBuilder has no from_params() method')

    class NotificationRules:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('NotificationRulesQueryBuilder has no from_params() method')

    class OnCalls:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('OnCallsQueryBuilder has no from_params() method')

    class NotificationGroups:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('NotificationGroupsQueryBuilder has no from_params() method')

    class EscalationRules:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('EscalationRulesQueryBuilder has no from_params() method')

    class Heartbeats:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('HeartbeatsQueryBuilder has no from_params() method')

    class ApiKeys:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('ApiKeysQueryBuilder has no from_params() method')

    class Users:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('UsersQueryBuilder has no from_params() method')

    class Groups:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('GroupsQueryBuilder has no from_params() method')

    class Permissions:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('PermissionsQueryBuilder has no from_params() method')

    class Customers:

        @staticmethod
        def from_params(params, customers=None, query_time=None):
            raise NotImplementedError('CustomersQueryBuilder has no from_params() method')
