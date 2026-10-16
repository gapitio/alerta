from datetime import UTC, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from alerta.app import db
from alerta.database.base import Query
from alerta.models.alert import Alert
from alerta.models.notification_channel import NotificationChannel
from alerta.models.notification_group import NotificationGroup
from alerta.models.user import NotificationInfo, User
from alerta.utils.response import absolute_url

JSON = Dict[str, Any]


class AdvancedTags:
    def __init__(self, all: 'list[str]', any: 'list[str]') -> None:
        self.all = all or []
        self.any = any or []

    @property
    def serialize(self):
        return {
            'all': self.all,
            'any': self.any,
        }

    def __repr__(self):
        return 'AdvancedTags(all={!r}, any={!r})'.format(
            self.all, self.any)

    @classmethod
    def parse(cls, obj: dict):
        return AdvancedTags(
            all=obj.get('all'),
            any=obj.get('any'),
        )

    @classmethod
    def from_db(cls, r):
        if r is not None:
            return AdvancedTags(
                all=r.all,
                any=r.any,
            )


class NotificationTriggers:
    def __init__(self, from_severity: 'list[str]', to_severity: 'list[str]', status: 'list[str]' = [], text: str = '') -> None:
        self.from_severity = from_severity or []
        self.to_severity = to_severity or []
        self.status = status or []
        self.text = text

    @property
    def serialize(self):
        return {
            'from_severity': self.from_severity,
            'to_severity': self.to_severity,
            'status': self.status,
            'text': self.text
        }

    def __repr__(self):
        return 'AdvancedSeverity(from={!r}, to={!r}, status={!r}, text={!r})'.format(
            self.from_severity, self.to_severity, self.status, self.text)

    @classmethod
    def parse(cls, r: dict):
        if r is not None:
            return NotificationTriggers(
                from_severity=r.get('from_severity', []),
                to_severity=r.get('to_severity', []),
                status=r.get('status', []),
                text=r.get('text')
            )

    @classmethod
    def from_db(cls, r):
        if r is not None:
            return NotificationTriggers(
                from_severity=r.from_severity,
                to_severity=r.to_severity,
                status=r.status,
                text=r.text
            )


class NotificationRuleHistory:
    def __init__(self, id: 'str', rule_id: 'str', user: 'str', update_type: 'str', create_time, data) -> None:
        self.id = id
        self.rule_id = rule_id
        self.user = user
        self.type = update_type
        self.create_time = create_time
        self.data = data

    @property
    def serialize(self):
        return {
            'id':self.id,
            'ruleId':self.rule_id,
            'user':self.user,
            'type':self.type,
            'createTime':self.create_time,
            'data':self.data,
        }

    def __repr__(self):
        return 'NotificationRuleHistory(id={!r}, type={!r}, rule={!r})'.format(
            self.id, self.type, self.rule_id)

    @classmethod
    def from_db(cls, r):
        if r is not None:
            return NotificationRuleHistory(
                id=r.id,
                rule_id=r.rule_id,
                user=r.user,
                update_type=r.type,
                create_time=r.create_time,
                data=r.rule_data
            )


class NotificationRule:
    def __init__(
        self, environment: str, channel_id: str, receivers: List[str], use_oncall: bool, **kwargs
    ) -> None:
        if not environment:
            raise ValueError('Missing mandatory value for "environment"')
        if not channel_id:
            raise ValueError('Missing mandatory value for "notification_channel"')
        if not isinstance(receivers, list):
            raise ValueError('Missing mandatory value for "receivers"')

        self.id = kwargs.get('id') or str(uuid4())
        self.name = kwargs.get('name')
        self.active = kwargs.get('active', True)
        self.environment = environment
        self.channel_id = channel_id
        self.receivers = receivers
        self.users_emails = kwargs.get('users_emails') or []
        self.group_ids = kwargs.get('group_ids') or []
        self.use_oncall = use_oncall
        self.start_time: time = kwargs.get('start_time') or None
        self.end_time: time = kwargs.get('end_time') or None
        self.service = kwargs.get('service', None) or list()
        self.resource = kwargs['resource'] if kwargs.get('resource', '') != '' else None
        self.event = kwargs['event'] if kwargs.get('event', '') != '' else None
        self.tags = kwargs.get('tags', None) or list()
        self.tags = kwargs.get('tags') or [AdvancedTags([], [])]
        self.excluded_tags = kwargs.get('excluded_tags', None) or [AdvancedTags([], [])]
        self.customer = kwargs.get('customer', None)
        self.days = kwargs.get('days', None) or list()
        self.triggers = kwargs.get('triggers') or [NotificationTriggers([], [], [])]
        self.reactivate = kwargs.get('reactivate', None)
        self.delay_time: timedelta = kwargs.get('delay_time', None)

        self.user = kwargs.get('user', None)
        self.create_time = (
            kwargs['create_time'] if 'create_time' in kwargs else datetime.now(UTC)
        )
        self.text = kwargs.get('text', None)
        self.subject = kwargs.get('subject', None)

        if self.environment:
            self.priority = 1
        if self.resource and not self.event:
            self.priority = 2
        elif self.service:
            self.priority = 3
        elif self.event and not self.resource:
            self.priority = 4
        elif self.resource and self.event:
            self.priority = 5
        elif self.tags:
            self.priority = 6

    @property
    def channel(self):
        return NotificationChannel.find_by_id(self.channel_id)

    @property
    def users(self):
        groups = [NotificationGroup.find_by_id(group_id) for group_id in self.group_ids]
        group_users = [db.get_notification_group_users(group.id) for group in groups if group is not None]
        users = {User.find_by_email(user_id).notification_info for user_id in self.users_emails if user_id is not None}
        for group in groups:
            if group is None:
                continue
            for index in range(max(len(group.phone_numbers), len(group.mails))):
                users.add(NotificationInfo(phone_number=group.phone_numbers[index] if index < len(group.phone_numbers) else None, email=group.mails[index] if index < len(group.mails) else None))
        for user_list in group_users:
            for email in user_list:
                if isinstance(email, dict):
                    users.add(User.find_by_email(email.get('email')).notification_info)
                else:
                    users.add(User.find_by_email(email.email).notification_info)
        return users

    @classmethod
    def parse(cls, json: JSON) -> 'NotificationRule':
        if not isinstance(json.get('service', []), list):
            raise ValueError('service must be a list')
        if not isinstance(json.get('tags', []), list):
            raise ValueError('tags must be a list')
        if not isinstance(json.get('excludedTags', []), list):
            raise ValueError('excluded tags must be a list')
        notification_rule = NotificationRule(
            id=json.get('id', None),
            name=json.get('name', None),
            active=json.get('active', True),
            environment=json['environment'],
            delay_time=json.get('delayTime', None),
            channel_id=json['channelId'],
            receivers=json['receivers'],
            users_emails=json.get('usersEmails'),
            group_ids=json.get('groupIds'),
            use_oncall=json.get('useOnCall', False),
            triggers=[NotificationTriggers(trigger.get('from_severity', []), trigger.get('to_severity', []), trigger.get('status'), trigger.get('text', '')) for trigger in json.get('triggers', [])],
            service=json.get('service', list()),
            resource=json.get('resource', None),
            event=json.get('event', None),
            tags=[AdvancedTags(tag.get('all', []), tag.get('any', [])) for tag in json.get('tags', [])],
            excluded_tags=[AdvancedTags(tag.get('all', []), tag.get('any', [])) for tag in json.get('excludedTags', [])],
            customer=json.get('customer', None),
            reactivate=json.get('reactivate', None),
            start_time=(
                datetime.strptime(json['startTime'], '%H:%M').time()
                if json['startTime'] is not None and json['startTime'] != ''
                else None
            )
            if 'startTime' in json
            else None,
            end_time=(
                datetime.strptime(json['endTime'], '%H:%M').time()
                if json['endTime'] is not None and json['endTime'] != ''
                else None
            )
            if 'endTime' in json
            else None,
            user=json.get('user', None),
            text=json.get('text', None),
            subject=json.get('subject', None),
            days=json.get('days', None),
        )
        return notification_rule

    @property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'active': self.active,
            'href': absolute_url('/notificationrule/' + self.id),
            'priority': self.priority,
            'environment': self.environment,
            'delayTime': self.delay_time,
            'channelId': self.channel_id,
            'receivers': self.receivers,
            'usersEmails': self.users_emails,
            'groupIds': self.group_ids,
            'useOnCall': self.use_oncall,
            'service': self.service,
            'triggers': [trigger.serialize for trigger in self.triggers],
            'resource': self.resource,
            'event': self.event,
            'tags': [tag.serialize for tag in self.tags],
            'excludedTags': [tag.serialize for tag in self.excluded_tags],
            'customer': self.customer,
            'user': self.user,
            'createTime': self.create_time,
            'reactivate': self.reactivate,
            'text': self.text,
            'subject': self.subject,
            'startTime': self.start_time.strftime('%H:%M')
            if self.start_time is not None
            else None,
            'endTime': self.end_time.strftime('%H:%M')
            if self.end_time is not None
            else None,
            'days': self.days,
        }

    def __repr__(self) -> str:
        more = ''
        if self.service:
            more += 'service=%r, ' % self.service
        if self.resource:
            more += 'resource=%r, ' % self.resource
        if self.event:
            more += 'event=%r, ' % self.event
        if self.customer:
            more += 'customer=%r, ' % self.customer
        if self.triggers:
            more += 'triggers=%r, ' % self.triggers

        return 'NotificationRule(id={!r}, priority={!r}, environment={!r}, receivers={!r}, {})'.format(
            self.id,
            self.priority,
            self.environment,
            self.receivers,
            more,
        )

    @classmethod
    def from_db(cls, r: Tuple | None) -> Optional['NotificationRule']:
        if r is not None:
            return NotificationRule(
                id=r.id,
                name=r.name,
                active=r.active,
                priority=r.priority,
                environment=r.environment,
                delay_time=r.delay_time,
                channel_id=r.channel_id,
                receivers=r.receivers,
                users_emails=r.users_emails,
                group_ids=r.group_ids,
                use_oncall=r.use_oncall,
                service=r.service,
                triggers=[NotificationTriggers.from_db(trigger) for trigger in r.triggers or []],
                resource=r.resource,
                event=r.event,
                tags=[AdvancedTags.from_db(tag) for tag in r.tags or []],
                excluded_tags=[AdvancedTags.from_db(tag) for tag in r.excluded_tags or []],
                customer=r.customer,
                user=r.user,
                create_time=r.create_time,
                reactivate=r.reactivate,
                text=r.text,
                subject=r.subject,
                start_time=r.start_time,
                end_time=r.end_time,
                days=r.days,
            )

    # create a notification rule
    def create(self) -> 'NotificationRule':
        # self.advanced_severity = [AdvancedSeverity(_from=advanced_severity["from"], _to=advanced_severity["to"]) for advanced_severity in self.advanced_severity]
        return NotificationRule.from_db(db.create_notification_rule(self))

    # get a notification rule
    @staticmethod
    def find_by_id(
        id: str, customers: List[str] = None
    ) -> Optional['NotificationRule']:
        return NotificationRule.from_db(db.get_notification_rule(id, customers))

    @staticmethod
    def find_all(
        query: Query = None, page: int = 1, page_size: int = 1000
    ) -> List['NotificationRule']:
        return [
            NotificationRule.from_db(notification_rule)
            for notification_rule in db.get_notification_rules(query, page, page_size)
        ]

    @staticmethod
    def find_all_by_notification_group(group_id: str) -> List['NotificationRule']:
        return [
            {'id': notification_rule.id, 'name': notification_rule.name}for notification_rule in db.get_notification_rules_by_notification_group(group_id)
        ]

    @staticmethod
    def count(query: Query = None) -> int:
        return db.get_notification_rules_count(query)

    @staticmethod
    def find_all_active(alert: 'Alert') -> 'list[NotificationRule]':
        if alert.duplicate_count:
            return []
        return [NotificationRule.from_db(db_notification_rule) for db_notification_rule in db.get_notification_rules_active(alert)]

    @staticmethod
    def find_all_active_status(alert: 'Alert', status: str) -> 'list[NotificationRule]':
        return [NotificationRule.from_db(db_notification_rule) for db_notification_rule in db.get_notification_rules_active_status(alert, status)]

    @staticmethod
    def find_all_reactivate(**kwargs) -> 'list[NotificationRule]':
        now = datetime.now(UTC)
        return [NotificationRule.from_db(db_notification_rule) for db_notification_rule in db.get_notification_rules_reactivate(now)]

    def get_notification_rule_alerts(self, page=None, page_size=None):
        dbRes = db.get_notification_rule_alerts(self, page, page_size)
        return ([Alert.from_db(alert) for alert in dbRes])

    def get_notification_rule_alerts_count(self):
        return db.get_notification_rule_alerts_count(self).count

    def create_notification_rule_history(self, update_type='update'):
        db.create_notification_rule_history(update_type, self)

    def get_notification_rule_history(self, page, page_size):
        return [NotificationRuleHistory.from_db(h) for h in db.get_notification_rule_history(self.id, page, page_size)]

    def history_count(self) -> int:
        return db.get_notification_rule_history_count(self.id)

    def update(self, **kwargs) -> 'NotificationRule':
        triggers = kwargs.get('triggers')
        if triggers is not None:
            kwargs['triggers'] = [NotificationTriggers.parse(trigger) for trigger in triggers]
        tags = kwargs.get('tags')
        if tags is not None:
            kwargs['tags'] = [AdvancedTags.parse(tag) for tag in tags]
        excluded = kwargs.get('excludedTags')
        if excluded is not None:
            kwargs['excludedTags'] = [AdvancedTags.parse(tag) for tag in excluded]
        rule = NotificationRule.from_db(db.update_notification_rule(self.id, **kwargs))
        return rule

    def delete(self) -> bool:
        return db.delete_notification_rule(self.id)
