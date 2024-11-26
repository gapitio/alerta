from datetime import datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

from alerta.app import db
from alerta.database.base import Query
from alerta.models.notification_channel import NotificationChannel
from alerta.models.notification_group import NotificationGroup
from alerta.models.user import NotificationInfo, User
from alerta.utils.response import absolute_url

if TYPE_CHECKING:
    from alerta.models.alert import Alert

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
            self.from_severity, self.to_severity)

    @classmethod
    def from_document(cls, doc):
        return AdvancedTags(
            all=doc.get('all', list()),
            any=doc.get('any', list()),
        )

    @classmethod
    def from_record(cls, rec):
        return AdvancedTags(
            all=rec.all,
            any=rec.any,
        )

    @classmethod
    def from_db(cls, r):
        if isinstance(r, dict):
            return cls.from_document(r)
        elif isinstance(r, tuple):
            return cls.from_record(r)


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
    def from_document(cls, doc):
        return NotificationTriggers(
            from_severity=doc.get('from_severity', list()),
            to_severity=doc.get('to_severity', list()),
            status=doc.get('status', list()),
            text=doc.get('text')
        )

    @classmethod
    def from_record(cls, rec):
        return NotificationTriggers(
            from_severity=rec.from_severity,
            to_severity=rec.to_severity,
            status=rec.status,
            text=rec.text
        )

    @classmethod
    def from_db(cls, r):
        if isinstance(r, dict):
            return cls.from_document(r)
        elif isinstance(r, tuple):
            return cls.from_record(r)


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
        self.user_ids = kwargs.get('user_ids') or []
        self.group_ids = kwargs.get('group_ids') or []
        self.use_oncall = use_oncall
        self.start_time: time = kwargs.get('start_time') or None
        self.end_time: time = kwargs.get('end_time') or None
        self.service = kwargs.get('service', None) or list()
        self.resource = kwargs['resource'] if kwargs.get('resource', '') != '' else None
        self.event = kwargs['event'] if kwargs.get('event', '') != '' else None
        self.group = kwargs['group'] if kwargs.get('group', '') != '' else None
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
            kwargs['create_time'] if 'create_time' in kwargs else datetime.utcnow()
        )
        self.text = kwargs.get('text', None)

        if self.environment:
            self.priority = 1
        if self.resource and not self.event:
            self.priority = 2
        elif self.service:
            self.priority = 3
        elif self.event and not self.resource:
            self.priority = 4
        elif self.group:
            self.priority = 5
        elif self.resource and self.event:
            self.priority = 6
        elif self.tags:
            self.priority = 7

    @property
    def channel(self):
        return NotificationChannel.find_by_id(self.channel_id)

    @property
    def users(self):
        groups = [NotificationGroup.find_by_id(group_id) for group_id in self.group_ids]
        group_users = [db.get_notification_group_users(group.id) for group in groups]
        users = {User.find_by_id(user_id).notification_info for user_id in self.user_ids}
        for group in groups:
            for index in range(max(len(group.phone_numbers), len(group.mails))):
                if index < len(group.phone_numbers) and index < len(group.mails):
                    users.add(NotificationInfo(phone_number=group.phone_numbers[index] if index < len(group.phone_numbers) else None, email=group.mails[index] if index < len(group.mails) else None))
        for user_list in group_users:
            for user in user_list:
                if isinstance(user, dict):
                    users.add(User.find_by_id(user.get('id')).notification_info)
                else:
                    users.add(User.find_by_id(user.id).notification_info)
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
            user_ids=json.get('userIds'),
            group_ids=json.get('groupIds'),
            use_oncall=json.get('useOnCall', False),
            triggers=[NotificationTriggers(trigger.get('from_severity', []), trigger.get('to_severity', []), trigger.get('status'), trigger.get('text', '')) for trigger in json.get('triggers', [])],
            service=json.get('service', list()),
            resource=json.get('resource', None),
            event=json.get('event', None),
            group=json.get('group', None),
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
            'userIds': self.user_ids,
            'groupIds': self.group_ids,
            'useOnCall': self.use_oncall,
            'service': self.service,
            'triggers': [trigger.serialize for trigger in self.triggers],
            'resource': self.resource,
            'event': self.event,
            'group': self.group,
            'tags': [tag.serialize for tag in self.tags],
            'excludedTags': [tag.serialize for tag in self.excluded_tags],
            'customer': self.customer,
            'user': self.user,
            'createTime': self.create_time,
            'reactivate': self.reactivate,
            'text': self.text,
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
        if self.group:
            more += 'group=%r, ' % self.group
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
    def from_document(cls, doc: Dict[str, Any]) -> 'NotificationRule':
        return NotificationRule(
            id=doc.get('id', None) or doc.get('_id'),
            name=doc.get('name', None),
            active=doc.get('active', True),
            priority=doc.get('priority', None),
            environment=doc['environment'],
            delay_time=doc.get('delayTime', None),
            channel_id=doc['channelId'],
            receivers=doc.get('receivers') or list(),
            user_ids=doc.get('userIds'),
            group_ids=doc.get('groupIds'),
            use_oncall=doc.get('useOnCall', False),
            service=doc.get('service', list()),
            triggers=[NotificationTriggers.from_db(trigger) for trigger in doc.get('triggers', [])],
            resource=doc.get('resource', None),
            event=doc.get('event', None),
            group=doc.get('group', None),
            tags=[AdvancedTags.from_db(tag) for tag in doc.get('tags', [])],
            excluded_tags=[AdvancedTags.from_db(tag) for tag in doc.get('excludedTags', [])],
            customer=doc.get('customer', None),
            user=doc.get('user', None),
            create_time=doc.get('createTime', None),
            reactivate=doc.get('reactivate', None),
            text=doc.get('text', None),
            start_time=(
                datetime.strptime(
                    f'{doc["startTime"] :.2f}'.replace('.', ':'), '%H:%M'
                ).time()
                if doc['startTime'] is not None
                else None
            )
            if 'startTime' in doc
            else None,
            end_time=(
                datetime.strptime(
                    f'{doc["endTime"] :.2f}'.replace('.', ':'), '%H:%M'
                ).time()
                if doc['endTime'] is not None
                else None
            )
            if 'endTime' in doc
            else None,
            days=doc.get('days', None),
        )

    @classmethod
    def from_record(cls, rec) -> 'NotificationRule':
        return NotificationRule(
            id=rec.id,
            name=rec.name,
            active=rec.active,
            priority=rec.priority,
            environment=rec.environment,
            delay_time=rec.delay_time,
            channel_id=rec.channel_id,
            receivers=rec.receivers,
            user_ids=rec.user_ids,
            group_ids=rec.group_ids,
            use_oncall=rec.use_oncall,
            service=rec.service,
            triggers=[NotificationTriggers.from_db(trigger) for trigger in rec.triggers or []],
            resource=rec.resource,
            event=rec.event,
            group=rec.group,
            tags=[AdvancedTags.from_db(tag) for tag in rec.tags or []],
            excluded_tags=[AdvancedTags.from_db(tag) for tag in rec.excluded_tags or []],
            customer=rec.customer,
            user=rec.user,
            create_time=rec.create_time,
            reactivate=rec.reactivate,
            text=rec.text,
            start_time=rec.start_time,
            end_time=rec.end_time,
            days=rec.days,
        )

    @classmethod
    def from_db(cls, r: Union[Dict, Tuple]) -> 'NotificationRule':
        if isinstance(r, dict):
            return cls.from_document(r)
        elif isinstance(r, tuple):
            return cls.from_record(r)

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
    def count(query: Query = None) -> int:
        return db.get_notification_rules_count(query)

    @ staticmethod
    def find_all_active(alert: 'Alert') -> 'list[NotificationRule]':
        if alert.duplicate_count:
            return []
        return [NotificationRule.from_db(db_notification_rule) for db_notification_rule in db.get_notification_rules_active(alert)]

    @ staticmethod
    def find_all_active_status(alert: 'Alert', status: str) -> 'list[NotificationRule]':
        return [NotificationRule.from_db(db_notification_rule) for db_notification_rule in db.get_notification_rules_active_status(alert, status)]

    @ staticmethod
    def find_all_reactivate(**kwargs) -> 'list[NotificationRule]':
        now = datetime.utcnow()
        return [NotificationRule.from_db(db_notification_rule) for db_notification_rule in db.get_notification_rules_reactivate(now)]

    def update(self, **kwargs) -> 'NotificationRule':
        triggers = kwargs.get('triggers')
        if triggers is not None:
            kwargs['triggers'] = [NotificationTriggers.from_document(trigger) for trigger in triggers]
        tags = kwargs.get('tags')
        if tags is not None:
            kwargs['tags'] = [AdvancedTags.from_document(tag) for tag in tags]
        excluded = kwargs.get('excludedTags')
        if excluded is not None:
            kwargs['excludedTags'] = [AdvancedTags.from_document(tag) for tag in excluded]
        return NotificationRule.from_db(db.update_notification_rule(self.id, **kwargs))

    def delete(self) -> bool:
        return db.delete_notification_rule(self.id)
