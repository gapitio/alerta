from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

from alerta.app import db
from alerta.database.base import Query
from alerta.models.alert import Alert
from alerta.models.notification_rule import AdvancedTags, NotificationTriggers
from alerta.utils.response import absolute_url

JSON = Dict[str, Any]


def alert_from_record(rec) -> 'dict':
    return {
        'id': rec.id,
        'resource': rec.resource,
        'event': rec.event,
        'severity': rec.severity,
        'environment': rec.environment,
        'service': rec.service,
        'timeout': rec.timeout,
        'value': rec.value,
        'text': rec.text,
    }


class EscalationRule:
    def __init__(
        self, environment: str, ttime: str, **kwargs
    ) -> None:
        if not environment:
            raise ValueError('Missing mandatory value for "environment"')

        self.id = kwargs.get('id') or str(uuid4())
        self.active = kwargs.get('active', True)
        self.environment = environment
        self.time: timedelta = ttime
        self.start_time: time = kwargs.get('start_time') or None
        self.end_time: time = kwargs.get('end_time') or None
        self.service = kwargs.get('service', None) or list()
        self.resource = kwargs.get('resource', None)
        self.event = kwargs.get('event', None)
        self.group = kwargs.get('group', None)
        self.tags = kwargs.get('tags') or [AdvancedTags([], [])]
        self.excluded_tags = kwargs.get('excluded_tags', None) or [AdvancedTags([], [])]
        self.customer = kwargs.get('customer', None)
        self.days = kwargs.get('days', None) or list()
        self.triggers = kwargs.get('triggers') or [NotificationTriggers([], [], [])]

        self.user = kwargs.get('user', None)
        self.create_time = (
            kwargs['create_time'] if 'create_time' in kwargs else datetime.utcnow()
        )

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

    @classmethod
    def parse(cls, json: JSON) -> 'EscalationRule':
        if not isinstance(json.get('service', []), list):
            raise ValueError('service must be a list')
        if not isinstance(json.get('tags', []), list):
            raise ValueError('tags must be a list')
        if not isinstance(json.get('excludedTags', []), list):
            raise ValueError('excluded tags must be a list')
        escalation_rule = EscalationRule(
            id=json.get('id', None),
            active=json.get('active', True),
            environment=json['environment'],
            ttime=json['time'],
            triggers=[
                NotificationTriggers(trigger['from_severity'], trigger['to_severity'])
                for trigger in json.get('triggers', [])
            ],
            service=json.get('service', list()),
            resource=json.get('resource', None),
            event=json.get('event', None),
            group=json.get('group', None),
            tags=(
                [
                    AdvancedTags(tag.get('all', []), tag.get('any', []))
                    for tag in json.get('tags', [])
                ]
                if len(json.get('tags')) and type(json.get('tags')[-1]) is not str
                else [AdvancedTags(json.get('tags'), [])]
            ),
            excluded_tags=[
                AdvancedTags(tag.get('all', []), tag.get('any', []))
                for tag in json.get('excludedTags', [])
            ],
            customer=json.get('customer', None),
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
            days=json.get('days', None),
        )
        return escalation_rule

    @property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'active': self.active,
            'href': absolute_url('/EscalationRule/' + self.id),
            'priority': self.priority,
            'environment': self.environment,
            'time': self.time,
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

        return 'EscalationRule(id={!r}, priority={!r}, environment={!r},time={!r},{})'.format(
            self.id,
            self.priority,
            self.environment,
            self.time,
            more,
        )

    @classmethod
    def from_document(cls, doc: Dict[str, Any]) -> 'EscalationRule':
        return EscalationRule(
            id=doc.get('id', None) or doc.get('_id'),
            active=doc.get('active', True),
            priority=doc.get('priority', None),
            environment=doc['environment'],
            ttime=doc['time'],
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
    def from_record(cls, rec) -> 'EscalationRule':
        return EscalationRule(
            id=rec.id,
            active=rec.active,
            priority=rec.priority,
            environment=rec.environment,
            ttime=rec.time,
            service=rec.service,
            triggers=[NotificationTriggers.from_db(trigger) for trigger in rec.triggers or []],
            use_advanced_severity=rec.use_advanced_severity,
            resource=rec.resource,
            event=rec.event,
            group=rec.group,
            tags=[AdvancedTags.from_db(tag) for tag in rec.tags or []],
            excluded_tags=[AdvancedTags.from_db(tag) for tag in rec.excluded_tags or []],
            customer=rec.customer,
            user=rec.user,
            create_time=rec.create_time,
            start_time=rec.start_time,
            end_time=rec.end_time,
            days=rec.days,
        )

    @classmethod
    def from_db(cls, r: Union[Dict, Tuple]) -> 'EscalationRule':
        if isinstance(r, dict):
            return cls.from_document(r)
        elif isinstance(r, tuple):
            return cls.from_record(r)

    # create a notification rule
    def create(self) -> 'EscalationRule':
        # self.advanced_severity = [AdvancedSeverity(_from=advanced_severity["from"], _to=advanced_severity["to"]) for advanced_severity in self.advanced_severity]
        return EscalationRule.from_db(db.create_escalation_rule(self))

    # get a notification rule
    @staticmethod
    def find_by_id(
        id: str, customers: List[str] = None
    ) -> Optional['EscalationRule']:
        return EscalationRule.from_db(db.get_escalation_rule(id, customers))

    @staticmethod
    def find_all(
        query: Query = None, page: int = 1, page_size: int = 1000
    ) -> List['EscalationRule']:
        return [
            EscalationRule.from_db(escalation_rule)
            for escalation_rule in db.get_escalation_rules(query, page, page_size)
        ]

    @staticmethod
    def count(query: Query = None) -> int:
        return db.get_escalation_rules_count(query)

    @ staticmethod
    def find_all_active() -> 'list[Alert]':
        return [Alert.parse(alert if isinstance(alert, dict) else alert_from_record(alert)) for alert in db.get_escalation_alerts()]

    def update(self, **kwargs) -> 'EscalationRule':
        triggers = kwargs.get('triggers')
        if triggers is not None:
            kwargs['triggers'] = [NotificationTriggers.from_document(trigger) for trigger in triggers]
        tags = kwargs.get('tags')
        if tags is not None:
            kwargs['tags'] = [AdvancedTags.from_document(tag) for tag in tags]
        excluded = kwargs.get('excludedTags')
        if excluded is not None:
            kwargs['excludedTags'] = [AdvancedTags.from_document(tag) for tag in excluded]
        return EscalationRule.from_db(db.update_escalation_rule(self.id, **kwargs))

    def delete(self) -> bool:
        return db.delete_escalation_rule(self.id)
