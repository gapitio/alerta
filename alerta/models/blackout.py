from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from flask import current_app

from alerta.app import db
from alerta.database.base import Query
from alerta.models.enums import BlackoutStatus
from alerta.utils.format import DateTime
from alerta.utils.response import absolute_url

JSON = Dict[str, Any]


class Blackout:

    def __init__(self, environment: str, **kwargs) -> None:
        if not environment:
            raise ValueError('Missing mandatory value for "environment"')
        if kwargs.get('resource') == '':
            raise ValueError('resource can not be an empty string')
        if kwargs.get('event') == '':
            raise ValueError('event can not be an empty string')
        if kwargs.get('origin') == '':
            raise ValueError('origin can not be an empty string')

        start_time = kwargs.get('start_time', None) or datetime.now(UTC)
        if kwargs.get('end_time', None):
            end_time = kwargs['end_time']
            duration = int((end_time - start_time).total_seconds())
        else:
            duration = kwargs.get('duration', None) or current_app.config['BLACKOUT_DURATION']
            end_time = start_time + timedelta(seconds=duration)

        self.id = kwargs.get('id') or str(uuid4())
        self.environment = environment
        self.service = kwargs.get('service', None) or list()
        self.resource = kwargs.get('resource', None)
        self.event = kwargs.get('event', None)
        self.tags = kwargs.get('tags', None) or list()
        self.origin = kwargs.get('origin', None)
        self.customer = kwargs.get('customer', None)
        self.start_time = start_time
        self.end_time = end_time
        self.duration = duration
        self.remaining = kwargs.get('remaining', duration)

        self.user = kwargs.get('user', None)
        self.create_time = kwargs['create_time'] if 'create_time' in kwargs else datetime.now(UTC)
        self.text = kwargs.get('text', None)

        if self.environment:
            self.priority = 0
        if self.origin:
            self.priority = 1
        if self.resource and not self.event:
            self.priority = 2
        elif self.service:
            self.priority = 3
        elif self.event and not self.resource:
            self.priority = 4
        elif self.resource and self.event:
            self.priority = 6
        elif self.tags:
            self.priority = 7
        if self.origin:
            self.priority = 8

    @property
    def status(self):
        now = datetime.now(UTC)
        if self.start_time <= now < self.end_time:
            return BlackoutStatus.Active
        if self.start_time > now:
            return BlackoutStatus.Pending
        if self.end_time <= now:
            return BlackoutStatus.Expired

    @classmethod
    def parse(cls, json: JSON) -> 'Blackout':
        if not isinstance(json.get('service', []), list):
            raise ValueError('service must be a list')
        if not isinstance(json.get('tags', []), list):
            raise ValueError('tags must be a list')

        return Blackout(
            id=json.get('id', None),
            environment=json['environment'],
            service=json.get('service', list()),
            resource=json.get('resource', None),
            event=json.get('event', None),
            tags=json.get('tags', list()),
            origin=json.get('origin', None),
            customer=json.get('customer', None),
            start_time=DateTime.parse(json['startTime']) if 'startTime' in json else None,
            end_time=DateTime.parse(json['endTime']) if 'endTime' in json else None,
            duration=json.get('duration', None),
            user=json.get('user', None),
            text=json.get('text', None)
        )

    @property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'href': absolute_url('/blackout/' + self.id),
            'priority': self.priority,
            'environment': self.environment,
            'service': self.service,
            'resource': self.resource,
            'event': self.event,
            'tags': self.tags,
            'origin': self.origin,
            'customer': self.customer,
            'startTime': self.start_time,
            'endTime': self.end_time,
            'duration': self.duration,
            'status': self.status,
            'remaining': self.remaining,
            'user': self.user,
            'createTime': self.create_time,
            'text': self.text
        }

    def __repr__(self) -> str:
        more = ''
        if self.service:
            more += f'service={self.service!r}, '
        if self.resource:
            more += f'resource={self.resource!r}, '
        if self.event:
            more += f'event={self.event!r}, '
        if self.tags:
            more += f'tags={self.tags!r}, '
        if self.origin:
            more += f'origin={self.origin!r}, '
        if self.customer:
            more += f'customer={self.customer!r}, '

        return 'Blackout(id={!r}, priority={!r}, status={!r}, environment={!r}, {}start_time={!r}, end_time={!r}, remaining={!r})'.format(
            self.id,
            self.priority,
            self.status,
            self.environment,
            more,
            self.start_time,
            self.end_time,
            self.remaining
        )

    @classmethod
    def from_db(cls, r: Tuple | None) -> Optional['Blackout']:
        if r is not None:
            return Blackout(
                id=r.id,
                priority=r.priority,
                environment=r.environment,
                service=r.service,
                resource=r.resource if r.resource != '' else None,
                event=r.event if r.event != '' else None,
                tags=r.tags,
                origin=r.origin if r.origin != '' else None,
                customer=r.customer,
                start_time=r.start_time.replace(tzinfo=UTC) if r.start_time is not None else None,
                end_time=r.end_time.replace(tzinfo=UTC) if r.end_time is not None else None,
                duration=r.duration,
                remaining=r.remaining,
                user=r.user,
                create_time=r.create_time,
                text=r.text
            )

    # create a blackout
    def create(self) -> 'Blackout':
        return Blackout.from_db(db.create_blackout(self))

    # get a blackout
    @staticmethod
    def find_by_id(id: str, customers: List[str] = None) -> Optional['Blackout']:
        return Blackout.from_db(db.get_blackout(id, customers))

    @staticmethod
    def find_all(query: Query = None, page: int = 1, page_size: int = 1000) -> List['Blackout']:
        return [Blackout.from_db(blackout) for blackout in db.get_blackouts(query, page, page_size)]

    @staticmethod
    def count(query: Query = None) -> int:
        return db.get_blackouts_count(query)

    def update(self, **kwargs) -> 'Blackout':
        if kwargs.get('startTime'):
            kwargs['startTime'] = DateTime.parse(kwargs['startTime'])
        if kwargs.get('endTime'):
            kwargs['endTime'] = DateTime.parse(kwargs['endTime'])
        return Blackout.from_db(db.update_blackout(self.id, **kwargs))

    def delete(self) -> bool:
        return db.delete_blackout(self.id)
