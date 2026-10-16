import logging
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from cryptography.fernet import Fernet
from flask import current_app

from alerta.app import db
from alerta.database.base import Query
from alerta.utils.response import absolute_url

LOG = logging.getLogger('alerta.models.notification_channel')

JSON = Dict[str, Any]


class NotificationChannel:

    def __init__(self, _type: str, api_token: str, sender: str, **kwargs) -> None:
        self.id = kwargs.get('id') or str(uuid4())
        self.type = _type
        self.api_token = api_token  # encrypted
        self.sender = sender
        self.host = kwargs.get('host', None)
        self.platform_id = kwargs.get('platform_id', None)
        self.platform_partner_id = kwargs.get('platform_partner_id', None)
        self.api_sid: 'str|None' = kwargs.get('api_sid', None)  # encrypted
        self.customer = kwargs.get('customer', None)
        self.verify = kwargs.get('verify', None)
        self.bearer = kwargs.get('bearer', None)
        self.bearer_timeout = kwargs.get('bearer_timeout', None)

    @classmethod
    def parse(cls, json: JSON) -> 'NotificationChannel':
        fernet = Fernet(current_app.config['NOTIFICATION_KEY'])
        return NotificationChannel(
            id=json.get('id', None),
            _type=json['type'],
            api_token=fernet.encrypt(str(json['apiToken']).encode()).decode(),
            api_sid=fernet.encrypt(str(json['apiSid']).encode()).decode() if 'apiSid' in json else None,
            sender=json['sender'],
            host=json.get('host', None),
            platform_id=json.get('platformId', None),
            platform_partner_id=json.get('platfromPartnerId', None),
            customer=json.get('customer', None),
            verify=json.get('verify', None),
        )

    @property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'href': absolute_url('/notificationchannel/' + self.id),
            'type': self.type,
            'sender': self.sender,
            'customer': self.customer,
            'host': self.host,
            'platformId': self.platform_id,
            'platformPartnerId': self.platform_partner_id,
            'verify': self.verify
        }

    @classmethod
    def _import(cls, json: JSON) -> 'NotificationChannel':
        return NotificationChannel(
            id=json.get('id', None),
            _type=json['type'],
            api_token=json['apiToken'],
            api_sid=json['apiSid'],
            sender=json['sender'],
            host=json.get('host', None),
            platform_id=json.get('platfromId', None),
            platform_partner_id=json.get('platfromPartnerId', None),
            customer=json.get('customer', None),
            verify=json.get('verify', None),
        )

    @property
    def export(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'href': absolute_url('/notificationchannel/' + self.id),
            'type': self.type,
            'sender': self.sender,
            'customer': self.customer,
            'host': self.host,
            'platformId': self.platform_id,
            'platformPartnerId': self.platform_partner_id,
            'verify': self.verify,
            'apiSid': self.api_sid,
            'apiToken': self.api_token
        }

    def __repr__(self) -> str:
        more = ''
        if self.customer:
            more += f'customer={self.customer}, '
        return f'NotificationChannel(id={self.id}, type={self.type}, sender={self.sender}, {more}'

    @classmethod
    def from_db(cls, r: Tuple | None) -> Optional['NotificationChannel']:
        if r is not None:
            return NotificationChannel(
                id=r.id,
                _type=r.type,
                api_token=r.api_token,
                api_sid=r.api_sid,
                sender=r.sender,
                host=r.host,
                platform_id=r.platform_id,
                platform_partner_id=r.platform_partner_id,
                customer=r.customer,
                verify=r.verify,
                bearer=r.bearer,
                bearer_timeout=r.bearer_timeout,
            )

    # create a notification rule
    def create(self) -> 'NotificationChannel':
        return NotificationChannel.from_db(db.create_notification_channel(self))

    # get a notification rule
    @staticmethod
    def find_by_id(id: str, customers: 'list[str]|None' = None) -> Optional['NotificationChannel']:
        return NotificationChannel.from_db(db.get_notification_channel(id, customers))

    @staticmethod
    def find_all(query: 'Query|None' = None, page: int = 1, page_size: int = 1000) -> List['NotificationChannel']:
        return [
            NotificationChannel.from_db(notification_channel)
            for notification_channel in db.get_notification_channels(query, page, page_size)
        ]

    @staticmethod
    def count(query: 'Query|None' = None) -> int:
        return db.get_notification_channels_count(query)

    def update(self, **kwargs) -> 'NotificationChannel':
        return NotificationChannel.from_db(db.update_notification_channel(self.id, **kwargs))

    def delete(self) -> bool:
        return db.delete_notification_channel(self.id)

    def update_bearer(self, bearer, timeout) -> 'NotificationChannel':
        return NotificationChannel.from_db(db.update_notification_channel(self.id, bearer=bearer, bearer_timeout=timeout))
