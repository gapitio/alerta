from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from alerta.app import db
from alerta.database.base import Query

JSON = Dict[str, Any]


class NotificationHistory:

    def __init__(self, sent: bool, message: str, channel: str, rule: str, alert: str, receiver: str, sender: str, **kwargs) -> None:
        self.id = kwargs.get('id') or str(uuid4())
        self.sent = sent
        self.sent_time = kwargs.get('sent_time', datetime.now(UTC))
        self.message = message
        self.channel = channel
        self.rule = rule
        self.alert = alert
        self.receiver = receiver
        self.sender = sender
        self.error = kwargs.get('error', None)
        self.confirmed = kwargs.get('confirmed', None)
        self.confirmed_time = kwargs.get('confirmed_time', None)
        # confirmed boolean,
        # confirmed_time timestamp without time zone

    @classmethod
    def parse(cls, json: JSON) -> 'NotificationHistory':
        return NotificationHistory(
            id=json.get('id', None),
            sent=json['sent'],
            message=json['message'],
            channel=json['channel'],
            rule=json['rule'],
            alert=json['alert'],
            receiver=json['receiver'],
            sender=json['sender'],
            error=json.get('error')
        )

    @property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'sent': self.sent,
            'message': self.message,
            'channel': self.channel,
            'rule': self.rule,
            'alert': self.alert,
            'receiver': self.receiver,
            'sender': self.sender,
            'sent_time': self.sent_time,
            'confirmed': self.confirmed,
            'confirmed_time': self.confirmed_time,
            'error': self.error
        }

    def __repr__(self) -> str:
        return f'NotificationHistory(id={self.id}'

    @classmethod
    def from_db(cls, r: Tuple | None) -> Optional['NotificationHistory']:
        if r is not None:
            return NotificationHistory(
                id=r.id,
                sent=r.sent,
                message=r.message,
                channel=r.channel,
                rule=r.rule,
                alert=r.alert,
                receiver=r.receiver,
                sender=r.sender,
                sent_time=r.sent_time,
                confirmed=r.confirmed,
                confirmed_time=r.confirmed_time,
                error=r.error
            )

    # create a notification rule
    def create(self) -> 'NotificationHistory':
        return NotificationHistory.from_db(db.create_notification_history(self))

    # get a notification rule
    @staticmethod
    def find_by_id(id: str, customers: 'list[str]|None' = None) -> Optional['NotificationHistory']:
        return NotificationHistory.from_db(db.get_notification_history(id, customers))

    @staticmethod
    def find_all(query: 'Query|None' = None, page: int = 1, page_size: int = 1000) -> List['NotificationHistory']:
        return [
            NotificationHistory.from_db(notification_history)
            for notification_history in db.get_notifications_history(query, page, page_size)
        ]

    @staticmethod
    def count(query: 'Query|None' = None) -> int:
        return db.get_notifications_history_count(query)

    def confirm(self) -> 'NotificationHistory':
        return NotificationHistory.from_db(db.confirm_notification_history(self.id))
