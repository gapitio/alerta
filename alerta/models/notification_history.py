from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime
from uuid import uuid4

from alerta.app import db
from alerta.database.base import Query


JSON = Dict[str, Any]


class NotificationHistory:

    def __init__(self, sent: bool, message: str, channel: str, rule: str, alert: str, receiver: str, sender: str, **kwargs) -> None:
        self.id = kwargs.get('id') or str(uuid4())
        self.sent = sent
        self.sent_time = kwargs.get('sent_time', datetime.utcnow())
        self.message = message
        self.channel = channel
        self.rule = rule
        self.alert = alert
        self.receiver = receiver
        self.sender = sender
        self.error = kwargs.get("error", None)
        self.confirmed = kwargs.get("confirmed", None)
        self.confirmed_time = kwargs.get("confirmed_time", None)
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

    @ property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            "sent": self.sent,
            "message": self.message,
            "channel": self.channel,
            "rule": self.rule,
            "alert": self.alert,
            "receiver": self.receiver,
            "sender": self.sender,
            "sent_time": self.sent_time,
            "confirmed": self.confirmed,
            "confirmed_time": self.confirmed_time,
            "error": self.error
        }

    def __repr__(self) -> str:
        return f'NotificationHistory(id={self.id}'

    @ classmethod
    def from_document(cls, doc: Dict[str, Any]) -> 'NotificationHistory':
        return NotificationHistory(
            id=doc.get('id', None) or doc.get('_id'),
            sent=doc["sent"],
            message=doc["message"],
            channel=doc["channel"],
            rule=doc["rule"],
            alert=doc["alert"],
            receiver=doc["receiver"],
            sender=doc["sender"],
            sent_time=doc["sent_time"],
            confirmed=doc["confirmed"],
            confirmed_time=doc["confirmed_time"],
            error=doc.get("error", None)
        )

    @ classmethod
    def from_record(cls, rec) -> 'NotificationHistory':
        return NotificationHistory(
            id=rec.id,
            sent=rec.sent,
            message=rec.message,
            channel=rec.channel,
            rule=rec.rule,
            alert=rec.alert,
            receiver=rec.receiver,
            sender=rec.sender,
            sent_time=rec.sent_time,
            confirmed=rec.confirmed,
            confirmed_time=rec.confirmed_time,
            error=rec.error
        )

    @ classmethod
    def from_db(cls, r: Union[Dict, Tuple]) -> 'NotificationHistory':
        if isinstance(r, dict):
            return cls.from_document(r)
        elif isinstance(r, tuple):
            return cls.from_record(r)

    # create a notification rule
    def create(self) -> 'NotificationHistory':
        return NotificationHistory.from_db(db.create_notification_history(self))

    # get a notification rule
    @ staticmethod
    def find_by_id(id: str, customers: 'list[str]|None' = None) -> Optional['NotificationHistory']:
        return NotificationHistory.from_db(db.get_notification_history(id, customers))

    @ staticmethod
    def find_all(query: 'Query|None' = None, page: int = 1, page_size: int = 1000) -> List['NotificationHistory']:
        return [
            NotificationHistory.from_db(notification_history)
            for notification_history in db.get_notifications_history(query, page, page_size)
        ]

    @ staticmethod
    def count(query: 'Query|None' = None) -> int:
        return db.get_notifications_history_count(query)

    def confirm(self) -> 'NotificationHistory':
        return NotificationHistory.from_db(db.confirm_notification_history(self.id))
