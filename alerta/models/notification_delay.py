from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

from alerta.app import db
from alerta.database.base import Query

JSON = Dict[str, Any]


class NotificationDelay:

    def __init__(self, alert_id: str, notification_rule_id: str, delay_time: datetime, **kwargs) -> None:
        self.id = kwargs.get('id') or str(uuid4())
        self.alert_id = alert_id
        self.notification_rule_id = notification_rule_id
        self.delay_time = delay_time

    @classmethod
    def parse(cls, json: dict[str, str]) -> 'NotificationDelay':
        return NotificationDelay(
            id=json.get('id', None),
            alert_id=json['alert_id'],
            notification_rule_id=json['notification_rule_id'],
            delay_time=json['delay_time']
        )

    @ property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'alert_id': self.alert_id,
            'notification_rule_id': self.notification_rule_id,
            'delay_time': self.delay_time
        }

    def __repr__(self) -> str:
        return f'NotificationDelay(id={self.id}'

    @ classmethod
    def from_document(cls, doc: Dict[str, Any]) -> 'NotificationDelay':
        return NotificationDelay(
            id=doc.get('id', None) or doc.get('_id'),
            alert_id=doc['alert_id'],
            notification_rule_id=doc['notification_rule_id'],
            delay_time=doc['delay_time']
        )

    @ classmethod
    def from_record(cls, rec) -> 'NotificationDelay':
        return NotificationDelay(
            id=rec.id,
            alert_id=rec.alert_id,
            notification_rule_id=rec.notification_rule_id,
            delay_time=rec.delay_time
        )

    @ classmethod
    def from_db(cls, r: Union[Dict, Tuple]) -> 'NotificationDelay':
        if isinstance(r, dict):
            return cls.from_document(r)
        elif isinstance(r, tuple):
            return cls.from_record(r)

    # create a notification rule
    def create(self) -> 'NotificationDelay':
        return NotificationDelay.from_db(db.create_delayed_notification(self))

    @ staticmethod
    def find_by_id(id) -> Optional['NotificationDelay']:
        return NotificationDelay.from_db(db.get_delayed_notification(id))

    @ staticmethod
    def find_firing() -> List['NotificationDelay']:
        return [NotificationDelay.from_db(notification_delay) for notification_delay in db.get_delayed_notifications_firing(datetime.now(UTC))]

    @ staticmethod
    def delete_alert(alert_id) -> List['NotificationDelay']:
        return db.delete_delayed_notifications_alert(alert_id)

    def delete(self) -> Optional['NotificationDelay']:
        return db.delete_delayed_notification(self.id)

    @ staticmethod
    def find_all(query: 'Query|None' = None, page: int = 1, page_size: int = 1000) -> List['NotificationDelay']:
        return [
            NotificationDelay.from_db(notification_delay)
            for notification_delay in db.get_delayed_notifications(query, page, page_size)
        ]

    @ staticmethod
    def count(query: 'Query|None' = None) -> int:
        return db.get_delayed_notifications_count(query)
