from typing import Any, Dict, List, Optional, Tuple

from alerta.app import db
from alerta.models.user import User

JSON = Dict[str, Any]


class NotificationSend:
    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get('id')
        self.name = kwargs.get('user_name') or kwargs.get('group_name')
        self.type = 'User' if kwargs.get('user_name') else 'Group'
        self.mail = kwargs.get('mail')
        self.sms = kwargs.get('sms')

    @property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'mail': self.mail,
            'sms': self.sms,
            'type': self.type
        }

    def __repr__(self) -> str:
        return 'NotificationSend(id={!r}, name={!r}, email={!r}, sms={!r})'.format(
            self.id,
            self.name,
            self.mail,
            self.sms,
        )

    @classmethod
    def from_db(cls, r: Tuple | None) -> Optional['NotificationSend']:
        if r is not None:
            return NotificationSend(
                id=r.id,
                user_name=r.user_name,
                group_name=r.group_name,
                mail=r.mail,
                sms=r.sms,
            )

    @staticmethod
    def find_all() -> List['NotificationSend']:
        return [NotificationSend.from_db(send) for send in db.get_notification_sends()]

    @staticmethod
    def update(id, **kwargs) -> 'NotificationSend':
        return NotificationSend.from_db(db.update_notification_send(id, **kwargs))


class NotificationSendInfo(NotificationSend):
    def __init__(self, **kwargs):
        NotificationSend.__init__(self, **kwargs)
        self.user_id = kwargs.get('user_id')

    @property
    def email(self) -> str | None:
        user = User.find_by_id(self.user_id) if self.user_id else None
        return user.email if user else None

    @classmethod
    def from_db(cls, r: Tuple | None):
        return NotificationSendInfo(
            id=r.id,
            user_name=r.user_name,
            group_name=r.group_name,
            mail=r.mail,
            sms=r.sms,
            user_id=r.user_id
        )

    @staticmethod
    def find_by_id(id: str) -> 'NotificationSendInfo':
        return NotificationSendInfo.from_db(db.get_notification_send(id))
