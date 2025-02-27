from typing import Any, Dict, List, Tuple, Union

from alerta.app import db

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
    def from_document(cls, doc: Dict[str, Any]) -> 'NotificationSend':
        return NotificationSend(
            id=doc.get('id', None) or doc.get('_id'),
            user_name=doc.get('user_name'),
            group_name=doc.get('group_name'),
            mail=doc.get('mail'),
            sms=doc.get('sms'),
        )

    @classmethod
    def from_record(cls, rec) -> 'NotificationSend':
        return NotificationSend(
            id=rec.id,
            user_name=rec.user_name,
            group_name=rec.group_name,
            mail=rec.mail,
            sms=rec.sms,
        )

    @classmethod
    def from_db(cls, r: Union[Dict, Tuple]) -> 'NotificationSend':
        if isinstance(r, dict):
            return cls.from_document(r)
        elif isinstance(r, tuple):
            return cls.from_record(r)

    @staticmethod
    def find_all() -> List['NotificationSend']:
        return [NotificationSend.from_db(send) for send in db.get_notification_sends()]

    @staticmethod
    def update(id, **kwargs) -> 'NotificationSend':
        return NotificationSend.from_db(db.update_notification_send(id, **kwargs))
