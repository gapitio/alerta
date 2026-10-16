from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from alerta.app import db
from alerta.database.base import Query

JSON = Dict[str, Any]


class NotificationGroup:
    def __init__(self, **kwargs) -> None:

        self.id = kwargs.get('id') or str(uuid4())
        self.name = kwargs.get('name')
        self.users_emails = kwargs.get('users_emails') or []
        self.phone_numbers = kwargs.get('phone_numbers', [])
        self.mails = kwargs.get('mails', [])

    @classmethod
    def parse(cls, json: JSON) -> 'NotificationGroup':
        if not isinstance(json.get('users_emails', []), list):
            raise ValueError('users_emails must be a list')
        if 'name' not in json:
            raise ValueError('Missing required key: "name"')

        notification_group = NotificationGroup(
            id=json.get('id'),
            name=json.get('name'),
            users_emails=json.get('usersEmails'),
            phone_numbers=json.get('phoneNumbers'),
            mails=json.get('mails')
        )
        return notification_group

    @property
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'usersEmails': self.users_emails,
            'phoneNumbers': self.phone_numbers,
            'mails': self.mails,
        }

    def __repr__(self) -> str:
        return 'NotificationGroup(id={!r}, name={!r}, users_emails={!r}, phone_numbers={!r}, mails={!r})'.format(
            self.id,
            self.name,
            self.users_emails,
            self.phone_numbers,
            self.mails,
        )

    @classmethod
    def from_db(cls, r: Tuple | None) -> Optional['NotificationGroup']:
        if r is not None:
            return NotificationGroup(
                id=r.id,
                name=r.name,
                users_emails=r.users_emails,
                phone_numbers=r.phone_numbers,
                mails=r.mails,
            )

    # create a notification rule
    def create(self) -> 'NotificationGroup':
        return NotificationGroup.from_db(db.create_notification_group(self))

    # get a notification rule
    @staticmethod
    def find_by_id(id: str, customers: List[str] = None) -> Optional['NotificationGroup']:
        return NotificationGroup.from_db(db.get_notification_group(id))

    @staticmethod
    def find_all(query: Query = None, page: int = 1, page_size: int = 1000) -> List['NotificationGroup']:
        return [NotificationGroup.from_db(notification_group) for notification_group in db.get_notification_groups(query, page, page_size)]

    @staticmethod
    def count(query: Query = None) -> int:
        return db.get_notification_groups_count(query)

    def update(self, **kwargs) -> 'NotificationGroup':
        return NotificationGroup.from_db(db.update_notification_group(self.id, **kwargs))

    def delete(self) -> bool:
        return db.delete_notification_group(self.id)
