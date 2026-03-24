from datetime import UTC, datetime, timedelta

from werkzeug.datastructures import MultiDict

from alerta.app import db

VALID_PARAMS = [
    'id',
    'resource',
    'event',
    'environment',
    'severity',
    'status',
    'service',
    'value',
    'text',
    'tag',
    'tags',
    'customTags',
    'attributes',
    'origin',
    'createTime',
    'timeout',
    'rawData',
    'customer',
    'duplicateCount',
    'previousSeverity',
    'receiveTime',
    'lastReceiveId',
    'lastReceiveTime',
    'updateTime',
]


class FilterTab:

    def __init__(self,name: str, index: int, **kwargs) -> None:
        if name is None:
            raise ValueError('Missing mandatory value for "name"')
        if index is None:
            raise ValueError('Missing mandatory value for "index"')

        self.name = name
        self.index = index
        self.filter = kwargs.get('filter') or {}

    @classmethod
    def parse(cls, json: dict[str, str | int | dict[str, str]]) -> 'FilterTab':
        if not isinstance(json.get('index'), int):
            raise ValueError('index must be an int')
        if not isinstance(json.get('name'), str):
            raise ValueError('name must be a string')

        return FilterTab(
            name=json['name'],
            index=json['index'],
            filter=json.get('filter', None),
        )

    @property
    def serialize(self):
        return {
            'name': self.name,
            'index': self.index,
            'filter': self.filter,
        }

    @property
    def filter_args(self):
        def to_isoformat(date: datetime):
            return date.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        data = []
        for key, value in self.filter.items():
            if key == 'dateRange':
                if value == {}:
                    continue
                if 'from' in value:
                    if value.get('select'):
                        from_time = datetime.fromtimestamp(value['from'], tz=UTC)
                    else:
                        from_time = (datetime.now(UTC) + timedelta(seconds=int(value['from'])))
                    data.append(('from-date', to_isoformat(from_time)))
                if 'to' in value:
                    to_time = datetime.fromtimestamp(value['to'], tz=UTC)
                    data.append(('to-date', to_isoformat(to_time)))
            if key == 'attributes':
                for attr_key, attr_value in value.items():
                    if isinstance(attr_value, list):
                        for item in attr_value:
                            data.append((f'attributes.{attr_key}', item))
                    else:
                        data.append((f'attributes.{attr_key}', attr_value))
            elif key not in VALID_PARAMS or isinstance(value, dict):
                continue
            elif isinstance(value, list):
                for val in value:
                    data.append((key, val))
            else:
                data.append(key, value)

        return MultiDict(data)

    def __repr__(self) -> str:
        return f'AlertTab(name={self.name}, index={self.index}, filter={self.filter},'

    @ classmethod
    def from_db(cls, rec) -> 'FilterTab':
        return FilterTab(
            name=rec.name,
            index=rec.index,
            filter=rec.filter
        )

    # create a filter tab
    def create(self) -> 'FilterTab':
        return FilterTab.from_db(db.create_filter_tab(self))

    # create a filter tabs
    @staticmethod
    def create_all(tabs: list['FilterTab']) -> list['FilterTab']:
        return [FilterTab.from_db(tab) for tab in db.create_filter_tabs(tabs)]

    # get a filter tab
    @ staticmethod
    def find_by_id(id: str):
        return FilterTab.from_db(db.get_filter_tab(id))

    @staticmethod
    def delete_all(ids: list[str]):
        return db.delete_filter_tabs(ids)

    @staticmethod
    def update_all(tabs: list['FilterTab']):
        return db.update_filter_tabs(tabs)

    @staticmethod
    def update_indexes(tabs):
        return db.update_filter_tab_indexes(tabs)

    @ staticmethod
    def find_all() -> list['FilterTab']:
        return [
            FilterTab.from_db(notification_channel)
            for notification_channel in db.get_filter_tabs()
        ]

    # def update(self, **kwargs) -> 'FilterTab':
    #     return FilterTab.from_db(db.update(self.id, **kwargs))

    def delete(self) -> bool:
        return db.delete_filter_tab(self.id)
