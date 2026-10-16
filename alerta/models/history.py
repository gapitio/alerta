from datetime import UTC, datetime

from alerta.utils.response import absolute_url


class History:

    def __init__(self, id, event, **kwargs):
        self.id = id
        self.event = event
        self.severity = kwargs.get('severity', None)
        self.status = kwargs.get('status', None)
        self.value = kwargs.get('value', None)
        self.text = kwargs.get('text', None)
        self.change_type = kwargs.get('change_type', kwargs.get('type', None)) or ''
        self.update_time = kwargs.get('update_time', None) or datetime.now(UTC)
        self.user = kwargs.get('user', None)
        self.timeout = kwargs.get('timeout', None)

    @property
    def serialize(self):
        return {
            'id': self.id,
            'href': absolute_url('/alert/' + self.id),
            'event': self.event,
            'severity': self.severity,
            'status': self.status,
            'value': self.value,
            'text': self.text,
            'type': self.change_type,
            'updateTime': self.update_time,
            'user': self.user,
            'timeout': self.timeout
        }

    def __repr__(self):
        return 'History(id={!r}, event={!r}, severity={!r}, status={!r}, type={!r})'.format(
            self.id, self.event, self.severity, self.status, self.change_type)

    @classmethod
    def from_db(cls, r):
        if r is not None:
            return History(
                id=r.id,
                event=r.event,
                severity=r.severity,
                status=r.status,
                value=r.value,
                text=r.text,
                change_type=r.type,
                update_time=r.update_time,
                user=getattr(r, 'user', None),
                timeout=getattr(r, 'timeout', None)
            )


class RichHistory:

    def __init__(self, resource, event, **kwargs):

        self.id = kwargs.get('id', None)
        self.alert_id = kwargs.get('alert_id', None)
        self.resource = resource
        self.event = event
        self.environment = kwargs.get('environment', None)
        self.severity = kwargs.get('severity', None)
        self.status = kwargs.get('status', None)
        self.service = kwargs.get('service', None) or list()
        self.group = kwargs.get('group', None)
        self.value = kwargs.get('value', None)
        self.text = kwargs.get('text', None)
        self.tags = kwargs.get('tags', None) or list()
        self.attributes = kwargs.get('attributes', None) or dict()
        self.origin = kwargs.get('origin', None)
        self.update_time = kwargs.get('update_time', None)
        self.user = kwargs.get('user', None)
        self.timeout = kwargs.get('timeout', None)
        self.change_type = kwargs.get('change_type', kwargs.get('type', None))
        self.customer = kwargs.get('customer', None)

    @property
    def serialize(self):
        data = {
            'id': self.id,
            'alertId': self.alert_id,
            'href': absolute_url('/alert/' + self.alert_id),
            'resource': self.resource,
            'event': self.event,
            'environment': self.environment,
            'service': self.service,
            'group': self.group,
            'text': self.text,
            'tags': self.tags,
            'attributes': self.attributes,
            'origin': self.origin,
            'updateTime': self.update_time,
            'user': self.user,
            'timeout': self.timeout,
            'type': self.change_type,
            'customer': self.customer
        }

        if self.severity:
            data['severity'] = self.severity

        if self.status:
            data['status'] = self.status

        if self.value:
            data['value'] = self.value

        return data

    def __repr__(self):
        return 'RichHistory(id={!r}, environment={!r}, resource={!r}, event={!r}, severity={!r},' \
               ' status={!r}, timeout={!r}, type={!r}, customer={!r})'.format(
                   self.id, self.environment, self.resource, self.event, self.severity,
                   self.status, self.timeout, self.change_type, self.customer)

    @classmethod
    def from_db(cls, r):
        if r is not None:
            return RichHistory(
                id=r.id,
                alert_id=getattr(r, 'alert_id', None),
                resource=r.resource,
                event=r.event,
                environment=r.environment,
                severity=r.severity,
                status=r.status,
                service=r.service,
                value=r.value,
                text=r.text,
                tags=r.tags,
                attributes=dict(r.attributes),
                origin=r.origin,
                update_time=r.update_time,
                user=getattr(r, 'user', None),
                timeout=getattr(r, 'timeout', None),
                change_type=r.type,
                customer=r.customer
            )
