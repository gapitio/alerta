import json
import logging
import unittest
from datetime import datetime, timedelta

from alerta.app import create_app, db, plugins
from alerta.models.key import ApiKey

LOG = logging.getLogger('test.test_notification_rule')


def get_id(object: dict):
    return object['id']


class EscalationRuleTestCase(unittest.TestCase):
    def setUp(self) -> None:
        test_config = {
            'TESTING': True,
            'AUTH_REQUIRED': True,
            'CUSTOMER_VIEWS': True,
            'PLUGINS': [],
        }
        self.app = create_app(test_config)
        self.client = self.app.test_client()

        self.prod_alert = {
            'resource': 'node404',
            'event': 'node_down',
            'environment': 'Production',
            'severity': 'minor',
            'correlate': ['node_down', 'node_marginal', 'node_up'],
            'service': ['Core', 'Web', 'Network'],
            'group': 'Network',
            'tags': ['level=20', 'switch:off'],
        }

        with self.app.test_request_context('/'):
            self.app.preprocess_request()
            self.admin_api_key = ApiKey(
                user='admin@alerta.io',
                scopes=['admin', 'read', 'write'],
                text='demo-key',
            )
            self.customer_api_key = ApiKey(
                user='admin@alerta.io',
                scopes=['admin', 'read', 'write'],
                text='demo-key',
                customer='Foo',
            )
            self.admin_api_key.create()
            self.customer_api_key.create()

        self.headers = {
            'Authorization': f'Key {self.admin_api_key.key}',
            'Content-type': 'application/json',
        }

    def tearDown(self) -> None:
        plugins.plugins.clear()
        db.destroy()

    def create_api_obj(self, apiurl: str, apidata: dict, apiheaders: dict, status_code: int = 201) -> dict:
        response = self.client.post(apiurl, data=json.dumps(apidata), headers=apiheaders)
        self.assertEqual(response.status_code, status_code)
        return json.loads(response.data.decode('utf-8'))

    def update_api_obj(self, apiurl: str, apidata: dict, apiheaders: dict, status_code: int = 200) -> dict:
        response = self.client.put(apiurl, data=json.dumps(apidata), headers=apiheaders)
        self.assertEqual(response.status_code, status_code)
        return json.loads(response.data.decode('utf-8'))

    def get_api_obj(self, apiurl: str, apiheaders: dict, status_code: int = 200) -> dict:
        response = self.client.get(apiurl, headers=apiheaders)
        self.assertEqual(response.status_code, status_code)
        return json.loads(response.data.decode('utf-8'))

    def delete_api_obj(self, apiurl: str, apiheaders: dict, status_code: int = 200) -> dict:
        response = self.client.delete(apiurl, headers=apiheaders)
        self.assertEqual(response.status_code, status_code)
        return json.loads(response.data.decode('utf-8'))

    def test_escalation_rule(self):

        escalation_rule = {
            'environment': 'Production',
            'tags': [],
            'time': '1 second'
        }

        self.create_api_obj('/escalationrules', escalation_rule, self.headers)

        start = datetime.now()
        alert = self.create_api_obj('/alert', self.prod_alert, self.headers)['alert']
        escalated_alerts = self.get_api_obj('/escalate', self.headers, 200)['alerts']
        self.assertNotIn(alert['id'], map(get_id, escalated_alerts))

        while len(escalated_alerts) == 0:
            escalated_alerts = self.get_api_obj('/escalate', self.headers, 200)['alerts']
        self.assertTrue(datetime.now() - start >= timedelta(seconds=1))
        self.assertIn(alert['id'], map(get_id, escalated_alerts))
        new_data = self.get_api_obj(f"/alert/{alert['id']}", self.headers)['alert']
        self.assertEqual(alert['severity'], 'minor')
        self.assertEqual(new_data['severity'], 'major')

    def test_detail(self):

        escalation_rule = {
            'environment': 'Production',
            'resource': 'node404',
            'group': 'Network',
            'service': ['Core', 'Web', 'Network'],
            'tags': [],
            'time': '0 second'
        }

        self.prod_alert['event'] = 'normal'
        wrong_service = {**self.prod_alert, 'service': ['Test'], 'event': 'wrong_service'}
        wrong_resource = {**self.prod_alert, 'resource': 'test', 'event': 'wrong_resource'}
        wrong_environment = {**self.prod_alert, 'environment': 'Development', 'event': 'wrong_environment'}
        wrong_group = {**self.prod_alert, 'group': 'test', 'event': 'wrong_group'}

        self.create_api_obj('/escalationrules', escalation_rule, self.headers)

        alert = self.create_api_obj('/alert', self.prod_alert, self.headers)['alert']
        wrong_service_alert = self.create_api_obj('/alert', wrong_service, self.headers)['alert']
        wrong_resource_alert = self.create_api_obj('/alert', wrong_resource, self.headers)['alert']
        wrong_environment_alert = self.create_api_obj('/alert', wrong_environment, self.headers)['alert']
        wrong_group_alert = self.create_api_obj('/alert', wrong_group, self.headers)['alert']
        escalated_alerts = self.get_api_obj('/escalate', self.headers, 200)['alerts']
        self.assertIn(alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(wrong_service_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(wrong_resource_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(wrong_environment_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(wrong_group_alert['id'], map(get_id, escalated_alerts))
        new_data = self.get_api_obj(f"/alert/{alert['id']}", self.headers)['alert']
        self.assertEqual(alert['severity'], 'minor')
        self.assertEqual(new_data['severity'], 'major')

    def test_event(self):

        escalation_rule = {
            'environment': 'Production',
            'event': 'event',
            'service': ['Core', 'Web', 'Network'],
            'tags': [],
            'time': '0 second'
        }

        self.prod_alert['event'] = 'event'
        wrong_event = {**self.prod_alert, 'service': ['Test'], 'event': 'wrong_service'}

        self.create_api_obj('/escalationrules', escalation_rule, self.headers)

        alert = self.create_api_obj('/alert', self.prod_alert, self.headers)['alert']
        wrong_event_alert = self.create_api_obj('/alert', wrong_event, self.headers)['alert']
        escalated_alerts = self.get_api_obj('/escalate', self.headers, 200)['alerts']
        self.assertIn(alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(wrong_event_alert['id'], map(get_id, escalated_alerts))
        new_data = self.get_api_obj(f"/alert/{alert['id']}", self.headers)['alert']
        self.assertEqual(alert['severity'], 'minor')
        self.assertEqual(new_data['severity'], 'major')

    def test_tags(self):

        escalation_rule = {
            'environment': 'Production',
            'tags': [{'all':['test', 'dev'], 'any':['all', 'any']}],
            'time': '0 second'
        }
        missing_tags = {
            'resource': 'missing_tags',
            'event': 'missing_tags',
            'environment': 'Production',
            'severity': 'minor',
            'service': ['Tags'],
            'group': 'Tags',
            'tags': [],
        }
        or_tags = {**missing_tags,'tags': ['any', 'all'], 'resource': 'or_tags'}
        and_tags = {**missing_tags,'tags': ['test', 'dev'], 'resource': 'and_tags'}
        partial_or_tags = {**missing_tags,'tags': ['test', 'dev', 'any'], 'resource': 'partial_or_tags'}
        all_tags = {**missing_tags,'tags': ['test', 'dev', 'any', 'all'], 'resource': 'all_tags'}

        self.create_api_obj('/escalationrules', escalation_rule, self.headers)

        alert = self.create_api_obj('/alert', missing_tags, self.headers)['alert']
        or_tags_alert = self.create_api_obj('/alert', or_tags, self.headers)['alert']
        and_tags_alert = self.create_api_obj('/alert', and_tags, self.headers)['alert']
        partial_or_tags_alert = self.create_api_obj('/alert', partial_or_tags, self.headers)['alert']
        all_tags_alert = self.create_api_obj('/alert', all_tags, self.headers)['alert']
        escalated_alerts = self.get_api_obj('/escalate', self.headers, 200)['alerts']
        self.assertNotIn(alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(or_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(and_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertIn(partial_or_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertIn(all_tags_alert['id'], map(get_id, escalated_alerts))

    def test_full_excluded_tags(self):
        escalation_rule = {
            'environment': 'Production',
            'tags': [],
            'excludedTags': [{'all':['test', 'dev'], 'any':['all', 'any']}],
            'time': '0 second'
        }
        missing_tags = {
            'resource': 'missing_tags',
            'event': 'missing_tags',
            'environment': 'Production',
            'severity': 'minor',
            'service': ['Tags'],
            'group': 'Tags',
            'tags': [],
        }
        or_tags = {**missing_tags,'tags': ['any', 'all'], 'resource': 'or_tags'}
        and_tags = {**missing_tags,'tags': ['test', 'dev'], 'resource': 'and_tags'}
        partial_or_tags = {**missing_tags,'tags': ['test', 'dev', 'any'], 'resource': 'partial_or_tags'}
        all_tags = {**missing_tags,'tags': ['test', 'dev', 'any', 'all'], 'resource': 'all_tags'}

        self.create_api_obj('/escalationrules', escalation_rule, self.headers)

        alert = self.create_api_obj('/alert', missing_tags, self.headers)['alert']
        or_tags_alert = self.create_api_obj('/alert', or_tags, self.headers)['alert']
        and_tags_alert = self.create_api_obj('/alert', and_tags, self.headers)['alert']
        partial_or_tags_alert = self.create_api_obj('/alert', partial_or_tags, self.headers)['alert']
        all_tags_alert = self.create_api_obj('/alert', all_tags, self.headers)['alert']
        escalated_alerts = self.get_api_obj('/escalate', self.headers, 200)['alerts']
        self.assertIn(alert['id'], map(get_id, escalated_alerts))
        self.assertIn(or_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertIn(and_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(partial_or_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(all_tags_alert['id'], map(get_id, escalated_alerts))

    def test_or_excluded_tags(self):
        escalation_rule = {
            'environment': 'Production',
            'tags': [],
            'excludedTags': [{'any':['all', 'any']}],
            'time': '0 second'
        }
        missing_tags = {
            'resource': 'missing_tags',
            'event': 'missing_tags',
            'environment': 'Production',
            'severity': 'minor',
            'service': ['Tags'],
            'group': 'Tags',
            'tags': [],
        }
        or_tags = {**missing_tags,'tags': ['any', 'all'], 'resource': 'or_tags'}
        and_tags = {**missing_tags,'tags': ['test', 'dev'], 'resource': 'and_tags'}
        partial_or_tags = {**missing_tags,'tags': ['test', 'dev', 'any'], 'resource': 'partial_or_tags'}
        all_tags = {**missing_tags,'tags': ['test', 'dev', 'any', 'all'], 'resource': 'all_tags'}

        self.create_api_obj('/escalationrules', escalation_rule, self.headers)

        alert = self.create_api_obj('/alert', missing_tags, self.headers)['alert']
        or_tags_alert = self.create_api_obj('/alert', or_tags, self.headers)['alert']
        and_tags_alert = self.create_api_obj('/alert', and_tags, self.headers)['alert']
        partial_or_tags_alert = self.create_api_obj('/alert', partial_or_tags, self.headers)['alert']
        all_tags_alert = self.create_api_obj('/alert', all_tags, self.headers)['alert']
        escalated_alerts = self.get_api_obj('/escalate', self.headers, 200)['alerts']
        self.assertIn(alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(or_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertIn(and_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(partial_or_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(all_tags_alert['id'], map(get_id, escalated_alerts))

    def test_all_excluded_tags(self):
        escalation_rule = {
            'environment': 'Production',
            'tags': [],
            'excludedTags': [{'all':['test', 'dev']}],
            'time': '0 second'
        }
        missing_tags = {
            'resource': 'missing_tags',
            'event': 'missing_tags',
            'environment': 'Production',
            'severity': 'minor',
            'service': ['Tags'],
            'group': 'Tags',
            'tags': [],
        }
        or_tags = {**missing_tags,'tags': ['any', 'all'], 'resource': 'or_tags'}
        and_tags = {**missing_tags,'tags': ['test', 'dev'], 'resource': 'and_tags'}
        partial_or_tags = {**missing_tags,'tags': ['test', 'dev', 'any'], 'resource': 'partial_or_tags'}
        all_tags = {**missing_tags,'tags': ['test', 'dev', 'any', 'all'], 'resource': 'all_tags'}

        self.create_api_obj('/escalationrules', escalation_rule, self.headers)

        alert = self.create_api_obj('/alert', missing_tags, self.headers)['alert']
        or_tags_alert = self.create_api_obj('/alert', or_tags, self.headers)['alert']
        and_tags_alert = self.create_api_obj('/alert', and_tags, self.headers)['alert']
        partial_or_tags_alert = self.create_api_obj('/alert', partial_or_tags, self.headers)['alert']
        all_tags_alert = self.create_api_obj('/alert', all_tags, self.headers)['alert']
        escalated_alerts = self.get_api_obj('/escalate', self.headers, 200)['alerts']
        self.assertIn(alert['id'], map(get_id, escalated_alerts))
        self.assertIn(or_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(and_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(partial_or_tags_alert['id'], map(get_id, escalated_alerts))
        self.assertNotIn(all_tags_alert['id'], map(get_id, escalated_alerts))
