import json
import os
import unittest
from datetime import datetime, timedelta


from alerta.app import create_app, db, plugins
from alerta.models.key import ApiKey


def get_id(object: dict):
    return object['id']


def get_delay_id(object: dict):
    return {'rule_id': object['notification_rule_id'], 'alert_id': object['alert_id']}


def get_history_id(object: dict):
    return {'rule_id': object['rule'], 'alert_id': object['alert']}


class ChannelNotificationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        test_config = {
            'TESTING': True,
            'AUTH_REQUIRED': True,
            'CUSTOMER_VIEWS': True,
            'PLUGINS': ['notification_rule'],
        }
        self.app = create_app(test_config)
        self.client = self.app.test_client()

        self.sms_channel = {
            'id': 'SMS_Channel',
            'sender': 'sender',
            'type': 'twilio_sms',
            'apiToken': 'api_token',
            'apiSid': 'api_sid',
        }

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
            self.admin_api_key.create()        

        self.headers = {
            'Authorization': f'Key {self.admin_api_key.key}',
            'Content-type': 'application/json',
        }

    def tearDown(self) -> None:
        plugins.plugins.clear()
        db.destroy()

    def get_api_obj(self, apiurl: str, apiheaders: dict, status_code: int = 200) -> dict:
        response = self.client.get(apiurl, headers=apiheaders)
        self.assertEqual(response.status_code, status_code)
        return json.loads(response.data.decode('utf-8'))

    def create_api_obj(self, apiurl: str, apidata: dict, apiheaders: dict, status_code: int = 201) -> dict:
        response = self.client.post(apiurl, data=json.dumps(apidata), headers=apiheaders)
        self.assertEqual(response.status_code, status_code)
        return json.loads(response.data.decode('utf-8'))    

    def test_delayed_notifications(self):
        notification_rule = {
            'environment': 'Production',
            'channelId': 'SMS_Channel',
            'service': ['Core'],
            'receivers': [],
        }

        delayed_notification_rule = {
            'environment': 'Production',
            'channelId': 'SMS_Channel',
            'service': ['Core'],
            'receivers': [],
            'delayTime': '1 second'
        }

        self.channel_id = self.create_api_obj('/notificationchannels', self.sms_channel, self.headers)['id']
        data = self.create_api_obj('/notificationrules', notification_rule, self.headers)
        notification_rule_id = data['id']

        delayed_notification_rule = self.create_api_obj('/notificationrules', delayed_notification_rule, self.headers)
        delayed_notification_rule_id = delayed_notification_rule['id']
        self.assertEqual(delayed_notification_rule['notificationRule']['delayTime'], 1)

        # new alert should activate notification_rule
        data = self.create_api_obj('/alert', self.prod_alert, self.headers)
        alert_id = data['id']
        start = datetime.now()
        active_notification_rules = self.create_api_obj('/notificationrules/active', data['alert'], self.headers, 200)['notificationRules']
        self.assertIn(
            notification_rule_id,
            map(get_id, active_notification_rules),
        )
        delayed_rules = self.get_api_obj('/notificationdelay', self.headers)['notificationDelays']
        self.assertNotEqual(delayed_rules, [])
        self.assertIn({'rule_id': delayed_notification_rule_id, 'alert_id': alert_id}, map(get_delay_id, delayed_rules))
        delayed_data = self.get_api_obj('/notificationdelay/fire', self.headers)
        self.assertEqual(delayed_data['notifications'], [])
        while len(delayed_data['notifications']) == 0:
            delayed_data = self.get_api_obj('/notificationdelay/fire', self.headers)
        self.assertTrue(datetime.now() - start >= timedelta(seconds=1))
        self.assertIn({'rule_id': delayed_notification_rule_id, 'alert_id': alert_id}, map(get_delay_id, delayed_data['notifications']))
    
    def test_twilio_sms_channel(self):
        try:
            twilio_config = {
                'token': os.environ['TWILIO_TOKEN'],
                'sid': os.environ['TWILIO_SID'],
                'sender': os.environ['TWILIO_SENDER'],
                'receiver': os.environ['TWILIO_RECEIVER']
            }
        except KeyError:
            self.skipTest('Missing required twilio environment')
        notification_rule = {
            'environment': 'Production',
            'channelId': 'sms',
            'service': ['Core'],
            'receivers': [twilio_config['receiver']],
        }

        channel = {
            'id': 'sms',
            'sender': twilio_config['sender'],
            'type': 'twilio_sms',
            'apiToken': twilio_config['token'],
            'apiSid': twilio_config['sid']
        }

        self.create_api_obj('/notificationchannels', channel, self.headers)
        data = self.create_api_obj('/notificationrules', notification_rule, self.headers)
        notification_rule_id = data['id']

        data = self.create_api_obj('/alert', self.prod_alert, self.headers)
        alert_id = data['id']
        active_notification_rules = self.create_api_obj('/notificationrules/active', data['alert'], self.headers, 200)['notificationRules']
        self.assertIn(
            notification_rule_id,
            map(get_id, active_notification_rules),
        )
        history = self.get_api_obj('notificationhistory', self.headers)['notificationHistory']

        while len(history) == 0:
            history = self.get_api_obj('notificationhistory', self.headers)['notificationHistory']

        self.assertIn(
            {'rule_id': notification_rule_id, 'alert_id': alert_id},
            map(get_history_id, history)
        )
        self.assertTrue(history[0]['sent'])

    def test_sendgrid_channel(self):
        try:
            sendgrid_config = {
                'token': os.environ['SENDGRID_TOKEN'],
                'sender': os.environ['SENDGRID_SENDER'],
                'receiver': os.environ['SENDGRID_RECEIVER']
            }
        except KeyError:
            self.skipTest('Missing required twilio environment')
        notification_rule = {
            'environment': 'Production',
            'channelId': 'sms',
            'service': ['Core'],
            'receivers': [sendgrid_config['receiver']],
        }

        channel = {
            'id': 'sms',
            'sender': sendgrid_config['sender'],
            'type': 'sendgrid',
            'apiToken': sendgrid_config['token'],
        }

        self.create_api_obj('/notificationchannels', channel, self.headers)
        data = self.create_api_obj('/notificationrules', notification_rule, self.headers)
        notification_rule_id = data['id']

        data = self.create_api_obj('/alert', self.prod_alert, self.headers)
        alert_id = data['id']
        active_notification_rules = self.create_api_obj('/notificationrules/active', data['alert'], self.headers, 200)['notificationRules']
        self.assertIn(
            notification_rule_id,
            map(get_id, active_notification_rules),
        )
        history = self.get_api_obj('notificationhistory', self.headers)['notificationHistory']

        while len(history) == 0:
            history = self.get_api_obj('notificationhistory', self.headers)['notificationHistory']

        self.assertIn(
            {'rule_id': notification_rule_id, 'alert_id': alert_id},
            map(get_history_id, history)
        )
        self.assertTrue(history[0]['sent'])