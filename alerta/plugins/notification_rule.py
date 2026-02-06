import json
import logging
import smtplib
from datetime import UTC, datetime, timedelta
from threading import Thread

import requests
from cryptography.fernet import Fernet, InvalidToken
from flask import current_app

from alerta.models.alert import Alert
from alerta.models.notification_delay import NotificationDelay
from alerta.models.notification_history import NotificationHistory
from alerta.models.notification_rule import (NotificationChannel,
                                             NotificationRule)
from alerta.models.on_call import OnCall
from alerta.models.user import NotificationInfo
from alerta.plugins import PluginBase

LOG = logging.getLogger('alerta.plugins.notification_rule')
TWILIO_MAX_SMS_LENGTH = 1600
TWILIO_BASE_URL = 'https://api.twilio.com/2010-04-01/Accounts'

LINK_MOBILITY_XML = """
<?xml version="1.0"?>
<SESSION>
  <CLIENT>%(username)s</CLIENT>
  <PW>%(password)s</PW>
  <MSGLST>
    <MSG>
      <TEXT>%(message)s</TEXT>
      <SND>%(sender)s</SND>
      <RCV>{receivers}</RCV>
    </MSG>
  </MSGLST>
</SESSION>
""".split('\n')


def remove_unspeakable_chr(message: str, unspeakables: 'dict[str,str]|None' = None):
    """
    Removes unspeakable characters from string like _,-,:.
    unspeakables: dictionary with keys as unspeakable charecters and value as replace string
    """
    unspeakable_chrs = {'_': ' ', ' - ': '. ', ' -': '.', '-': ' ', ':': '.'}
    unspeakable_chrs.update(unspeakables or {})
    speakable_message = message
    for unspeakable_chr, replacement_str in unspeakable_chrs.items():
        speakable_message = speakable_message.replace(unspeakable_chr, replacement_str)
    return speakable_message


def make_call(message: str, channel: NotificationChannel, receiver: str, fernet: Fernet, **kwargs):
    twiml_message = f'<Response><Pause/><Say>{remove_unspeakable_chr(message)}</Say></Response>'
    data = {'Twiml': twiml_message, 'From': channel.sender, 'To': receiver}
    api_sid = fernet.decrypt(channel.api_sid.encode()).decode()
    api_token = fernet.decrypt(channel.api_token.encode()).decode()
    send_sms(message, channel, receiver, fernet)
    return requests.post(f'{TWILIO_BASE_URL}/{api_sid}/Calls.json', data=data, headers={'Content-Encoding': 'application/json'}, auth=(api_sid, api_token))


def send_sms(message: str, channel: NotificationChannel, receiver: str, fernet: Fernet, **kwargs):
    restricted_message = message[: TWILIO_MAX_SMS_LENGTH - 4]
    body = message if len(message) <= TWILIO_MAX_SMS_LENGTH else restricted_message[: restricted_message.rfind(' ')] + ' ...'
    data = {'Body': body, 'From': channel.sender, 'To': receiver}
    api_sid = fernet.decrypt(channel.api_sid.encode()).decode()
    api_token = fernet.decrypt(channel.api_token.encode()).decode()
    return requests.post(f'{TWILIO_BASE_URL}/{api_sid}/Messages.json', data=data, headers={'Content-Encoding': 'application/json'}, auth=(api_sid, api_token))


def update_bearer(channel: NotificationChannel, fernet):
    if channel.type == 'my_link':
        now = datetime.now()
        if channel.bearer is None or channel.bearer_timeout < (now + timedelta(0, 600)):
            response = mylink_bearer_request(channel, fernet)
            if response.status_code == 200:
                data = response.json()
                bearer = data['access_token']
                timeout = now + timedelta(0, data['expires_in'])
                channel = channel.update_bearer(bearer, timeout)
                LOG.info(f'Updated access_token for myLink channel {channel.id}')
            else:
                LOG.error(f'Failed to update access token for myLink channel {channel.id} with response: {response.status_code} {response.content}')
    return channel


def mylink_bearer_request(channel: NotificationChannel, fernet: Fernet):
    try:
        data = {
            'client_id': fernet.decrypt(channel.api_sid.encode()).decode(),
            'client_secret': fernet.decrypt(channel.api_token.encode()).decode(),
            'grant_type': 'client_credentials'
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        return requests.post('https://sso.linkmobility.com/auth/realms/CPaaS/protocol/openid-connect/token', headers=headers, data=data)
    except InvalidToken:
        LOG.error('NotificationChannel: Failed to decrypt authentication keys. Hint: check that NOTIFICATION_KEY environment variable is set and unchanged since the channel was made')
        return


def send_mylink_sms(message: str, channel: NotificationChannel, receivers: 'list[str]', fernet: Fernet, **kwargs):
    bearer = channel.bearer
    data = json.dumps([{'recipient': receiver, 'content': {'text': message, 'options': {'sms.sender': channel.sender}}} for receiver in receivers])
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {bearer}'}
    return requests.post('https://api.linkmobility.com/sms/v1', data=data, headers=headers)


def send_link_mobility_xml(message: str, channel: NotificationChannel, receivers: 'list[str]', fernet: Fernet, **kwargs):
    content = {'message': message, 'username': fernet.decrypt(channel.api_sid.encode()).decode(), 'sender': channel.sender, 'password': fernet.decrypt(channel.api_token.encode()).decode()}

    xml_content: 'list[str]' = kwargs['xml']
    for line in xml_content:
        receive_start = line.find('{receivers}')
        if receive_start == -1:
            continue
        _receiver_lines = [line.replace('{receivers}', receiver.replace('+', '')) for receiver in receivers]
        xml_content[xml_content.index(line)] = ''.join(_receiver_lines)
    xml_string = ''.join(xml_content)

    data = xml_string.replace('{', '%(').replace('}', ')s') % content

    headers = {'Content-Type': 'application/xml'}
    return requests.post(f'{channel.host}', data, headers=headers, verify=channel.verify if channel.verify is None or channel.verify.lower() != 'false' else False)


def send_smtp_mail(message: str, channel: NotificationChannel, receivers: set, fernet: Fernet, **kwargs):
    server = smtplib.SMTP_SSL(channel.host)
    api_sid = fernet.decrypt(channel.api_sid.encode()).decode()
    api_token = fernet.decrypt(channel.api_token.encode()).decode()
    server.login(api_sid, api_token)
    server.sendmail(channel.sender, list(receivers), f"From: {channel.sender}\nTo: {','.join(receivers)}\nSubject: Alerta\n\n{message}")
    server.quit()


def send_email(message: str, channel: NotificationChannel, receivers: set, fernet: Fernet, **kwargs):
    data = {
        'personalizations': [
            {'to': [{'email': email} for email in receivers]}
        ],
        'from': {'email': channel.sender},
        'subject': 'Alerta',
        'content': [{'type': 'text/html', 'value': message.replace('\n', '<br>')}],
    }
    api_token = fernet.decrypt(channel.api_token.encode()).decode()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_token}',
    }
    return requests.post('https://api.sendgrid.com/v3/mail/send', json=data, headers=headers)


def get_message_obj(alertobj: 'dict') -> 'dict':
    alertobjcopy = alertobj.copy()
    for objname, objval in alertobj.items():
        try:
            value_type = type(objval)
            if objname != 'history' and objname != 'twilioRules' and objname != 'notificationRules' and objname != 'onCalls' and value_type == list:
                alertobjcopy[objname] = ', '.join(objval)
            if value_type == str and objname == 'severity':
                alertobjcopy[objname] = objval.capitalize()
            if value_type == dict:
                for cmpxobjname, cmpxobjval in objval.items():
                    alertobjcopy[f'{objname}.{cmpxobjname}'] = cmpxobjval
            if value_type == list:
                for index, value in enumerate(objval):
                    alertobjcopy[f'{objname}[{index}]'] = value
        except Exception as err:
            LOG.error(f'Error while handling message objs: {str(err)}')
            continue
    return alertobjcopy


def log_notification(sent: bool, message: str, channel: NotificationChannel, rule: str, alert: str, receivers: 'list[str]', error: str = None, id: str = None):
    NotificationHistory.parse({
        'id': id,
        'sent': sent,
        'message': message,
        'channel': channel.id,
        'rule': rule,
        'alert': alert,
        'sender': channel.sender,
        'receiver': ','.join(receivers),
        'error': error
    }).create()


def delay_notification(alert: Alert, notification_rule: NotificationRule):
    delay_time = datetime.now(UTC) + notification_rule.delay_time
    NotificationDelay.parse({
        'alert_id': alert.id,
        'notification_rule_id': notification_rule.id,
        'delay_time': delay_time
    }).create()


def handle_channel(message: str, channel: NotificationChannel, notification_rule: NotificationRule, users: 'set[NotificationInfo]', fernet: Fernet, alert: str):
    notification_type = channel.type
    phone_numbers = {*notification_rule.receivers, *[f'{user.country_code}{user.phone_number}' for user in users if user.phone_number is not None]}
    mails = {*[receiver.lower() for receiver in notification_rule.receivers], *[user.email.lower() for user in users if user.email is not None]}

    if notification_type == 'sendgrid':
        if len(mails) == 0:
            return
        try:
            response = send_email(message, channel, mails, fernet)
            if response.status_code != 202:
                data = response.json()['errors'][0]
                log_notification(False, message, channel, notification_rule.id, alert, mails, f'Got status code {response.status_code}: {data["message"]}')
                LOG.error('NotificationRule: %s', f'Got status code {response.status_code}: {data["message"]} for field {data["field"]} With help: {data["help"]}')
            else:
                log_notification(True, message, channel, notification_rule.id, alert, mails)
        except InvalidToken:
            log_notification(False, message, channel, notification_rule.id, alert, mails, 'NotificationChannel: Failed to decrypt authentication keys')
            LOG.error('NotificationChannel: Failed to decrypt authentication keys. Hint: check that NOTIFICATION_KEY environment variable is set and unchanged since the channel was made')

    elif notification_type == 'smtp':
        if len(mails) == 0:
            return
        try:
            send_smtp_mail(message, channel, mails, fernet)
            log_notification(True, message, channel, notification_rule.id, alert, mails)
        except InvalidToken:
            log_notification(False, message, channel, notification_rule.id, alert, mails, 'NotificationChannel: Failed to decrypt authentication keys')
            LOG.error('NotificationChannel: Failed to decrypt authentication keys. Hint: check that NOTIFICATION_KEY environment variable is set and unchanged since the channel was made')
        except Exception as err:
            log_notification(False, message, channel, notification_rule.id, alert, mails, str(err))
            LOG.error('NotificationRule: %s', str(err))

    elif 'twilio' in notification_type:
        for number in phone_numbers:
            if number is None or number == '':
                continue
            try:
                if 'call' in notification_type:
                    response = make_call(message, channel, number, fernet)
                elif 'sms' in notification_type:
                    response = send_sms(message, channel, number, fernet)

                response_data = response.json()
                if response.status_code != 201:
                    log_notification(False, message, channel, notification_rule.id, alert, [number], error=f"Got status code {response.status_code} with error code {response_data['code']}: {response_data['message']}")
                    LOG.error('NotificationRule: %s', f"Got status code {response.status_code} with error code {response_data['code']}: {response_data['message']}. More info: {response_data['more_info']}")
                else:
                    log_notification(True, message, channel, notification_rule.id, alert, [number], id=response_data['sid'])

            except InvalidToken:
                LOG.error('NotificationChannel: Failed to decrypt authentication keys. Hint: check that NOTIFICATION_KEY environment variable is set and unchanged since the channel was made')
                log_notification(False, message, channel, notification_rule.id, alert, [number], error='NotificationChannel: Failed to decrypt authentication keys')

    elif notification_type == 'link_mobility_xml':
        if len(phone_numbers) == 0:
            return
        try:
            response = send_link_mobility_xml(message, channel, phone_numbers, fernet, xml=LINK_MOBILITY_XML.copy())
            if response.content.decode().find('FAIL') != -1:
                LOG.error(response.content)
                log_notification(False, message, channel, notification_rule.id, alert, phone_numbers, error=response.content.decode())
            else:
                LOG.info(response.content)
                log_notification(True, message, channel, notification_rule.id, alert, [number])
        except InvalidToken:
            log_notification(False, message, channel, notification_rule.id, alert, phone_numbers, error='NotificationChannel: Failed to decrypt authentication keys')
            LOG.error('NotificationChannel: Failed to decrypt authentication keys. Hint: check that NOTIFICATION_KEY environment variable is set and unchanged since the channel was made')

    elif notification_type == 'my_link':
        if len(phone_numbers) == 0:
            return
        response = send_mylink_sms(message, channel, phone_numbers, fernet)
        if response.status_code != 202:
            LOG.error(f'Failed to send myLink message with response: {response.content}')
            for number in phone_numbers:
                log_notification(False, message, channel, notification_rule.id, alert, [number], error=response.content.decode())
        else:
            LOG.info(f'Successfully Sent message to myLink with response: {response.content}')
            for msg in response.json()['messages']:
                log_notification(True, message, channel, notification_rule.id, alert, [msg['recipient']], id=msg['messageId'])


def handle_test(channel: NotificationChannel, info: NotificationRule, config):
    message = info.text if info.text != '' else 'this is a test message for testing a notification_channel in alerta'
    fernet = Fernet(config['NOTIFICATION_KEY'])
    channel = update_bearer(channel, fernet)
    handle_channel(message, channel, info, info.users, fernet, 'Test Notification Channel')


def get_notification_trigger_text(rule: NotificationRule, alert: Alert, status: str):
    for trigger in rule.triggers:
        from_check = trigger.from_severity == [] or alert.previous_severity in trigger.from_severity
        to_check = trigger.to_severity == [] or alert.severity in trigger.to_severity
        if (from_check and to_check) and ((status == '' or status in trigger.status) and (status != '' or trigger.status == [] or alert.status in trigger.status)):
            return trigger.text.replace('%(default)s', rule.text) if trigger.text != '' and trigger.text is not None else rule.text


def handle_notifications(alert: 'Alert', notifications: 'list[tuple[NotificationRule,NotificationChannel, list[set[NotificationInfo | None]]]]', on_users: 'list[set[NotificationInfo | None]]', fernet: Fernet, app_context, status: str = ''):
    app_context.push()
    standard_message = '%(environment)s: %(severity)s alert for %(service)s - %(resource)s is %(event)s'
    for notification_rule, channel, users in notifications:
        if channel is None:
            return

        if notification_rule.use_oncall:
            users.update(on_users)
        msg_obj = {**alert.serialize, 'status': status} if status != '' else alert.serialize
        text = get_notification_trigger_text(notification_rule, alert, status)
        message = (
            text if text != '' and text is not None else standard_message
        ) % get_message_obj(msg_obj)

        handle_channel(message, channel, notification_rule, users, fernet, alert.id)


def handle_alert(alert: Alert, config, stat: str = ''):
    notification_rules = NotificationRule.find_all_active(alert) if stat == '' else NotificationRule.find_all_active_status(alert, stat)
    if len(notification_rules) == 0:
        return
    fernet = Fernet(config['NOTIFICATION_KEY'])
    notifications = []
    for rule in notification_rules:
        if rule.delay_time is not None:
            delay_notification(alert, rule)
        else:
            notifications.append([rule, update_bearer(rule.channel, fernet), rule.users])
    on_users = set()
    for on_call in OnCall.find_all_active(alert):
        on_users.update(on_call.users)
    Thread(target=handle_notifications, args=[alert, notifications, on_users, fernet, current_app.app_context(), stat]).start()


def handle_delay(delay: NotificationDelay):
    fernet = Fernet(current_app.config['NOTIFICATION_KEY'])
    alert = Alert.find_by_id(delay.alert_id)
    rule = NotificationRule.find_by_id(delay.notification_rule_id)
    channel = update_bearer(rule.channel, fernet)
    on_users = [on_call.users for on_call in OnCall.find_all_active(alert)]
    Thread(target=handle_notifications, args=[alert, [(rule, channel, rule.users)], on_users, fernet, current_app.app_context()]).start()


class NotificationRulesHandler(PluginBase):
    """
    Default notification rules handler for sending messages and making calls
    when a notification rule is active during new alert status
    """

    def pre_receive(self, alert, **kwargs):
        return alert

    def post_receive(self, alert: 'Alert', **kwargs):
        if not alert:
            return
        NotificationDelay.delete_alert(alert.id)
        config = kwargs.get('config')
        if alert.repeat:
            return
        handle_alert(alert, config)

    def status_change(self, alert, status, text, **kwargs):
        if not alert:
            return
        NotificationDelay.delete_alert(alert.id)
        stat = status if isinstance(status, str) else status.value
        config = kwargs.get('config')
        handle_alert(alert, config, stat)

    def take_action(self, alert, action, text, **kwargs):
        raise NotImplementedError

    def delete(self, alert, **kwargs) -> bool:
        raise NotImplementedError
