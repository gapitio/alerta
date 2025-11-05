from flask import jsonify, request
from flask_cors import cross_origin

from alerta.app import plugins
from alerta.auth.decorators import permission
from alerta.exceptions import ApiError
from alerta.models.enums import Scope
from alerta.models.notification_channel import NotificationChannel
from alerta.models.notification_rule import NotificationRule
from alerta.models.notification_send import NotificationSend
from alerta.plugins.notification_rule import handle_test
from alerta.utils.response import jsonp

from . import api


@api.route('/notificationsends', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.read_notification_groups)
@permission(Scope.admin_users)
@permission(Scope.write_notification_sends)
@jsonp
def list_notification_sends():
    notification_sends = NotificationSend.find_all()

    if notification_sends:
        return jsonify(
            status='ok',
            notificationSends=[send.serialize for send in notification_sends],
            total=len(notification_sends),
        )
    else:
        return jsonify(
            status='ok',
            message='not found',
            notificationSends=[],
            total=0,
        )


@api.route('/notificationsends/<notification_send_id>', methods=['OPTIONS', 'PUT'])
@cross_origin()
@permission(Scope.write_notification_sends)
@jsonp
def update_notification_send(notification_send_id):
    if not request.json:
        raise ApiError('nothing to change', 400)

    updated = NotificationSend.update(notification_send_id, **request.json)
    if updated:
        return jsonify(status='ok', notificationSend=updated.serialize)
    else:
        raise ApiError('failed to update notificationsend', 500)


@api.route('/notificationsends/<notification_channel_id>/send', methods=['OPTIONS', 'POST'])
@cross_origin()
@permission(Scope.write_notification_sends)
@jsonp
def notification_send(notification_channel_id):
    notification_channel = NotificationChannel.find_by_id(notification_channel_id)
    data = request.json
    users = [notification['id'] for notification in data['notifications'] if notification['type'] == 'User']
    groups = [notification['id'] for notification in data['notifications'] if notification['type'] == 'Group']
    try:
        notification_rule = NotificationRule.parse({'usersEmails': users, 'groupIds': groups, 'receivers': [], 'text': data['text'], 'channelId': notification_channel_id, 'environment': plugins.config.get('DEFAULT_ENVIRONMENT')})
    except Exception as e:
        raise ApiError(str(e), 400)
    try:
        handle_test(notification_channel, notification_rule, plugins.config)
    except Exception as e:
        raise ApiError(str(e), 500)

    return jsonify(status='ok')
