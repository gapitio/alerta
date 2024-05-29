import logging
from flask import g, jsonify, request
from flask_cors import cross_origin

from alerta.app import qb
from alerta.exceptions import ApiError
from alerta.auth.decorators import permission
from alerta.models.enums import Scope
from alerta.models.notification_delay import NotificationDelay
from alerta.plugins.notification_rule import handle_delay
from alerta.utils.paging import Page
from alerta.utils.response import jsonp

from . import api

LOG = logging.getLogger('alerta/views/notification_history')


@api.route('/notificationdelay', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.read_notification_rules)
@jsonp
def get_notification_delays():
    query = qb.notification_delay.from_params(request.args, customers=g.customers)
    total = NotificationDelay.count(query)
    paging = Page.from_params(request.args, total)
    notification_delays = NotificationDelay.find_all(query, page=paging.page, page_size=paging.page_size)

    if notification_delays:
        return jsonify(
            status='ok',
            page=paging.page,
            pageSize=paging.page_size,
            pages=paging.pages,
            more=paging.has_more,
            notificationDelays=[notification.serialize for notification in notification_delays],
            total=total,
        )
    else:
        return jsonify(
            status='ok',
            notificationDelays=[],
            total=0,
        )


@api.route('/notificationdelay/fire', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.write_notification_rules)
@jsonp
def fire_notification_delays():
    notifications = NotificationDelay.find_firing()
    for notification in notifications:
        handle_delay(notification)
        notification.delete()
    return jsonify(status='ok', total=len(notifications), notifications=[notification.serialize for notification in notifications])


@api.route('/notificationdelay/<notification_delay_id>', methods=['OPTIONS', 'DELETE'])
@cross_origin()
@permission(Scope.write_notification_rules)
@jsonp
def delete_notification_delay(notification_delay_id):
    notification_delay = NotificationDelay.find_by_id(notification_delay_id)
    if not notification_delay:
        raise ApiError('not found', 404)

    if notification_delay.delete():
        return jsonify(status='ok')
    else:
        raise ApiError('failed to delete notification rule', 500)
