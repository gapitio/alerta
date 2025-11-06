import logging

from flask import g, jsonify, request
from flask_cors import cross_origin

from alerta.app import qb
from alerta.auth.decorators import permission
from alerta.models.enums import Scope
from alerta.models.notification_history import NotificationHistory
from alerta.utils.paging import Page
from alerta.utils.response import jsonp

from . import api

LOG = logging.getLogger('alerta/views/notification_history')


@api.route('/notificationhistory', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.read_notification_history)
@jsonp
def list_notification_history():
    query = qb.notification_history.from_params(request.args, customers=g.customers)
    total = NotificationHistory.count(query)
    paging = Page.from_params(request.args, total)
    notification_history = NotificationHistory.find_all(query, page=paging.page, page_size=paging.page_size)

    if notification_history:
        return jsonify(
            status='ok',
            page=paging.page,
            pageSize=paging.page_size,
            pages=paging.pages,
            more=paging.has_more,
            notificationHistory=[history.serialize for history in notification_history],
            total=total,
        )
    else:
        return jsonify(
            status='ok',
            notificationHistory=[],
            page=paging.page,
            pageSize=paging.page_size,
            pages=paging.pages,
            more=paging.has_more,
            total=0
        )
