from flask import jsonify, request
from alerta.auth.decorators import permission

from alerta.models.escalation_rule import EscalationRule
from alerta.models.notification_rule import NotificationRule, NotificationChannel, NotificationGroup
from alerta.models.on_call import OnCall
from alerta.models.blackout import Blackout
from alerta.models.user import User
from alerta.models.enums import Scope
from alerta.models.customer import Customer
from alerta.models.permission import Permission
from alerta.models.key import ApiKey

from psycopg2.errors import UniqueViolation

from alerta.utils.response import jsonp

from . import api


@api.route('/export', methods=['GET'])
@permission(Scope.admin_alerta)
def export():
    escalation_rules = [item.serialize for item in EscalationRule.find_all()]
    notification_rules = [item.serialize for item in NotificationRule.find_all()]
    notification_channels = [item.export for item in NotificationChannel.find_all()]
    blackouts = [item.serialize for item in Blackout.find_all()]
    notification_groups = [item.serialize for item in NotificationGroup.find_all()]
    on_calls = [item.serialize for item in OnCall.find_all()]
    users = [item.serialize for item in User.find_all()]
    perms = [item.serialize for item in Permission.find_all()]
    keys = [item.serialize for item in ApiKey.find_all()]
    customers = [item.serialize for item in Customer.find_all()]

    return jsonify(
        escalationRules=escalation_rules,
        notificationRules=notification_rules,
        notificationChannels=notification_channels,
        blackouts=blackouts,
        notificationGroups=notification_groups,
        onCalls=on_calls,
        users=users,
        perms=perms,
        keys=keys,
        customers=customers
    )


@api.route('/import', methods=['POST'])
@permission(Scope.admin_alerta)
@jsonp
def import_all():
    data = request.json
    users = []
    for user in data.get('users'):
        try:
            users.append(User.parse(user).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    perms = []
    for perm in data.get('perms'):
        try:
            perms.append(Permission.parse(perm).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    keys = []
    for key in data.get('keys'):
        try:
            keys.append(ApiKey._import(key).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    customers = []
    for customer in data.get('customers'):
        try:
            customers.append(Customer.parse(customer).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    blackouts = []
    for blackout in data.get('blackouts'):
        try:
            blackouts.append(Blackout.parse(blackout).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    escalation_rules = []
    for rule in data.get('escalationRules'):
        try:
            escalation_rules.append(EscalationRule.parse(rule).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    notification_channels = []
    for channel in data.get('notificationChannels'):
        try:
            notification_channels.append(NotificationChannel._import(channel).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    notification_groups = []
    for group in data.get('notificationGroups'):
        try:
            notification_groups.append(NotificationGroup.parse(group).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    on_calls = []
    for on_call in data.get('onCalls'):
        try:
            on_calls.append(OnCall.parse(on_call).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    notification_rules = []
    for rule in data.get('notificationRules'):
        try:
            notification_rules.append(NotificationRule.parse(rule).create())
        except UniqueViolation as pg_error:
            pg_error.cursor.connection.rollback()
            continue

    return jsonify(
        escalationRules=[item.serialize for item in escalation_rules],
        notificationRules=[item.serialize for item in notification_rules],
        notificationChannels=[item.serialize for item in notification_channels],
        blackouts=[item.serialize for item in blackouts],
        notificationGroups=[item.serialize for item in notification_groups],
        onCalls=[item.serialize for item in on_calls],
        users=[item.serialize for item in users],
        perms=[item.serialize for item in perms],
        keys=[item.serialize for item in keys],
        customers=[item.serialize for item in customers]
    )
