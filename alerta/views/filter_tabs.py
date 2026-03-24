import logging

from flask import current_app, g, jsonify, request
from flask_cors import cross_origin

from alerta.app import qb
from alerta.auth.decorators import permission
from alerta.exceptions import ApiError
from alerta.models.alert import Alert
from alerta.models.enums import Scope
from alerta.models.filter_tab import FilterTab
from alerta.utils.audit import write_audit_trail
from alerta.utils.response import absolute_url, jsonp

from . import api

LOGGER = logging.getLogger('alerta/views/filter_tabs')


@api.route('/filtertabs/<filter_tab_id>', methods=['OPTIONS', 'DELETE'])
@cross_origin()
@permission(Scope.admin_alerts)
@jsonp
def delete_filter_tab(filter_tab_id):
    filter_tab = FilterTab.find_by_id(filter_tab_id)

    if not filter_tab:
        raise ApiError('not found', 404)

    write_audit_trail.send(
        current_app._get_current_object(),
        event='filter_tab-deleted',
        message='',
        user=g.login,
        customers=g.customers,
        scopes=g.scopes,
        resource_id=filter_tab.name,
        type='filter_tab',
        request=request,
    )

    if filter_tab.delete():
        return jsonify(status='ok')
    else:
        raise ApiError('failed to delete filter tab', 500)


@api.route('/filtertabs', methods=['DELETE'])
@cross_origin()
@permission(Scope.admin_alerts)
@jsonp
def delete_filter_tabs():
    requested_ids = request.args.getlist('id[]', None)
    if requested_ids is None:
        raise ApiError('Missing required param id as list of ids to delete', 400)
    elif len(requested_ids) == 0:
        raise ApiError('Id list is emtpy', 400)

    deleted_ids = FilterTab.delete_all(requested_ids)

    write_audit_trail.send(
        current_app._get_current_object(),
        event='filter_tabs-deleted',
        message='',
        user=g.login,
        customers=g.customers,
        scopes=g.scopes,
        resource_id=deleted_ids,
        type='filter_tab',
        request=request,
    )

    if deleted_ids == requested_ids:
        return jsonify(status='ok')
    elif len(deleted_ids):
        return jsonify(status='warning', deleted=deleted_ids, not_found=[id for id in requested_ids if id not in deleted_ids])
    else:
        raise ApiError('failed to delete filtertabs', 500)


@api.route('/filtertab', methods=['OPTIONS', 'POST'])
@cross_origin()
@permission(Scope.admin_alerts)
@jsonp
def create_filter_tab():
    try:
        filter_tab = FilterTab.parse(request.json)
    except ValueError as e:
        LOGGER.info('Got illegal input for tabs %s', e)
        raise ApiError(str(e), 400)
    except Exception as e:
        LOGGER.error('Failed to parse filter tab with error message: %s', e)
        raise ApiError('parse of data for filter tabs failed', 500)

    try:
        filter_tab = filter_tab.create()
    except Exception as e:
        LOGGER.error('Failed to create filter tab with error message: %s', e)
        raise ApiError('create filter tab failed', 500)

    write_audit_trail.send(
        current_app._get_current_object(),
        event='filter_tab-created',
        message='',
        user=g.login,
        customers=g.customers,
        scopes=g.scopes,
        resource_id=filter_tab.name,
        type='filter_tab',
        request=request,
    )

    if filter_tab:
        return (
            jsonify(status='ok', id=filter_tab.name, filterTab=filter_tab.serialize),
            201,
            {'Location': absolute_url('/filtertabs/' + filter_tab.name)},
        )
    else:
        raise ApiError('insert filter tab failed', 500)


@api.route('/filtertabs', methods=['OPTIONS', 'POST'])
@cross_origin()
@permission(Scope.admin_alerts)
@jsonp
def create_filter_tabs():
    try:
        tabs = request.json
        filter_tabs = [FilterTab.parse(tab).serialize for tab in tabs]
    except ValueError as e:
        LOGGER.info('Got illegal input for tabs %s', e)
        raise ApiError(str(e), 400)
    except Exception as e:
        LOGGER.error('Failed to parse filter tab with error message: %s', e)
        raise ApiError('parse of data for filter tabs failed', 500)
    try:
        filter_tabs = FilterTab.create_all(filter_tabs)
    except Exception as e:
        LOGGER.error('Failed to create filter tab with error message: %s', e)
        raise ApiError('create filter tab failed', 500)

    write_audit_trail.send(
        current_app._get_current_object(),
        event='filter_tabs-created',
        message='',
        user=g.login,
        customers=g.customers,
        scopes=g.scopes,
        resource_id=[tab.name for tab in filter_tabs],
        type='filter_tab',
        request=request,
    )

    if len(filter_tabs) > 0:
        return (
            jsonify(status='ok', filterTabs=[tab.serialize for tab in filter_tabs]),
            201,
        )
    else:
        raise ApiError('unable to return inserted filter tabs', 500)


@api.route('/filtertabs/<filter_tab_id>', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.admin_alerts)
@jsonp
def filter_tab(filter_tab_id):
    filter_tab = FilterTab.find_by_id(filter_tab_id)

    if filter_tab:
        return jsonify(status='ok', total=1, filterTab=filter_tab.serialize)
    else:
        raise ApiError('not found', 404)


@api.route('/filtertabs', methods=['PUT'])
@cross_origin()
@permission(Scope.admin_alerts)
@jsonp
def update_filter_tab():
    # updates = [FilterTab.parse(update) for update in request.json]
    updates = request.json
    updated = FilterTab.update_all(updates)

    if len(updated) > 0:
        return jsonify(status='ok', total=len(updated), updated=updated)
    else:
        raise ApiError('not found', 404)


@api.route('/filtertabs/index', methods=['PUT'])
@cross_origin()
@permission(Scope.admin_alerts)
@jsonp
def update_filter_tab_index():
    # updates = [FilterTab.parse(update) for update in request.json]
    updates = request.json
    updated = FilterTab.update_indexes(updates)

    if len(updated) > 0:
        return jsonify(status='ok', total=len(updated), updated=updated)
    else:
        raise ApiError('not found', 404)


@api.route('/filtertabs', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.read_alerts)
@jsonp
def get_filter_tabs():
    filter_tabs = FilterTab.find_all()
    filters = [(filter_tab.name, filter_tab.filter_args) for filter_tab in filter_tabs]
    queries = [(name, qb.alerts.from_params(filter, customers=g.customers)) for name, filter in filters]
    history_queries = [(name, qb.history.from_params(filter, customers=g.customers)) for name, filter in filters]
    counts = {name:Alert.get_count(query) for name, query in queries}
    history_counts = {name:Alert.get_history_count(query) for name, query in history_queries}

    if filter_tabs:
        return jsonify(
            status='ok',
            filterTabs=[tab.serialize for tab in filter_tabs],
            counts=counts,
            historyCounts=history_counts,
            total=len(filter_tabs),
        )
    else:
        return jsonify(
            status='ok',
            message='not found',
            filterTabs=[],
            counts={},
            total=0,
        )


@api.route('/filtertabs/count', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.read_alerts)
@jsonp
def get_filter_tabs_counts():
    filters = [(filter_tab.name, filter_tab.filter_args) for filter_tab in FilterTab.find_all()]
    queries = [(name, qb.alerts.from_params(filter, customers=g.customers)) for name, filter in filters]
    counts = {name:Alert.get_count(query) for name, query in queries}

    if filters:
        return jsonify(
            status='ok',
            counts=counts,
            total=len(filters),
        )
    else:
        return jsonify(
            status='ok',
            message='not found',
            filterTabs=[],
            total=0,
        )
