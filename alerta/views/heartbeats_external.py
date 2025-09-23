import requests
from flask import current_app
from flask_cors import cross_origin
from urllib3 import disable_warnings, exceptions

from alerta.auth.decorators import permission
from alerta.exceptions import ApiError
from alerta.models.enums import Scope
from alerta.utils.response import jsonp

from . import api

disable_warnings(exceptions.InsecureRequestWarning)


@api.route('/heartbeat/<heartbeat_id>', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.read_heartbeats)
@jsonp
def get_heartbeat(heartbeat_id):
    verify = current_app.config['HEARTBEAT_VERIFY']
    res = requests.get(
        f'{current_app.config["HEARTBEAT_URL"]}/heartbeat/{heartbeat_id}',
        headers={'Authorization': f'Key {current_app.config["HEARTBEAT_KEY"]}'},
        verify=verify if verify.lower() != 'false' else False
    )

    return res.text


@api.route('/heartbeats', methods=['OPTIONS', 'GET'])
@cross_origin()
@permission(Scope.read_heartbeats)
@jsonp
def list_heartbeats():
    verify = current_app.config['HEARTBEAT_VERIFY']
    res = requests.get(
        f'{current_app.config["HEARTBEAT_URL"]}/heartbeats',
        headers={'Authorization': f'Key {current_app.config["HEARTBEAT_KEY"]}'},
        verify=verify if verify.lower() != 'false' else False
    )
    if res.status_code != 200:
        current_app.logger.error(f'failed to get heartbeat with status code: {res.status_code}')
        raise ApiError('failed to get heartbeat', 500)
    return res.text


@api.route('/heartbeat/<heartbeat_id>', methods=['OPTIONS', 'DELETE'])
@cross_origin()
@permission(Scope.write_heartbeats)
@jsonp
def delete_heartbeat(heartbeat_id):
    verify = current_app.config['HEARTBEAT_VERIFY']
    res = requests.delete(
        f'{current_app.config["HEARTBEAT_URL"]}/heartbeat/{heartbeat_id}',
        headers={'Authorization': f'Key {current_app.config["HEARTBEAT_KEY"]}'},
        verify=verify if verify.lower() != 'false' else False
    )
    if res.status_code != 200:
        current_app.logger.error(f'failed to delete heartbeat with status code: {res.status_code}')
        raise ApiError('failed to delete heartbeat', 500)
    return res.text
