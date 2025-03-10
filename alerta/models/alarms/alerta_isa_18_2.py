"""
Alarm states and transition paths.

See ANSI/ISA 18.2 Management of Alarm Systems for the Process Industries
https://www.isa.org/store/ansi/isa-182-2016/46962105

"""


from flask import current_app

from alerta.exceptions import InvalidAction
from alerta.models.alarms import AlarmModel
from alerta.models.enums import Severity, Status

SEVERITY_MAP = {
    Severity.Security: 10,
    Severity.Critical: 9,
    Severity.Major: 8,
    Severity.Minor: 7,
    Severity.Warning: 6,
    Severity.Indeterminate: 5,
    Severity.Informational: 4,
    Severity.Normal: 3,
    Severity.Ok: 3,
    Severity.Cleared: 3,
    Severity.Debug: 2,
    Severity.Trace: 1,
    Severity.Unknown: 0
}
DEFAULT_NORMAL_SEVERITY = Severity.Normal
DEFAULT_INFORM_SEVERITY = Severity.Informational
DEFAULT_PREVIOUS_SEVERITY = Severity.Normal

COLOR_MAP = {
    'severity': {
        Severity.Security: 'blue',
        Severity.Critical: 'red',
        Severity.Major: 'orange',
        Severity.Minor: 'yellow',
        Severity.Warning: 'dodgerblue',
        Severity.Indeterminate: 'lightblue',
        Severity.Cleared: '#00CC00',  # lime green
        Severity.Normal: '#00CC00',
        Severity.Ok: '#00CC00',
        Severity.Informational: '#00CC00',
        Severity.Debug: '#9D006D',  # purple
        Severity.Trace: '#7554BF',  # violet
        Severity.Unknown: 'silver'
    },
    'status': {
        Status.Ack: 'skyblue',
        Status.Unack: 'skyblue'
    },
    'text': 'black'
}

F_DSUPR = 'DSUPR'
G_OOSRV = 'OOSRV'

STATUS_MAP = {
    Status.Closed: 'A',
    Status.Open: 'B',
    Status.Ack: 'C',
    Status.Unack: 'D',
    Status.Shelved: 'E',
    F_DSUPR: 'F',
    G_OOSRV: 'G'
}

MORE_SEVERE = 'moreSevere'
NO_CHANGE = 'noChange'
LESS_SEVERE = 'lessSevere'

ACTION_ACK = 'ack'
ACTION_UNACK = 'unack'
ACTION_SHELVE = 'shelve'
ACTION_UNSHELVE = 'unshelve'
ACTION_OPEN = 'open'


class StateMachine(AlarmModel):

    def register(self, app):
        from alerta.management.views import __version__
        self.name = f'alerta {__version__} ISA 18.2'

        StateMachine.Severity = app.config['SEVERITY_MAP'] or SEVERITY_MAP
        StateMachine.Colors = app.config['COLOR_MAP'] or COLOR_MAP
        StateMachine.Status = STATUS_MAP

        StateMachine.DEFAULT_STATUS = Status.Closed
        StateMachine.DEFAULT_NORMAL_SEVERITY = app.config['DEFAULT_NORMAL_SEVERITY'] or DEFAULT_NORMAL_SEVERITY
        StateMachine.DEFAULT_INFORM_SEVERITY = app.config['DEFAULT_INFORM_SEVERITY'] or DEFAULT_INFORM_SEVERITY
        StateMachine.DEFAULT_PREVIOUS_SEVERITY = app.config['DEFAULT_PREVIOUS_SEVERITY'] or DEFAULT_PREVIOUS_SEVERITY

    def trend(self, previous, current):
        valid_severities = sorted(StateMachine.Severity, key=StateMachine.Severity.get)
        assert previous in StateMachine.Severity, f"Severity is not one of {', '.join(valid_severities)}"
        assert current in StateMachine.Severity, f"Severity is not one of {', '.join(valid_severities)}"

        if StateMachine.Severity[previous] < StateMachine.Severity[current]:
            return MORE_SEVERE
        elif StateMachine.Severity[previous] > StateMachine.Severity[current]:
            return LESS_SEVERE
        else:
            return NO_CHANGE

    def transition(self, alert, current_status=None, previous_status=None, action=None, **kwargs):
        state = current_status or StateMachine.DEFAULT_STATUS

        current_severity = alert.severity
        previous_severity = alert.previous_severity or StateMachine.DEFAULT_PREVIOUS_SEVERITY

        def next_state(rule, severity, status):
            current_app.logger.info(
                'State Transition: Rule {}: STATE={} => SEVERITY={}, STATUS={}'.format(
                    rule,
                    state,
                    severity,
                    status
                )
            )
            return severity, status

        if not action and alert.status != StateMachine.DEFAULT_STATUS:
            statuses = StateMachine.Status.keys()
            assert alert.status in statuses, f"Status is not one of {', '.join(statuses)}"
            return next_state('External State Change, Any (*) -> Any (*)', current_severity, alert.status)

        if action == ACTION_SHELVE:
            return next_state('Operator Shelve, Any (*) -> Shelve (E)', current_severity, Status.Shelved)

        if action == ACTION_UNSHELVE:
            if current_severity == StateMachine.DEFAULT_NORMAL_SEVERITY:
                return next_state('Operator Unshelve, Shelve (E) -> Normal (A)', current_severity, Status.Closed)
            else:
                return next_state('Operator Unshelve, Shelve (E) -> Unack (B)', current_severity, Status.Open)

        if action == ACTION_OPEN:
            if state == Status.Open:
                raise InvalidAction(f'alert is already in {state} status')
            if state == Status.Closed:
                return next_state('Operator Open, Any(*) to Open', current_severity, Status.Unack)

            return next_state('Operator Open, Any(*) to Open', current_severity, Status.Open)

        if action == ACTION_ACK:
            if state == Status.Open:
                return next_state('Operator Ack, Unack (B) -> Ack (C)', current_severity, Status.Ack)
            if state == Status.Unack:
                return next_state(' Operator Ack, RTN Unack (D) -> Normal (A)', current_severity, Status.Closed)

        if action == ACTION_UNACK:
            if state == Status.Ack:
                return next_state('Operator Unack, Ack (C) -> Unack (B)', current_severity, Status.Open)

        if state == Status.Unack:
            if current_severity != StateMachine.DEFAULT_NORMAL_SEVERITY:
                return next_state('Alarm Occurs, Unack (A) -> Open (B)', current_severity, Status.Open)

        if state == Status.Closed:
            if current_severity != StateMachine.DEFAULT_NORMAL_SEVERITY:
                return next_state('Alarm Occurs, Normal (A) -> Unack (B)', current_severity, Status.Open)

        if state == Status.Ack:
            if current_severity == StateMachine.DEFAULT_NORMAL_SEVERITY:
                return next_state('Ack (C) -> Normal', current_severity, Status.Closed)
            if self.trend(previous_severity, current_severity) == MORE_SEVERE:
                if previous_severity != StateMachine.DEFAULT_PREVIOUS_SEVERITY:
                    return next_state('Re-Alarm, Ack (C) -> Unack (B)', current_severity, Status.Open)
        if state == Status.Open:
            if current_severity == StateMachine.DEFAULT_NORMAL_SEVERITY:
                return next_state('Open -> Unack', current_severity, Status.Unack)

        # Return from Suppressed-by-design (F) -> Normal (A) or Unack (B)
        if state == F_DSUPR:
            if current_severity == StateMachine.DEFAULT_NORMAL_SEVERITY:
                return next_state('Return from Suppressed-by-design, Suppressed-by-design (G) -> Normal (A)', current_severity, Status.Closed)
            else:
                return next_state('Return from Suppressed-by-design, Suppressed-by-design (G) -> Unack (B)', current_severity, Status.Open)

        # Return from Out-of-service (G) -> Normal (A) or Unack (B)
        if state == G_OOSRV:
            if current_severity == StateMachine.DEFAULT_NORMAL_SEVERITY:
                return next_state('Return from Out-of-service, Out-of-service (G) -> Normal (A)', current_severity, Status.Closed)
            else:
                return next_state('Return from Out-of-service, Out-of-service (G) -> Unack (B)', current_severity, Status.Open)

        return next_state('NOOP', current_severity, state)

    @staticmethod
    def is_suppressed(alert):
        return alert.status in [F_DSUPR, G_OOSRV]
