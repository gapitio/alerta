import logging

from alerta.app import alarm_model
from alerta.plugins import PluginBase

LOG = logging.getLogger('alerta.plugins')


class NotesHandler(PluginBase):
    """
    Removes notes if alert goes Normal
    """

    def pre_receive(self, alert, **kwargs):
        return alert

    def post_receive(self, alert, **kwargs):
        if alert.severity == alarm_model.DEFAULT_NORMAL_SEVERITY:
            for note in alert.get_alert_notes():
                alert.delete_note(note.id)
        return

    def status_change(self, alert, status, text, **kwargs):
        return

    def take_action(self, alert, action, text, **kwargs):
        raise NotImplementedError

    def delete(self, alert, **kwargs) -> bool:
        raise NotImplementedError
