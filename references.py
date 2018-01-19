import pandas as pd


class Reference:
    def __init__(self, commenter, addressee, comment_id, parent_thread, project_stats, timestamp, thread_type):
        self.commenter = commenter
        self.addressee = addressee
        self.comment_id = int(comment_id)
        self.timestamp = timestamp
        self.thread_type = thread_type
        self._parent_thread = parent_thread
        self._project_stats = project_stats

        self._valid = self._validate()
        self._add_to_report()

        self._start_pos = None

    def set_start_pos(self, start_pos):
        self._start_pos = start_pos

    def get_start_pos(self):
        return self._start_pos

    def get_info_as_dict(self):
        return ({"commenter": self.commenter,
                 "addressee": self.addressee,
                 "comment_id": self.comment_id,
                 "ref_type": type(self).__name__,
                 "timestamp": self.timestamp,
                 "thread_type": self.thread_type})

    def _validate(self):
        if self.addressee != self.commenter and \
                self._parent_thread.is_participant(self.addressee) or \
                self._parent_thread.parent_project.is_participant(self.addressee):
                return True
        else:
            return False

    def is_valid(self):
        return self._valid

    def _add_to_report(self):
        pass


class Mention(Reference):

    def _add_to_report(self):
        self._project_stats.add_mentions(self.comment_id, self._valid, self.thread_type)


class Quote(Reference):

    def _add_to_report(self):
        self._project_stats.add_quote(self.comment_id, self._valid, self.thread_type)


class ContextualReply(Reference):

    def _add_to_report(self):
        self._project_stats.add_contextual(self.comment_id, self._valid, self.thread_type)