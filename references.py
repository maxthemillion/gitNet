import pandas as pd


class Reference:
    def __init__(self, commenter, addressee, comment_id, parent_thread, project_stats, timestamp):
        self.commenter = commenter
        self.addressee = addressee
        self.comment_id = int(comment_id)
        self.timestamp = timestamp
        self._parent_thread = parent_thread
        self._project_stats = project_stats

        self._valid = self._validate()
        self._add_to_report()

    def get_info_as_series(self):
        return pd.Series({"commenter": self.commenter,
                          "addressee": self.addressee,
                          "comment_id": self.comment_id,
                          "ref_type": type(self).__name__,
                          "timestamp": self.timestamp})

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


# TODO: Don't pass the parent_thread but the report object
class DirectReply(Reference):
    def __init__(self, commenter, addressee, comment_id, parent_thread, project_stats, timestamp, row_index):
        Reference.__init__(self, commenter, addressee, comment_id, parent_thread, project_stats, timestamp)
        self._start_pos = row_index


class Mention(DirectReply):

    def _add_to_report(self):
        self._project_stats.add_mentions(self.comment_id, self._valid)


class Quote(DirectReply):

    def _add_to_report(self):
        self._project_stats.add_quote(self.comment_id, self._valid)


class ContextualReply(Reference):

    def _add_to_report(self):
        self._project_stats.add_contextual(self.comment_id, self._valid)