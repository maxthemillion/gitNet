import pandas as pd

class Reference:
    def __init__(self, commenter, addressee, comment_id, parent_thread):
        self.commenter = commenter
        self.addressee = addressee
        self.comment_id = int(comment_id)
        self._parent_thread = parent_thread
        self._valid = self._validate()
        self._add_to_report()

    def get_info_as_series(self):
        return pd.Series({"commenter": self.commenter,
                          "addressee": self.addressee,
                          "comment_id": self.comment_id,
                          "ref_type": str(type(self))})

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
        self._parent_thread.report.add_mentions(self.comment_id)


class Quote(Reference):
    def _add_to_report(self):
        self._parent_thread.report.add_quote(self.comment_id, self._valid)
