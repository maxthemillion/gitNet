class Reference:

    def __init__(self,
                 commenter_id: int,
                 addressee,
                 comment_id: int,
                 parent_thread,
                 project_stats,
                 thread_type: str):
        """
        :param commenter_id:            commenter id
        :param addressee:                adressee login or id
        :param comment_id:
        :param parent_thread:
        :param project_stats:           Project stats object
        :param thread_type:             'pullreq', 'issue' or 'commit'
        """
        self.commenter_id = commenter_id
        self.comment_id = int(comment_id)
        self.thread_type = thread_type
        self._parent_thread = parent_thread
        self._project_stats = project_stats
        self.addressee_id = self._convert_login_to_id(addressee)

        self._valid = self._validate()
        self._add_to_report()

        self._start_pos = None

    def is_valid(self):
        return self._valid

    def set_start_pos(self, start_pos):
        self._start_pos = start_pos

    def get_start_pos(self):
        return self._start_pos

    def get_info_as_dict(self):
        """
        :return:          info required for import to neo4j
        """
        return ({"addressee": self.addressee_id,
                 "comment_id": self.comment_id,
                 "ref_type": type(self).__name__,
                 "thread_type": self.thread_type})

    def _convert_login_to_id(self, actor):
        if type(actor) is str:
            actor = self._parent_thread.parent_project.get_actor_id(actor)
        return actor

    def _validate(self):
        """
        Check if the reference is valid.
        It is valid, if...
        ... addressee and commenter differ
        ... addressee participates in (one of) the repository's threads.

        Invalid references will be removed.

        :return:        true or false
        """
        if self.addressee_id is None or self.commenter_id is None:
            return False

        elif self.addressee_id != self.commenter_id and \
                self._parent_thread.is_participant(self.addressee_id) or \
                self._parent_thread.parent_project.is_participant(self.addressee_id):
                return True
        else:
            return False

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