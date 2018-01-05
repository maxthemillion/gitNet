import pandas as pd
import warnings
from references import Mention, Quote, ContextualReply


class Thread:
    """contains a pull request communication thread and its thread analytics"""
    def __init__(self, thread_data, thread_type, project_stats, parent_project):

        self._thread_data = thread_data.sort_values(by="id", axis="rows", ascending=True)
        # assert that only one pull request id appears in each thread
        assert len(thread_data["thread_id"].unique()) is 1  # TODO: move to testing file
        self.no_comments = len(self._thread_data)

        self.owner = "fooOwner"

        self._type = thread_type
        self.parent_project = parent_project

        self._participants = (self._thread_data["user"].str.lower()).unique()

        self._references_strict = None
        self._references_relaxed = None

        self._analysis_performed = False  # TODO: remove, if not needed

        self._project_stats = project_stats
        self._project_stats.add_thread()
        self._project_stats.add_comments(len(self._thread_data))

    def run(self):
        self._references_relaxed = self._find_references_relaxed()
        self._analysis_performed = True

    # -------- getters --------
    def get_references(self, which):

        if which == "strict":
            result = self._references_strict
        elif which == "relaxed":
            result = self._references_relaxed
        else:
            raise ValueError("Parameter 'which' not set correctly \n"
                             "Specify, which references ('relaxed' or 'strict') should"
                             "be returned!")
        return result

    def get_references_as_list(self):
        # TODO: get the info as plain dict or list and then create the DF in one go to speed up performance.
        ref_list = []
        if self._references_relaxed:
            for reference in self._references_relaxed:
                ref_dict = reference.get_info_as_dict()
                if ref_list:
                    ref_list.append(ref_dict)
                else:
                    ref_list = [ref_dict]

        return ref_list

    def get_participants(self):
        return self._participants

    # -------- is ---------
    def is_participant(self, name):
        return name in self._participants

    @staticmethod
    def _is_quote(s, i):
        if i >= 2 and s[i - 2:i] == "\r\n":
            return True
        elif i >= 1 and s[i - 1:i] == ">":
            return Thread._is_quote(s, i-1)
        elif i == 0:
            return True
        else:
            return False

    @staticmethod
    def _clear_markdown_close(md_list):
        md_list.sort()
        cleared_md_list = []
        distance = 1

        if not md_list:
            return []
        elif len(md_list) == 1:
            cleared_md_list = md_list
        else:
            for i in range(1, len(md_list)):
                if md_list[i] - md_list[i-1] > distance:
                    if cleared_md_list:
                        cleared_md_list.append(md_list[i-1])
                    else:
                        cleared_md_list = [md_list[i-1]]

            cleared_md_list.append(md_list[len(md_list)-1])
        return cleared_md_list

    def _detect_mentions_in_row(self, row, index):
        mentions_list = []

        body = row["body"]
        commenter = row["user"]
        comment_id = row["id"]
        timestamp = row["created_at"]

        start_pos_list = self._find_all(body, "@")
        for start_pos in start_pos_list:
            stop_pos = Thread._find_end_username(body, start_pos)
            addressee = str.lower(body[start_pos + 1:stop_pos])

            mention = Mention(commenter, addressee, comment_id, self, self._project_stats, timestamp, index)
            mention.set_start_pos(start_pos)

            if mentions_list:
                mentions_list.append(mention)
            else:
                mentions_list = [mention]

        return mentions_list

    def _detect_quotes_in_row(self, row, index):
        # TODO: source can't be found if quote was altered slightly (spelling corrected, etc.)
        quote_list = []

        body = row["body"]
        commenter = row["user"]
        comment_id = row["id"]
        timestamp = row["created_at"]

        # filter '>' that define quotes
        close_temp = []
        markdown_close = self._find_all(body, ">")

        # remove > that follow each other too closely
        markdown_close = self._clear_markdown_close(markdown_close)

        for item in markdown_close:
            if self._is_quote(body, item):
                close_temp.append(item)

        start_pos_list = close_temp
        for start_pos in start_pos_list:
            stop_pos = Thread._find_end_quote(body, start_pos)
            # don't consider the first 5 values
            quote_body = body[start_pos + 5:stop_pos]

            addressee = self._find_source(quote_body, index)

            new_quote = Quote(commenter, addressee, comment_id, self, self._project_stats, timestamp, index)
            new_quote.set_start_pos(start_pos)

            if quote_list:
                quote_list.append(new_quote)
            else:
                quote_list = [new_quote]

        return quote_list

    def _detect_contextuals(self, mentions, m_indices, quotes, q_indices):

        contextuals_list = []
        comment_sequence = self._thread_data["user"]
        m_i = 0
        q_i = 0

        for r in range(0, len(comment_sequence)):

            new_contextual = None

            u_prev = self.owner
            if r > 0:
                u_prev = comment_sequence[r-1]

            u_current = comment_sequence[r]

            comment_id = self._thread_data["id"].iloc[r]
            timestamp = self._thread_data["created_at"].iloc[r]

            if len(comment_sequence[0:r].unique()) > 2:
                if m_indices[r] > 0 or q_indices[r] > 0:

                    create = True

                    i = 0
                    while create and i < m_indices[r]:
                        if mentions[m_i + i].get_start_pos() == 0:
                            create = False
                        i += 1

                    m_i += m_indices[r]

                    i = 0
                    while create and i < q_indices[r]:
                        if quotes[q_i + i].get_start_pos() == 0:
                            create = False
                        i += 1

                    q_i += q_indices[r]

                    if create:
                        new_contextual = ContextualReply(u_current, u_prev, comment_id, self, self._project_stats,
                                                         timestamp)

                else:
                    new_contextual = ContextualReply(u_current, u_prev, comment_id, self, self._project_stats, timestamp)
            else:
                if u_current == u_prev:
                    pass
                else:
                    new_contextual = ContextualReply(u_current, u_prev, comment_id, self, self._project_stats, timestamp)

            if contextuals_list and new_contextual is not None:
                contextuals_list.append(new_contextual)
            elif new_contextual is not None:
                contextuals_list = [new_contextual]

        return contextuals_list

    @staticmethod
    def _remove_invalid_references(reference_list):
        ind_invalid = []
        for i in range(0, len(reference_list)):
            if not reference_list[i].is_valid():
                if ind_invalid:
                    ind_invalid.append(i)
                else:
                    ind_invalid = [i]

        for i in sorted(ind_invalid, reverse=True):
            del reference_list[i]
        return reference_list

    @staticmethod
    def _consolidate_references(mentions, quotes, contextuals):
        # TODO: implement proper consolidation
        reference_list = mentions + quotes + contextuals

        return reference_list

    def _find_references_relaxed(self):
        """finds references in the thread according to the relaxed rule set"""
        all_mentions = []
        all_quotes = []

        mentions_indices = []
        quotes_indices = []

        for index in range(0, len(self._thread_data)):
            row = self._thread_data.iloc[index]

            mentions = self._detect_mentions_in_row(row, index)
            mentions = self._remove_invalid_references(mentions)

            quotes = self._detect_quotes_in_row(row, index)
            quotes = self._remove_invalid_references(quotes)

            if mentions and all_mentions:
                all_mentions.extend(mentions)
            elif mentions:
                all_mentions = mentions

            if mentions_indices:
                mentions_indices.append(len(mentions))
            else:
                mentions_indices = [len(mentions)]

            if quotes and all_quotes:
                all_quotes.extend(quotes)
            elif quotes:
                all_quotes = quotes

            if quotes_indices:
                quotes_indices.append(len(quotes))
            else:
                quotes_indices = [len(quotes)]

        all_contextuals = self._detect_contextuals(all_mentions, mentions_indices, all_quotes, quotes_indices)
        all_contextuals = self._remove_invalid_references(all_contextuals)

        ref_relaxed = self._consolidate_references(all_mentions, all_quotes, all_contextuals)

        return ref_relaxed

    def _find_references_strict(self):
        # TODO: implement strict rule set
        pass

    def _find_source(self, quote, stop_row):
        i = 0
        while stop_row > i:
            if self._thread_data["body"].iloc[i].find(quote) > -1:
                return self._thread_data["user"].iloc[i].lower()
            i = i + 1

        return None

    # --------- static string searches -------
    @staticmethod
    def _find_all(s, ch):
        """find all occurrences of ch in s"""
        return [i for i, ltr in enumerate(s) if ltr == ch]

    @staticmethod
    def _find_end_username(s, start_pos):
        """find the position in s where the username ends. a username ends with one of the
        characters in char_list or with the comment end"""
        char_list = {' ', "'", '.', '@', '`', ',', "!", "?",
                      "(", ")", "{", "}", "[", "]", "/",
                      "\\", "\"", "\n", "\t", "\r"}
        ind = next((i for i, ch in enumerate(s) if ch in char_list and i > start_pos + 1), len(s))
        return ind

    @staticmethod
    def _find_end_quote(quote, start_pos):
        """find the end of a quote. quotes end with \r\n or with the comment end.
        long quotes are cut short to speed up the finding process"""
        characters = {"\r"}
        ind = next((i for i, ch in enumerate(quote) if ch in characters and i > start_pos + 1), len(quote))
        cut_val = 60
        if ind - start_pos > cut_val:
            ind = start_pos + cut_val
        return ind