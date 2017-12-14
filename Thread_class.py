import pandas as pd
import warnings
from Run_Report_class import AnalysisReport
from Mention import Mention


class Thread:
    """contains a pull request communication thread and its thread analytics"""
    # TODO: change this class so that it can handle all types of threads
    def __init__(self, thread_data, thread_type, parent_project):

        self._thread_data = thread_data.sort_values(by="id", axis="rows", ascending=True)
        # assert that only one pull request id appears in each thread
        assert len(thread_data["thread_id"].unique()) is 1

        self._type = thread_type
        self._parent_project = parent_project

        self._participants = (self._thread_data["user"].str.lower()).unique()

        self._references_strict = None
        self._references_relaxed = None

        self._analysis_performed = False

        self.report = AnalysisReport()

    # -------- getters --------
    def get_references(self, which):
        if not self._analysis_performed:
            warnings.warn("get_references() called, but analysis has not been performed, yet! \n")

        if which == "strict":
            result = self._references_strict
        elif which == "relaxed":
            result = self._references_relaxed
        else:
            raise ValueError("Parameter 'which' not set correctly \n"
                             "Specify, which references ('relaxed' or 'strict') should"
                             "be returned!")
        return result

    def get_participants(self):
        return self._participants

    # -------- is ---------
    def is_participant(self, name):
        return name in self._participants

    # -------- thread analysis --------
    def analyze_references(self):
        """can be called to start the analysis process"""
        #mentions_strict = Thread._recognize_references_strict()
        ref_relaxed = self._recognize_references_relaxed()

        #self._references_strict = mentions_strict
        self._references_relaxed = ref_relaxed

        self._analysis_performed = True

    def _get_mentions_from_row(self, row):
        mentions_list = []

        body = row["body"]
        commenter = row["user"]

        start_pos_list = self._find_all(body, "@")
        # if start_pos_list is not None:
        for start_pos in start_pos_list:
            stop_pos = Thread._find_end_username(body, start_pos)
            addressee = str.lower(body[start_pos + 1:stop_pos])
            mention = Mention(commenter, addressee, comment_id)
            if mentions_list:
                mentions_list.append(mention)
            else:
                mentions_list = [mention]

        return mentions_list

    def _get_quotes_from_row(self, row):
        pass

    def _test_append_mention(self, mention):
        if addressee != commenter and \
                self.is_participant(addressee) or self._parent_project.is_participant(addressee):
            ref_relaxed = ref_relaxed.append(pd.DataFrame(reference))
            self.report.add_mentions(comment_id)

    def _test_append_quote(self, quote):
        pass

    def _recognize_references_relaxed(self):
        """finds references in the thread according to the relaxed rule set"""
        # TODO: test this rule set by feeding sample data to it.

        ref_relaxed = pd.DataFrame(columns=["commenter", "addressee", "comment_id", "ref_type"])

        for index in range(0, len(self._thread_data)):
            row = self._thread_data.iloc[index]




            ref_type = "quote"
            # filter '>' that define quotes
            close_temp = []
            markdown_close = self._find_all(body, ">")

            for item in markdown_close:
                if item == 0:
                    close_temp.append(item)
                elif body[item-2:item] == "\r\n":
                    close_temp.append(item)

            start_pos_list = close_temp

            for start_pos in start_pos_list:
                stop_pos = Thread._find_end_quote(body, start_pos)
                # don't consider the first 5 values to get rid of disturbing effects.
                quote = body[start_pos + 5:stop_pos]
                author = self._find_source(quote, index)
                # assert author is not None
                # TODO: author can't be found if quote was altered
                # -> assert removed, log None-occurences instead
                row = [{'commenter': commenter,
                        'addressee': author,
                        'comment_id': comment_id,
                        'ref_type': ref_type}]
                if author is not None and author != commenter and \
                        self.is_participant(author) or self._parent_project.is_participant(author):

                        ref_relaxed = ref_relaxed.append(pd.DataFrame(row))
                        self.report.add_quote(comment_id, True)
                elif author is None:
                    self.report.add_quote(comment_id, False)
        # TODO: consolidate references
        return ref_relaxed

    def _recognize_references_strict(self):
        # TODO: implement strict rule set
        pass

    def _find_source(self, quote, stop_row):
        i = 0
        while stop_row > i:
            if self._thread_data["body"].iloc[i].find(quote) > -1:
                return self._thread_data["user"].iloc[i]
            i = i + 1

        # print("source not found for the following quote:")
        # print(quote)

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
                      "\\", "\"", "\n", "\t"}
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