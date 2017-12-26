import pandas as pd
import warnings
from reports import AnalysisReport
from references import Mention, Quote


class Thread:
    """contains a pull request communication thread and its thread analytics"""
    def __init__(self, thread_data, thread_type, parent_project):

        self._thread_data = thread_data.sort_values(by="id", axis="rows", ascending=True)
        # assert that only one pull request id appears in each thread
        assert len(thread_data["thread_id"].unique()) is 1
        self.no_comments = len(self._thread_data)

        self._type = thread_type
        self.parent_project = parent_project

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

    def get_references_as_df(self):
        df = pd.DataFrame()
        if self._references_relaxed:
            for reference in self._references_relaxed:
                df = df.append(reference.get_info_as_series(), ignore_index=True)
        return df

    def get_participants(self):
        return self._participants

    # -------- is ---------
    def is_participant(self, name):
        return name in self._participants

    # -------- thread analysis --------
    def analyze_references(self):
        """can be called to start the analysis process"""
        #mentions_strict = Thread._recognize_references_strict()
        self._references_relaxed = self._find_references_relaxed()

        self._analysis_performed = True

    def _detect_mentions_in_row(self, row):
        mentions_list = []

        body = row["body"]
        commenter = row["user"]
        comment_id = row["id"]

        start_pos_list = self._find_all(body, "@")
        for start_pos in start_pos_list:
            stop_pos = Thread._find_end_username(body, start_pos)
            addressee = str.lower(body[start_pos + 1:stop_pos])
            mention = Mention(commenter, addressee, comment_id, self)
            if mentions_list:
                mentions_list.append(mention)
            else:
                mentions_list = [mention]

        return mentions_list

    def _detect_quotes_in_row(self, row, index):
        # TODO: author can't be found if quote was altered

        quote_list = []

        body = row["body"]
        commenter = row["user"]
        comment_id = row["id"]

        # filter '>' that define quotes
        close_temp = []
        markdown_close = self._find_all(body, ">")

        for item in markdown_close:
            if item == 0:
                close_temp.append(item)
            elif body[item - 2:item] == "\r\n":
                close_temp.append(item)

        start_pos_list = close_temp
        for start_pos in start_pos_list:
            stop_pos = Thread._find_end_quote(body, start_pos)
            # don't consider the first 5 values
            quote_body = body[start_pos + 5:stop_pos]

            addressee = self._find_source(quote_body, index)
            new_quote = Quote(commenter, addressee, comment_id, self)
            if quote_list:
                quote_list.append(new_quote)
            else:
                quote_list = [new_quote]

        return quote_list

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

    def _find_references_relaxed(self):
        """finds references in the thread according to the relaxed rule set"""
        ref_relaxed = []
        for index in range(0, len(self._thread_data)):
            row = self._thread_data.iloc[index]

            mentions_list = self._detect_mentions_in_row(row)
            mentions_list = self._remove_invalid_references(mentions_list)

            quote_list = self._detect_quotes_in_row(row, index)
            quote_list = self._remove_invalid_references(quote_list)

            if ref_relaxed:
                ref_relaxed.extend((mentions_list + quote_list))
            else:
                ref_relaxed = mentions_list + quote_list

        # TODO: consolidate references
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