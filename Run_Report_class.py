class AnalysisReport:
    def __init__(self):
        self._quotes = []
        self._mentions = []

        self._quotes_sourced = 0
        self._quotes_not_sourced = 0

        self._mentions_found = 0

    # -------- getters -------
    def get_quotes_sourced(self):
        return self._quotes_sourced

    def get_quotes_not_sourced(self):
        return self._quotes_not_sourced

    def get_mentions_found(self):
        return self._mentions_found

    # --------
    def add_quote(self, comment_id_quote, sourced):
        self._quotes.append([comment_id_quote])
        if sourced:
            self._quotes_sourced += 1
        else:
            self._quotes_not_sourced += 1

    def add_mentions(self, id_found):
        self._mentions.append([id_found])
        self._mentions_found += 1
