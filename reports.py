class AnalysisReport:
    def __init__(self):
        self._quotes = []
        self._mentions = []
        self._contextuals = []

        self._quotes_sourced = 0
        self._quotes_not_sourced = 0

        self._mentions_found_total = 0
        self._mentions_found_valid = 0

        self._contextuals_found_total = 0
        self._contextuals_found_valid = 0

    # -------- getters -------
    def get_quotes_sourced(self):
        return self._quotes_sourced

    def get_quotes_not_sourced(self):
        return self._quotes_not_sourced

    def get_mentions_found_total(self):
        return self._mentions_found_total

    def get_mentions_found_valid(self):
        return self._mentions_found_valid

    # --------
    def add_quote(self, comment_id, sourced):
        self._quotes.append([comment_id])
        if sourced:
            self._quotes_sourced += 1
        else:
            self._quotes_not_sourced += 1

    def add_mentions(self, comment_id, valid):
        self._mentions.append([comment_id])
        self._mentions_found_total += 1
        if valid:
            self._mentions_found_valid += 1

    def add_contextual(self, comment_id, valid):
        self._contextuals.append([comment_id])
        self._contextuals_found_total += 1
        if valid:
            self._contextuals_found_valid += 1
