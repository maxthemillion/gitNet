import pandas as pd
import warnings

#sample comment

class ThreadAnalyzer:
    """Runs the analytics on threads"""

    @staticmethod
    def _first_index(s, start_pos):
        characters = {' ', "'", '.', '@', '`', ',', "!", "?",
                      "(", ")", "{", "}", "[", "]", "/",
                      "\\", "\"", "\n", "\t"}
        ind = next((i for i, ch in enumerate(s) if ch in characters and i > start_pos), None)
        if ind is not None:
            return ind
        raise ValueError

    @staticmethod
    def _yield_participants(user):
        participants = (user.str.lower()).unique()
        return participants

    @staticmethod
    def _recognize_mentions(data, participants):
        ref_type = "mentions"
        references = pd.DataFrame()
        for index, row in data.iterrows():
            # mentions-references
            # TODO: improve mentions-recognition
            # mentions are not case sensitive
            # mentions can theoretically be used wrongly as @ username, where a space follows @
            # there can be more than one @username reference per comment.
            # TODO: test this algorithm by feeding sample data to it.
            commenter = str.lower(row["user"])
            body = row["body"]
            comment_id = row["id"]

            start_pos = body.find("@") + 1
            if start_pos > 0:
                try:
                    stop_pos = ThreadAnalyzer._first_index(body, start_pos)
                    addressee = str.lower(body[start_pos:stop_pos])
                    row = [{'commenter': commenter,
                            'addressee': addressee,
                            'comment_id': comment_id,
                            'ref_type': ref_type}]

                    # check validity of identified username
                    # add a reference, if the username is at least 2 characters
                    # long and if it occurs in the thread's participants list
                    # furthermore, commenter and addressee cannot be the same
                    if len(addressee) > 1 and \
                            addressee in participants and \
                            addressee != commenter:
                        references = references.append(pd.DataFrame(row))
                except ValueError:
                    message = "an ending character to @username has not been found for comment " + str(comment_id)
                    warnings.warn(message=message)
        return references

    @staticmethod
    def _recognize_quotes():
        # TODO: implement quote-references linkage here
        # ref_type = "quote"
        warnings.warn("recognize quotes functionality has not been implemented, yet!")

    @staticmethod
    def run_analysis(data):
        # TODO: assert data validity
        participants = ThreadAnalyzer._yield_participants(data["user"])
        references = ThreadAnalyzer._recognize_mentions(data, participants)
        # references = references.append(ThreadAnalyzer.recognize_quotes())
        return references, participants
