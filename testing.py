import unittest
from project import Project
from references import Mention, Quote
import pandas as pd
from threads import Thread


class ThreadTestCase(unittest.TestCase):
    def setUp(self):
        test_issue_data = pd.DataFrame(pd.read_json("TestData/test_issue_data.json"))
        test_pullreq_data = pd.DataFrame(pd.read_json("TestData/test_pullreq_data.json"))

        self.new_project = Project(test_pullreq_data, test_issue_data)
        self.thread_list = self.new_project._split_threads(self.new_project._issue_data, "issue")

        self.thread_with_quotes = self.thread_list[1]

        user1 = self.thread_with_quotes.get_participants()[0]
        user2 = self.thread_with_quotes.get_participants()[1]

        self.fake_row_1 = pd.Series({"body": "this is a sample comment without quote!",
                                     "user": user1,
                                     "id": 9999})

    # TODO: write test case for quote detection
    def test_quote_detection(self):
        # self.failIf(Thread._detect_mentions_in_row())
        pass

    # TODO: write test case for mentions detection

class ReferencesTestCaseRelaxed(unittest.TestCase):
    def setUp(self):
        test_issue_data = pd.DataFrame(pd.read_json("TestData/test_issue_data.json"))
        test_pullreq_data = pd.DataFrame(pd.read_json("TestData/test_pullreq_data.json"))

        self.new_project = Project(test_pullreq_data, test_issue_data)
        self.thread_list = self.new_project._split_threads(self.new_project._issue_data, "issue")
        sample_thread1 = self.thread_list[0]
        sample_thread2 = self.thread_list[1]

        fakeuser1 = "asdfacasdfdf"
        fakeuser2 = "acaöjaösfkjöajdgf"
        user1_from_t1 = sample_thread1.get_participants()[0]
        user1_from_t2 = sample_thread2.get_participants()[0]
        user2_from_t2 = sample_thread2.get_participants()[1]

        fakeID = 99999

        self.new_mention_fakeuser = Mention(fakeuser1, fakeuser2, fakeID , sample_thread2)
        self.new_quote_fakeuser = Quote(fakeuser1, fakeuser2, fakeID, sample_thread2)

        self.new_mention_trueuser_not_in_same_thread = Mention(user1_from_t1, user1_from_t2, fakeID, sample_thread2)
        self.new_quote_trueuser_not_in_same_thread = Quote(user1_from_t1, user1_from_t2, fakeID, sample_thread2)

        self.new_mention_trueuser_in_same_thread = Mention(user1_from_t2, user2_from_t2, fakeID, sample_thread2)
        self.new_quote_trueuser_in_same_thread = Quote(user1_from_t2, user2_from_t2, fakeID, sample_thread2)

        self.new_mention_trueuser_as_commenter_and_addressee = Mention(user1_from_t2, user1_from_t2, fakeID, sample_thread2)
        self.new_quote_trueuser_as_commenter_and_addressee = Quote(user1_from_t2, user1_from_t2, fakeID, sample_thread2)


    def test_assert_type(self):
        self.assertIsInstance(self.new_mention_fakeuser, Mention)
        self.assertIsInstance(self.new_quote_fakeuser, Quote)

    def test_validation_with_nonexisting_users(self):
        self.assertFalse(self.new_mention_fakeuser.is_valid())
        self.assertFalse(self.new_quote_fakeuser.is_valid())

    def test_validation_with_existing_users_from_different_threads(self):
        self.assertTrue(self.new_mention_trueuser_not_in_same_thread.is_valid())
        self.assertTrue(self.new_quote_trueuser_not_in_same_thread.is_valid())

    def test_validation_with_existing_users_from_same_thread(self):
        self.assertTrue(self.new_mention_trueuser_in_same_thread.is_valid())
        self.assertTrue(self.new_quote_trueuser_in_same_thread.is_valid())

    def test_validation_with_existing_user_as_addressee_and_commenter(self):
        self.assertFalse(self.new_mention_trueuser_as_commenter_and_addressee.is_valid())
        self.assertFalse(self.new_quote_trueuser_as_commenter_and_addressee.is_valid())


def main():
    unittest.main()


if __name__ == '__main__':
    main()
