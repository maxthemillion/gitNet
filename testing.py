import unittest
from project import Project
from references import Mention, Quote
import pandas as pd
from threads import Thread


class StringSearchTestCase(unittest.TestCase):

    def test_find_all_return_val(self):
        test_string = "abafart"
        result = Thread._find_all(test_string, "@")
        self.assertIsInstance(result, list)

    def test_find_all(self):
        test_string = "abcd@fgh@ asdfasdf! @"
        result = Thread._find_all(test_string, "@")
        self.assertTrue(len(result) == 3)
        self.assertTrue(result == [4, 8, 20])

    def test_find_end_username_return_val(self):
        teststring = "somestring"
        result = Thread._find_end_username(teststring, 0)
        self.assertIsInstance(result, int)

    def test_find_end_username(self):
        teststrings = ["@username This is a test.",
                       "This is another @username!",
                       "this is also @username",
                       "This is a final @username\r\n further text"]

        start_pos = [0, 16, 13, 16]
        expected_results = [9, 25, 22, 25]

        for i in range(0, len(teststrings)):
            result = Thread._find_end_username(teststrings[i], start_pos[i])
            self.assertTrue(result == expected_results[i], "Test failed with i =  {0}".format(i))

    def test_find_end_quote_return_val(self):
        teststring = "somestring"
        result = Thread._find_end_quote(teststring, 0)
        self.assertIsInstance(result, int)

    def test_find_end_quote(self):
        test_quotes = [">About @mentions\r\n Interesting however",
                       "something here \r\n> This is the worst <idea> i ever had."]

        start_pos = [0, 17]
        expected_results = [16, len(test_quotes[1])]

        for i in range(0, len(test_quotes)):
            result = Thread._find_end_quote(test_quotes[i], start_pos[i])
            self.assertTrue(result == expected_results[i], "Test failed with i = {0}".format(i))


class DataCleaningTestCase(unittest.TestCase):
    def setUp(self):
        self.fake_pullreq_data = pd.DataFrame(pd.read_json("TestData/test_pullreq_data.json"))
        self.fake_issue_data = pd.DataFrame(pd.read_json("TestData/test_issue_data.json"))

    def test_data_cleaning_column_names(self):
        data = Project.clean_input(self.fake_pullreq_data)
        self.assertTrue("thread_id" in data.columns)
        self.assertFalse("pullreq_id" in data.columns)
        self.assertFalse("issue_id" in data.columns)
        self.assertFalse("commit_id" in data.columns)

        self.assertTrue("repo" in data.columns)
        self.assertTrue("owner" in data.columns)
        self.assertTrue("user" in data.columns)
        self.assertTrue("created_at" in data.columns)
        self.assertTrue("id" in data.columns)

        self.assertTrue(len(data.columns) == 7)


class MentionsDetectionTestCase(unittest.TestCase):
    def setUp(self):
        fake_pullreq_data = pd.DataFrame(pd.read_json("TestData/synthetic_data_pullreq.json"))
        fake_issue_data = pd.DataFrame(pd.read_json("TestData/synthetic_data_issue.json"))
        self.fake_project = Project(fake_pullreq_data, fake_issue_data)
        self.fake_issue_thread_list = self.fake_project._split_threads("issue")

        self.fake_comment_id = 9999
        self.fakeuser1 = "fakeuser1"
        self.addressee1 =  "addressee1"
        self.fake_body = "A fakebody with some @" + self.addressee1

        self.thread = self.fake_issue_thread_list[0]
        self.standard_fake_row = {"user": self.fakeuser1,
                                  "id": self.fake_comment_id,
                                  "body": self.fake_body}

    def test_mentions_detection(self):
        # TODO: implement unit test for mentions detection
        pass
        mentions_list = self.thread._detect_mentions_in_row(self.standard_fake_row)
        self.assertTrue(len(mentions_list) == 1)

    def test_mentions_detection_input_output(self):
        mentions_list = self.thread._detect_mentions_in_row(self.standard_fake_row)
        for m in mentions_list:
            self.assertTrue(m.comment_id == self.fake_comment_id)
            self.assertTrue(m.addressee == self.addressee1)

class QuoteDetectionTestCase(unittest.TestCase):
    def setUp(self):
        test_issue_data = pd.DataFrame(pd.read_json("TestData/test_issue_data.json"))
        test_pullreq_data = pd.DataFrame(pd.read_json("TestData/test_pullreq_data.json"))

        self.new_project = Project(test_pullreq_data, test_issue_data)
        self.thread_list = self.new_project._split_threads("issue")

        self.thread_with_quotes = self.thread_list[1]

        user1 = self.thread_with_quotes.get_participants()[0]
        user2 = self.thread_with_quotes.get_participants()[1]

        self.fake_row_1 = pd.Series({"body": "this is a sample comment without quote!",
                                     "user": user1,
                                     "id": 9999})

    def test_quote_detection(self):
        # TODO: write test case for quote detection
        # self.failIf(Thread._detect_mentions_in_row())
        pass

    # TODO: write test case for mentions detection


class ReferencesTestCaseRelaxed(unittest.TestCase):
    def setUp(self):
        test_issue_data = pd.DataFrame(pd.read_json("TestData/test_issue_data.json"))
        test_pullreq_data = pd.DataFrame(pd.read_json("TestData/test_pullreq_data.json"))

        self.new_project = Project(test_pullreq_data, test_issue_data)
        self.thread_list = self.new_project._split_threads("issue")
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
