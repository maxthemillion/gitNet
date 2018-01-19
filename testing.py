import unittest
import pandas as pd
import json
import datetime
from project import Project, ProjectStats
from references import Mention, Quote, ContextualReply
from threads import Thread
from main import _clean_comment_data
from analysis import Analyzer
from neocontroller import Neo4jController


def setup_sample_threads():
    with open("TestData/syn_data.json") as d:
        syn_data = json.load(d)

    pc = pd.DataFrame(syn_data['pc'])
    ic = pd.DataFrame(syn_data['ic'])
    cc = pd.DataFrame(syn_data['cc'])

    pc, ic, cc = _clean_comment_data(pc, ic, cc)

    fake_project = Project(pc, ic, cc, "fooOwner", "fooRepo")
    return fake_project._split_threads("issue")


def setup_sample_data():
    with open("TestData/syn_data.json") as d:
        syn_data = json.load(d)

    pc = pd.DataFrame(syn_data['pc'])
    ic = pd.DataFrame(syn_data['ic'])
    cc = pd.DataFrame(syn_data['cc'])

    return _clean_comment_data(pc, ic, cc)


class AnalyzerTests(unittest.TestCase):
    def setUp(self):
        self.owner = "Homebrew"
        self.repo = "brew"
        self.analyzer = Analyzer(self.owner, self.repo)

        date = datetime.date(year=2016, month=10, day=1)
        self.datestring = date.strftime("%Y-%m-%d")

    @unittest.skip
    def testNxLouvain(self):
        res = self.analyzer._louvain_networkx()
        self.assertIsInstance(res, list)

    def testrunnal(self):
        louvain, dc, bc = self.analyzer._individual_measures()
        self.assertTrue(True)

class StringSearch(unittest.TestCase):

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


class MentionsDetectionOutputTests(unittest.TestCase):
    def setUp(self):
        fake_issue_thread_list = setup_sample_threads()

        fakeuser1 = "fakeuser1"
        self.fake_comment_id = 9999
        self.addressee1 = "addressee1"
        fake_body = "A fakebody with some @" + self.addressee1

        fake_date = "2016-04-04T18:09:46Z"

        self.thread = fake_issue_thread_list[0]
        self.standard_fake_row = {"user": fakeuser1,
                                  "id": self.fake_comment_id,
                                  "body": fake_body,
                                  "created_at": fake_date}

    def test_mentions_detection_output_type(self):
        mentions_list = self.thread._detect_mentions_in_row(self.standard_fake_row)
        self.assertIsInstance(mentions_list, list)

    def test_mentions_detection_output_list_content_types(self):
        mentions_list = self.thread._detect_mentions_in_row(self.standard_fake_row)
        first_mention = mentions_list.pop(0)
        self.assertIsInstance(first_mention, Mention)

    def test_mentions_detection_input_output_match(self):
        mentions_list = self.thread._detect_mentions_in_row(self.standard_fake_row)
        for m in mentions_list:
            self.assertEqual(m.comment_id, self.fake_comment_id)
            self.assertEqual(m.addressee, self.addressee1)

    def test_mentions_detection_standard_fake_row(self):
        mentions_list = self.thread._detect_mentions_in_row(self.standard_fake_row)
        self.assertTrue(len(mentions_list) == 1)


class MentionsDetectionFindMentions(unittest.TestCase):
    def setUp(self):
        dummy_issue_thread_list = setup_sample_threads()
        self.dummy_thread = dummy_issue_thread_list[0]

        fakeuser1 = "fakeuser1"
        fakeuser2 = "fakeuser2"
        fakeuser3 = "fakeuser3"

        self.fake_comment_id = 9999

        fake_date = "2016-04-04T18:09:46Z"

        self.fake_row_1 = pd.Series({"body": "this is a sample comment without mention!",
                                     "user": fakeuser1,
                                     "id": 9999,
                                     "created_at": fake_date})

        self.fake_row_2 = pd.Series({"body": "@{0} this is a sample comment with mention at the beginning!\r\n"
                                             "And some arbitrary text.".format(fakeuser1),
                                     "user": fakeuser2,
                                     "id": 9999,
                                     "created_at": fake_date})

        self.fake_row_3 = pd.Series({"body": "Arbitrary text. \r\n"
                                             "this is a sample comment with @{0} at the end!".format(fakeuser1),
                                     "user": fakeuser2,
                                     "id": 9999,
                                     "created_at": fake_date})

        self.fake_row_4 = pd.Series({"body": "Arbitrary text with <html tag> and some email@dress.com",
                                     "user": fakeuser2,
                                     "id": 9999,
                                     "created_at": fake_date})

        self.fake_row_5 = pd.Series({"body": "@{0} first mention and another one @{1}".format(fakeuser1, fakeuser2),
                                     "user": fakeuser3,
                                     "id": 9999,
                                     "created_at": fake_date})

    def test_fake_row_1(self):
        result = self.dummy_thread._detect_mentions_in_row(self.fake_row_1)
        self.assertEqual(result, [])

    def test_fake_row_2(self):
        result = self.dummy_thread._detect_mentions_in_row(self.fake_row_2)
        self.assertEqual(len(result), 1)

    def test_fake_row_3(self):
        result = self.dummy_thread._detect_mentions_in_row(self.fake_row_3)
        self.assertEqual(len(result), 1)

    def test_fake_row_4(self):
        # TODO: is there a way to distinguish arbitrary usage of @ from @mentions?
        # currently, this is solved by comparing the found usernames to the names in the discussion participants
        # list. (validation inside the reference objects)
        result = self.dummy_thread._detect_mentions_in_row(self.fake_row_4)
        self.assertEqual(len(result), 1)

    def test_fake_row_4_validation(self):
        result = self.dummy_thread._detect_mentions_in_row(self.fake_row_4)
        self.assertFalse(result[0].is_valid())

    def test_fake_row_5(self):
        result = self.dummy_thread._detect_mentions_in_row(self.fake_row_5)
        self.assertEqual(len(result), 2)


class QuoteDetectionFindQuotes(unittest.TestCase):
    def setUp(self):

        pc, ic, cc = setup_sample_data()

        self.new_project = Project(pc, ic, cc, "fooOwner", "fooRepo")
        self.thread_list = self.new_project._split_threads("issue")

        self.thread_with_quotes = self.thread_list[1]

        user1 = self.thread_with_quotes.get_participants()[0]
        user2 = self.thread_with_quotes.get_participants()[1]

        fake_date = "2016-04-04T18:09:46Z"

        self.fake_row_1 = pd.Series({"body": "this is a sample comment without quote!",
                                     "user": user1,
                                     "id": 9999,
                                     "created_at": fake_date})

        self.fake_row_2 = pd.Series({"body": ">this is a sample comment with quote at the beginning!\r\n"
                                             "And some arbitrary text.",
                                     "user": user1,
                                     "id": 9999,
                                     "created_at": fake_date})

        self.fake_row_3 = pd.Series({"body": "Arbitrary text. \r\n"
                                             ">this is a sample comment with quote at the end!",
                                     "user": user1,
                                     "id": 9999,
                                     "created_at": fake_date})

        self.fake_row_4 = pd.Series({"body": "Arbitrary text with <html tag> and some other >quote-like construct",
                                     "user": user1,
                                     "id": 9999,
                                     "created_at": fake_date})

        self.fake_row_5 = pd.Series({"body": ">quote1 \r\nArbitrary text with <html tag> and some other "
                                             "\r\n>quote at the end",
                                     "user": user1,
                                     "id": 9999,
                                     "created_at": fake_date})

    def test_output_datatype(self):
        self.assertIsInstance(self.thread_with_quotes._detect_quotes_in_row(self.fake_row_1, 1), list)

    def test_fakerow_1(self):
        result = self.thread_with_quotes._detect_quotes_in_row(self.fake_row_1, 1)
        self.assertTrue(len(result) == 0)

    def test_fakerow_2_result_length(self):
        result = self.thread_with_quotes._detect_quotes_in_row(self.fake_row_2, 1)
        self.assertTrue(len(result) == 1)

    def test_fakerow_2_list_content_type(self):
        result = self.thread_with_quotes._detect_quotes_in_row(self.fake_row_2, 1)
        self.assertTrue(type(result[0]) == Quote)

    def test_fakerow_3_result_length(self):
        result = self.thread_with_quotes._detect_quotes_in_row(self.fake_row_3, 1)
        self.assertTrue(len(result) == 1)

    def test_fakerow_4_result_length(self):
        result = self.thread_with_quotes._detect_quotes_in_row(self.fake_row_4, 1)
        self.assertTrue(len(result) == 0)

    def test_fakerow_5_result_length(self):
        result = self.thread_with_quotes._detect_quotes_in_row(self.fake_row_5, 1)
        self.assertTrue(len(result) == 2)


class QuoteDetectionSourceQuotes(unittest.TestCase):
    def setUp(self):
        pc, ic, cc = setup_sample_data()

        self.new_project = Project(pc, ic, cc, "fooOwner", "fooRepo")
        self.thread_list = self.new_project._split_threads("issue")

        self.thread_with_quotes = self.thread_list[0]
        self.thread_with_quotes.run()

    def testGetReferencesOutputType(self):
        ref = self.thread_with_quotes.get_references_as_list()
        self.assertTrue(type(ref) == list)

    # TODO: test quote sourcing
    def testSourcing(self):
        ref = self.thread_with_quotes.get_references_as_list()
        ref_df = pd.DataFrame(ref)
        quote_ref = ref_df[ref_df["ref_type"] == "Quote"]
        self.assertTrue(len(quote_ref) == 2)


class QuoteDetectionIsQuote(unittest.TestCase):
    def setUp(self):
        self.fake_string_1 = ">>sample string"
        self.fake_string_2 = "\r\n>> sample quote"
        self.fake_string_3 = ">>>sample quote"
        self.fake_string_4 = "no quote >>>quote-like construct"

    def testIsQuoteFS1(self):
        is_quote = Thread._is_quote(self.fake_string_1, 1)
        self.assertTrue(is_quote)

    def testIsQuoteFS2(self):
        is_quote = Thread._is_quote(self.fake_string_2, 3)
        self.assertTrue(is_quote)

    def testIsQuoteFS3(self):
        is_quote = Thread._is_quote(self.fake_string_3, 2)
        self.assertTrue(is_quote)

    def testIsQuoteFS4(self):
        is_quote = Thread._is_quote(self.fake_string_4, 1)
        self.assertFalse(is_quote)


class QuoteDetectionClearMarkdownList(unittest.TestCase):
    def setUp(self):
        self.md_list_1 = [0, 1, 2, 4]
        self.expected_result_1 = [2, 4]

        self.md_list_2 = [0]
        self.expected_result_2 = [0]

        self.md_list_3 = []
        self.expected_result_3 = []

    def testClearMarkdown1(self):
        cleared_list = Thread._clear_markdown_close(self.md_list_1)
        self.assertTrue(cleared_list == self.expected_result_1)

    def testClearMarkdown2(self):
        cleared_list = Thread._clear_markdown_close(self.md_list_2)
        self.assertTrue(cleared_list == self.expected_result_2)

    def testClearMarkdown3(self):
        cleared_list = Thread._clear_markdown_close(self.md_list_3)
        self.assertTrue(cleared_list == self.expected_result_3)


class ContextualsDetectionOutputTests(unittest.TestCase):
    def setUp(self):
        fake_issue_thread_list  = setup_sample_threads()
        self.thread = fake_issue_thread_list[0]

    def test_contextuals_detection_output_type(self):
        contextuals = self.thread._detect_contextuals([], [], [], [])
        self.assertIsInstance(contextuals, list)

    def test_contextuals_detection_output_list_content_types(self):
        contextuals = self.thread._detect_contextuals([], [], [], [])
        self.assertIsInstance(contextuals.pop(0), ContextualReply)


class ContextualsDetectionFindContextuals(unittest.TestCase):
    def setUp(self):

        fake_issue_thread_list = setup_sample_threads()

        self.thread1 = fake_issue_thread_list[0]
        self.thread2 = fake_issue_thread_list[1]

    def test_contextuals_detection_in_thread1_with_quotes_and_mentions(self):
        self.assertTrue(False)

    def test_contextuals_detection_in_thread2_with_quotes_and_mentions(self):
        self.assertTrue(False)


class ConsolidateReferences(unittest.TestCase):
    def setUp(self):
        pass

    def test_consolidate_references_ouptut_type(self):
        result = Thread._consolidate_references([], [], [])
        self.assertIsInstance(result, list)


class FindReferencesRelaxed(unittest.TestCase):
    def setUp(self):

        pc, ic, cc = setup_sample_data()

        new_project = Project(pc, ic, cc, "fooOwner", "fooRepo")
        thread_list = new_project._split_threads("issue")
        sample_thread1 = thread_list[0]
        sample_thread2 = thread_list[1]

        fakeuser1 = "asdfacasdfdf"
        fakeuser2 = "acaöjaösfkjöajdgf"
        user1_from_t1 = sample_thread1.get_participants()[0]
        user1_from_t2 = sample_thread2.get_participants()[0]
        user2_from_t2 = sample_thread2.get_participants()[1]

        fake_stats = ProjectStats(new_project)
        fake_date = "2016-04-04T18:09:46Z"

        fakeID = 99999

        self.new_mention_fakeuser = Mention(fakeuser1,
                                            fakeuser2,
                                            fakeID,
                                            sample_thread2,
                                            fake_stats,
                                            fake_date)
        self.new_quote_fakeuser = Quote(fakeuser1,
                                        fakeuser2,
                                        fakeID,
                                        sample_thread2,
                                        fake_stats,
                                        fake_date)

        self.new_mention_trueuser_not_in_same_thread = Mention(user1_from_t1,
                                                               user1_from_t2,
                                                               fakeID,
                                                               sample_thread2,
                                                               fake_stats,
                                                               fake_date)
        self.new_quote_trueuser_not_in_same_thread = Quote(user1_from_t1,
                                                           user1_from_t2,
                                                           fakeID,
                                                           sample_thread2,
                                                           fake_stats,
                                                           fake_date)

        self.new_mention_trueuser_in_same_thread = Mention(user1_from_t2,
                                                           user2_from_t2,
                                                           fakeID,
                                                           sample_thread2,
                                                           fake_stats,
                                                           fake_date)
        self.new_quote_trueuser_in_same_thread = Quote(user1_from_t2,
                                                       user2_from_t2,
                                                       fakeID,
                                                       sample_thread2,
                                                       fake_stats,
                                                       fake_date)

        self.new_mention_trueuser_as_commenter_and_addressee = Mention(user1_from_t2,
                                                                       user1_from_t2,
                                                                       fakeID,
                                                                       sample_thread2,
                                                                       fake_stats,
                                                                       fake_date)
        self.new_quote_trueuser_as_commenter_and_addressee = Quote(user1_from_t2,
                                                                   user1_from_t2,
                                                                   fakeID,
                                                                   sample_thread2,
                                                                   fake_stats,
                                                                   fake_date)


    def test_assert_type(self):
        print()
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
