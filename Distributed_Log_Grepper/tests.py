import pytest
from server import Server
from constants import *
import json


class Tests:

    # These 10 tests are for frequent patterns
    @pytest.mark.parametrize("query", FREQ_PATTERN)
    def test_grep_freq_pattern(self, query):

        # getting the count of lines matched from original grep
        expected = (
            os.popen("grep -Ec '{}' {}".format(query, LOG_PATH))
            .read()
            .split(".")[0]
            .split("\n")[0]
        )
        print("Answer for running grep command on", curr_host, "for ")
        dict_query = {}
        dict_query["pattern"] = query
        dict_query = json.dumps(dict_query)
        Server_obj = Server()
        # validating it with values obtained from the server
        assert expected == Server_obj.grep(dict_query)

    # These 10 tests are for infrequent patterns
    @pytest.mark.parametrize("query", INFREQ_PATTERN)
    def test_grep_infreq_pattern(self, query):
        # getting the count of lines matched from original grep
        expected = (
            os.popen("grep -Ec '{}' {}".format(query, LOG_PATH))
            .read()
            .split(".")[0]
            .split("\n")[0]
        )
        dict_query = {}
        dict_query["pattern"] = query
        dict_query = json.dumps(dict_query)
        Server_obj = Server()
        # validating it with values obtained from the server
        assert expected == Server_obj.grep(dict_query)

    # These 10 tests are for frequent text
    @pytest.mark.parametrize("query", FREQ_TEXT)
    def test_grep_freq_text(self, query):
        # getting the count of lines matched from original grep
        expected = (
            os.popen("grep -c '{}' {}".format(query, LOG_PATH))
            .read()
            .split(".")[0]
            .split("\n")[0]
        )
        dict_query = {}
        dict_query["pattern"] = query
        dict_query = json.dumps(dict_query)
        Server_obj = Server()
        # validating it with values obtained from the server
        assert expected == Server_obj.grep(dict_query)

    # These 10 tests are for infrequent text
    @pytest.mark.parametrize("query", INFREQ_TEXT)
    def test_grep_infreq_text(self, query):
        # getting the count of lines matched from original grep
        expected = (
            os.popen("grep -c '{}' {}".format(query, LOG_PATH))
            .read()
            .split(".")[0]
            .split("\n")[0]
        )
        dict_query = {}
        dict_query["pattern"] = query
        dict_query = json.dumps(dict_query)
        Server_obj = Server()
        # validating it with values obtained from the server
        assert expected == Server_obj.grep(dict_query)
