from flask import Flask, render_template, request
from py_grepper import py_grepper
from client import api
from constants import *


app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
app.config["TEMPLATES_AUTO_RELOAD"] = True


@app.route("/", methods=["GET", "POST"])
def index():
    """default path is "/"
    GET: returns a html form and asks for user input
    POST: return a html form along with the results of the previous query
    """
    if request.method == "GET":
        return render_template("search.html", counts=None)
    elif request.method == "POST":
        option = request.form.get(
            "option"
        )  # type: str, contains option as patterm or text
        match = request.form.get("match")  # type: str, contains pattern or text to grep
        cached = int(
            request.form.get("cached")
        )  # type: int, 1 for cached query, 0 for un cached query
        if option == "pattern":
            # api is a function imported from client.py, that is used to query servers with
            # the given pattern or text
            data = api(match, None, cached)
        else:
            data = api(None, match, cached)
        if data is None:
            # Ask user to enter valid pattern/text
            return "Please enter valid pattern/text"
        counts = data[0]
        log_files = data[1]
        timemap = data[2]
        total_count = data[3]
        total_time_cq = data[4]
        total_time_q = data[5]
        return render_template(
            "search.html",
            match=match,
            all_nodes=all_nodes,
            log_files=log_files,
            counts=counts,
            timemap=timemap,
            total_count=total_count,
            total_time_cq=total_time_cq,
            total_time_q=total_time_q,
        )
    else:
        # Allows on GET or POST methods
        return "Method not allowed"


app.run(host="0.0.0.0", port=8080)
