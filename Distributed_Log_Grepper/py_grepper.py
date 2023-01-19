from constants import *
import argparse
from os import system


class py_grepper:
    """py_grepper is a wrapper around os grep"""

    def __init__(self):
        self.log_file = LOG_PATH

    def grep(self, option, match):
        count = 0
        count = (
            os.popen("grep -{} '{}' {}".format(option, match, self.log_file))
            .read()
            .split(".")[0]
            .split("\n")[0]
        )
        return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", "-p", help="Query pattern")
    parser.add_argument("--text", "-t", help="Query text")

    args = parser.parse_args()
    print(args)
    if not args.pattern and not args.text:
        print("Please input either pattern or text to parse the logs")

    py_grepper_obj = py_grepper()
    if args.pattern:
        print(py_grepper_obj.grep("Ec", args.pattern))
    elif args.text:
        print(py_grepper_obj.grep("c", args.text))


if __name__ == "__main__":
    main()
