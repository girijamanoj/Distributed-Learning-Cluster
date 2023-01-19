import os

BUFFER_SIZE = 1024
num_of_nodes = 10
PORT = 5000
curr_host = os.popen("hostname").read().split(".")[0]

# Freq texts from the logs
FREQ_TEXT = [
    "Mozilla",
    "Firefox",
    "AppleWebKit",
    "Windows",
    "explore",
    "Macintosh",
    "content",
    "Version",
    "category",
    "HTTP",
]

# Infreq texts from the logs
INFREQ_TEXT = [
    "dickerson",
    "huffman",
    "kirby",
    "santana",
    "spears",
    "valenzuela",
    "beard",
    "blackwell",
    "cameron",
    "mcdowell",
]
# Freq patterns from the logs
FREQ_PATTERN = [
    "Mo*a",
    "Fir*",
    "A*t",
    "W*.",
    "expl..e",
    "A.*t",
    "W*.s",
    "c*y",
    "V..*n",
    "M..*.h",
]

# Infrequent patterns from the logs
INFREQ_PATTERN = [
    "d*n",
    "huff*",
    "mcd*",
    "*ard",
    ".*s",
    "m*l",
    "k...y",
    "b.*",
    "s...r.",
    "c*r.n",
]

LOG_BASE_PATH = "/var/log/"

# Get the log path of this particular Vm
LOG_PATH = LOG_BASE_PATH + "vm" + str(int(curr_host[-2:])) + ".log"

all_nodes = []
for i in range(1, num_of_nodes + 1):
    # Storing all the node names to all_nodes list
    all_nodes.append("fa22-cs425-19{}".format(str(i).zfill(2)))

nodes = []
for i in all_nodes:
    if i != curr_host:
        # nodes will contain all the names of other VMs except itself
        nodes.append(i)
