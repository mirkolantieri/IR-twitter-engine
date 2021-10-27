# OUTPUT COLORS
RESET = "\033[0m"
bw = lambda s: "\033[1m\033[37m" + str(s) + RESET  # bold white
w = lambda s: "\033[1m" + str(s) + RESET  # white
g = lambda s: "\033[32m" + str(s) + RESET  # green
y = lambda s: "\033[33m" + str(s) + RESET  # yellow
r = lambda s: "\033[31m" + str(s) + RESET  # red

def pprint(*arguments):
    # output formatting helper function
    print(bw("["), *arguments, bw("]"))