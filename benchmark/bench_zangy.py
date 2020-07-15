from random import randint

from benchmark import Suite
from zangy import Reader

startBuffer = b"$100\r\nabcdefghij'"
chunkBuffer = b"abcdefghijabcdefghijabcdefghij"
stringBuffer = b"+testing a simple string\r\n"
integerBuffer = b":1237884\r\n"
bigIntegerBuffer = b":184467440737095516171234567890\r\n"
errorBuffer = b"-Error ohnoesitbroke\r\n"
endBuffer = b"\r"
lorem = (
    b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"
    b" incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis"
    b" nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
    b" Duis aute irure dolor in"
)
loremSplit = lorem.split()
bigStringArray = [loremSplit[i % len(loremSplit)] for i in range(2 ** 16)]
startBigBuffer = b"$" + str(4 * 1024 * 1024).encode() + b"\r\n"

chunks = [b" ".join(bigStringArray)[:4096] for _ in range(1024)]

arraySize = 100
arrayBuffer = b"*" + str(arraySize).encode() + b"\r\n"
size = 0
for i in range(arraySize):
    arrayBuffer += b"$"
    size = randint(1, 10)
    arrayBuffer += str(size).encode() + b"\r\n" + lorem[0:size] + b"\r\n"

bigArraySize = 160
bigArrayChunks = [b"*1\r\n*1\r\n*" + str(bigArraySize).encode()]
for i in range(bigArraySize):
    size = 65000 + i
    if i % 2:
        # The "x" in the beginning is important to prevent benchmark manipulation due to a minor JSParser optimization
        bigArrayChunks.append(
            b"x\r\n$"
            + str(size).encode()
            + b"\r\n"
            + b"a" * (size + 1)
            + b"\r\n:"
            + str(randint(0, 1000000)).encode()
        )
    else:
        bigArrayChunks.append(
            b"\r\n+this is some short text about nothing\r\n:"
            + str(size).encode()
            + b"\r\n$"
            + str(size).encode()
            + b"\r\n"
            + b"b" * size
        )
bigArrayChunks.append(b"\r\n")

chunkedStringPart1 = b"+foobar"
chunkedStringPart2 = b"bazEND\r\n"

parserBuffer = Reader()
suite = Suite()

# BULK STRINGS


def bench_bulk_string():
    parserBuffer.feed(startBuffer)
    parserBuffer.feed(chunkBuffer)
    parserBuffer.feed(chunkBuffer)
    parserBuffer.feed(chunkBuffer)
    parserBuffer.feed(endBuffer)
    parserBuffer.gets()


suite.add("PY PARSER BUF: $ multiple chunks in a bulk string", bench_bulk_string)

# CHUNKED STRINGS


def bench_multiple_chunks():
    parserBuffer.feed(chunkedStringPart1)
    parserBuffer.feed(chunkedStringPart2)
    parserBuffer.gets()


suite.add("PY PARSER BUF: + multiple chunks in a string", bench_multiple_chunks)

# BIG BULK STRING


def four_mb_bulk_string():
    parserBuffer.feed(startBigBuffer)
    for i in chunks:
        parserBuffer.feed(i)
    parserBuffer.feed(endBuffer + b"\n")
    parserBuffer.gets()


suite.add("PY PARSER BUF: $ 4mb bulk string", four_mb_bulk_string)

# STRINGS


def simple_string():
    parserBuffer.feed(stringBuffer)
    parserBuffer.gets()


suite.add("PY PARSER BUF: + simple string", simple_string)

# INTEGERS


def integers():
    parserBuffer.feed(integerBuffer)
    parserBuffer.gets()


suite.add("PY PARSER BUF: : integer", integers)

# BIG INTEGER


def big_integer():
    parserBuffer.feed(bigIntegerBuffer)
    print(parserBuffer.get_buffer())
    parserBuffer.gets()


suite.add("PY PARSER BUF: : big integer", big_integer)

# ARRAYS


def arrays():
    parserBuffer.feed(arrayBuffer)
    parserBuffer.gets()


suite.add("PY PARSER BUF: * array", arrays)

# BIG NESTED ARRAYS


def big_nested_arrays():
    for i in bigArrayChunks:
        parserBuffer.feed(i)
    parserBuffer.gets()


suite.add("PY PARSER BUF: * big nested array", big_nested_arrays)

# ERRORS


def errors():
    parserBuffer.feed(errorBuffer)
    parserBuffer.gets()


suite.add("PY PARSER BUF: - error", errors)


def event(ev):
    print(ev)


suite.on("cycle", event)

suite.run(delay=1, min_samples=150)
