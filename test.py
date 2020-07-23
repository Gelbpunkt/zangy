import zangy
r = zangy.Reader()
r.feed(b"*2\r\n+OK\r\n:1234\r\n")
print(r.gets())
print(r.get_buffer())
