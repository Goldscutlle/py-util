#!/usr/bin/env python
import inspect

print "Content-Type: text/html"
try:
    import xml.sax.xmlreader # replace this with module
except ImportError:
    #print "Content-Type: text/html"
    #print
    print """\
    <html>
    <body>
    <h2>Module is not importable.</h2>
    </body>
    </html>
    """
    exit()
print
classes = inspect.getmembers(xml.sax.xmlreader, predicate=inspect.isclass) # 1st parameter should be same module as line 6
print """\
<html>
<body>
<h2>Classes</h2></br>"""
for c in classes:
    print "<h3><li>", c[0], c[1], "</li></h3></br>"
    methods = inspect.getmembers(c[1], predicate=inspect.ismethod)
    if len(methods):
        print "<h2>Methods</h2></br>"
        for m in methods:
            print "<li>", m[0], m[1], "</li></br>"
print """</body></html>"""
print

print """\
<html>
<body>"""
functions = inspect.getmembers(xml.sax.xmlreader, predicate=inspect.isfunction) # 1st parameter should be same module as line 6
print "<h2>Functions</h2></br>"
for f in functions:
    print "<li>", f[0], f[1], "</li></br>"
print """</body></html>"""
print