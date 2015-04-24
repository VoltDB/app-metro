#!/usr/bin/env python
# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

"""
    Simple web server

    Usage: python exportServer.py [port=<listener port>]
        method "exportRows":
            receives rows from the VoltDB server via "GET".
            processes according to "notify" preference in the row
            updates queue of last N notifications, either text or email
            
        method "htmlRows" returns a static html page of latest MAXROWS exported rows

    The request contains the row, URL-encoded and packed like this example row:
        "CardId=161771&ExportTime=1429897821439&StationName=Ruggles&Name=J%20Ryder%20161771&Phone=6174567890&Email=jryder%40gmail.com&Notify=0&AlertMessage=Insufficient%20Balance"

    Basic web server fragments from http://lucumr.pocoo.org/2007/5/21/getting-started-with-wsgi/

"""

import sys, os
from time import ctime
from urlparse import parse_qs
from twilio.rest import TwilioRestClient
from wsgiref.simple_server import make_server
 
def checkPhoneNum(phone):
    # Twilio wants a leading country code
    if len(phone) > 2 and phone[0:1] != "+1": 
        return "+1" + phone
    else:
        return phone

def sendSMS(toPhone):
    # Account Sid and Auth Token from twilio.com/user/account
    # ACCOUNT SID
    accountSid = "AC543483989cdc36a28dc297572e096b1a"

    # AUTH TOKEN
    authToken = "6ccf0793c2b5d70424a0f2dffeb7e7e2"

    client = TwilioRestClient(accountSid, authToken)

    message = client.messages.create(body="Twilio test message <3",
        to=checkPhoneNum(toPhone),
        from_="+16173963192") # My Twilio number
    print message.sid

def processRow(row):
    """ For each row, check notify flag and process accordingly:
        notify == 0: no notification
        notify == 1: email
        notify == 2: text message to phone number
    """
    try:
        # print "Rider %s, \"%s\" at station %s: " % (row["Name"][0], row["AlertMessage"][0], row["StationName"][0]),
        if row["Notify"][0] == "1":
            # sendEmail(row["Email"][0])
            updateHTML(ctime(int(row["ExportTime"][0])/1000), row["Name"][0], row["Email"][0], "Complete")
            # print "email sent to %s" % row["Email"][0]
        elif row["Notify"][0] == "2":
            # sendSMS(row["Phone"][0])
            updateHTML(ctime(int(row["ExportTime"][0])/1000), row["Name"][0], row["Phone"][0], "Complete")
            # print "text message sent to %s" % row["Phone"][0]
        else:
            print "Not notified"
    except KeyError as err:
        print "Exception: key %s not in GET query string." % err
    except:
        print "Unknown exception in processRow"

def htmlRows(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])
    return assembleTable()

rowQueue = []
MAXROWS = 10

def assembleTable():
    """ Create an html page with latest N rows """
    table = "<html><body><meta http-equiv=\"refresh\" content=\"5\"><table border=\"1\"><tr><th>Event Time</th><th>Name</th><th>Contact</th><th>Status</th></tr>"
    # print "RowQueue: " + str(rowQueue)
    for r in rowQueue:
        table += r
    table += "</table></body></html>"
    # print "Table: " + str(table)
    return table

def updateHTML(time, name, contact, status):
    """ Keep a queue of last 10 exported rows """
    if len(rowQueue) >= MAXROWS:
        dc = rowQueue.pop(0)
    rowQueue.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (time, name, contact, status))

def htmlRows(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])
    return assembleTable()

def exportRows(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])

    if "QUERY_STRING" in environ:
        row = parse_qs(environ["QUERY_STRING"])
        processRow(row)
    return []

getFuncs = {
    "htmlRows" : htmlRows,
    "exportRows" : exportRows,
}

def application(environ, start_response):
    """
    The main WSGI application. Dispatch the current request to
    the functions either "exportRows", the VoltDB export endpoint,
    or to "htmlRows", the source static html of recent notifications.

    If neither, call the `not_found` function.
    """
    path = environ.get('PATH_INFO', '').lstrip('/')
    # print "Path: " + path
    if path in getFuncs:
        return getFuncs[path](environ, start_response)
    return not_found(environ, start_response)

def not_found(environ, start_response):
    """Called if no URL matches."""
    start_response('404 NOT FOUND', [('Content-Type', 'text/plain')])
    return ['Not Found']

if __name__ == '__main__':
    if len(sys.argv) > 1 and "port" in sys.argv[1]:
        port = sys.argv[1].split("=")[1]
    else:
        port = 8082
    try:
        httpd = make_server('', int(port), application)
        print('Serving on port %s...' % str(port))
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('Goodbye.')
