#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Web: https://code.google.com/p/ndb-py
# License: GPLv2

import datetime
import sys

sys.argv = [sys.argv[0], "--datastore_type", "memory"]
import ndb

# We will use the convention key == email. Therefore, the email property must be required.
class User(ndb.Model):
  email = ndb.StringProperty(required=True)
  description = ndb.TextProperty()
  numClicks = ndb.IntegerProperty(default=1)
  isAdmin = ndb.BooleanProperty(required=True, default=False)
  created = ndb.DateTimeProperty(auto_now_add=True)
  updated = ndb.DateTimeProperty(auto_now=True)

# Create user john@example.com, who is an admin and plays in beer commercials.
john = User.get_or_insert("john@example.com", email="john@example.com")
john.description = "The most interesting man in the world"
john.numClicks = 42
john.isAdmin = True

assert(john.to_dict()["email"] == "john@example.com")
assert(john.to_dict()["description"] == "The most interesting man in the world")
assert(john.to_dict()["numClicks"] == 42)
assert(john.to_dict()["isAdmin"] == True)
print "\nJohn before put():"
print john.to_dict()

john.put()

# Read back john's info
john = User.get_by_id("john@example.com")
assert(john.to_dict()["email"] == "john@example.com")
assert(john.to_dict()["description"] == "The most interesting man in the world")
assert(john.to_dict()["numClicks"] == 42)
assert(john.to_dict()["isAdmin"] == True)
print "\nJohn after put():"
print john.to_dict()


# Create some more users
User.get_or_insert("bob@example.com",
                    email="bob@example.com",
                    isAdmin=True)

User.get_or_insert("adam@example.com",
                    email="adam@example.com",
                    numClicks=5)

User.get_or_insert("irene@example.com",
                    email="irene@example.com",
                    numClicks=7)

User.get_or_insert("tim@example.com",
                    email="tim@example.com",
                    numClicks=28)

User.get_or_insert("diane@example.com",
                    email="diane@example.com",
                    numClicks=3)

User.get_or_insert("kim@example.com",
                    email="kim@example.com",
                    numClicks=2)

User.get_or_insert("charlie@example.com",
                    email="charlie@example.com")

User.get_or_insert("fiona@example.com",
                    email="fiona@example.com")

User.get_or_insert("horatio@example.com",
                    email="horatio@example.com")

User.get_or_insert("george@example.com",
                    email="george@example.com")

User.get_or_insert("eva@example.com",
                    email="eva@example.com")

# Let's list all the users
q = User.query()
print "\nAll users:"
for u in q.iter():
  print u.to_dict()

# Let's list all the admins
q = User.query()
q = q.filter(User.isAdmin == True)
print "\nAll admins:"
for u in q.iter():
  print u.to_dict()

# Let's list all the non-admin users, sorted by clicks in descending order, then email
q = User.query()
q = q.filter(User.isAdmin != True)
q = q.order(-User.numClicks)
q = q.order(User.email)
print "\nUsers, sorted by activity and e-mail:"
for u in q.iter():
  print u.to_dict()
