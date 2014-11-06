#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Web: https://code.google.com/p/ndb-py
# License: GPLv2

import datetime
import math
import os
import profile
import random
import shutil
import sys
import threading

def runTest():
  class User(ndb.Model):
    email = ndb.StringProperty(required=True)
    seed = ndb.IntegerProperty()
    description = ndb.TextProperty()
    numClicks = ndb.IntegerProperty(default=1)
    isAdmin = ndb.BooleanProperty(required=True, default=False)
    created = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)
    born = ndb.DateTimeProperty()
    becomesSmart = ndb.DateTimeProperty()
    weight = ndb.FloatProperty(default=75.72)


  class Message(ndb.Model):
    fromEmail = ndb.StringProperty(required=True)
    toEmail = ndb.StringProperty()
    content = ndb.TextProperty()
    importance = ndb.StringProperty(choices=["low", "normal", "urgent"], default="normal")
    created = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)
    seed = ndb.IntegerProperty()


  def getEmail(i):
    return ("test%d@example.com" % (129488375 - 17 * i))

  def checkEmail(i, value):
    return getEmail(i) == value

  def getDescription(i):
    s = u"He is a"
    s += " good" * (i % 3000)
    s += " person."
    return s

  def checkDescription(i, value):
    return getDescription(i) == value

  def getNumClicks(i):
    return i + 5

  def checkNumClicks(i, value):
    return getNumClicks(i) == value

  def getIsAdmin(i):
    if i == 7:
      return True
    return None

  def checkIsAdmin(i, value):
    if getIsAdmin(i) is not None:
      return getIsAdmin(i) == value
    return value == False

  def getCreated(i):
    global startTime
    if startTime is None:
      startTime = datetime.datetime.now()
    return None

  def checkCreated(i, value):
    global startTime
    now = datetime.datetime.now()
    return startTime < value and value < now

  def getUpdated(i):
    global startTime
    if startTime is None:
      startTime = datetime.datetime.now()
    return None

  def checkUpdated(i, value):
    global startTime
    now = datetime.datetime.now()
    return startTime < value and value < now

  def getBorn(i):
    return datetime.datetime.fromtimestamp(i)

  def checkBorn(i, value):
    return getBorn(i) == value

  def getBecomesSmart(i):
    return None

  def checkBecomesSmart(i, value):
    return getBecomesSmart(i) == value

  def getWeight(i):
    if i == 10:
      return 0.0
    return 10.0 + (i % 100 % 2)

  def checkWeight(i, value):
    return getWeight(i) == value

  print "Populating datastore"
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = User.get_or_insert(getEmail(i), email=getEmail(i))
    user.seed = i
    user.description = getDescription(i)
    user.numClicks = getNumClicks(i)
    user.isAdmin = getIsAdmin(i)
    user.created = getCreated(i)
    user.updated = getUpdated(i)
    user.born = getBorn(i)
    user.becomesSmart = getBecomesSmart(i)
    user.weight = getWeight(i)
    user.put()
  print ""

  print "Reading datastore"
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = User.get_by_id(getEmail(i)) if i % 2 == 0 else User.get_or_insert(getEmail(i), email="lol")
    assert i == user.seed
    assert checkEmail(i, user.email)
    assert checkDescription(i, user.description)
    assert checkNumClicks(i, user.numClicks)
    assert checkIsAdmin(i, user.isAdmin)
    assert checkCreated(i, user.created)
    assert checkUpdated(i, user.updated)
    assert checkBorn(i, user.born)
    assert checkBecomesSmart(i, user.becomesSmart)
    assert checkWeight(i, user.weight)
  print ""

  print "Changing datastore"
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = User.get_by_id(getEmail(i))
    i = i + 10
    user.seed = i
    user.description = getDescription(i)
    user.numClicks = getNumClicks(i)
    user.isAdmin = getIsAdmin(i)
    user.created = getCreated(i)
    user.updated = getUpdated(i)
    user.born = getBorn(i)
    user.becomesSmart = getBecomesSmart(i)
    user.weight = getWeight(i)
    user.put()
  print ""

  print "Reading datastore"
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = User.get_by_id(getEmail(i))
    assert checkEmail(i, user.email)
    i = i + 10
    assert i == user.seed
    assert checkDescription(i, user.description)
    assert checkNumClicks(i, user.numClicks)
    assert checkIsAdmin(i, user.isAdmin)
    assert checkCreated(i, user.created)
    assert checkUpdated(i, user.updated)
    assert checkBorn(i, user.born)
    assert checkBecomesSmart(i, user.becomesSmart)
    assert checkWeight(i, user.weight)
  print ""

  print "Querying datastore"
  q = User.query()
  all = [u for u in q.iter()]
  assert len(all) == n
  all.sort(key=lambda u: u.seed)
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = all[i]
    assert checkEmail(i, user.email)
    i = i + 10
    assert i == user.seed
    assert checkDescription(i, user.description)
    assert checkNumClicks(i, user.numClicks)
    assert checkIsAdmin(i, user.isAdmin)
    assert checkCreated(i, user.created)
    assert checkUpdated(i, user.updated)
    assert checkBorn(i, user.born)
    assert checkBecomesSmart(i, user.becomesSmart)
    assert checkWeight(i, user.weight)
  print ""


  print "Querying datastore with filters"
  q = User.query()
  q = q.filter(User.numClicks < 1000)
  q = q.filter(User.description > "He is a good")
  q = q.filter(User.born <= datetime.datetime.fromtimestamp(n / 2))
  q = q.filter(User.weight >= 3.0)
  all = [u for u in q.iter()]
  numFiltered = len(all)
  print "numFiltered =", numFiltered
  all.sort(key=lambda u: u.seed)
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = User.get_by_id(getEmail(i))
    if user.numClicks < 1000 and \
       user.description > "He is a good" and \
       user.born <= datetime.datetime.fromtimestamp(n / 2) and \
       user.weight >= 3.0:
      user = all.pop(0)
      assert checkEmail(i, user.email)
      i = i + 10
      assert i == user.seed
      assert checkDescription(i, user.description)
      assert checkNumClicks(i, user.numClicks)
      assert checkIsAdmin(i, user.isAdmin)
      assert checkCreated(i, user.created)
      assert checkUpdated(i, user.updated)
      assert checkBorn(i, user.born)
      assert checkBecomesSmart(i, user.becomesSmart)
      assert checkWeight(i, user.weight)
  assert len(all) == 0
  print ""


  print "Querying datastore with sorting"
  q = User.query()
  q = q.order(-User.description)
  q = q.order(User.weight)
  all = [u for u in q.iter()]
  assert len(all) == n
  all2 = sorted(all, key=lambda u: u.weight)
  all2 = sorted(all2, key=lambda u: u.description, reverse=True)

  for j in range(len(all)):
    if j % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = all[j]
    i = user.seed
    assert checkEmail(i - 10, user.email)
    assert checkDescription(i, user.description)
    assert checkNumClicks(i, user.numClicks)
    assert checkIsAdmin(i, user.isAdmin)
    assert checkCreated(i, user.created)
    assert checkUpdated(i, user.updated)
    assert checkBorn(i, user.born)
    assert checkBecomesSmart(i, user.becomesSmart)
    assert checkWeight(i, user.weight)
    user = all2[j]
    assert checkEmail(i - 10, user.email)
    assert checkDescription(i, user.description)
    assert checkNumClicks(i, user.numClicks)
    assert checkIsAdmin(i, user.isAdmin)
    assert checkCreated(i, user.created)
    assert checkUpdated(i, user.updated)
    assert checkBorn(i, user.born)
    assert checkBecomesSmart(i, user.becomesSmart)
    assert checkWeight(i, user.weight)
  print ""

  print "Querying datastore with filters and sorting"
  q = User.query(User.numClicks < 1000, User.description > "He is a good")
  q = q.filter(User.born <= datetime.datetime.fromtimestamp(n / 2))
  q = q.filter(User.weight >= 3.0)
  q = q.order(User.weight, -User.email)
  all = [u for u in q.iter()]
  assert len(all) == numFiltered
  all2 = sorted(all, key=lambda u: u.email, reverse=True)
  all2 = sorted(all2, key=lambda u: u.weight)
  for j in range(len(all)):
    if j % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = all[j]
    i = user.seed
    assert checkEmail(i - 10, user.email)
    assert checkDescription(i, user.description)
    assert checkNumClicks(i, user.numClicks)
    assert checkIsAdmin(i, user.isAdmin)
    assert checkCreated(i, user.created)
    assert checkUpdated(i, user.updated)
    assert checkBorn(i, user.born)
    assert checkBecomesSmart(i, user.becomesSmart)
    assert checkWeight(i, user.weight)
    user = all2[j]
    assert user.seed == i
    assert checkEmail(i - 10, user.email)
    assert checkDescription(i, user.description)
    assert checkNumClicks(i, user.numClicks)
    assert checkIsAdmin(i, user.isAdmin)
    assert checkCreated(i, user.created)
    assert checkUpdated(i, user.updated)
    assert checkBorn(i, user.born)
    assert checkBecomesSmart(i, user.becomesSmart)
    assert checkWeight(i, user.weight)
  print ""


  print "Populating datastore"
  messages = []
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user1 = User.get_by_id(getEmail(i))
    user2 = User.get_by_id(getEmail((i + 1) % n))
    expectCrash = False
    if i % 7 == 0:
      msg = Message(fromEmail=user1.email,
                    toEmail=user2.email,
                    content=user1.description)
    elif i % 7 == 1:
      msg = Message(fromEmail=user1.email,
                    toEmail=user2.email,
                    content=user1.description,
                    importance=None)
    elif i % 7 == 2:
      msg = Message(fromEmail=user1.email,
                    toEmail=user2.email,
                    content=user1.description,
                    importance="")
      expectCrash = True
    elif i % 7 == 3:
      msg = Message(fromEmail=user1.email,
                    toEmail=user2.email,
                    content=user1.description,
                    importance="asdf")
      expectCrash = True
    elif i % 7 == 4:
      msg = Message(fromEmail=user1.email,
                    toEmail=user2.email,
                    content=user1.description,
                    importance="low")
    elif i % 7 == 5:
      msg = Message(fromEmail=user1.email,
                    toEmail=user2.email,
                    content=user1.description,
                    importance="normal")
    elif i % 7 == 6:
      msg = Message(fromEmail=user1.email,
                    toEmail=user2.email,
                    content=user1.description,
                    importance="urgent")
    msg.seed = i
    try:
      messages.append(msg.put())
      assert not expectCrash
    except:
      assert expectCrash
  print ""

  print "Reading datastore"
  for j in range(len(messages)):
    if j % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    key = messages[j]
    msg = Message.get_by_id(key)
    i = msg.seed
    user1 = User.get_by_id(getEmail(i))
    user2 = User.get_by_id(getEmail((i + 1) % n))
    assert msg.fromEmail == user1.email
    assert msg.toEmail == user2.email
    assert msg.content == user1.description
    if i % 7 == 0:
      assert msg.importance == "normal"
    elif i % 7 == 1:
      assert msg.importance == "normal"
    elif i % 7 == 4:
      assert msg.importance == "low"
    elif i % 7 == 5:
      assert msg.importance == "normal"
    elif i % 7 == 6:
      assert msg.importance == "urgent"
  print ""

  print "Changing datastore"
  for j in range(len(messages)):
    if j % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    key = messages[j]
    msg = Message.get_by_id(key)
    i = msg.seed
    msg.content = msg.content.upper()
    msg.importance = "urgent"
    msg.put()
  print ""

  print "Reading datastore"
  for j in range(len(messages)):
    if j % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    key = messages[j]
    msg = Message.get_by_id(key)
    i = msg.seed
    user1 = User.get_by_id(getEmail(i))
    user2 = User.get_by_id(getEmail((i + 1) % n))
    assert msg.fromEmail == user1.email
    assert msg.toEmail == user2.email
    assert msg.content == user1.description.upper()
    assert msg.importance == "urgent"
  print ""

  def threadFunc1(randomAccess, offset):
    seeds = range(n)
    if randomAccess:
      random.shuffle(seeds)
    for i in seeds:
      user = User.get_by_id(getEmail(i))
      assert checkEmail(i, user.email)
      i = i + offset
      assert i == user.seed
      assert checkDescription(i, user.description)
      assert checkNumClicks(i, user.numClicks)
      assert checkIsAdmin(i, user.isAdmin)
      assert checkCreated(i, user.created)
      assert checkUpdated(i, user.updated)
      assert checkBorn(i, user.born)
      assert checkBecomesSmart(i, user.becomesSmart)
      assert checkWeight(i, user.weight)

  def threadFunc2(randomAccess, offset):
    seeds = range(n)
    if randomAccess:
      random.shuffle(seeds)
    for i in seeds:
      user = User.get_by_id(getEmail(i))
      i = i + offset
      user.seed = i
      user.description = getDescription(i)
      user.numClicks = getNumClicks(i)
      user.isAdmin = getIsAdmin(i)
      user.created = getCreated(i)
      user.updated = getUpdated(i)
      user.born = getBorn(i)
      user.becomesSmart = getBecomesSmart(i)
      user.weight = getWeight(i)
      user.put()

  def threadFunc3(randomAccess, offset):
    seeds = range(n)
    if randomAccess:
      random.shuffle(seeds)
    for i in seeds:
      user = User.get_by_id(getEmail(i))
      assert checkEmail(i, user.email)
      i = i + offset
      assert i == user.seed
      assert checkDescription(i, user.description)
      assert checkNumClicks(i, user.numClicks)
      assert checkIsAdmin(i, user.isAdmin)
      assert checkCreated(i, user.created)
      assert checkUpdated(i, user.updated)
      assert checkBorn(i, user.born)
      assert checkBecomesSmart(i, user.becomesSmart)
      assert checkWeight(i, user.weight)

  def threadFunc4(offset):
    q = User.query()
    all = [u for u in q.iter()]
    assert len(all) == n
    all.sort(key=lambda u: u.seed)
    seeds = range(n)
    for i in seeds:
      user = all[i]
      assert checkEmail(i, user.email)
      i = i + offset
      assert i == user.seed
      assert checkDescription(i, user.description)
      assert checkNumClicks(i, user.numClicks)
      assert checkIsAdmin(i, user.isAdmin)
      assert checkCreated(i, user.created)
      assert checkUpdated(i, user.updated)
      assert checkBorn(i, user.born)
      assert checkBecomesSmart(i, user.becomesSmart)
      assert checkWeight(i, user.weight)

  print "Reading datastore, with %d threads" % (nThreads)
  threads = []
  for i in range(nThreads):
    t = threading.Thread(target=threadFunc1, args=[False, 10])
    threads.append(t)
    t.start()
  for t in threads:
    t.join()

  print "Changing datastore, with %d threads" % (nThreads)
  threads = []
  for i in range(nThreads):
    t = threading.Thread(target=threadFunc2, args=[False, 20])
    threads.append(t)
    t.start()
  for t in threads:
    t.join()

  print "Reading datastore, with %d threads" % (nThreads)
  threads = []
  for i in range(nThreads):
    t = threading.Thread(target=threadFunc3, args=[False, 20])
    threads.append(t)
    t.start()
  for t in threads:
    t.join()

  print "Querying datastore, with %d threads" % (nThreads)
  threads = []
  for i in range(nThreads):
    t = threading.Thread(target=threadFunc4, args=[20])
    threads.append(t)
    t.start()
  for t in threads:
    t.join()

  print "Reading datastore, with %d threads, random access" % (nThreads)
  threads = []
  for i in range(nThreads):
    t = threading.Thread(target=threadFunc1, args=[True, 20])
    threads.append(t)
    t.start()
  for t in threads:
    t.join()

  print "Changing datastore, with %d threads, random access" % (nThreads)
  threads = []
  for i in range(nThreads):
    t = threading.Thread(target=threadFunc2, args=[True, 20])
    threads.append(t)
    t.start()
  for t in threads:
    t.join()

  print "Reading datastore, with %d threads, random access" % (nThreads)
  threads = []
  for i in range(nThreads):
    t = threading.Thread(target=threadFunc3, args=[True, 20])
    threads.append(t)
    t.start()
  for t in threads:
    t.join()

  print "Deleting from datastore"
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    user = User.get_by_id(getEmail(i))
    if i % 3 == 0:
      user.delete()
  print ""

  print "Reading datastore"
  for i in range(n):
    if i % 1000 == 0:
      sys.stdout.write(".")
      sys.stdout.flush()
    expectError = i % 3 == 0
    try:
      user = User.get_by_id(getEmail(i))
      assert not expectError
    except:
      assert expectError
      continue
    assert checkEmail(i, user.email)
    i = user.seed
    assert checkDescription(i, user.description)
    assert checkNumClicks(i, user.numClicks)
    assert checkIsAdmin(i, user.isAdmin)
    assert checkCreated(i, user.created)
    assert checkUpdated(i, user.updated)
    assert checkBorn(i, user.born)
    assert checkBecomesSmart(i, user.becomesSmart)
    assert checkWeight(i, user.weight)
  print ""

  print "Inspecting datastore"
  kinds = ndb.getDatastore().get_kinds()
  kindsRef = ["Message", "User"]
  assert kinds == kindsRef

  print "Test completed."

def dir_size(path):
  total_size = 0
  for dirpath, dirnames, filenames in os.walk(path):
    for f in filenames:
      fp = os.path.join(dirpath, f)
      st = os.stat(fp)
      du = st.st_blocks * st.st_blksize
      total_size += du # os.path.getsize(fp)
  return total_size

useProfiler = False
n = 10000
nThreads = 4
startTime = None

oldArgv = sys.argv

stats = {}
for datastore in ["memory", "leveldb", "lmdb", "bdb"]:
  print "================================================"
  print "Testing datastore", datastore
  path = "/tmp/ndb-test-" + str(os.urandom(16).encode("hex"))
  print "Creating datastore in", path
  try:
    shutil.rmtree(path)
  except:
    pass
  sys.argv = [oldArgv[0],
              "--datastore_type", datastore,
              "--datastore_path", path]

  time1 = (datetime.datetime.now() - datetime.datetime.utcfromtimestamp(0)).total_seconds()

  import ndb
  if ndb.getDatastore() is None:
    ndb.initDatastoreFromParams()

  if useProfiler:
    profile.run("runTest()")
  else:
    runTest()

  ndb.closeDatastore()

  time2 = (datetime.datetime.now() - datetime.datetime.utcfromtimestamp(0)).total_seconds()
  size = dir_size(path)

  print "Deleting datastore from", path
  try:
    shutil.rmtree(path)
  except:
    pass

  print "Test time:", "{0:,.2f}".format(time2 - time1), "seconds."
  print "Disk usage:", "{:,}".format(size), "bytes."
  stats[datastore] = {"time": time2 - time1, "disk": size}

print "================================================"
print "Summary"
for datastore in stats:
  print ""
  print "Datastore:", datastore
  print "Test time:", "{0:,.2f}".format(stats[datastore]["time"]), "seconds."
  print "Disk usage:", "{:,}".format(stats[datastore]["disk"]), "bytes."
