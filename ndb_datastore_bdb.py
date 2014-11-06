#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Web: https://code.google.com/p/ndb-py
# License: GPLv2

import bsddb
import json
import ndb_datastore
import os

# Datastore implemented on top of BerkeleyDB via the bsddb module.
class BDBDatastore(ndb_datastore.Datastore):
  def __init__(self, path="bsd.db"):
    self.path = path
    try:
      os.makedirs(self.path)
    except:
      pass
    self.db = bsddb.btopen(os.path.join(self.path, "datastore.db"), "c")

  def set(self, kind, key, value):
    kind = json.dumps(kind).encode("hex")
    key = json.dumps(key).encode("hex")
    value = json.dumps(value)
    self.db["%s %s" % (kind, key)] = value

  def get(self, kind, key):
    kind = json.dumps(kind).encode("hex")
    key = json.dumps(key).encode("hex")
    value = None
    try:
      value = self.db["%s %s" % (kind, key)]
    except:
      value = None
    if value is None:
      return None
    value = json.loads(value)
    return value

  def delete(self, kind, key):
    try:
      del self.db["%s %s" % (kind, key)]
    except:
      pass

  def iter(self, kind):
    return BDBDatastoreIterator(self, kind)

  def get_kinds(self):
    kinds = set()
    offset = ""
    while True:
      try:
        key, _ = self.db.set_location(offset)
        if (not key):
          break
        key = key.split(" ")[0]
        offset = key + "."
        kinds.add(json.loads(key.decode("hex")))
      except:
        break
    return sorted(list(kinds))

  def get_path(self):
    return self.path

  def close(self):
    self.db.close()
    self.db = None


class BDBDatastoreIterator:
  def __init__(self, datastore, kind):
    self.datastore = datastore
    kind = json.dumps(kind).encode("hex")
    self.kind = kind
    self.offset = self.kind + " "

  def __iter__(self):
    return self

  def next(self):
    try:
      key, _ = self.datastore.db.set_location(self.offset)
      if (not key) or (not key.startswith("%s " % self.kind)):
        raise StopIteration()
      self.offset = key + "."
    except:
      raise StopIteration()
    key = key.split(" ")[1]
    key = json.loads(key.decode("hex"))
    return key
