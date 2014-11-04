#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Web: https://code.google.com/p/ndb-py
# License: GPLv2

import json
import leveldb
import ndb_datastore

# Datastore implemented on top of LevelDB.
class LevelDBDatastore(ndb_datastore.Datastore):
  def __init__(self, path="level.db"):
    self.path = path
    self.db = leveldb.DB(self.path, create_if_missing=True)

  def set(self, kind, key, value):
    kind = json.dumps(kind).encode("hex")
    key = json.dumps(key).encode("hex")
    value = json.dumps(value)
    self.db.put("%s %s" % (kind, key), value)

  def get(self, kind, key):
    kind = json.dumps(kind).encode("hex")
    key = json.dumps(key).encode("hex")
    value = self.db.get("%s %s" % (kind, key))
    if value is None:
      return None
    value = json.loads(value)
    return value

  def delete(self, kind, key):
    self.db.delete("%s %s" % (kind, key))

  def iter(self, kind):
    return LevelDBDatastoreIterator(self, kind)

  def get_path(self):
    return self.path

  def close(self):
    self.db.close()
    self.db = None

class LevelDBDatastoreIterator:
  def __init__(self, datastore, kind):
    self.datastore = datastore
    kind = json.dumps(kind).encode("hex")
    self.kind = kind
    self.datastoreIterator = self.datastore.db.scope(self.kind).__iter__()

  def __iter__(self):
    return self

  def next(self):
    key = self.datastoreIterator.next().key.strip()
    key = json.loads(key.decode("hex"))
    return key
