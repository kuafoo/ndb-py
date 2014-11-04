#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Web: https://code.google.com/p/ndb-py
# License: GPLv2

import json
import lmdb
import ndb_datastore

# Datastore implemented on top of OpenLDAP's LMDB.
class LMDBDatastore(ndb_datastore.Datastore):
  def __init__(self, path="lm.db"):
    self.path = path
    self.db = lmdb.open(self.path,
                        map_size=(100 * 1024 * 1024 * 1024),
                        metasync=False,
                        sync=False,
                        map_async=True,
                        writemap=True)

  def set(self, kind, key, value):
    kind = json.dumps(kind).encode("hex")
    key = json.dumps(key).encode("hex")
    value = json.dumps(value)
    with self.db.begin(write=True) as txn:
      txn.put("%s %s" % (kind, key), value)

  def get(self, kind, key):
    kind = json.dumps(kind).encode("hex")
    key = json.dumps(key).encode("hex")
    with self.db.begin(write=False) as txn:
      value = txn.get("%s %s" % (kind, key))
    if value is None:
      return None
    value = json.loads(value)
    return value

  def delete(self, kind, key):
    with self.db.begin(write=True) as txn:
      txn.delete("%s %s" % (kind, key))

  def iter(self, kind):
    return LMDBDatastoreIterator(self, kind)

  def get_path(self):
    return self.path

  def close(self):
    self.db.close()
    self.db = None

class LMDBDatastoreIterator:
  def __init__(self, datastore, kind):
    self.datastore = datastore
    kind = json.dumps(kind).encode("hex")
    self.kind = kind
    self.offset = "%s " % self.kind
    self.txn = self.datastore.db.begin(write=False)
    self.cursor = self.txn.cursor()
    self.cursor.set_range(self.offset)

  def __iter__(self):
    return self

  def next(self):
    if self.cursor is None:
      raise StopIteration()
    key = self.cursor.key()
    self.cursor.next()
    self.offset = self.cursor.key()
    if (not key) or (not key.startswith("%s " % self.kind)):
      self.txn.commit()
      self.txn = None
      self.cursor = None
      raise StopIteration()
    key = key.split(" ")[1]
    key = json.loads(key.decode("hex"))
    return key
