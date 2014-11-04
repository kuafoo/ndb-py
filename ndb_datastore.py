#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Web: https://code.google.com/p/ndb-py
# License: GPLv2

# Interface for the datastore.
# Outside the module you should not care about the datastore, except for creating it. See the function setDatastore
# below.
class Datastore:
  def __init__(self):
    pass

  # Inserts a value with a given kind and key.
  # The kind is analogous to an SQL table.
  def set(self, kind, key, value):
    pass

  def get(self, kind, key):
    pass

  def delete(self, kind, key):
    pass

  # Iterates over the keys with a given kind.
  def iter(self, kind):
    pass

  def get_path(self):
    return None

  def close(self):
    pass

# In-memory, dictionary-based datastore.
class MemDatastore(Datastore):
  def __init__(self):
    self.data = {}

  def set(self, kind, key, value):
    if kind not in self.data:
      self.data[kind] = {}
    self.data[kind][key] = value

  def get(self, kind, key):
    if kind not in self.data:
      return None
    if key not in self.data[kind]:
      return None
    return self.data[kind][key]

  def delete(self, kind, key):
    if kind not in self.data:
      return
    if key not in self.data[kind]:
      return
    del self.data[kind][key]

  def iter(self, kind):
    if kind not in self.data:
      self.data[kind] = {}
    return MemDatastoreIterator(self, kind)


class MemDatastoreIterator:
  def __init__(self, datastore, kind):
    self.datastore = datastore
    self.kind = kind
    self.datastoreIterator = self.datastore.data[kind].__iter__()

  def __iter__(self):
    return self

  def next(self):
    return self.datastoreIterator.next()
