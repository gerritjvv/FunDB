(ns fundb.db.index.veb-storage
  (:import (java.io DataInputStream FileInputStream))
  "Load and store veb indexes to disk"
  (:require [taoensso.nippy :as nippy]
            [clojure.java.io :as io])
  (:import [java.io DataInputStream FileInputStream]))


(defn load-index
  "From a file load the VEB index using nippy"
  [file]
  (io!
    (-> file io/input-stream nippy/thaw-from-in!)))

(defn save-index
  "Saves the VEB index using nippy"
  [file]
  (io!
    (-> file io/output-stream nippy/freeze-to-out!)))
