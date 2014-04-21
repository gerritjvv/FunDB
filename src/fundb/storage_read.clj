(ns fundb.storage-read

  (:require [fundb.storage :refer [get-table get-index get-data-cache]]
            [fundb.veb :as veb]
            [fundb.snappy.core :as sn-util]
            [clojure.core.cache :as cache])
  (:import  [java.io RandomAccessFile]
            [java.nio ByteBuffer MappedByteBuffer]
            [java.nio.channels FileChannel FileChannel$MapMode]
            [org.xerial.snappy Snappy SnappyCodec]))

;module related to reading data from tables

;cached data, all data for files are cached during the first read,
;


(defn
  read-file
  "Read and uncompress the file and return a vector of messages of type map with keys pos, size, i, buff"
  [file]
  (let [^RandomAccessFile raf (RandomAccessFile. (clojure.java.io/file file) "r")
        ^FileChannel ch (.getChannel raf)
        ^MappedByteBuffer source-bb (.map ch FileChannel$MapMode/READ_ONLY 0 (.size ch))]
    (try
      (let [header (sn-util/read-header source-bb)]
        (prn "header " header " header bytes : " (map str (:magic-header header)))
        (loop [state [] i 0]
          (if (> (.remaining source-bb) 0)
            (let [[messages pos] (sn-util/read-message-meta (sn-util/read-snappy-block source-bb) state i)]
              (recur (into state messages) (long pos)))
            state)))
      (finally
       (.close ch)
       (.close raf)))))


(defn load-file-array [file]
  ; read the file and decompress
  ;read in each message and assign to position n a tuple [start-pos end-pos]
  (let [messages (read-file file)]
    {:messages messages :file file}))

(defn read-from-source [cache {:keys [file i]}]
  (let [c (dosync
           (alter cache
                  (fn [m]
                    (cache/through (fn [_]
                               (load-file-array file)) m (str file)))))
        file-array (cache/lookup c file)]))





(defn get-data [db-name table-name k]
  (let [ind (get-index db-name table-name)]
    (if-let [ data (veb/veb-data ind k)]
      (read-from-source (get-data-cache db-name table-name) data))))



