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
  read-file-compressed-block
  "Read and uncompress the file and return a vector of messages of type map with keys pos, size, i, buff"
  [file]
  (let [^RandomAccessFile raf (RandomAccessFile. (clojure.java.io/file file) "r")
        ^FileChannel ch (.getChannel raf)
        ^MappedByteBuffer source-bb (.map ch FileChannel$MapMode/READ_ONLY 0 (.size ch))]
    (try
      (let [header (sn-util/read-header source-bb)]
        (sn-util/read-snappy-block source-bb)))))



(defn
  read-file
  "Read and uncompress the file and return a vector of messages of type map with keys pos, size, i, buff"
  [file]
  (let [^RandomAccessFile raf (RandomAccessFile. (clojure.java.io/file file) "r")
        ^FileChannel ch (.getChannel raf)
        ^MappedByteBuffer source-bb (.map ch FileChannel$MapMode/READ_ONLY 0 (.size ch))]
    (try
      (let [header (sn-util/read-header source-bb)]
        (loop [state [] i 0]
          (if (> (.remaining source-bb) 0)
            (let [
                  snappy-block (sn-util/read-snappy-block source-bb)

                  [messages pos] (sn-util/read-message-meta snappy-block state i)]
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
        {:keys [messages]} (cache/lookup c file)]
    (get messages i)))




(defn get-data
  "public function that gets a data value from a table indexed by k
   returns a map e.g {:pos 4, :size 6, :i 0, :buff #<DirectByteBuffer java.nio.DirectByteBuffer[pos=218 lim=218 cap=218]>}
   use des-bytes to get the actual value bytes"
  [db-name table-name k]
  (let [ind (get-index db-name table-name)]
    (if-let [ data (veb/veb-data ind k)]

      (read-from-source (get-data-cache db-name table-name) data)
      )))


(defn ^"[B" des-bytes
  "public helper function that reads the bytes from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
   [{:keys [pos size ^ByteBuffer buff]}]
    (let [bt (byte-array size)]
      (.get (doto buff .slice (.position (int pos))) bt 0 (int size))
      bt))

(defn ^Long des-int
  "public helper function that reads the int from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [{:keys [^ByteBuffer buff pos size]}]
  (if buff
    (let [^ByteBuffer temp-buff (doto (.slice buff) (.position (int pos)) (.limit (+ pos size)))]
      (.getInt temp-buff))))


(defn ^Long des-long
  "public helper function that reads the Long from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [{:keys [^ByteBuffer buff pos size]}]
  (if buff
    (let [^ByteBuffer temp-buff (doto (.slice buff) (.position (int pos)) (.limit (+ pos size)))]
      (.getLong temp-buff))))


(defn ^Double des-double
  "public helper function that reads the Long from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [{:keys [^ByteBuffer buff pos size]}]
  (if buff
    (let [^ByteBuffer temp-buff (doto (.slice buff) (.position (int pos)) (.limit (+ pos size)))]
      (.getDouble temp-buff))))


(defn ^Float des-float
  "public helper function that reads the Long from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [{:keys [^ByteBuffer buff pos size]}]
  (if buff
    (let [^ByteBuffer temp-buff (doto (.slice buff) (.position (int pos)) (.limit (+ pos size)))]
      (.getFloat temp-buff))))


(defn ^Byte des-byte
  "public helper function that reads the Long from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [{:keys [^ByteBuffer buff pos size]}]
  (if buff
    (let [^ByteBuffer temp-buff (doto (.slice buff) (.position (int pos)) (.limit (+ pos size)))]
      (.get temp-buff))))


(defn des-short
  "public helper function that reads the Long from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [{:keys [^ByteBuffer buff pos size]}]
  (if buff
    (let [^ByteBuffer temp-buff (doto (.slice buff) (.position (int pos)) (.limit (+ pos size)))]
      (.getShort temp-buff))))


(defn des-char
  "public helper function that reads the Long from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [{:keys [^ByteBuffer buff pos size]}]
  (if buff
    (let [^ByteBuffer temp-buff (doto (.slice buff) (.position (int pos)) (.limit (+ pos size)))]
      (.getChar temp-buff))))

(defn ^Boolean des-bool
  "public helper function that reads the Long from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [{:keys [^ByteBuffer buff pos size]}]
  (if buff
    (let [^ByteBuffer temp-buff (doto (.slice buff) (.position (int pos)) (.limit (+ pos size)))]
      (= 1 (.getByte temp-buff)))))

(defn ^String des-str
  "public helper function that reads a UTF-8 String from a message, this function takes the ByteBuffer and reads the bytes into
   a byte array copying the data into the Java Heap"
  [data]
  (String. (des-bytes data) "UTF-8"))
