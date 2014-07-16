(ns fundb.db.index.nio.veb
  (:require [fundb.veb-utils :as vutils]
            [clojure.java.io :as io])
  (:import
    [java.io File RandomAccessFile]
    [java.nio MappedByteBuffer ByteBuffer Buffer]
    [io.netty.buffer ByteBuf Unpooled]
    (java.nio.channels FileChannel FileChannel$MapMode)))

(def ^Long VERSION 1)
(def ^Long NOT_DELETED 0)
(def ^Long DELETED 1)
(def ^Long INIT_CLUSTER_REF -1)

;Bytes
;Description
;5
;FUNDB header
;1
;Index version
;4
;Free position pointer i.e points to the index of the next free index
;82 bytes
;Root Node
;82 | 60
;Next node | Cluster Ref Segment
;[1 byte of which  the first bit is a tombstone flag bit 1 == deleted][8 bytes u][8 bytes min][8 bytes min-data][8 bytes max]

(def ^"[B" INDEX_HEADER (.getBytes (String. "FUNDB")))

;cluster-pos the position at which the cluster for the node starts
(defrecord Node [deleted ^Long u ^Long min ^Long min-data ^Long max ^Long cluster-pos])
(defrecord Index [^Long u ^ByteBuf buff ^MappedByteBuffer mbuff ^FileChannel file-channel])

(declare cluster-byte-size)

(defn ^ByteBuf write-position-pointer
  "Note: this function does not alter the bb position"
  [^ByteBuf bb ^Long pointer]
  (.setInt bb 6 (int pointer)))

(defn ^Long read-position-pointer
  "Note: this function does not alter the bb position"
  ([^ByteBuf bb]
   (.getInt bb 6)))

(defn- ^ByteBuf put-init-cluster!
  "Write a (* sqrt (+ 4 2)) byte array to the buff starting at the current position
   Returns the buff"
  [^ByteBuf buff ^Long u]
  (if (> u 2)
    (.writeBytes buff (byte-array (* (vutils/upper-sqrt u) (+ 4 2)) (byte INIT_CLUSTER_REF)))
    buff))

(defn- ^ByteBuf put-init-cluster
  "Write a (* sqrt (+ 4 2)) byte array to the buff
   Returns the buff"
  [^ByteBuf buff ^Long pos ^Long u]
  (if (> u 2)
    (.setBytes buff ^Long (+ pos 33) (byte-array (* (vutils/upper-sqrt u) (+ 4 2)) (byte INIT_CLUSTER_REF)))
    buff))


(defn ^String read-header [^ByteBuf buff]
  (let [^"[B" bts (byte-array 5)]
    (.getBytes buff 0 bts)
    (String. bts "UTF-8")))

(defn ^ByteBuffer write-header
  "Note: does not alter the bb position"
  [^ByteBuf buff]
  (.setBytes buff 0 INDEX_HEADER))

(defn write-version
  "Note: this function does not alter the bb position"
  [^ByteBuf bb]
  (.setByte bb 5 (byte VERSION)))

(defn ^Long read-version
  "Note: this function does not alter the bb position"
  [^ByteBuf bb]
  (.getByte bb 5))

(defn read-deleted
  "Returns the deleted flag 0 == is deleted 1 == not
   @param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (= (.getByte buff pos) DELETED))

(defn read-u
  [^ByteBuf buff ^Long pos]
  (.getLong buff (inc pos)))

(defn ^Long write-u
  [^ByteBuf buff ^Long pos ^Long u]
  (.setLong buff (inc pos) u))


(defn ^Long read-min
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (.getLong buff (+ pos 8 1)))

(defn ^ByteBuf write-min
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos ^Long v-min]
  (.setLong buff (+ pos 8 1) v-min)
  buff)

(defn ^Long read-max
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (.getLong buff (+ pos 8 8 8 1)))

(defn ^ByteBuf write-max
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos ^Long v-max]
  (.setLong buff (+ pos 8 8 8 1) v-max))

(defn ^Long read-min-data
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (.getLong buff (+ pos 8 8 1)))

(defn ^ByteBuf write-min-data
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos ^Long data]
  (.setLong buff (+ pos 8 8 1) data))

(defn ^Long read-cluster-ref
  "Return a cluster reference from the node pos and based on the integer i, the value returned is always an Int
   @param buff ByteBuffer
   @param pos Long node position
   @param i cluster index
   @return Long"
  [^ByteBuf buff ^Long pos ^Long i]
  ;remember a cluster ref is 4 byte index 2 bytes (short) file index
  ;(prn "read-cluster-ref pos: " (+ pos 8 8 8 8 1 (* i 6)) "; i: " i "; r: " (.getInt buff (+ pos 8 8 8 8 1 (* i 6))))
  (.getInt buff (+ pos 8 8 8 8 1 (* i 6))))


(defn ^ByteBuf write-cluster-ref
  "Write a cluster's ref and file index
   @param buff ByteBuffer
   @param pos Long node position
   @param i cluster index
   @param r the cluster's ref
   @param f-i the cluster's file index
   @return ByteBuffer"
  [^ByteBuf buff ^Long pos ^Long i ^Long r ^Long f-i]
  ;remember a cluster ref is 4 byte index 2 bytes (short) file index
  (let [pos1 (+ pos 8 8 8 8 1 (* i 6))]
    ;(prn "write-cluster-ref pos: " pos1 "; i: " i  "; r: " r)
    (doto buff
      (.setInt pos1 (int r))
      (.setShort (+ pos1 4) (short f-i)))))


(defn ^Long read-cluster-file-index
  "Return a value of type Short that represents the file index value
   @param buff ByteBuffer
   @param pos Long node position
   @param i cluster index
   @return Short cluster's file index"
  [^ByteBuf buff ^Long pos ^Long i]
  ;remember a cluster ref is 4 byte index 2 bytes (short) file index
  (.getShort buff (+ pos 8 8 8 8 1 4 (* i 6))))


(defn ^Long cluster-byte-size
  "@param n the number of items in the cluster e.g sqrt(u)
   @return The number of bytes required by the cluster which is (* n 6)"
  [^Long n]
  (* n 6))

(defn ^Long node-byte-size [u]
  (+ 33 (cluster-byte-size (vutils/upper-sqrt u))))

(defn ^Long init-file-size [u]
  (+ (count INDEX_HEADER) 1 4 (node-byte-size u)))

(defn ^ByteBuffer write-node
  "@param buff ByteBuffer
   @param Node
   @param position in the buffer
   @return ByteBuffer"
  [^ByteBuf buff pos {:keys [^Long u ^Long min ^Long min-data ^Long max] :as node}]
  (.setByte buff pos (byte NOT_DELETED))
  (write-u buff pos u)
  (write-min buff pos min)
  (write-min-data buff pos min-data)
  (write-max buff pos max)
  (put-init-cluster buff pos u)
  buff)


(defn ^Node read-node
  ""
  [^ByteBuf buff pos]
  (->Node
      (read-deleted buff pos)
      (read-u buff pos)
      (read-min buff pos)
      (read-min-data buff pos)
      (read-max buff pos)
      pos))

;@TODO create the index file, write headder, version and the root node
;@TODO test get set position pointer
(defn create-index
  "Create a new file and write the index header and root node"
  [f u]
  (with-open [^FileChannel ch (-> f io/file (RandomAccessFile. "rw") .getChannel)]
    ;
    (let [^MappedByteBuffer mbb (.map ch FileChannel$MapMode/READ_WRITE 0 (init-file-size u))
          ^ByteBuf bb (Unpooled/wrappedBuffer mbb)]
      (write-header bb)
      (write-version bb)
      (write-position-pointer bb 0)
      (write-node bb 10 (->Node NOT_DELETED u -1 -1 -1 -1))
      (write-position-pointer bb (.writerIndex bb))
      (.force mbb)
      )))

;(defrecord Index [^Long u ^ByteBuf buff ^MappedByteBuffer mbuff ^FileChannel file-channel])
(defn load-index
  "Load the start information for the index to be read"
  [f]
  (let [^File file (io/file f)
        ^FileChannel ch (-> file (RandomAccessFile. "rw") .getChannel)
        ^MappedByteBuffer mbb (.map ch FileChannel$MapMode/READ_WRITE 0 (.length file))
        ^ByteBuf bb (Unpooled/wrappedBuffer mbb)
        header (read-header bb)
        version (read-version bb)]
    (assert (and (= header INDEX_HEADER) (= version VERSION)) (str "Wrong index version and or header version: " version  " head: " header))
    (Index. (read-u bb 10) bb mbb ch)))


(defn close-index!
  "Force edits and close the index"
  [{:keys [^MappedByteBuffer mbuff ^FileChannel file-channel]}]
  (.force mbuff)
  (.close file-channel))


(declare _insert!)

(defn- check-child-capacity
  "check if the current buffer has capacity for the child insert
   if not the file is resized and a new buffer is created"
  [{:keys [^ByteBuf buff ^FileChannel file-channel] :as index} pos ^Long position-pointer]
  (let [u (read-u buff pos)
        child-u (vutils/upper-sqrt u)
        bts-size (node-byte-size child-u)]
    (if (>= bts-size (- (.capacity buff) position-pointer))
      (let [mmap (.map file-channel FileChannel$MapMode/READ_WRITE 0 (+ (* 10 bts-size) (.capacity buff)))]

        (assoc index
          :mmap mmap
          :file-channel file-channel
          :buff (Unpooled/wrappedBuffer ^ByteBuffer mmap)))
      index)))

(defn- write-case-one
  "If k < min, overwrite current min with k and re-insert min"
  [{:keys [^ByteBuf buff] :as index} pos k data-id]
  (let [curr-min-data (read-min-data buff pos)
        v-min (read-min buff pos)]
    (write-min buff pos k)
    (write-min-data buff pos data-id)
    (_insert! index 10 v-min curr-min-data)))

(defn- write-case-two [{:keys [^ByteBuf buff] :as index} pos k data-id]
  (write-min buff pos k)
  (write-max buff pos k)
  (write-min-data buff pos data-id)
  index)

(defn- write-case-three [{:keys [^ByteBuf buff ^Long u] :as index} pos k data-id]
  (let [u (read-u buff pos)
        i (vutils/high u k)
        position-pointer (read-position-pointer buff)
        index2 (check-child-capacity index pos position-pointer)]
    (prn "case3 position-pointer: " position-pointer)
    (write-node (:buff index2) position-pointer {:u (vutils/upper-sqrt u) :min k :max k :min-data data-id})
    (write-cluster-ref (:buff index2) pos i position-pointer 1)
    (write-position-pointer (:buff index2) pos)

    index2))

(defn- write-case-four [{:keys [buff] :as index} pos k data-id]
  (let [u (read-u buff pos)]
    (prn "case-four u " u " k " k " low " (vutils/low u k))
    (_insert! index
              (read-cluster-ref buff pos (vutils/high u k))   ;get the next position from the cluster-ref
              (vutils/low u k)                              ;change k to low
              data-id)))

(defn _insert! [{:keys [^ByteBuf buff] :as index} ^Long pos ^Long k ^Long data-id]
  (let [^Long v-min (read-min buff pos)
        ^Long u (read-u buff pos)]
    (prn "pos " pos " k " k " v-min " v-min  " u " u)

    (cond
      (< k v-min)
      (write-case-one index pos k data-id)
      (= -1 v-min)
      (write-case-two index pos k data-id)
      (= -1 (read-cluster-ref buff pos (vutils/high u k)))
      (write-case-three index pos k data-id)
      :else
      (write-case-four index pos k data-id))))


(defn v-insert! [index k data-id]
  (assert (and (number? k) (number? data-id)))
  (_insert! index 10 k data-id))

(defn- _v-get [{:keys [buff] :as index} ^Long pos ^Long k]
  (let [v-min (read-min buff pos)
        u (read-u buff pos)]
    (cond
      (= k v-min)
      (read-min-data buff pos)
      (> k v-min)
      (let [^Long pos2 (read-cluster-ref buff pos (vutils/high u k))]
        (when (> pos2 -1)
          (_v-get index pos2 (vutils/low u k)))))))

(defn v-get
  "Returns the data-id at value k if it exists, otherwise nil"
  [index k]
  (when (and k (< k (read-u (:buff index) 10)))
    (_v-get index 10 k)))