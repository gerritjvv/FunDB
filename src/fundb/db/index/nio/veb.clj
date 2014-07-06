(ns fundb.db.index.nio.veb
  (:require [fundb.veb-utils :as vutils])
  (:import [java.nio MappedByteBuffer ByteBuffer Buffer]))

(def ^Long VERSION 1)
(def ^Long NOT_DELETED 0)
(def ^Long DELETED 1)
(def ^Long INIT_CLUSTER_REF -1)

;cluster-pos the position at which the cluster for the node starts
(defrecord Node [deleted ^Long u ^Long min ^Long min-data ^Long max ^Long cluster-pos])

(defn- ^ByteBuffer put-init-cluster
  "Write a (* sqrt (+ 4 2)) byte array to the buff starting at the current position
   Returns the buff"
  [^ByteBuffer buff ^Long u]
  (let [^Long sqrt (vutils/upper-sqrt u)]
    (if (> sqrt 2)
      (.put buff (byte-array (* sqrt (+ 4 2)) (byte INIT_CLUSTER_REF)))
      buff)))

(defn ^String read-header [^ByteBuffer buff]
  (let [bts (byte-array 5)]
    (String. ^"[B" (.get buff bts))))

(defn ^ByteBuffer write-node!!
  "Move the cursor of the buffer
   @param buff ByteBuffer
   @param Node
   @param position in the buffer
   @return ByteBuffer"
  [^ByteBuffer buff {:keys [^Long u ^Long min ^Long min-data ^Long max] :as node}]
  {:pre [buff (number? u) (number? min) (number? min-data) (number? max)
         (>= (.remaining buff) 33)]}
  (doto buff
    (.put (byte NOT_DELETED))
    (.putLong u)
    (.putLong min)
    (.putLong min-data)
    (.putLong max)
    (put-init-cluster u)
    ))


(defn ^ByteBuffer safe-write-node
  "@param buff ByteBuffer must be at position zero
   @param pos Long position to start writing
   @return ByteBuffer"
  [^ByteBuffer buff node ^Long pos]
  {:pre [(= (.position buff) 0)]}
  (doto buff
    .slice
    (.position (int pos))
    (write-node!! node)))

(defn ^Node read-node
  "@param buff ByteBuffer performs a slice on the buffer before reading
   @return Node"
  [^ByteBuffer buff]
  (let [^ByteBuffer buff2 (.slice buff)]
    (->Node (= (.get buff2) DELETED)                                 ;deleted
            (.getLong buff2)                                ;u
            (.getLong buff2)                                ;min
            (.getLong buff2)                                ;min-data
            (.getLong buff2)                                ;max
            (.position buff)                                ;cluster-pos
            )))

(defn read-deleted
  "Returns the deleted flag 0 == is deleted 1 == not
   @param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuffer buff ^Long pos]
  (= (.get buff) DELETED))

(defn read-u
  [^ByteBuffer buff ^Long pos]
  (.getLong buff (inc pos)))

(defn ^Long read-min
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuffer buff ^Long pos]
  (.getLong buff (+ pos 8 1)))

(defn ^Long read-max
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuffer buff ^Long pos]
  (.getLong buff (+ pos 8 8 8 1)))

(defn ^Long read-min-data
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuffer buff ^Long pos]
  (.getLong buff (+ pos 8 8 1)))

(defn ^Long read-cluster-pos
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuffer buff ^Long pos]
  (.getLong buff (+ pos 8 8 8 8 1)))

(defn ^Long read-cluster-reaf
  "Return a cluster reference from the node pos and based on the integer i
   @param buff ByteBuffer
   @param pos Long node position
   @param i cluster index
   @return Long"
  [^ByteBuffer buff ^Long pos ^Long i]
  ;remember a cluster ref is 4 byte index 2 bytes (short) file index
  (.getLong buff (+ pos 8 8 8 8 1 (* i 6))))


(defn ^Long cluster-byte-size
  "@param n the number of items in the cluster e.g sqrt(u)
   @return The number of bytes required by the cluster which is (* n 6)"
  [^Long n]
  (* n 6))