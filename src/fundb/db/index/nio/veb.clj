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
  [^ByteBuf bb]
  (.getInt bb 6))

(defn- ^ByteBuf put-init-cluster!
  "Write a (* sqrt (+ 4 2)) byte array to the buff starting at the current position
   Returns the buff"
  [^ByteBuf buff ^Long u]
  (if (> u 2)
    (.writeBytes buff (byte-array (* (vutils/upper-sqrt u) (+ 4 2)) (byte INIT_CLUSTER_REF)))
    buff))

(defn ^String read-header [^ByteBuf buff]
  (let [^"[B" bts (byte-array 5)]
    (.getBytes buff 0 bts)
    (String. bts "UTF-8")))

(defn ^ByteBuffer write-header
  "Note: does not alter the bb position"
  [^ByteBuf buff]
  (.setBytes buff 0 INDEX_HEADER))

(defn ^ByteBuffer write-node!!
  "Move the cursor of the buffer
   @param buff ByteBuffer
   @param Node
   @param position in the buffer
   @return ByteBuffer"
  [^ByteBuf buff {:keys [^Long u ^Long min ^Long min-data ^Long max] :as node}]
  {:pre [buff (number? u) (number? min) (number? min-data) (number? max)
         (>= (.readableBytes buff) (+ 33 (cluster-byte-size u)))]}
  (doto buff
    (.writeByte (byte NOT_DELETED))
    (.writeLong u)
    (.writeLong min)
    (.writeLong min-data)
    (.writeLong max)
    (put-init-cluster! u)
    ))

(defn write-version
  "Note: this function does not alter the bb position"
  [^ByteBuf bb]
  (.setByte bb 5 (byte VERSION)))

(defn ^Long read-version
  "Note: this function does not alter the bb position"
  [^ByteBuf bb]
  (.getByte bb 5))

(defn ^Node read-node
  "@param buff ByteBuffer performs a slice on the buffer before reading
   @return Node"
  [^ByteBuf buff]
  (let [^ByteBuf buff2 (.slice buff)]
    (->Node (= (.readByte buff2) DELETED)                                 ;deleted
            (.readLong buff2)                                ;u
            (.readLong buff2)                                ;min
            (.readLong buff2)                                ;min-data
            (.readLong buff2)                                ;max
            (.readerIndex buff)                                ;cluster-pos
            )))

(defn read-deleted
  "Returns the deleted flag 0 == is deleted 1 == not
   @param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (= (.getByte buff pos) DELETED))

(defn read-u
  [^ByteBuf buff ^Long pos]
  (.getLong buff (inc pos)))

(defn ^Long read-min
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (.getLong buff (+ pos 8 1)))

(defn ^Long read-max
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (.getLong buff (+ pos 8 8 8 1)))

(defn ^Long read-min-data
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (.getLong buff (+ pos 8 8 1)))

(defn ^Long read-cluster-ref
  "Return a cluster reference from the node pos and based on the integer i, the value returned is always an Int
   @param buff ByteBuffer
   @param pos Long node position
   @param i cluster index
   @return Long"
  [^ByteBuf buff ^Long pos ^Long i]
  ;remember a cluster ref is 4 byte index 2 bytes (short) file index
  (.getInt buff (+ pos 8 8 8 8 1 (* i 6))))


(defn ^Long read-cluster-file-index
  "Return a value of type Short that represents the file index value
   @param buff ByteBuffer
   @param pos Long node position
   @param i cluster index
   @return Short cluster's file index"
  [^ByteBuf buff ^Long pos ^Long i]
  ;remember a cluster ref is 4 byte index 2 bytes (short) file index
  (.getShort buff (+ pos 8 8 8 8 1 4 (* i 6))))

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

(defn- ^Long init-file-size [u]
  (+ (count INDEX_HEADER) 1 4 (node-byte-size u)))

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
      (.writerIndex bb 10)
      (write-node!! bb (->Node NOT_DELETED u -1 -1 -1 -1))
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
