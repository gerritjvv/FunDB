(ns fundb.db.index.nio.veb
  "Reading and writing a VEB Index to a File using FileChannel MappedByteBuffer"
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

(defonce ^Long PAGE_SIZE Integer/MAX_VALUE)

;cluster-pos the position at which the cluster for the node starts
(defrecord Node [deleted ^Long u ^Long min ^Long min-data ^Long max ^Long cluster-pos])
(defrecord Index [^Long u pages ^FileChannel file-channel])

(declare cluster-byte-size)

(defn- ^Long calc-page [^Long pos]
  (long (Math/floor (/ pos PAGE_SIZE))))

(defn ^Long relative-pos
  "Pos spans multiple pages this function returns the relative position inside a page"
  [pos]
  (mod pos PAGE_SIZE))

(defn- ^ByteBuf load-page-buff [^FileChannel file-channel ^Long page]
  (let [size (.size file-channel)
        ^Long start (* page PAGE_SIZE)
        pos-end (> size (+ start PAGE_SIZE)) PAGE_SIZE size
        mmap (.map file-channel FileChannel$MapMode/READ_WRITE start pos-end)]
    [(Unpooled/wrappedBuffer mmap) mmap]))

(defn get-page-buff
  "May modify the index so that the new index is returned as the second parameter in the return tuple
   Returns [^ByteBuf buff index]"
  [{:keys [pages file-channel] :as index} ^Long pos]
  (let [page (calc-page pos)
        page-buff (first (get pages page))]
    (if page-buff
      [page-buff index]
      (let [v (load-page-buff file-channel page)]
        [(first v) (assoc pages page v)]))))


(defn ^ByteBuf write-position-pointer
  "Note: this function does not alter the bb position"
  [^ByteBuf bb ^Long pointer]
  (.setInt bb 6 (int pointer)))

(defn ^ByteBuf write-index-position-pointer
  "Note: this function does not alter the bb position"
  [index ^Long pointer]
  (write-position-pointer (first (get-page-buff index 0)) pointer))

(defn ^Long read-position-pointer
  "Note: this function does not alter the bb position"
  ([^ByteBuf bb]
   (.getInt bb 6)))

(defn ^Long read-index-position-pointer [index]
  (read-position-pointer (first (get-page-buff index 0))))

(defn- ^ByteBuf put-init-cluster!
  "Write a (* sqrt (+ 4 2)) byte array to the buff starting at the current position
   Returns the buff"
  [^ByteBuf buff ^Long u]
  (if (> u 2)
    (.writeBytes buff (byte-array (* (Math/ceil (vutils/veb-sqrt u)) (+ 4 2)) (byte INIT_CLUSTER_REF)))
    buff))

(defn- ^ByteBuf put-init-cluster
  "Write a (* sqrt (+ 4 2)) byte array to the buff
   Returns the buff"
  [^ByteBuf buff ^Long pos ^Long u]
  (if (> u 2)
    (.setBytes buff ^Long (+ pos 41) (byte-array (* (Math/ceil (vutils/veb-sqrt u)) (+ 4 2)) (byte INIT_CLUSTER_REF)))
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


(defn ^ByteBuf write-max-data
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos ^Long v-max]
  (.setLong buff (+ pos 8 8 8 8 1) v-max))


(defn ^Long read-max-data
  "@param buff ByteBuffer
   @param pos Long node position"
  [^ByteBuf buff ^Long pos]
  (.getLong buff (+ pos 8 8 8 8 1)))

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
  (prn "read-cluster-ref: u: " (read-u buff pos) ", pos: " pos "[" i "]")
  ;(if (= i 7 ) (throw (RuntimeException. "FUCK")))
  ;remember a cluster ref is 4 byte index 2 bytes (short) file index
  ;(prn "read-cluster-ref pos: " (+ pos 8 8 8 8 1 (* i 6)) "; i: " i "; r: " (.getInt buff (+ pos 8 8 8 8 1 (* i 6))))
  (let [u (read-u buff pos)]
    (if (or (zero? i) (< 0 i (Math/ceil (vutils/veb-sqrt u))))
      (.getInt buff (+ pos 8 8 8 8 1 (* i 6)))
      (throw (IndexOutOfBoundsException. (str "The cluster index " i " is not in range 0 <= u < " (vutils/upper-sqrt u)))))))

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
  (+ 41 (cluster-byte-size (vutils/upper-sqrt u))))

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
  (write-max-data buff pos -1)

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

(defn create-index
  "Create a new file and write the index header and root node"
  [f u]
  (with-open [^FileChannel ch (-> f io/file (RandomAccessFile. "rw") .getChannel)]
    ;
    (let [^MappedByteBuffer mbb (.map ch FileChannel$MapMode/READ_WRITE 0 (init-file-size u))
          ^ByteBuf buff (Unpooled/wrappedBuffer mbb)]
      (write-header buff)
      (write-version buff)
      (write-position-pointer buff 10)
      (write-node buff 10 (->Node NOT_DELETED u -1 -1 -1 -1))
      (write-position-pointer buff (+ 10 (node-byte-size u)))
      (.force mbb)
      )))

;(defrecord Index [^Long u pages ^FileChannel file-channel])
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
    (Index. (read-u bb 10) {0 [bb mbb]} ch)))


(defn close-index!
  "Force edits and close the index"
  [{:keys [^FileChannel file-channel pages]}]
  (doseq [[_ [_ ^MappedByteBuffer mmap]] pages]
    (.force mmap))
  (.close file-channel))


(declare _insert!)

(defn- within-page-size [size]
  (if (> size PAGE_SIZE) PAGE_SIZE size))

(defn- check-child-capacity
  "check if the current buffer has capacity for the child insert
   if not the file is resized and a new buffer is created"
  [{:keys [pages ^FileChannel file-channel] :as index} pos ^Long position-pointer]
  (prn " >>>>>>>>>>>>>>>>> increase capacity")
  (let [[buff index2] (get-page-buff index pos)
        rel-pos (relative-pos pos)
        u (read-u buff rel-pos)
        child-u (vutils/upper-sqrt u)
        bts-size (node-byte-size child-u)]

    (cond
      (> (+ bts-size (.capacity buff)) PAGE_SIZE)           ;safety check that we never overflow the current buffer
      (throw (RuntimeException. (str "The bts-size[ " bts-size " ] + buff.capacity[ " (.capacity buff) " ] cannot be bigger than PAGE_SIZE[ "  PAGE_SIZE " ]")))
      (>= bts-size (- (.capacity buff) (relative-pos position-pointer)))
      (let [mmap (.map file-channel FileChannel$MapMode/READ_WRITE 0 (within-page-size (+ (* 10 bts-size) (.capacity buff))))]
        (assoc-in
          (assoc index2 :file-channel file-channel)
          [:pages (calc-page pos)]
          [(Unpooled/wrappedBuffer ^ByteBuffer mmap) mmap]))
      :else index2)))

(defn- write-case-one
  "If k < min, overwrite current min with k and re-insert min"
  [index pos k data-id]
  (let [[^ByteBuf buff index2] (get-page-buff index pos)
        curr-min-data (read-min-data buff pos)
        v-min (read-min buff pos)]
    (write-min buff pos k)
    (write-min-data buff pos data-id)
    (_insert! index2 10 v-min curr-min-data)))

(defn- write-case-two [index pos k data-id]
  (let [[^ByteBuf buff index2] (get-page-buff index pos)]
    (write-min buff pos k)
    (write-max buff pos k)
    (write-min-data buff pos data-id)
    index2))

(defn- boundy-aware-position-pointer
  "If the pointer's relative pos + size does not fit within the page, a pointer will be returned to the next
   starting point of the next page, otherwise the same pointer is returned"
  [pointer ^Long size]
  (let [rel-pos (relative-pos pointer)]
    (if (> (+ rel-pos size) PAGE_SIZE) (+ pointer (- PAGE_SIZE rel-pos)) pointer)))

(defn- write-case-three [index pos k data-id]
  (let [[buff index2] (get-page-buff index pos)
        rel-pos (relative-pos pos)
        u (read-u buff rel-pos)
        child-u (vutils/upper-sqrt u)
        i (vutils/high u k)
        low (vutils/low u k)
        node-bts (node-byte-size child-u)
        position-pointer (boundy-aware-position-pointer (read-index-position-pointer index2) node-bts)
        updated-pointer (+ position-pointer node-bts)
        [child-buff index3] (get-page-buff (check-child-capacity index pos position-pointer) position-pointer) ;get the buff to insert the child

        ]

    (write-node child-buff (relative-pos position-pointer) {:u child-u :min low :max low :min-data data-id})
    (write-cluster-ref buff rel-pos i position-pointer -1)
    (write-index-position-pointer index3 updated-pointer)
    index3))

(defn- write-case-four [index pos k data-id]
  (let [[buff index2] (get-page-buff index pos)
        rel-pos (relative-pos pos)
        u (read-u buff rel-pos)]
    (prn "case four read-cluster-ref " (read-cluster-ref buff rel-pos (vutils/high u k)))
    (_insert! index2
              (read-cluster-ref buff rel-pos (vutils/high u k))   ;get the next position from the cluster-ref
              (vutils/low u k)                                      ;change k to low
              data-id)))

(defn- write-case-five [index pos k data-id]
  (let [[buff index2] (get-page-buff index pos)
        rel-pos (relative-pos pos)
        v-max (read-max buff rel-pos)]
    ;note this expects u == 2 and v-min > -1
    (if (> k v-max)
      (do
        (write-max buff rel-pos k)
        (write-max-data buff rel-pos data-id)
        index2)
      (throw (Exception. (str "No space in index for u " (read-u buff rel-pos) " pos " pos " k " k))))))

(defn _insert! [index ^Long pos ^Long k ^Long data-id]
  (let [[buff index2] (get-page-buff index pos)
        rel-pos (relative-pos pos)
        ^Long v-min (read-min buff rel-pos)
        ^Long u (read-u buff rel-pos)]
    (prn "insert u: "  u " k: " k)
    (cond
      (or (= -1 v-min) (= k v-min))                         ;overwrite if equal
      (write-case-two index2 pos k data-id)
      (< k v-min)
      (write-case-one index2 pos k data-id)
      :else
      (if (> u 2)
        (cond
          (= -1 (read-cluster-ref buff rel-pos (vutils/high u k)))
          (write-case-three index2 pos k data-id)
          :else
          (write-case-four index2 pos k data-id))
        (write-case-five index2 pos k data-id)))))


(defn v-insert!
  "Insert k with data data-id into the index and returns the new index"
  [index k data-id]
  (if (and (number? k) (number? data-id) (< k (read-u (first (get-page-buff index 0)) 10)))
    (_insert! index 10 k data-id)
    (throw (IllegalArgumentException. (str "k [" k "] must be < u " (read-u (first (get-page-buff index 0)) 10))))))

(defn- _v-get [index ^Long pos ^Long k]
  (let [[buff _] (get-page-buff index pos)
        rel-pos (relative-pos pos)
        v-min (read-min buff rel-pos)
        v-max (read-max buff rel-pos)
        u (read-u buff rel-pos)]
    (cond
      (= k v-min)
      (read-min-data buff rel-pos)
      (= k v-max)
      (read-max-data buff rel-pos)
      (> k v-min)
      (let [^Long pos2 (read-cluster-ref buff rel-pos (vutils/high u k))]
        (when (> pos2 -1)
          (_v-get index pos2 (vutils/low u k)))))))

(defn v-get
  "Returns the data-id at value k if it exists, otherwise nil"
  [index k]
  (when (and k (< k (read-u (first (get-page-buff index 0)) 10)))
    (_v-get index 10 k)))


(defn max-number-of-bytes
  "Calculates the max number of bytes a tree will use"
  [u]
  (let [x (vutils/upper-sqrt u)]
    (if (> x 2) (+ (node-byte-size x) (* x (max-number-of-bytes x)))
                (node-byte-size 2))))