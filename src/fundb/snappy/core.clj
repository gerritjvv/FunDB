(ns fundb.snappy.core
  (:import [java.nio ByteBuffer MappedByteBuffer]
           [org.xerial.snappy Snappy SnappyCodec]))


(defn read-header [^ByteBuffer buff]
  (let [^"[B" magic-header (byte-array SnappyCodec/MAGIC_LEN)]
    (.get buff magic-header (int 0) (int SnappyCodec/MAGIC_LEN))
    (let [version (.getInt buff)
          compVersion (.getInt buff)]
      {:magic-header magic-header :version version :compatible-version compVersion})))



(defn read-message-meta
  "Reads the mesages written in [len][msg]... format where len is an integer
   A vector is returned where each element is a map with keys msg-pos, size, i, buff"
  [^ByteBuffer buff state pos]
  (let [ref-buff (.slice buff)]                             ;we use this buff to read the values from, so that the position is always at the start
    (loop [state2 state i pos]
      (if (> (.remaining buff) 0)
        (let [size (.getInt buff)
              msg-pos (.position buff)]
          (.position buff (+ msg-pos size))
          (recur (conj state2 {:pos msg-pos :size size :i i :buff ref-buff}) (inc i)))
        [state2 i]))))

(defn
  ^ByteBuffer
  read-snappy-block
  "Reads a snappy block and returns a ByteBuffer (direct allocated) of uncompressed data"
  [^ByteBuffer buff]
  (let [size (.getInt buff)
        pos (.position buff)
        ^ByteBuffer compressed-buff (-> buff .slice (.limit (int size)))
        ;TODO use a Pool maybe from the netty project
        ^ByteBuffer uncompressed-buff (ByteBuffer/allocateDirect (Snappy/uncompressedLength compressed-buff))]

    ;uncompress block from compressed-buff into uncompressed-buff
    (Snappy/uncompress compressed-buff uncompressed-buff)
    (.position buff (+ pos size))
    uncompressed-buff))

