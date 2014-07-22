(ns fundb.converters
  (:import
    [io.netty.buffer ByteBuf Unpooled]
    [fundb.utils BytesUtil]
    [java.nio ByteBuffer]
    ))

(defprotocol FromBytes
  (from-bytes [x] "Converts x a bytearray to the type expected"))

(defprotocol TComparable
  (compareTo [a b] "Return -1 if a is smaller than b, 0 if a == b and 1 if a > b"))

(defprotocol ToByteBuf
  (to-bytebuf [x] "Converts x to a netty io ByteBuf"))

(defprotocol ToByteBuffer
  (to-bytebuffer [x] "Converts x to a java nio ByteBuffer"))

(defprotocol ToByteArray
  (to-bytearray [x] "Converts x to a java byte array"))

(extend-type ByteBuf
  TComparable
  (compareTo [^ByteBuf a ^ByteBuf b] (.compareTo a (to-bytebuf b)))

  ToByteBuf
  (to-bytebuf [x] x)

  ToByteBuffer
  (to-bytebuffer [^ByteBuf x] (.nioBuffer x))

  ToByteArray
  (to-bytearray [^ByteBuf x]
    (let [^"[B" bts (byte-array (.readableBytes x))
          reader-index (.readerIndex x)]
      (.getBytes x reader-index bts)
      bts)))

(extend-type ByteBuffer

  TComparable
  (compareTo [^ByteBuffer a b] (.compareTo a (to-bytebuffer b)))


  ToByteBuf
  (to-bytebuf [x] (Unpooled/wrappedBuffer ^ByteBuffer x))

  ToByteBuffer
  (to-bytebuffer [x] x)

  ToByteArray
  (to-bytearray [^ByteBuffer x] (let [buff (.slice x)
                                      ^"[B" bts (byte-array (.remaining x))]
                                  (.get x bts)
                                  bts)))


(extend-type (Class/forName "[B")

  TComparable
  (compareTo [a b] (compareTo (to-bytebuf a) (to-bytebuf b)))

  ToByteBuf
  (to-bytebuf [x] (Unpooled/wrappedBuffer ^"[B" x))

  ToByteBuffer
  (to-bytebuffer [^"[B" x] (ByteBuffer/wrap x))

  ToByteArray
  (to-bytearray [x] x))

(extend-type java.lang.Long

  TComparable
  (compareTo [a b] (compareTo (to-bytebuf a) (to-bytebuf b)))

  ToByteBuf
  (to-bytebuf [x] (to-bytebuf (to-bytearray x)))

  ToByteBuffer
  (to-bytebuffer [x] (to-bytebuffer (to-bytearray x)))

  ToByteArray
  (to-bytearray [x] (BytesUtil/toArray (long x)))

  FromBytes
  (from-bytes [^"[B" x]
    (BytesUtil/toLong x)))

(extend-type String
  ToByteArray
  (to-bytearray [^String x] (.getBytes x)))
