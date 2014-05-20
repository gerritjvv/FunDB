(ns fundb.converter-tests
  (:require [fundb.converters :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))


(defspec convert-array-should-convert-bytebuf-bytebuffer-and-back
  10
  (prop/for-all [n gen/nat]
                (let [bts (byte-array n)]

                  (and
                   (= (compareTo (to-bytearray bts) bts) 0)
                   (= (compareTo  (-> bts to-bytebuffer to-bytearray) bts) 0)
                   (= (compareTo (-> bts to-bytebuf to-bytearray) bts) 0)))))



(defspec convert-nat-should-convert-bytebuf-bytebuffer-and-back
  10
  (prop/for-all [n gen/nat]
                (let [bts (byte-array n)]

                  (prn "n " n)
                  (prn "a " (compareTo (to-bytearray bts) n))
                  (prn "b " (-> n to-bytebuffer to-bytearray) bts)
                  (prn "c " (compareTo (-> n to-bytebuf to-bytearray) bts))
                  (and
                   (= (compareTo (to-bytearray bts) n) 0)
                   (= (compareTo  (-> n to-bytebuffer to-bytearray) bts) 0)
                   (= (compareTo (-> n to-bytebuf to-bytearray) bts) 0)))))
