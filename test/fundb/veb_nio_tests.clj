(ns fundb.veb-nio-tests
  (:require [fundb.db.index.nio.veb :as veb]
            [fundb.veb-utils :as vebutils]

            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop])
  (:import  [java.nio ByteBuffer]))

(defspec write-read-node-no-cluster
         100
         (prop/for-all [node (gen/map (gen/elements [:min :max]) gen/nat)]
                       (let [node2 (merge {:min 0 :max 10 :u 2 :min-data 100} node)
                             ^ByteBuffer buff (ByteBuffer/allocate 33)
                             {:keys [u max min]} (-> buff
                                                     (veb/write-node!! node2)
                                                     (.flip)
                                                     (veb/read-node))]
                         (and
                           (= u 2)
                           (= min (:min node2))
                           (= max (:max node2)))
                         )))


(defspec write-read-node-with-cluster
         100
         (prop/for-all [node (gen/map (gen/elements [:min :max]) gen/nat)
                        u (gen/such-that #(and (pos? %) (< % 500)) gen/nat)]

                       (let [ u-sqrt (vebutils/upper-sqrt u)
                              node2 (merge {:min 0 :max 10 :u u :min-data 100} node)
                             ^ByteBuffer buff (ByteBuffer/allocate (+ 33 (veb/cluster-byte-size u-sqrt)))
                             {:keys [u max min]} (-> buff
                                                     (veb/write-node!! node2)
                                                     (.flip)
                                                     (veb/read-node))]
                         (and
                           (= u u)
                           (= min (:min node2))
                           (= max (:max node2)))
                         )))