(ns fundb.veb-nio-tests
  (:require [fundb.db.index.nio.veb :as veb]
            [fundb.veb-utils :as vebutils]

            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop])
  (:import  [java.nio ByteBuffer]))

(def all-true? (partial reduce #(and %1 %2) true))

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

(defspec write-read-node-with-individual-get-functions
         100
         (prop/for-all [node (gen/map (gen/elements [:min :max]) gen/nat)
                        u (gen/such-that #(and (pos? %) (< % 500)) gen/nat)]

                       (let [ u-sqrt (vebutils/upper-sqrt u)
                              node2 (merge {:min 0 :max 10 :u u :min-data 100} node)
                              ^ByteBuffer buff (ByteBuffer/allocate (+ 33 (veb/cluster-byte-size u-sqrt)))
                             ]
                         (doto buff (veb/write-node!! node2) (.flip))

                         (and
                           (not (veb/read-deleted buff 0))
                           (= (:u node2) (veb/read-u buff 0))
                           (= (:min node2) (veb/read-min buff 0))
                           (= (:max node2) (veb/read-max buff 0))
                           (= (:min-data node2) (veb/read-min-data buff 0)))
                         )))


(defspec write-read-cluster-refs
         100
         (prop/for-all [node (gen/map (gen/elements [:min :max]) gen/nat)
                        u (gen/such-that #(and (> % 3) (< % 500)) gen/nat)]

                       (let [ u-sqrt (vebutils/upper-sqrt u)
                              node2 (merge {:min 0 :max 10 :u u :min-data 100} node)
                              ^ByteBuffer buff (ByteBuffer/allocate (+ 33 (veb/cluster-byte-size u-sqrt)))
                              ]
                         (doto buff (veb/write-node!! node2) (.flip))

                         (dotimes [i u-sqrt]
                           (veb/write-cluster-ref buff 0 i i (inc i)))

                         (all-true?
                           (map #(and (= % (veb/read-cluster-ref buff 0 %)) (= (inc %) (veb/read-cluster-file-index buff 0 %))) (range u-sqrt)))
                         )))

(defspec write-read-header
         100
         (prop/for-all [n gen/nat]
                       (let [^ByteBuffer buff (ByteBuffer/allocate 5)]
                         (= (String. veb/INDEX_HEADER)
                            (-> buff veb/write-header .flip veb/read-header)))))