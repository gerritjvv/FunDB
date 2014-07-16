(ns fundb.veb-nio-tests
  (:require [fundb.db.index.nio.veb :as veb]
            [fundb.veb-utils :as vebutils]

            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop])
  (:import
    [io.netty.buffer Unpooled ByteBuf]
    [java.nio ByteBuffer]))

(def all-true? (partial reduce #(and %1 %2) true))

(defspec write-read-node-no-cluster
         100
         (prop/for-all [node (gen/map (gen/elements [:min :max]) gen/nat)]
                       (let [node2 (merge {:min 0 :max 10 :u 2 :min-data 100} node)
                             ^ByteBuf buff (Unpooled/buffer 33)
                             {:keys [u max min]} (-> buff
                                                     (veb/write-node 0 node2)
                                                     (veb/read-node 0))]
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
                              ^ByteBuf buff (Unpooled/buffer (+ 33 (veb/cluster-byte-size u-sqrt)))
                              {:keys [u max min]} (-> buff
                                                      (veb/write-node 0 node2)
                                                      (veb/read-node 0))]
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
                              ^ByteBuf buff (Unpooled/buffer (+ 33 (veb/cluster-byte-size u-sqrt)))
                              ]
                         (veb/write-node buff 0 node2)
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
                              ^ByteBuf buff (Unpooled/buffer (+ 33 (veb/cluster-byte-size u-sqrt)))
                              ]
                         (doto buff (veb/write-node 0 node2))

                         (dotimes [i u-sqrt]
                           (veb/write-cluster-ref buff 0 i i (inc i)))

                         (all-true?
                           (map #(and (= % (veb/read-cluster-ref buff 0 %)) (= (inc %) (veb/read-cluster-file-index buff 0 %))) (range u-sqrt)))
                         )))

(defspec write-read-header
         100
         (prop/for-all [n gen/nat]
                       (let [^ByteBuf buff (Unpooled/buffer 5 5)]
                         (= (String. veb/INDEX_HEADER)
                            (-> buff veb/write-header veb/read-header)))))

(defspec write-read-version-and-position-pointer
         100
         (prop/for-all [n gen/nat]
                       (let [^ByteBuf buff (Unpooled/buffer 10 10)]
                         (veb/write-header buff)
                         (veb/write-version buff)
                         (veb/write-position-pointer buff (int n))

                         (and
                           (= (String. veb/INDEX_HEADER)
                              (-> buff veb/write-header veb/read-header))
                           (= (int n) (veb/read-position-pointer buff))
                           (= veb/VERSION (veb/read-version buff))))))

(defspec create-load-close-index
         100
         (prop/for-all [n (gen/such-that #(and (> % 4) (< % 31)) gen/nat)]
                       (let [f "target/test/create-load-close-index/index.dat"
                             u (long (Math/pow 2 n))]
                         (-> f clojure.java.io/file .getParentFile .mkdirs)
                         (veb/create-index f u)
                         (let [i (veb/load-index f)]
                           (veb/close-index! i)
                           (and
                             (= u (:u i))
                             (= (veb/read-position-pointer (:buff i)) (veb/init-file-size u)))))))



(defn- write-child-node [buff pos u i k]
  ;write children
  (let [child-u (vebutils/lower-sqrt u)
        updated-pointer (+
                          (veb/read-position-pointer buff)
                          (veb/node-byte-size child-u))
        position-pointer (veb/read-position-pointer buff)]
    ;(prn "pos-pointer " position-pointer " updated " updated-pointer)
    (veb/write-node buff position-pointer {:u child-u :min k :max 5 :min-data 6})
    (veb/write-cluster-ref buff pos i position-pointer -1)
    (veb/write-position-pointer buff updated-pointer)))

(defn- check-child-node [buff pos u i k child-pointer]
  (let [
         child-cluster-pointer (veb/read-cluster-ref buff 10 i)]
    (prn " i " i "; child-pointer " child-pointer "; child-cluster-pointer " child-cluster-pointer)
    (and
      (= child-pointer child-cluster-pointer))
    ))


(defspec check-node-overwrite
         10
         (prop/for-all [n (gen/such-that #(and (> % 4) (< % 31)) gen/nat)]
                       (let [f "target/test/check-node-overwrite/index.dat"
                             u 16]
                         (let [buff (Unpooled/buffer (+ 10
                                                        (veb/node-byte-size 16)
                                                        (* 4 (veb/node-byte-size 4))
                                                        (* 8 (veb/node-byte-size 2))
                                                        ))]
                           (veb/write-header buff)
                           (veb/write-version buff)
                           (veb/write-position-pointer buff 10)
                           ;write root
                           (veb/write-node buff 10 {:u 16 :min 1 :max 2 :min-data 3})
                           (veb/write-position-pointer buff (+ 10 (veb/node-byte-size 16)))



                           ;check
                           (let [root-and (and
                                            ;check root
                                            (= (veb/read-u buff 10) u)
                                            (= (veb/read-min buff 10) 1)
                                            (= (veb/read-max buff 10) 2)
                                            (= (veb/read-min-data buff 10) 3)
                                            )
                                 pre-insert-pos-pointer (veb/read-position-pointer buff)]
                             (dotimes [i (vebutils/lower-sqrt u)]
                               (write-child-node buff 10 u i i))

                             (prn "ps-pointer: "
                                  (veb/read-position-pointer buff)
                                  "; "
                                  (veb/node-byte-size 4))
                             (reduce #(and %1 %2) root-and
                                      (for [i (range (vebutils/lower-sqrt u))
                                            :let [pos (veb/read-cluster-ref buff 10 i)
                                                  child-pointer (+ pre-insert-pos-pointer
                                                                   (* i (veb/node-byte-size 4)))]]
                                        (check-child-node buff pos u i i child-pointer))

                                      ))

                           ))))
(comment





  )