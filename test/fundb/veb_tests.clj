(ns fundb.veb_tests
  (:require [fundb.veb :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))



(defspec member-is-min-or-max-return-true
         100
         (prop/for-all [v-init (gen/map (gen/elements [:min :max]) gen/nat)]
                       (let [v {:u 2 :min {:v (:min v-init) } :max {:v (:max v-init)}}
                             x (if (:min v-init) (:min v-init) (:max v-init))]
                         (if (nil? x)
                           (= (member? v x) false)
                           (= (member? v x) true)))))

(defspec successor-v-base-case-0-1
  100
  (prop/for-all [v-init (gen/map (gen/elements [:min :max]) gen/nat)
                 x gen/nat]
                (let [v (assoc {} :max {:v (:max v-init)} :min {:v (:min v-init)})
                      m (assoc v :u 2)
                      max (:max v-init)
                      succ (successor-v m x)]
                  (if (and (= max 1) (= x 0))
                    (= succ 1)
                    (nil? succ)))))

(defspec successor-v-u-not-two-flat-always-nil
  10
  (prop/for-all [min gen/nat
                 max gen/nat
                 x gen/nat]
                (let [m  {:u 5 :min {:v min} :max {:v max}}
                      succ (successor-v m x)]
                  (nil? succ))))


(defspec predecessor-base-case-0-1
  1000
  (prop/for-all [min gen/nat
                 max gen/nat
                 x gen/nat]
                (let [m {:u 2 :min {:v min} :max {:v max}}
                      pred (predecessor m x)]
                  (if (and (= min 0) (= x 1))
                    (= pred 0)
                    (nil? pred)))))

(defspec predecessor-u-not-two-flat-always-nil
  10
  (prop/for-all [min gen/nat
                 max gen/nat
                 x gen/nat]
                (let [m  {:u 5 :min {:v min} :max {:v max}}
                      succ (predecessor m x)]
                  (nil? succ))))

(defn powers-of-two [from]
  (lazy-seq (cons (int (Math/pow 2 from)) (powers-of-two (inc from)))))


;test with powers of 2 lower than 27
(defspec insert-show-as-member-int
  100
  (prop/for-all [u-index (gen/such-that #(< % 27) gen/nat)]
                (let [u (nth (powers-of-two 4) u-index)
                      r-seq (take 100 (repeatedly (partial rand-int u)))
                      v-root (create-root u)
                      v (loop [v2 v-root s r-seq]
                          (if-let [x (first s)]
                            (do
                              (recur (insert v2 x {:file "bla"}) (rest s)))
                            v2))]

                  ;loop through each of the items in r-seq and test that it is a member of v
                  (loop [res true s r-seq]
                    (if res
                      (if-let [x (first s)]
                        (recur (member? v x) (rest s))
                        res))))))


;Test with powers of 2 higher than 30
(defspec insert-show-as-member-long
  100
  (prop/for-all [u-index (gen/such-that #(> % 27) gen/nat)]
                (let [u (Math/pow 2 u-index)
                      r-seq (take 100 (range (- u 99) (inc u)))
                      v-root (create-root u)
                      v (loop [v2 v-root s r-seq]
                          (if-let [x (first s)]
                            (do
                              (recur (insert v2 x {:file "bla"}) (rest s)))
                            v2))]

                  ;loop through each of the items in r-seq and test that it is a member of v
                  (loop [res true s r-seq]
                    (if res
                      (if-let [x (first s)]
                        (recur (member? v x) (rest s))
                        res))))))

(comment
(defspec delete-not-show-member
  1000
  (prop/for-all [u-index (gen/such-that #(< % 27) gen/nat)]
                (let [u (nth (powers-of-two 4) u-index)
                      r-seq (take 100 (filter #(< % u) (repeatedly (partial rand-int u))))
                      v-root (create-root u)
                      v (loop [v2 v-root s r-seq]
                          (if-let [x (first s)]
                            (do
                              (recur (insert v2 x {:file "bla"}) (rest s)))
                            v2))]
                  ;loop through each of the items in r-seq and test that it is a member of v
                  (loop [v2 v res true s r-seq]
                    (if res
                      (if-let [x (first s)]
                        (let [v3 (delete v2 x)]
                          (recur v3 (not (member? v3 x)) (rest s)))
                        res)))))))
