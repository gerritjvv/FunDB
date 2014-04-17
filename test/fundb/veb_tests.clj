(ns fundb.veb_tests
  (:require [fundb.veb :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))



(defspec member-is-min-or-max-return-true
         100
         (prop/for-all [v (gen/map (gen/elements [:min :max]) gen/nat)]
                       (= (member? (assoc v :u 2) (if (:min v) (:min v) (:max v))) (if (empty? v) false true))))


(defspec successor-base-case-0-1
  100
  (prop/for-all [v (gen/map (gen/elements [:min :max]) gen/nat)
                 x gen/nat]
                (let [m (assoc v :u 2)
                      max (:max v)
                      succ (successor m x)]
                  (if (and (= max 1) (= x 0))
                    (= succ 1)
                    (nil? succ)))))

(defspec successor-u-not-two-flat-always-nil
  10
  (prop/for-all [min gen/nat
                 max gen/nat
                 x gen/nat]
                (let [m  {:u 5 :min min :max max}
                      succ (successor m x)]
                  (nil? succ))))


(defspec predecessor-base-case-0-1
  1000
  (prop/for-all [min gen/nat
                 max gen/nat
                 x gen/nat]
                (let [m {:u 2 :min min :max max}
                      pred (predecessor m x)]
                  (if (and (= min 0) (= x 1))
                    (= pred 0)
                    (nil? pred)))))

(defspec predecessor-u-not-two-flat-always-nil
  10
  (prop/for-all [min gen/nat
                 max gen/nat
                 x gen/nat]
                (let [m  {:u 5 :min min :max max}
                      succ (predecessor m x)]
                  (nil? succ))))

