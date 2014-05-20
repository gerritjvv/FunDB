(ns fundb.hashes-tests
  (:require [fundb.hashes :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))


(defspec hash-for-nat-should-be-order-preserving
  100000
  (prop/for-all [n gen/nat]
                (< (o-hash n) (o-hash (inc n)))))
