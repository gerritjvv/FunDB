(ns fundb.db-init-tests
  (:require [fundb.db.init :refer :all]
            [clojure.java.io :as io]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))


(defspec test-create-load-db-def
          10
          (prop/for-all [n gen/nat]
                        (let [dir (str "target/test/test-create-load-db-def/" n)]
                          (create-db "mydb" dir)
                          (= (load-db-def dir)  {:name "mydb", :tables []})
                          )))