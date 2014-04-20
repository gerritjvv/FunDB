(ns fundb.storage_tests
  (:require [fundb.storage :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))


(defspec create-db-should-return-db
  100
  (prop/for-all [n gen/nat]
              (let [
                 db-name (str n)
                 f (str "/tmp/mydbs/" db-name)
                 db (create-db db-name f)]
                (and (not (nil? db))
                     (= (:name db) db-name)
                     (= (clojure.java.io/file f) (:dir db))
                     (not (nil? (:tables db)))))))


(defspec create-table-should-return-table
  100
  (prop/for-all [n gen/nat]
              (let [
                 db-name "mydb"
                 table-name (str n)
                 f (str "/tmp/mydbs/" db-name)
                 table-f (str "/tmp/mydbs/mydb/" table-name)
                 table (create-table db-name table-name table-f)]

                (and (not (nil? table))
                     (= (:name table) table-name)
                     (= (clojure.java.io/file table-f) (:dir table))))))
