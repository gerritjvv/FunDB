(ns fundb.insert-bench
  (:import (java.util TreeMap))
  (:require
    [fundb.db.init :refer :all]
    [fundb.db.writer :refer :all]
    [cupboard.core :as cb])
  (:use
    cupboard.utils))

(defonce bench-counter 100000)
(defonce u-value 100000000)

(defonce id (System/currentTimeMillis))

(defn setup-cupboard [file]
  (cb/open-cupboard! file)
  (cb/defpersist test-msg
                 ((:id :index :unique)
                  (:val ))))

(defn insert-test-msg [id val]
  (cb/make-instance test-msg [id val]))


(defn setup-fundb [file]
  (let [db-name (str "test-db" (System/currentTimeMillis))]
    (create-db db-name (str "target/" db-name))
    (create-table (get (load-dbs "target") db-name) "mytest-table")
    {:db-name db-name :writer (decorate-db-writer (get (load-dbs "target") db-name))}))


(defn insert-fundb [{:keys [writer] :as db} i v]
  (try
    (write-table writer "mytest-table" i v)
    (catch Exception e nil)))


(defonce ^:private test-msg (slurp "resources/bench-msg.txt"))


(defn insert-bench []
  (setup-cupboard (str "target/mytest" (System/currentTimeMillis)))


  (time (dotimes [i 1000000]
          (insert-test-msg (+ i 10000) test-msg))))



(defn insert-fundb-bench []
  (let [db (setup-fundb (str "target/fundb-bench-test-" (System/currentTimeMillis)))]
    (time
      (dotimes [i 10000000]
        (insert-fundb db i test-msg)))))

(comment
  )
