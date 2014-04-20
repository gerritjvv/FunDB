(ns fundb.storage
  (:require [fileape.core :refer :all]
            [fundb.veb :refer [create-root insert]])
  (:import [java.io File DataOutputStream]))


(def databases (ref {}))


(defn create-db
  "If the database already exists the current db is returned otherwise it is created and returned"
  [db-name dir]
     (if-let [stored-db (get @databases db-name)]
       stored-db
       (get (dosync
             (alter databases (fn [m]
                               (if-let [stored-db (get m db-name)]
                                 m
                                 (assoc m db-name
                                   {:name db-name :dir (clojure.java.io/file dir) :tables {}}))))) db-name)))

(defn get-database [db-name] (get @databases db-name))


(defn create-ape
  "Create ape connection that writes data for a database"
  [dir callbacks]
  (ape {:codec :snappy
        :base-dir dir
                :check-freq 1000
                :rollover-size 536870912 ;512mb
                :rollover-timeout 1000
                :roll-callbacks callbacks}))

(defn create-table-indexes [db-name table-name key-size]
  (create-root key-size))

(defn get-table [db-name table-name]
  (get-in @databases [db-name :tables table-name]))



(defn file-roll-callback [db-name table-name {:keys [^File file]}]
  (prn "rollback " table-name)
  (try
    (if-let [table (get-table db-name table-name)]
      (let [file-name (.getAbsolutePath file)
            {:keys [insert-data indexes]} table
            {:keys [file-i]} (dosync (alter
                                      insert-data
                                      (fn [m]
                                        (assoc m :file-i (inc (:file-i m)) (inc (:file-i m)) []))))
            last-file-i (dec file-i)
            file-keys (get @insert-data last-file-i)]
        ;for each file-key insert each key into the indexes with data :file file-name :i i
        (if (not (empty? file-keys))
          (let
            [new-indexes
              (loop [ks file-keys i 0 ind @@indexes]
                (if-let [k (first ks)]
                  (do
                    (recur (rest ks) (inc i) (insert ind k {:file file-name :i i})))
                  ind))]

            (dosync
             (alter indexes (fn [_] (delay new-indexes)))
             (alter insert-data dissoc last-file-i)))))
      (prn "NO TABLE FOUND"))
    (catch Exception e (.printStackTrace e))))




(defn create-table [db-name table-name dir]
  (if-let [table (get-in @databases [db-name :tables table-name])]
      table
      (get-in
         (dosync
            (alter databases (fn [m]
                               (if-let [table (get-in m [db-name :tables table-name])]
                                 m
                                 (assoc-in m [db-name :tables table-name]
                                           {:name table-name :dir (clojure.java.io/file dir)
                                            :indexes (ref (delay (create-table-indexes db-name table-name Long/MAX_VALUE)))
                                            :ape (delay (create-ape dir [(partial file-roll-callback db-name table-name)]))
                                            :insert-data (ref {:file-i 0 0 [] })})))))

         [db-name :tables table-name])))


(defn write-table [db-name table-name k ^"[B" bts]
  ;write data
  (let [{:keys [insert-data ape]} (get-in @databases [db-name :tables table-name])]
    (write @ape "data"
           (fn [^DataOutputStream out]
             (.write out (count bts))
             (.write out bts 0 (count bts))))
    (dosync
     (alter insert-data
            (fn [data]
              ;file-i points to a vector, this vector contains the current keys to be inserted into the index
              ;this delay to index writing is done because we only know the file name of the data once it has been rolled.
              (let [file-i (:file-i data)
                    data-vec (get data file-i)]
                (assoc data file-i (conj data-vec k))))))))




