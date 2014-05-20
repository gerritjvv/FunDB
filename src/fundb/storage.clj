(ns fundb.storage
  (:require [fileape.core :refer :all]
            [fundb.veb :refer [create-root insert]]
            [fundb.converters :refer [to-bytearray to-bytebuf]]
            [io.netty.buffer ByteBuf PooledByteBufAllocator ByteBufAllocator]
            [clojure.core.cache :as cache])
  (:import [java.io File DataOutputStream]
           [java.util.concurrent.atomic AtomicLong]))


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
  (prn "roll file " db-name " " table-name " " file))


(defn- _get-index [ind]
  (if (delay? ind) @ind ind))

(defn get-index
  "Returns the main index of a table, the index is a vEB tree"
  [db-name table-name]
  (_get-index @(get-in @databases [db-name :tables table-name :indexes])))


(defn get-data-cache [db-name table-name]
  (:data-cache (get-table db-name table-name)))

(defn create-table [db-name table-name dir & {:keys [read-cache-limit] :or {read-cache-limit 32}}]
  (if-let [table (get-in @databases [db-name :tables table-name])]
      table
      (get-in
         (dosync
            (alter databases (fn [m]
                               (if-let [table (get-in m [db-name :tables table-name])]
                                 m
                                 (assoc-in m [db-name :tables table-name]
                                           {:name table-name :dir (clojure.java.io/file dir)
                                            :indexes (ref (delay (
                                                                  create-table-indexes db-name table-name (long (Math/pow 2 47)))))
                                            :ape (delay (create-ape dir [(partial file-roll-callback db-name table-name)]))
                                            :data-cache (ref (cache/lru-cache-factory {:threshold read-cache-limit})) ;used by the storage-read module
                                            :allocator (PooledByteBufAllocator. true)
                                            })))))

         [db-name :tables table-name])))


;TODO use ByteBuf and then to write to the DataOutputStream do
; If byte array backed, just use the array,
; else copy the data into an array of bytes and then write

(defn write-table [db-name table-name k v]
  ;write data
  (let [{:keys [ape indexes]} (get-in @databases [db-name :tables table-name])
        ^"[B" bts (to-bytearray v)]
    (write @ape "data"
           (fn [{:keys [^DataOutputStream out future-file-name] :as file-data}]
             (.writeInt out (count bts))
             (.write out bts 0 (count bts))
             (let [i (.get ^AtomicLong (:record-counter file-data))]
               (dosync (alter indexes (fn [ind]
                                        (insert (_get-index ind) k {:file future-file-name :i i})))))))))




