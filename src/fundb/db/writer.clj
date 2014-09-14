(ns fundb.db.writer
  "Write module for tables"
  (:require [clojure.core.cache :as cache]
            [fundb.converters :refer [to-bytearray to-bytebuf]]
            [fileape.core :refer :all]
            [fundb.db.index.nio.veb :refer [v-insert!]])
  (:import [java.io File DataOutputStream]
           [io.netty.buffer ByteBuf PooledByteBufAllocator ByteBufAllocator]
           [java.util.concurrent.atomic AtomicLong]))


(defn file-roll-callback [{:keys [^File file]}]
  (prn "roll file " file))

(defn create-ape
  "Create ape connection that writes data for a database"
  [dir]
  (ape {:codec            :snappy
        :base-dir         dir
        :check-freq       1000
        :rollover-size    536870912                         ;512mb
        :rollover-timeout 1000
        :roll-callbacks   [file-roll-callback]}))

(defn create-data-cache [& {:keys [read-cache-limit] :or {read-cache-limit 100}}]
  (ref (cache/lru-cache-factory {:threshold read-cache-limit})) )

(defn decorate-writer
  "Adds writer decor to a map"
  [m dir & {:keys [read-cache-limit]}]
  (assoc m :data-cache (create-data-cache :read-cache-limit read-cache-limit) :ape (create-ape dir)))

(defn decorate-db-writer
  "Adds writer decor to a db structure as returned by fundb.db.init"
  [{:keys [tables] :as db} & {:keys [read-cache-limit]}]
  (when tables
    (assoc db :tables (into {}
                            (map
                              (fn [[k v]] [k (decorate-writer v (:dir v) :read-cache-limit read-cache-limit)])
                              tables)))))


(defn remove-decorations
  "Removes any writer decorations and close any resources used"
  [m]
  (if-let [ape-conn (:ape m)]
    (close ape-conn))
  (dissoc m :data-cache :ape))


(defn extract-ts [file-name]
  (Long/parseLong (re-find #"\d{13,}" file-name)))

(defn write-table
  "

  "
  [db-writer table-name k v]
  ;write data
  (let [{:keys [ape indexes]} (get-in db-writer [:tables table-name])
        ^"[B" bts (to-bytearray v)]

    (assert (not (nil? indexes)))
    ;(prn ">>> v-insert!: " k)
    (write ape "data"
           (fn [{:keys [^DataOutputStream out future-file-name] :as file-data}]
             (.writeInt out (count bts))
             (.write out bts 0 (count bts))
             (let [i (.get ^AtomicLong (:record-counter file-data))] ;get teh recor counter value at insert
               (dosync (alter indexes (fn [indexes-m]
                                        ;(prn "indexes-m " indexes-m)
                                        ;(prn "extracted: " (get indexes-m "primary") )
                                        (try
                                          (assoc-in indexes-m
                                            ["primary" :data] (v-insert! (:data (get indexes-m "primary")) k (extract-ts future-file-name)))
                                          (catch Exception e (do
                                                               ;(.printStackTrace e)
                                                               indexes-m)))))))))))


(comment
  (use 'fundb.db.init :reload)
  (use 'fundb.db.writer :reload)
  (def dbs (load-dbs "."))
  (def db-writer (decorate-db-writer (get dbs "testdb")))
  (write-table db-writer "mytable" 1 "abc")

  (def i (-> db-writer :tables (get "mytable") :indexes deref (get "primary")))
  (v-get (:data i) 1)
  )