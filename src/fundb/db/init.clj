(ns fundb.db.init
  "Load database and table definitions into memory"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [fundb.db.index.nio.veb :as veb-storage])
  (:import [java.io File]))


(defn- load-indexes [indexes]
  (reduce (fn [state [name {:keys [type file]}]]
            (assoc name {:type type :data (veb-storage/load-index file)})) ))

(defn- load-table
  "Loads a table and its indexes into memory"
  [{:keys [name dir]}]
  (let [d (edn/read-string (slurp (str dir "/.table-" name)))]
    (assoc d
      :indexes (load-indexes (:indexes d)))))

(defn load-db-def
  "Loads a database and its tables into memory"
  [^File path]
  (let [d (edn/read-string (slurp (io/as-file (str path "/.fundb"))))]
    (assoc d
      :tables (map load-table (:tables d)))))


(defn load-db-meta
  "Acceps a directory and returns a table definition"
  [path]
  (let [^File def-file (io/as-file (str path "/.fundb"))]
    (if (.exists def-file)
      (load-db-def def-file))))

(defn create-db [db-name path]
  (let [^File def-file (io/as-file (str path "/.fundb"))]
    (when-not (.exists def-file)
      (io/make-parents def-file)
      (spit def-file {:name db-name :tables []} ))))

(defn create-table [db-def path]
  (let [^File def-file (io/as-file (str path "/.fundb"))]))