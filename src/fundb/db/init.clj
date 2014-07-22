(ns fundb.db.init
  "Load database and table definitions into memory"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [fundb.db.index.nio.veb :as veb]
            [clojure.tools.logging :refer [error info]])
  (:import [java.io File RandomAccessFile]))


(defn- load-indexes [indexes]
  (into {}
        (for [[name {:keys [type file]}] indexes]
          [name {:type type :data (veb/load-index file)}])))

(defn- load-table
  "Loads a table and its indexes into memory"
  [file]
  (let [d (edn/read-string (slurp file))]
    (assoc d
      :indexes (ref (load-indexes (:indexes d))))))

(defn load-db-def
  "Loads a database and its tables into memory"
  [^File path]
  (let [isFile (= (.getName path) ".fundb")
        ^File f (if isFile path (io/as-file (str path "/.fundb")))

        d (edn/read-string (slurp f))]
    (assoc d
      :dir (if isFile (.getParent path) path)
      :file (.getAbsolutePath f)
      :tables (into {} (map
                         (fn [x] [(:name x) x])
                         (map load-table (:tables d)))))))


(defn create-db [db-name path]
  (let [^File def-file (io/as-file (str path "/.fundb"))]
    (when-not (.exists def-file)
      (io/make-parents def-file)
      (spit def-file {:name db-name :tables []} ))))

(defn create-load-db-def [db-name ^File path]
  (create-db db-name path)
  (load-db-def path))

(defn- update-db-add-table [{:keys [dir file]} table-name def-file]
  (let [channel (-> (RandomAccessFile. (io/file file) "rw") .getChannel)
        lock (.lock channel)
       ]
      (try
        (let [db-def (edn/read-string (slurp file))]
          (spit file (assoc db-def :tables (conj (:tables db-def) (-> def-file io/file .getAbsolutePath)))))
        (finally (.release lock)))))

(defn create-table [{:keys [dir file] :as db-def} table-name]
  (let [^File def-file (io/as-file (str dir "/" table-name "/.table-" table-name))
        index-file  (str dir "/" table-name "/.index-" table-name)]
    (when-not (.exists def-file)
      (veb/create-index index-file Integer/MAX_VALUE)
      (io/make-parents def-file)
      (spit def-file {:name table-name :dir (str dir "/" table-name) :indexes {"primary" {:type :veb :file index-file}}})
      (update-db-add-table db-def table-name def-file)
      )))

(defn get-table [db-def table-name]
  (-> db-def :tables (get table-name)))


(defn get-primary-index [db-def table-name]
  (-> db-def :tables (get table-name) :indexes (get "primary")))

(defn- safe-load-db-def [path]
  (try
    (load-db-def path)
    (catch Exception e (error e (str "Error loading db " path)))))

(defn load-dbs
  "Search the path structure for .fundb files and load each db"
  [path]
  (let [dbs (filter (fn [^File f] (= (.getName f) ".fundb")) (-> path io/file file-seq))]
    (into {}
          (map (fn [{:keys [name] :as db}] [name db])
               (filter (complement nil?)
                       (map safe-load-db-def dbs))))))