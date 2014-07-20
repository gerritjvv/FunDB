(ns fundb.db.init
  "Load database and table definitions into memory"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [fundb.db.index.nio.veb :as veb])
  (:import [java.io File]))


(defn- load-indexes [indexes]
  (reduce (fn [state [name {:keys [type file]}]]
            (assoc name {:type type :data (veb/load-index file)})) ))

(defn- load-table
  "Loads a table and its indexes into memory"
  [{:keys [name dir]}]
  (let [d (edn/read-string (slurp (str dir "/.table-" name)))]
    (assoc d
      :indexes (load-indexes (:indexes d)))))

(defn load-db-def
  "Loads a database and its tables into memory"
  [^File path]
  (let [^File f (io/as-file (str path "/.fundb"))
        d (edn/read-string (slurp f))]
    (assoc d
      :dir path
      :file (.getAbsolutePath f)
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

(defn create-load-db-def [db-name ^File path]
  (create-db db-name path)
  (load-db-def path))

(defn create-table [{:keys [dir file] :as db-def} table-name]
  (let [^File def-file (io/as-file (str dir "/" table-name "/.table-" table-name))
        index-file  (str dir "/" table-name "/.index-" table-name)]
    (if-not (.exists def-file)
      (do
        (veb/create-index Integer/MAX_VALUE)
        (io/make-parents def-file)
        (spit def-file {:name table-name :dir (str dir "/" table-name) :indexes [:type :veb :file index-file]})
        (let [db-def2 (assoc db-def :tables (conj (:tables db-def) (.getAbsolutePath def-file)))]
          (spit file db-def2)
          db-def2))
      db-def)))