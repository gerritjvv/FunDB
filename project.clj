(defproject fundb "0.1.0-SNAPSHOT"
  :description "Fast storage engine based on Cache-Oblivious Streaming B-trees"
  :url "https://github.com/gerritjvv/FunDB"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
 		             [fileape "0.5.0-SNAPSHOT"]
                 [prismatic/schema "0.2.4"]
                 [org.clojure/core.cache "0.6.3"]
                 [org.apache.hadoop/hadoop-common "2.2.0"]
                 [io.netty/netty-buffer "4.0.19.Final"]
                 [com.taoensso/nippy "2.6.3"]
		             [rhizome "0.2.1"]
                 [criterium "0.4.3" :scope "provided"]
                 [org.clojure/test.check "0.5.7" :scope "test"]
		 [org.clojure/tools.trace "0.7.8" :scope "provided"]]

  :plugins [[perforate "0.3.3"]]

  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :global-vars {*warn-on-reflection* true
                *assert* false}

  :java-source-paths ["java"]
  :perforate {:benchmark-paths ["bench/"]})
