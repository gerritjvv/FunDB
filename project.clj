(defproject fundb "0.1.0-SNAPSHOT"
  :description "Fast storage engine based on Cache-Oblivious Streaming B-trees"
  :url "https://github.com/gerritjvv/FunDB"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
 		             [fileape "0.5.0-SNAPSHOT"]
                 [org.clojure/core.cache "0.6.3"]
                 [org.apache.hadoop/hadoop-common "2.2.0"]
                 [io.netty/netty-buffer "4.0.19.Final"]
                 [org.clojure/test.check "0.5.7" :scope "test"]
		 [org.clojure/tools.trace "0.7.8" :scope "provided"]]

  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :global-vars {*warn-on-reflection* true
                *assert* false}

  :java-source-paths ["java"] )
