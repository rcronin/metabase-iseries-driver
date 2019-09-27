(defproject metabase/db2-driver "1.0.0-SNAPSHOT-11.1.4.4"
  :min-lein-version "2.5.0"

  :dependencies
   [[com.ibm.db2/jcc "11.1.4.4"]]

  :jvm-opts 
   ["-Ddb2.jcc.charsetDecoderEncoder=3"]

  :profiles
  {:provided
   {:dependencies [[metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "db2.metabase-driver.jar"}})
