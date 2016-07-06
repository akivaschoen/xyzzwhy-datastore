(defproject xyzzwhy/datastore "1.0.0-SNAPSHOT"
  :description "xyzzwhy's datastore wrapper for RethinkDB."
  :url "https://github.com/akivaschoen/xyzzwhy-datastore"
  :license {:name "GNU General Public License"
            :url "http://www.gnu.org/licenses/gpl.html"}

  :min-lein-version "2.6.1"

  :dependencies
  [[org.clojure/clojure "1.8.0"]
   [com.apa512/rethinkdb "0.15.24"]
   [environ "1.0.3"]
   [pluralex "1.0.0-SNAPSHOT"]]

  :plugins
  [[lein-environ "1.0.3"]]

  :profiles
  {:uberjar
   [:prod-config
    {:env {:production "true"}
     :omit-source true
     :aot :all}]

   :dev
    {:env {:dev "true"}
     :source-paths ["dev"]}}

  :repl-options
  {:caught clj-stacktrace.repl/pst+})
