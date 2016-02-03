(def project 'xyzzwhy/datastore)
(def version "1.0.0-SNAPSHOT")

(set-env! :resource-paths #{"resources" "src"}
          :source-paths   #{"test"}
          :dependencies   '[[boot-environ "1.0.2"]
                            [seancorfield/boot-expectations "1.0.5" :scope "test"]
                            [jeluard/boot-notify "0.2.1" :scope "test"]
                            [cider/cider-nrepl "0.11.0-SNAPSHOT"]
                            [org.clojure/clojure "RELEASE"]
                            [environ "1.0.2"]
                            [pluralex "1.0.0-SNAPSHOT"]
                            [refactor-nrepl "2.0.0-SNAPSHOT"]
                            [com.apa512/rethinkdb "0.11.0"]
                            [org.danielsz/system "0.3.0-SNAPSHOT"]
                            [org.clojure/tools.nrepl "0.2.12"]])

(task-options!
 pom {:project     project
      :version     version
      :description "xyzzwhy's datastore wrapper for RethinkDB"
      :url         "http://example/FIXME"
      :scm         {:url "https://github.com/akivaschoen/xyzzwhy-datastore"}
      :license     {"Eclipse Public License"
                    "http://www.eclipse.org/legal/epl-v10.html"}})

(deftask build
  "Build and install the project locally."
  []
  (comp (pom) (jar) (install)))

(require '[seancorfield.boot-expectations :refer :all]
         '[jeluard.boot-notify :refer [notify]]
         '[environ.boot :refer [environ]]
         ;; '[example.systems :refer [dev-system]]
         ;; '[reloaded.repl :as repl :refer [start stop go reset]]
         ;; '[system.boot :refer [system run]]
         )
