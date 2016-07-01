(ns xyzzwhy.datastore
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [rethinkdb.query :as r]))

(defonce db-name "xyzzwhy_corpora")

(defn ->table-name
  [c]
  (let [n (-> (if (map? c)
                (:classname c)
                c)
              name
              str
              (str/replace "-" "_"))]
    (if (str/ends-with? n "s")
      n
      (str n "s"))))

(declare class-action get-class-info)

(defn create-class
  [c]
  (class-action r/table-create c)
  (merge c (get-class-info c)))

(defn add-metadata
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        (r/insert {:id (:id c)
                   :name (:classname c)
                   :config (:config c)
                   :type (:type c)})
        (r/run conn)))
  c)

(defn add-fragments
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/insert (:fragment c))
        (r/run conn)))
  c)

;; Existing in a pre-macro state (probably)
(defn class-action
  [dsfn c]
  ((fn []
     (with-open [conn (r/connect)]
       (-> (r/db db-name)
           (dsfn (->table-name c))
           (r/run conn))))))

(defn class-query
  [dsfn]
  ((fn []
     (with-open [conn (r/connect)]
       (-> (r/db db-name)
           dsfn
           (r/run conn))))))

(defn create-database
  []
  (with-open [conn (r/connect)]
    (r/run (r/db-create db-name) conn)))

(defn delete-class
  [c]
  (let [c (if (or (string? c)
                  (keyword? c))
            (get-class-info (->table-name c))
            c)]
    (class-action r/table-drop (:name c))
    c))

(defn delete-fragments
  [c]
  (let [c (if (or (string? c)
                  (keyword? c))
            (get-class-info (->table-name c))
            c)]
    (with-open [conn (r/connect)]
      (-> (r/db db-name)
          (r/table (->table-name (:classname c)))
          (r/delete)
          (r/run conn)))
    c))

(defn delete-metadata
  [c]
  (let [c (if (or (string? c)
                  (keyword? c))
            (get-class-info (->table-name c))
            c)]
    (with-open [conn (r/connect)]
      (-> (r/db db-name)
          (r/table "classes")
          (r/filter (r/fn [table]
                      (r/eq (:id c) (r/get-field table :id))))
          (r/delete)
          (r/run conn)))
    c))

(defn initialize-database
  []
  (with-open [conn (r/connect)]
    (class-action r/table-create "classes")
    (-> (r/db db-name)
        (r/table "classes")
        (r/index-create "name" (r/fn [row]
                                 (r/get-field row :classname)))
        (r/run conn))
    (-> (r/db db-name)
        (r/table "classes")
        (r/index-create "type" (r/fn [row]
                                 (r/get-field row :type)))
        (r/run conn))))

(defn update-nested-map
  "Find a map which contains search-key search-val and merge this map with add-map."
  [m1 m2]
  (clojure.walk/postwalk (fn [x]
                           (if (map? x)
                             (cond
                               (= :config (first x)) (merge m2 (first {(first x) (mapv keyword (second x))}))
                               (string? (second x)) (merge m2 (first {(first x) (keyword (second x))}))
                               :else
                               (merge m2 (first {(first x) (second x)})))
                             {(first x) (second x)}))
                         m1))

(defn cvals
  [x]
  (println x)
  (if (map? x)
    (assoc x (first x)
           (cond
             (= :text (first x)) (second x)
             (= :config (first x)) (mapv keyword (second x))
             (string? (second x)) (keyword (second x))
             :else
             (second x)))
    x))

(defn cast-values
  "Returns a map with its values converted to keywords as necessary."
  [m]
  (reduce (fn [acc item]
            (assoc acc (first item)
                   (cond
                     (= :text (first item)) (second item)
                     (= :prep (first item)) (second item)
                     (= :article (first item)) (second item)
                     (= :config (first item)) (into #{} (map keyword (second item)))
                     (string? (second item)) (keyword (second item))
                     :else
                     (second item))))
          {}
          m))

(defn fix-sub-map
  "Returns a map with its :sub entries' keys converted from keyword to
  integer.

  (RethinkDB converts them the opposite way when storing.)"
  [fragment]
  (if (contains? fragment :sub)
    (assoc fragment :sub
           (reduce (fn [acc item]
                     (assoc acc (-> (key item)
                                    name
                                    Integer/parseInt)
                            (cast-values (val item))))
                   {}
                   (:sub fragment)))
    fragment))

(defn get-classes
  [t]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        (r/get-all [(->table-name t)] {:index "type"})
        (r/run conn))))

;;
;; Public API
;;
(declare list-classes)

(defn class-exists?
  [c]
  (some (partial = (->table-name c)) (list-classes)))

(defn get-class
  [c]
  (class-action r/table c))

(defn get-class-info
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/info)
        (r/without [:type])
        (r/run conn))))

(defn get-fragment
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/sample 1)
        (r/without [:id])
        (r/run conn)
        first
        fix-sub-map)))

(defn get-metadata
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        #_(r/get-all [(->table-name c)] {:index "name"})
        (r/filter {:name c})
        (r/without [:id :name])
        (r/run conn)
        first)))

(defn list-classes
  []
  (vec (remove #{"classes"} (class-query r/table-list))))

(def add-class (comp add-metadata add-fragments create-class))
(def reload-fragments (comp add-fragments delete-fragments))
(def remove-class (comp delete-metadata delete-class))
