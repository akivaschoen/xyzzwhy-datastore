(ns xyzzwhy.datastore.test.datastore
  (:require [xyzzwhy.datastore :as sut]
            [clojure.test :refer [are deftest is testing] :as t]))

(defn map-entry
  [[k v]]
  (new clojure.lang.MapEntry k v))

(deftest test-table-name
  (are [table-name expected] (= (sut/table-name table-name) expected)
    :test "tests"
    :underline-work "underline_works" 
    {:classname :map-test-work} "map_test_works"))

(deftest test-cast-values
  (is (instance? String (sut/cast-value (map-entry [:text "test"])))
      "Expected a string with key :text.")
  (is (instance? String (sut/cast-value (map-entry [:prep "test"])))
      "Expected a string with key :prep.")
  (is (instance? String (sut/cast-value (map-entry [:article "test"])))
      "Expected a string with key :article.")
  (is (instance? clojure.lang.PersistentHashSet
                 (sut/cast-value (map-entry [:config ["this" "that"]])))
      "Expected a two-item set with key :config.")
  (is (instance? clojure.lang.Keyword (sut/cast-value (map-entry [:key "test"])))
      "Expected a keyword with an unknown key that has a string value.")
  (is (instance? clojure.lang.Keyword (sut/cast-value (map-entry [:default :test])))
      "Expected a keyword with an unknown key that has a keyword value."))
