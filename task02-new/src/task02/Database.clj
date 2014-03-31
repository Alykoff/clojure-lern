(ns task02.Database
  (:use [task02 db query])
  ;; Объявить класс task02.Database с двумя статическими функциями доступными из Java:
  ;;  - void InitDatabase() - должна выполнять начальную загрузку данных используя функцию
  ;;       task02.db/load-initial-data
  ;;  - String Select(String query) - должна выполнять запрос к базе данных
  ;;       (task02.query/perform-query) и возвращать результат в виде строки в формате EDN.

  ;; Hint: load-initial-data, pr-str, perform-query
  ; :implement-me
  (:gen-class
    :main false
    :name "task02.Database"
    :methods [^:static [InitDatabase [] String]
              ^:static [Select [String] String]]))

(defn InitDatabase []
  (load-initial-data))

(defn Select ^String [^String query]
  (pr-str (perform-query query)))
  
