(ns task02.query
  (:require [instaparse.core :as insta])
  (:use [task02 helpers db]
        [clojure.core.match :only (match)]))

(def ^:private select-pattern 
  "sentence = select (<whitespace> token)*
   <token> =  where | order-by | limit | join
   select = <'select' <whitespace>> table-name
   where = <'where' <whitespace>> column <whitespace> comp-op <whitespace> value
   order-by = <'order by' <whitespace>> column
   limit = <'limit' <whitespace>> N
   join = <'join' <whitespace>> other-table <<whitespace> 'on' <whitespace>> 
              left-column <<whitespace> '=' <whitespace>> right-column
   table-name = name
   other-table = name
   left-column = name
   right-column = name
   column = name
   value = any
   N = num
   comp-op = '=' | '!=' | '<' | '>' | '<=' | '>='
   <whitespace> = #'\\s+'
   <any> = #'([0-9]+)|(\\'[0-9a-zA-Z]+\\')'
   <name> = #'[a-zA-Z][0-9&a-z&A-Z&\\-&_]*'
   <num> = #'[0-9]+'")

(def ^:private parser (insta/parser select-pattern))

(defn- parse [query-str]
  (let [result (parser query-str)]
    (if-not (insta/failure? result)
      result
      nil)))

(defn make-where-function [& args]
  (let [column (keyword (nth args 0))
        cond-fun (nth args 1)
        raw-value (nth args 2)
        value (if (.startsWith raw-value "'")
                (.substring raw-value 1 (.length raw-value))
                (parse-int raw-value))]
   (match [cond-fun]
    ["="] #(= (column %) value)
    ["!="] #(not (= (column %) value))
    ["<"] #(< (column %) value)
    [">"] #(> (column %) value)
    ["<="] #(<= (column %) value)
    [">="] #(>= (column %) value))))

(def ^:private transform-map
  "Map'а-связи элементов ноды с функцией преобразования этой ноды."
  {:select 
   (fn [tbl-nm] (hash-map :select (tbl-nm 1)))
   :where
   (fn [column comp-op value] 
     (hash-map 
       :where 
       (make-where-function (column 1) (comp-op 1) (value 1))))
   :limit
   (fn [n] (hash-map :limit (parse-int (n 1))))
   :order-by
   (fn [column] (hash-map :order-by (keyword (column 1))))
   :join
   (fn [other-table left-column right-column]
     (hash-map 
       :join
        [(keyword (left-column 1))
         (other-table 1)
         (keyword (right-column 1))]))})

(defn- transform 
  "На входе - маппа распарсенных значений.
   Обходит мапу и заменяет ноды в мапе при помощи
   функции сопоставленной с заменяемой нодой."
  [parse-tree] 
  (if-not parse-tree
    nil
    (insta/transform transform-map parse-tree)))
  
;; Функция выполняющая парсинг запроса переданного пользователем
;;
;; Синтаксис запроса:
;; SELECT table_name [WHERE column comp-op value] [ORDER BY column] [LIMIT N] [JOIN other_table ON left_column = right_column]
;;
;; - Имена колонок указываются в запросе как обычные имена, а не в виде keywords. В
;;   результате необходимо преобразовать в keywords
;; - Поддерживаемые операторы WHERE: =, !=, <, >, <=, >=
;; - Имя таблицы в JOIN указывается в виде строки - оно будет передано функции get-table для получения объекта
;; - Значение value может быть либо числом, либо строкой в одинарных кавычках ('test').
;;   Необходимо вернуть данные соответствующего типа, удалив одинарные кавычки для строки.
;;
;; - Ключевые слова --> case-insensitive
;;
;; Функция должна вернуть последовательность со следующей структурой:
;;  - имя таблицы в виде строки
;;  - остальные параметры которые будут переданы select
;;
;; Если запрос нельзя распарсить, то вернуть nil

;; Примеры вызова:
;; > (parse-select "select student")
;; ("student")
;; > (parse-select "select student where id = 10")
;; ("student" :where #<function>)
;; > (parse-select "select student where id = 10 limit 2")
;; ("student" :where #<function> :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2")
;; ("student" :where #<function> :order-by :id :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2 join subject on id = sid")
;; ("student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-select "werfwefw")
;; nil
(defn parse-select [^String sel-string]
  (let [parsed-data 
          (->>
            (parse sel-string)
            (next)
            (transform))
        joins 
          (->>
            (filter :join parsed-data)
            (map :join)
            (into []))
        not-joins 
          (->> 
            (remove :join parsed-data)
            (into {}))
        data 
          (if (empty? joins)
            not-joins
            (merge not-joins (hash-map :joins joins)))
        meta-keys 
          [:select :where :order-by :limit :joins]]
    (loop [meta-keys* meta-keys
           acc []]
     (if-not meta-keys*
       (next (seq acc))
       (recur 
         (next meta-keys*)
          (let [meta-key* (first meta-keys*)
                elem (meta-key* data)]
            (if-not elem
              acc
              (conj acc meta-key* elem))))))))

;; Выполняет запрос переданный в строке.  Бросает исключение если не удалось распарсить запрос

;; Примеры вызова:
;; > (perform-query "select student")
;; ({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "select student order by year")
;; ({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"})
;; > (perform-query "select student where id > 1")
;; ({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "not valid")
;; exception...
(defn perform-query [^String sel-string]
  (if-let [query (parse-select sel-string)]
    (apply select (get-table (first query)) (rest query))
    (throw (IllegalArgumentException. (str "Can't parse query: " sel-string)))))
