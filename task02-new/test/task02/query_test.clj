(ns task02.query-test
  (:require [clojure.test :refer :all]
            [task02.query :refer :all]
            [task02.db :as db]))


(db/load-initial-data)

(deftest parse-select-test
  (testing (str "parse-select on 'select student'")
    (let [[tb-name & {:keys [where limit order-by joins]}]
          (parse-select "select student")]
      (is (= tb-name "student"))
      (is (nil? where))
      (is (nil? order-by))
      (is (nil? joins))
      (is (nil? limit))))

  (testing (str "parse-select on 'select student where id = 10'")
    (let [[tb-name & {:keys [where limit order-by joins]}]
          (parse-select "select student where id = 10")]
      (is (= tb-name "student"))
      (is (fn? where))
      (is (nil? order-by))
      (is (nil? joins))
      (is (nil? limit))))

  (testing (str "parse-select on 'select student where id = 10'")
    (let [[tb-name & {:keys [where limit order-by joins]}]
          (parse-select "select student where id = 10 order by year limit 5 join subject on id = sid")]
      (is (= tb-name "student"))
      (is (fn? where))
      (is (= order-by :year))
      (is (= limit 5))
      (is (= joins [[:id "subject" :sid]]))))
  )


(deftest perform-query-test
  (testing "perform-query"
      (is (perform-query "select student where year = 1997")
          '({:year 1997, :surname "Petrov", :id 2}))
      (is (perform-query "select student where year = 1111")
          '())))
