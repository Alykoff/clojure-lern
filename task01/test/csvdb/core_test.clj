(ns csvdb.core-test
  (:require [clojure.test :refer :all]
            [csvdb.core :refer :all]))

(deftest table-keys-test
  (testing
    (is (= (vec (table-keys student-tbl))
           [:id :surname :year :group_id]))))

(deftest key-value-pairs-test
  (testing
    (is (= (vec (key-value-pairs [:id :surname :year :group_id] ["1" "Ivanov" "1996"]))
           (vec '(:id "1" :surname "Ivanov" :year "1996"))))))

(deftest data-record-test
  (testing
      (is (= (data-record [:id :surname :year :group_id] ["1" "Ivanov" "1996"])
             {:surname "Ivanov", :year "1996", :id "1"}))))

(deftest data-table-test
  (testing
      (is (= (vec (data-table student-tbl))
             (vec '({:surname "Ivanov", :year "1998", :id "1"} {:surname "Petrov", :year "1997", :id "2"} {:surname "Sidorov", :year "1996", :id "3"}))))))

(deftest str-field-to-int-test
  (testing
      (is (= (str-field-to-int :id {:surname "Ivanov", :year "1996", :id "1"})
             {:surname "Ivanov", :year "1996", :id 1}))))

(deftest where*-test
  (testing
      (is (= (vec (where* student (fn [rec] (> (:id rec) 1))))
             (vec '({:surname "Petrov", :year 1997, :id 2} {:surname "Sidorov", :year 1996, :id 3}))))))

(deftest limit*-test
  (testing
      (is (= (vec (limit* student 1))
             (vec '({:surname "Ivanov", :year 1998, :id 1}))))))

(deftest order-by*-test
  (testing
      (is (= (vec (order-by* student :year))
             (vec '({:surname "Sidorov", :year 1996, :id 3} {:surname "Petrov", :year 1997, :id 2} {:surname "Ivanov", :year 1998, :id 1}))))))

(deftest join*-test
  (testing
      (is (= (vec (join* (join* student-subject :student_id student :id) :subject_id subject :id))
             [{:subject "Math", :subject_id 1, :surname "Ivanov", :year 1998, :student_id 1, :id 1} {:subject "Math", :subject_id 1, :surname "Petrov", :year 1997, :student_id 2, :id 2} {:subject "CS", :subject_id 2, :surname "Petrov", :year 1997, :student_id 2, :id 2} {:subject "CS", :subject_id 2, :surname "Sidorov", :year 1996, :student_id 3, :id 3}]))))

(deftest perform-joins-test
  (testing
      (is (= (vec (perform-joins student-subject [[:student_id student :id] [:subject_id subject :id]]))
             [{:subject "Math", :subject_id 1, :surname "Ivanov", :year 1998, :student_id 1, :id 1} {:subject "Math", :subject_id 1, :surname "Petrov", :year 1997, :student_id 2, :id 2} {:subject "CS", :subject_id 2, :surname "Petrov", :year 1997, :student_id 2, :id 2} {:subject "CS", :subject_id 2, :surname "Sidorov", :year 1996, :student_id 3, :id 3}]))))

(deftest select-test
  (testing
      (is (= (vec (select student))
             [{:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"}]))
    (is (= (vec (select student :order-by :year))
           (vec '({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"}))))
    (is (= (vec (select student :where #(> (:id %) 1)))
           (vec '({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"}))))
    (is (= (vec (select student :limit 2))
           (vec '({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"}))))
    (is (= (vec (select student :where #(> (:id %) 1) :limit 1))
           (vec '({:id 2, :year 1997, :surname "Petrov"}))))
    (is (= (vec (select student :where #(> (:id %) 1) :order-by :year :limit 2))
           (vec '({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"}))))
    (is (= (vec (select student-subject :joins [[:student_id student :id] [:subject_id subject :id]]))
           (vec '[{:subject "Math", :subject_id 1, :surname "Ivanov", :year 1998, :student_id 1, :id 1} {:subject "Math", :subject_id 1, :surname "Petrov", :year 1997, :student_id 2, :id 2} {:subject "CS", :subject_id 2, :surname "Petrov", :year 1997, :student_id 2, :id 2} {:subject "CS", :subject_id 2, :surname "Sidorov", :year 1996, :student_id 3, :id 3}])))
    (is (= (vec (select student-subject :limit 2 :joins [[:student_id student :id] [:subject_id subject :id]]))
           (vec '({:subject "Math", :subject_id 1, :surname "Ivanov", :year 1998, :student_id 1, :id 1} {:subject "Math", :subject_id 1, :surname "Petrov", :year 1997, :student_id 2, :id 2}))))))
