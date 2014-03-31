(ns task02.core
  (:gen-class)
  (:use [task02 helpers network db]))

(defn -main [& args]
  (let [port (if (empty? args)
               9997
               (parse-int (first args)))]
    (println "Starting the network server on port" port)
    (load-initial-data)
    (run port)
    (shutdown-agents)))
