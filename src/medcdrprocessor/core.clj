(ns medcdrprocessor.core
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clj-time.local :as l]
            [clj-time.format :as f]
            [clojure.tools.logging :as log]
            [clojure-watch.core :refer [start-watch]]
            [medcdrprocessor.utils :as utils]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.csv :as csv]
            [hikari-cp.core :as hikari]

            [clojure.core.async :as async])
  (:import (java.io File)))


#_(defonce my-executor
         (let [executor-svc (Executors/newFixedThreadPool
                              1
                              (conc/counted-thread-factory "async-dispatch-%d" true))]
           (reify protocols/Executor
             (protocols/exec [this r]
               (.execute executor-svc ^Runnable r)))))

#_(alter-var-root #'clojure.core.async.impl.dispatch/executor
                (constantly (delay my-executor)))

(def databaseInfo (atom {}))

(def ds (delay (hikari/make-datasource
                     {:connection-timeout 30000
                      :idle-timeout 600000
                      :max-lifetime 1800000
                      :minimum-idle 10
                      :maximum-pool-size  10
                      :adapter "postgresql"
                      :username (:username @databaseInfo)
                      :password (:password @databaseInfo)
                      :database-name (:dbname @databaseInfo)
                      :server-name (:dburl @databaseInfo)
                      :port-number (:dbport @databaseInfo)})))



(defn- check-float [val]
  (if (empty? val) 0.0 val))
;CREATE TABLE public.tbl_tmp_reco_loan_cdrs_20191105 PARTITION OF public.tbl_tmp_reco_loan_cdrs
;    FOR VALUES FROM ('2019-11-05 00:00:00+01') TO ('2019-11-06 00:00:00+01');
(defn parse-text [from tempfile fname]
  "Convert a text into rows of columns"
  (let [reader (io/reader from)
        datacount (count (first (csv/read-csv reader :separator \>)))]
    (condp = datacount
      ;refillDA
      51 (do
           (with-open [writer (io/writer tempfile)]
             (doall
               (->> (csv/read-csv reader :separator \>)
                    (map #(list (nth % 0 nil) (nth % 1 nil) (nth % 2 nil) (nth % 4 nil) (nth % 5 nil) (nth % 6 nil)
                                (nth % 7 nil)
                                (-> (f/formatter "yyyyMMddHHmmss")
                                    (f/parse (str (nth % 8 nil) (nth % 9 nil)))
                                    (l/format-local-time :mysql)
                                    (try (catch Exception _ nil)))
                                (nth % 10 nil) (nth % 11 nil) (nth % 12 nil)
                                (nth % 13 nil) (nth % 14 nil) (nth % 15 nil) (nth % 16 nil) (nth % 17 nil) (nth % 18 nil)
                                (nth % 19 nil) (nth % 20 nil) (nth % 21 nil) (nth % 22 nil) (nth % 25 nil) (nth % 27 nil)
                                (nth % 28 nil) fname (nth % 8 nil) (nth % 9 nil)))
                    (csv/write-csv writer))))
           :da)
      ;;refillMA
      46 (do
           (with-open [writer (io/writer tempfile)]
             (doall
               (->> (csv/read-csv reader :separator \>)
                    (map #(list (nth % 0 nil) (nth % 1 nil) (nth % 3 nil) (nth % 5 nil)
                                (-> (f/formatter "yyyyMMddHHmmss")
                                    (f/parse (str (nth % 6 nil) (nth % 7 nil)))
                                    (l/format-local-time :mysql)
                                    (try (catch Exception _ nil))) (nth % 8 nil)
                                (nth % 9 nil) (nth % 12 nil) (nth % 13 nil) (nth % 15 nil) (nth % 16 nil) (nth % 17 nil) (nth % 18 nil)
                                (nth % 19 nil) (nth % 20 nil) (nth % 21 nil) (nth % 22 nil) (nth % 32 nil) (nth % 33 nil)
                                (nth % 35 nil) (nth % 36 nil) (nth % 45 nil) fname (nth % 6 nil) (nth % 7 nil)))
                    (csv/write-csv writer))))
           :ma)
      (log/errorf "Unknown file data" from))))



(defn- table->str
  "Transform a table spec to an entity name for SQL. The table spec may be a
  string, a keyword or a map with a single pair - table name and alias."
  [table entities]
  (let [entities (or entities identity)]
    (if (map? table)
      (let [[k v] (first table)]
        (str (jdbc/as-sql-name entities k) " " (jdbc/as-sql-name entities v)))
      (jdbc/as-sql-name entities table))))

(defn- col->str
  "Transform a column spec to an entity name for SQL. The column spec may be a
  string, a keyword or a map with a single pair - column name and alias."
  [col entities]
  (if (map? col)
    (let [[k v] (first col)]
      (str (jdbc/as-sql-name entities k) " AS " (jdbc/as-sql-name entities v)))
    (jdbc/as-sql-name entities col)))

(defn- clean-data [values col-count line]
  (loop [b (first values)
         c (rest values)
         res []]
    (if (nil? b)
      res
      (if (or (not (= (count b) col-count)) (empty? (get b line)))
        (do
          (log/errorf "inconsistent number of columns, value=%s" b)
          (recur (first c) (rest c) res))
        (recur (first c) (rest c) (conj res b))))))

(defn- insert-multi-row
    "Given a table and a list of columns, followed by a list of column value sequences,
		return a vector of the SQL needed for the insert followed by the list of column
		value sequences. The entities function specifies how column names are transformed."
    [table columns values entities type]
    (let [nc (count columns)
          ;vcs (map count values)
          line (if (= type :da) 7 4)
          _ (log/debugf "%s count of columns=%s||first count of values=%s <=line %s"type nc (count (first values)) line)
          newdata (clean-data values nc line)]
      (when (not (empty? newdata))
        (log/infof "inserting data %s" [table entities type])
       (cond (= type :da)(into [(str "INSERT INTO " (table->str table entities)
                                      (when (seq columns)
                                        (str " ( "
                                             (str/join ", " (map (fn [col] (col->str col entities)) columns))
                                             " )"))
                                      " VALUES (?,?,?,?,?,?,?,?::timestamp,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")]
                                newdata)
         (= type :ma) (into [(str "INSERT INTO " (table->str table entities)
                                   (when (seq columns)
                                     (str " ( "
                                          (str/join ", " (map (fn [col] (col->str col entities)) columns))
                                          " )"))
                                   " VALUES (?,?,?,?,?::timestamp,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")]
                             newdata)))))

(defn- insert-cols
  "Given a database connection, a table name, a sequence of columns names, a
  sequence of vectors of column values, one per row, and an options map,
  insert the rows into the database."
  [db table cols values opts type]
  (let [_ (log/infof "filtering data %s" [db table opts type])
        {:keys [entities transaction? tempfile] :as opts}
        (merge {:entities identity :transaction? true} (when (map? db) db) opts)
        sql-params (insert-multi-row table cols values entities type)]
    ;(log/infof "sql-params=%s" sql-params)
    (if (jdbc/db-find-connection db)
      (jdbc/db-do-prepared db transaction? sql-params (assoc opts :multi? true))
      (doall
        (with-open [con (jdbc/get-connection db opts)]
          (jdbc/db-do-prepared (jdbc/add-connection db con) transaction? sql-params
                        (assoc opts :multi? true)))
          (log/info "loadfile >>" tempfile)))))

;(insert-cols db table cols-or-row [values-or-opts] {})

(defn insert-stream
  [array tempfile file-cdr-type]
  (log/infof "insert-stream [%s|%s]" tempfile file-cdr-type)
  (condp = file-cdr-type
    :da (insert-cols {:datasource @ds}                      ;
                     :tbl_air_refill_da
                     [:origin_node_type :origin_host_name :origin_transaction_id
                      :da_ua_type :da_ua_id :da_ua_account_balance_before :da_ua_account_balance_after
                      :transaction_start_date_time :current_service_class
                      :voucher_based_refill :transaction_type :transaction_code :transaction_currency
                      :refill_type :voucher_serial_number :voucher_group_id :subscriber_number
                      :account_number :external_data1 :external_data2
                      :refill_division_amount :dedicated_account_unit :account_expiry_date :primary_recharge_account_id
                      :filename :transaction_start_date :transaction_start_time]
                     array
                     {:tempfile tempfile}
                     :da)
    :ma (insert-cols {:datasource @ds}                      ;
                     :tbl_air_refill_ma
                     [:origin_node_type :origin_host_name :origin_transaction_id :host_name
                      :transaction_start_date_time :current_service_class :voucher_based_refill
                      :transaction_amount :transaction_currency :refill_division_amount
                      :refill_type :voucher_serial_number :voucher_group_key :account_number :subscriber_number
                      :account_balance_before :account_balance_after :external_data1 :external_data2
                      :cell_identifier :primary_recharge_account_id :location_number
                      :filename :transaction_start_date :transaction_start_time]
                     array
                     {:tempfile tempfile}
                     :ma)
    (throw (Exception. (format "cdr type (expected=[da ma], found=%s)" file-cdr-type))))
  (io/delete-file (io/as-file tempfile))
  )


(defn text->map
  "Parses file and returns a maplist"
  [file tempdir archivedir]
  (log/info "file ->" file)
  (let [fname (.getName file)
        archive (str archivedir fname)
        tempfile (str tempdir fname ".txt")
        file-cdr-type (parse-text file tempfile fname)
        fstream (slurp tempfile)
        streamarray (map #(str/split % #",")
                         (str/split-lines fstream))]

    ;(log/info "streamarray" (count col) "====" vcs)
    ;;remove header
    (insert-stream (rest streamarray) tempfile file-cdr-type)
   #_((->> (rest streamarray)
         (partition 10000)
         (map (fn [batch] (insert-stream batch tempfile file-cdr-type)))
         (dorun))
    (io/delete-file (io/as-file tempfile)))

    (->> (File. archive)
         (.renameTo file))
    (count tempfile)))

(defn processfile [event filename tempdir archivedir]
  (when (str/ends-with? (.getName filename) "csv")
    (log/info event filename)
    (async/go (utils/with-func-timed "processFile" filename (text->map filename tempdir archivedir)))))

(defn -main
  [& args]
  (let [config (System/getProperty "medconfig")
        configdetails (read-string (slurp config))
        {:keys [incomingdir tempdir archivedir dbinfo]} configdetails
        _ (log/infof "Details [%s]" configdetails)
        _ (reset! databaseInfo dbinfo)]
    ;(watch-dir println (io/file path))
    (start-watch [{:path        incomingdir
                   :event-types [:create :modify]
                   :bootstrap   (fn [path] (let [_ (log/info "Starting to watch " path)
                                                 files (file-seq (io/file path))]
                                             (loop [n (second files)
                                                    nx (drop 2 files)]
                                               (when-not (nil? n)
                                                 ;(log/info "=>>"n "=="nx)
                                                 (processfile :initial n tempdir archivedir)
                                                 (recur (first nx) (rest nx))))))
                   :callback    (fn [event filename] (processfile event (io/as-file filename) tempdir archivedir))
                   :options     {:recursive false}}])))




