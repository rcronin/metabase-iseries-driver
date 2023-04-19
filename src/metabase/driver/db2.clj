(ns metabase.driver.db2
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clj-time
             [coerce :as tcoerce]
             [core :as tc]
             [format :as time]]
            [java-time :as t]
            [metabase.driver :as driver]
            [metabase.driver.common :as driver.common]
            [metabase.query-processor
             [store :as qp.store]
             [util :as qputil]]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql-jdbc
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [common :as sql-jdbc.common]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql-jdbc.execute.legacy-impl :as legacy]
            [metabase.util.honey-sql-2 :as h2x]
            [metabase.util.honeysql-extensions :as hx]
            [metabase.util.date-2 :as du]
            [metabase.util.ssh :as ssh]
            [metabase.driver.sql :as sql]
            [metabase.query-processor.timezone :as qp.timezone]
            [schema.core :as s])
  (:import java.sql.Time)
  (:import [java.sql ResultSet Time Timestamp]
           [java.time Instant LocalDateTime OffsetDateTime OffsetTime ZonedDateTime LocalDate LocalTime]))

(set! *warn-on-reflection* true)

(driver/register! :db2
                  :parent #{:sql-jdbc ::legacy/use-legacy-classes-for-read-and-set})

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :db2 [_] "DB2")

(defmethod driver/supports? [:db2 :set-timezone] [_ _] false)

(defmethod driver/database-supports? [:db2 :now] [_driver _feat _db] true)

(defmethod sql.qp/honey-sql-version :db2
  [_driver]
  2)

(defmethod driver/db-start-of-week :db2
  [_]
  :sunday)

;; Needs improvements and tests
(defmethod driver.common/current-db-time-date-formatters :db2 [_]
  (mapcat
   driver.common/create-db-time-formatters
   ["yyyy-MM-dd HH:mm:ss"
    "yyyy-MM-dd HH:mm:ss.SSS"
    "yyyy-MM-dd HH:mm:ss.SSSSS"
    "yyyy-MM-dd'T'HH:mm:ss.SSS"
    "yyyy-MM-dd HH:mm:ss.SSSZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    "yyyy-MM-dd HH:mm:ss.SSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    "yyyy-MM-dd HH:mm:ss.SSSSSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZZ"
    "yyyy-MM-dd HH:mm:ss.SSSSSSSSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSZZ"]))
(defmethod driver.common/current-db-time-native-query :db2 [_]
  "SELECT TO_CHAR(CURRENT TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') FROM SYSIBM.SYSDUMMY1")

(defmethod driver/current-db-time :db2 [& args]
  (apply driver.common/current-db-time args))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- date-format [format-str expr] [:varchar_format expr (h2x/literal format-str)])
(defn- str-to-date [format-str expr] [:to_date expr (h2x/literal format-str)])

(defn- trunc-with-format [format-str expr]
(str-to-date format-str (date-format format-str expr)))

(defn- trunc [format-str expr]
  [:trunc_timestamp expr (h2x/literal format-str)])

;; Wrap a HoneySQL datetime EXPRession in appropriate forms to cast/bucket it as UNIT.
;; See [this page](https://www.ibm.com/developerworks/data/library/techarticle/0211yip/0211yip3.html) for details on the functions we're using.
(defmethod sql.qp/date [:db2 :default]        [_ _ expr] expr)
(defmethod sql.qp/date [:db2 :minute]         [_ _ expr] (trunc :mi expr))
(defmethod sql.qp/date [:db2 :minute-of-hour] [_ _ expr] [::h2x/extract :minute (h2x/->timestamp expr)])
(defmethod sql.qp/date [:db2 :hour]           [_ _ expr] (trunc :hh expr))
(defmethod sql.qp/date [:db2 :hour-of-day]    [_ _ expr] [::h2x/extract :minute (h2x/->timestamp expr)])
(defmethod sql.qp/date [:db2 :day]            [_ _ expr] (trunc :dd expr))
(defmethod sql.qp/date [:db2 :day-of-month]   [_ _ expr] (trunc :day expr))
(defmethod sql.qp/date [:db2 :week] [driver _ expr] (sql.qp/adjust-start-of-week driver (partial trunc :day) expr))
(defmethod sql.qp/date [:db2 :month]          [_ _ expr] (trunc :month expr))
(defmethod sql.qp/date [:db2 :month-of-year]  [_ _ expr] (trunc :month expr))
(defmethod sql.qp/date [:db2 :quarter]        [_ _ expr] (trunc :q expr))
(defmethod sql.qp/date [:db2 :year]           [_ _ expr] (trunc :year expr))
(defmethod sql.qp/date [:db2 :week-of-year]   [_ _ expr] [:week expr])
(defmethod sql.qp/date [:db2 :day-of-week]     [_ _ expr] [:dayofweek expr])
(defmethod sql.qp/date [:db2 :day-of-year]    [_ _ expr] [:dayofyear expr])

(def ^:private ->timestamp (partial conj [:timestamp]))

(defmethod sql.qp/add-interval-honeysql-form :db2 [_ hsql-form amount unit]
  (h2x/+ (h2x/->timestamp hsql-form) (case unit
    :second  (hx/raw (format "%d seconds" (int amount)))
    :minute  (hx/raw (format "%d minutes" (int amount)))
    :hour    (hx/raw (format "%d hours" (int amount)))
    :day     (hx/raw (format "%d days" (int amount)))
    :week    (hx/raw (format "%d days" (* amount 7)))
    :month   (hx/raw (format "%d months" (int amount)))
    :quarter (hx/raw (format "%d months" (* amount 3)))
    :year    (hx/raw (format "%d years" (int amount)))
  )))

(defmethod sql.qp/unix-timestamp->honeysql [:db2 :seconds] [_ _ expr]
  (h2x/+ (hx/raw "timestamp('1970-01-01 00:00:00')") (hx/raw (format "%d seconds" (int expr))))

(defmethod sql.qp/unix-timestamp->honeysql [:db2 :milliseconds] [driver _ expr]
  (h2x/+ (hx/raw "timestamp('1970-01-01 00:00:00')") (hx/raw (format "%d seconds" (int (h2x// expr 1000)))))))

(defmethod sql.qp/->honeysql [:db2 Boolean]
  [_ bool]
  (if bool 1 0))

;; ;; Use LIMIT OFFSET support DB2 v9.7 https://www.ibm.com/developerworks/community/blogs/SQLTips4DB2LUW/entry/limit_offset?lang=en
;; ;; Maybe it could not to be necessary with the use of DB2_COMPATIBILITY_VECTOR
;; (defmethod sql.qp/apply-top-level-clause [:db2 :limit]
;;   [_ _ honeysql-query {value :limit}]
;;   (merge honeysql-query
;;          (hx/raw (format "FETCH FIRST %d ROWS ONLY" value))))

;; (defmethod sql.qp/apply-top-level-clause [:db2 :page]
;;   [driver _ honeysql-query {{:keys [items page]} :page}]
;;   (let [offset (* (dec page) items)]
;;     (if (zero? offset)
;;       ;; if there's no offset we can use the single-nesting implementation for `apply-limit`
;;       (sql.qp/apply-top-level-clause driver :limit honeysql-query {:limit items})
;;       ;; if we need to do an offset we have to do double-nesting
;;       {:select [:*]
;;        :from   [{:select [:tmp.* [(hx/raw "ROW_NUMBER() OVER()") :rn]]
;;                  :from   [[(merge {:select [:*]}
;;                                   honeysql-query)
;;                            :tmp]]}]
;;        :where  [(hx/raw (format "rn BETWEEN %d AND %d" offset (+ offset items)))]})))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql date workarounds                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/->honeysql [:db2 Timestamp]
  [_ date]
  		(h2x/->timestamp (t/format "yyyy-MM-dd HH:mm:ss" date)))

(defn- zero-time? [t]
  (= (t/local-time t) (t/local-time 0)))

(defmethod sql.qp/->honeysql [:db2 LocalDate]
  [_ t]
  [:date (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:db2 LocalTime]
  [_ t]
  [:time (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:db2 OffsetTime]
  [_ t]
  [:time (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:db2 Boolean]
  [_ bool]
  (if bool 1 0))

(defmethod sql.qp/->honeysql [:db2 :concat]
  [driver [_ & args]]
  (into
   [:||]
   (mapv (partial sql.qp/->honeysql driver) args)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.conn/connection-details->spec :db2
  [_ {:keys [host port dbname]
      :or   {host "localhost", port 3386, dbname ""}
      :as   details}]
  (-> (merge {:classname "com.ibm.as400.access.AS400JDBCDriver"   ;; must be in classpath
          :subprotocol "as400"
          :subname (str "//" host ":" port "/" dbname)}                    ;; :subname (str "//" host "/" dbname)}   (str "//" host ":" port "/" (or dbname db))}
         (dissoc details :host :port :dbname))
  (sql-jdbc.common/handle-additional-options details, :separator-style :semicolon)))

(defmethod driver/can-connect? :db2 [driver details]
  (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel! details))]
    (= 1 (first (vals (first (jdbc/query connection ["VALUES 1"])))))))

;; Mappings for DB2 types to Metabase types.
;; See the list here: https://docs.tibco.com/pub/spc/4.0.0/doc/html/ibmdb2/ibmdb2_data_types.htm
(defmethod sql-jdbc.sync/database-type->base-type :db2 [_ database-type]
  ({:BIGINT       :type/BigInteger
    :BINARY       :type/*
    :BLOB         :type/*
    :BOOLEAN      :type/Boolean
    :CHAR         :type/Text
    :CLOB         :type/Text
    :DATALINK     :type/*
    :DATE         :type/Date
    :DBCLOB       :type/Text
    :DECIMAL      :type/Decimal
    :DECFLOAT     :type/Decimal
    :DOUBLE       :type/Float
    :FLOAT        :type/Float
    :GRAPHIC      :type/Text
    :INTEGER      :type/Integer
    :NUMERIC      :type/Decimal
    :REAL         :type/Float
    :ROWID        :type/*
    :SMALLINT     :type/Integer
    :TIME         :type/Time
    :TIMESTAMP    :type/DateTime
    :VARBINARY    :type/*
    :VARCHAR      :type/Text
    :VARGRAPHIC   :type/Text
    :XML          :type/Text
    (keyword "CHAR () FOR BIT DATA")      :type/*
    (keyword "CHAR() FOR BIT DATA")       :type/*
    (keyword "LONG VARCHAR")              :type/*
    (keyword "LONG VARCHAR FOR BIT DATA") :type/*
    (keyword "LONG VARGRAPHIC")           :type/*
    (keyword "VARCHAR () FOR BIT DATA")   :type/*
    (keyword "VARCHAR() FOR BIT DATA")    :type/*} database-type))

(defmethod sql-jdbc.sync/excluded-schemas :db2 [_]
  #{"SQLJ"
    "QSYS"
    "QSYS2"
    "SYSCAT"
    "SYSFUN"
    "SYSIBM"
    "SYSIBMADM"
    "SYSIBMINTERNAL"
    "SYSIBMTS"
    "SPOOLMAIL"
    "SYSPROC"
    "SYSPUBLIC"
    "SYSTOOLS"
    "SYSSTAT"
    "QHTTPSVR"
    "QUSRSYS"})

(defmethod sql-jdbc.execute/set-timezone-sql :db2 [_]
  "SET SESSION TIME ZONE = %s")

(defmethod sql-jdbc.sync/have-select-privilege? :db2 [driver conn table-schema table-name]
  true)

;; Overridden to have access to the database with the configured property dbnames (inclusion list)
;; which will be used to filter the schemas.
(defmethod driver/describe-database :db2
  [_ database]
  {:tables
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (set
      (for [{:keys [schema, table ]} (jdbc/query {:connection conn} ["select table_schem as schema, table_name as table from sysibm.sqltableprivileges where grantee = 'METABASE' and privilege = 'SELECT'"])]
        {:name   (not-empty table) ; column name differs depending on server (SparkSQL, hive, Impala)
         :schema (not-empty schema)})))})