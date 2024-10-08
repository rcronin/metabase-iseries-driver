(ns metabase.driver.iseries
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.set :as set]
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
            [metabase.util.date-2 :as du]
            [metabase.util.ssh :as ssh]
            [metabase.util.i18n :refer [trs]]
            [metabase.driver.sql :as sql]
            [metabase.query-processor.timezone :as qp.timezone]
            [schema.core :as s])
  (:import java.sql.Time)
  (:import [java.sql ResultSet Time Timestamp Types]
           [java.time Instant LocalDateTime OffsetDateTime OffsetTime ZonedDateTime LocalDate LocalTime]))

(set! *warn-on-reflection* true)

(driver/register! :iseries
                  :parent #{:sql-jdbc ::legacy/use-legacy-classes-for-read-and-set})

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :iseries [_] "DB2 for i")

;; (defmethod driver/supports? [:iseries :set-timezone] [_ _] false)

;; (defmethod driver/database-supports? [:iseries :now] [_driver _feat _db] false)
(doseq [[feature supported?] {:set-timezone         false
                              :now                  false}]
  (defmethod driver/database-supports? [:iseries feature] [_driver _feature _db] supported?))

(defmethod sql.qp/honey-sql-version :iseries
  [_driver]
  2)

(defmethod driver/db-start-of-week :iseries
  [_]
  :sunday)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- date-format [format-str expr] [:varchar_format expr (h2x/literal format-str)])
(defn- str-to-date [format-str expr] [:to_date expr (h2x/literal format-str)])

(defn- trunc-with-format [format-str expr]
(str-to-date format-str (date-format format-str expr)))

(defn- trunc [format-str expr]
  [:trunc_timestamp expr (h2x/literal format-str)])

(def ^:private timestamp-types
  #{"timestamp"})

(defn- cast-to-timestamp-if-needed
  "If `hsql-form` isn't already one of the [[timestamp-types]], cast it to `timestamp`."
  [hsql-form]
  (h2x/cast-unless-type-in "timestamp" timestamp-types hsql-form))

(defn- cast-to-date-if-needed
  "If `hsql-form` isn't already one of the [[timestamp-types]] *or* `date`, cast it to `date`."
  [hsql-form]
  (h2x/cast-unless-type-in "date" (conj timestamp-types "date") hsql-form))

(def ^:private ->date     (partial conj [:date]))

;; Wrap a HoneySQL datetime EXPRession in appropriate forms to cast/bucket it as UNIT.
;; See [this page](https://www.ibm.com/developerworks/data/library/techarticle/0211yip/0211yip3.html) for details on the functions we're using.
(defmethod sql.qp/date [:iseries :default]        [_ _ expr] expr)
(defmethod sql.qp/date [:iseries :second]         [_ _ expr] [::h2x/extract :second (h2x/->timestamp expr)])
(defmethod sql.qp/date [:iseries :second-of-minute] [_ _ expr] [::h2x/extract :second (h2x/->timestamp expr)])
(defmethod sql.qp/date [:iseries :minute]         [_ _ expr] [::h2x/extract :minute (h2x/->timestamp expr)])
(defmethod sql.qp/date [:iseries :minute-of-hour] [_ _ expr] [::h2x/extract :minute (h2x/->timestamp expr)])
(defmethod sql.qp/date [:iseries :hour]           [_ _ expr] [::h2x/extract :hour (h2x/->timestamp expr)])
(defmethod sql.qp/date [:iseries :hour-of-day]    [_ _ expr] [::h2x/extract :hour (h2x/->timestamp expr)])
(defmethod sql.qp/date [:iseries :day]            [_ _ expr] (->date expr))
(defmethod sql.qp/date [:iseries :day-of-month]   [_ _ expr] (->date expr))
(defmethod sql.qp/date [:iseries :week] [driver _ expr] (sql.qp/adjust-start-of-week driver (partial trunc :day) expr))
(defmethod sql.qp/date [:iseries :month]          [_ _ expr] (trunc :month expr))
(defmethod sql.qp/date [:iseries :month-of-year]  [_ _ expr] (trunc :month expr))
(defmethod sql.qp/date [:iseries :quarter]        [_ _ expr] (trunc :q expr))
(defmethod sql.qp/date [:iseries :year]           [_ _ expr] (trunc :year expr))
(defmethod sql.qp/date [:iseries :week-of-year]   [_ _ expr] [:week expr])
(defmethod sql.qp/date [:iseries :day-of-week]     [_ _ expr] [:dayofweek expr])
(defmethod sql.qp/date [:iseries :day-of-year]    [_ _ expr] [:dayofyear expr])

(defmethod sql.qp/add-interval-honeysql-form :iseries [_ hsql-form amount unit]
  (h2x/+ (cast-to-timestamp-if-needed hsql-form) (case unit
    :second  [:raw (format "%d seconds" (int amount))]
    :minute  [:raw (format "%d minutes" (int amount))]
    :hour    [:raw (format "%d hours" (int amount))]
    :day     [:raw (format "%d days" (int amount))]
    :week    [:raw (format "%d days" (* amount 7))]
    :month   [:raw (format "%d months" (int amount))]
    :quarter [:raw (format "%d months" (* amount 3))]
    :year    [:raw (format "%d years" (int amount))]
  )))

(defmethod sql.qp/unix-timestamp->honeysql [:iseries :seconds] [_ _ expr]
  (h2x/+ [:raw "timestamp('1970-01-01 00:00:00')"] [:raw (format "%d seconds" (int expr))]))

(defmethod sql.qp/unix-timestamp->honeysql [:iseries :milliseconds] [driver _ expr]
  (h2x/+ [:raw "timestamp('1970-01-01 00:00:00')"] [:raw (format "%d seconds" (int (h2x// expr 1000)))]))

(defmethod sql.qp/->honeysql [:iseries Boolean]
  [_ bool]
  (if bool 1 0))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql date workarounds                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/->honeysql [:iseries Timestamp]
  [_ date]
  		(h2x/->timestamp (t/format "yyyy-MM-dd HH:mm:ss" date)))

(defn- zero-time? [t]
  (= (t/local-time t) (t/local-time 0)))

(defmethod sql.qp/->honeysql [:iseries LocalDate]
  [_ t]
  [:date (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:iseries LocalTime]
  [_ t]
  [:time (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:iseries OffsetTime]
  [_ t]
  [:time (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:iseries Boolean]
  [_ bool]
  (if bool 1 0))

(defmethod sql.qp/->honeysql [:iseries :concat]
  [driver [_ & args]]
  (into
   [:||]
   (mapv (partial sql.qp/->honeysql driver) args)))

(defmethod sql.qp/current-datetime-honeysql-form :iseries
  [_]
  (h2x/with-database-type-info [:raw "CURRENT_TIMESTAMP"] "timestamp"))

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/DATE]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        t))))

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/TIME]
  [_driver ^ResultSet rs _rsmeta ^Long i]
  (fn read-time []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        t))))

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/TIMESTAMP]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        t))))

(defmethod sql-jdbc.execute/set-parameter [:iseries OffsetDateTime]
  [driver ps i t]
  (sql-jdbc.execute/set-parameter driver ps i (t/sql-timestamp (t/with-offset-same-instant t (t/zone-offset 0)))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.conn/connection-details->spec :iseries
  [_ {:keys [host port dbname]
      :or   {host "localhost", port 3386, dbname ""}
      :as   details}]
  (-> (merge {:classname "com.ibm.as400.access.AS400JDBCDriver"   ;; must be in classpath
          :subprotocol "as400"
          :subname (str "//" host ":" port "/" dbname)}                    ;; :subname (str "//" host "/" dbname)}   (str "//" host ":" port "/" (or dbname db))}
         (dissoc details :host :port :dbname))
  (sql-jdbc.common/handle-additional-options details, :separator-style :semicolon)))

(defmethod driver/can-connect? :iseries [driver details]
  (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel! details))]
    (= 1 (first (vals (first (jdbc/query connection ["VALUES 1"])))))))

;; Mappings for DB2 types to Metabase types.
;; See the list here: https://docs.tibco.com/pub/spc/4.0.0/doc/html/ibmiseries/ibmiseries_data_types.htm
(defmethod sql-jdbc.sync/database-type->base-type :iseries [_ database-type]
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

(defmethod sql-jdbc.sync/excluded-schemas :iseries [_]
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

(defmethod sql-jdbc.execute/set-timezone-sql :iseries [_]
  "SET SESSION TIME ZONE = %s")

(defmethod sql-jdbc.sync/have-select-privilege? :iseries [driver conn table-schema table-name]
  true)

(defmethod driver/describe-database :iseries
  [_ database]
  {:tables
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (set
      (for [{:keys [schema, table, description ]} (jdbc/query {:connection conn} ["select schema, table, coalesce(case when table_text = '' then null else table_text end,'') as description from etllib.metabase_table_metadata allowed left join qsys2.systables on schema = table_schema and table_name = table"])]
        {:name   (not-empty table) ; column name differs depending on server (SparkSQL, hive, Impala)
         :schema (not-empty schema)
         :description (not-empty description)})))})