(ns metabase.driver.iseries
  "Metabase IBM DB2 for IBM i Driver"
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [java-time.api :as t]
   [metabase.driver :as driver]
   [metabase.driver.sql :as driver.sql]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql.parameters.substitution :as sql.params.substitution]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.util :as sql.u]
   [metabase.util.date-2 :as u.date]
   [honey.sql :as sql]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.log :as log]
   [metabase.util.ssh :as ssh]
   [schema.core :as s])
  (:import [java.sql ResultSet Types]
           java.util.Date)
  (:import (java.sql DatabaseMetaData ResultSet))
  (:import (java.sql ResultSet Timestamp Types)
           (java.util Date)
           (java.time LocalDateTime OffsetDateTime OffsetTime ZonedDateTime LocalDate LocalTime)
           (java.time.temporal Temporal)))

(set! *warn-on-reflection* true)

(driver/register! :iseries, :parent :sql-jdbc)

(doseq [[feature supported?] {:connection-impersonation  false 
                              :describe-fields           true 
                              :describe-fks              true 
                              :native-parameters         false
                              :upload-with-auto-pk       true
                              :uuid-type                 false
                              :identifiers-with-spaces   false
                              :nested-field-columns      false
                              :test/jvm-timezone-setting false}]
  (defmethod driver/database-supports? [:iseries feature] [_driver _feat _db] supported?))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :iseries [_] "IBM i")

(defmethod driver/humanize-connection-error-message :iseries
  [_ message]
  (condp re-matches message
    #"^FATAL: database \".*\" does not exist$"
    :database-name-incorrect

    #"^No suitable driver found for.*$"
    :invalid-hostname

    #"^Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.$"
    :cannot-connect-check-host-and-port

    #"^FATAL: role \".*\" does not exist$"
    :username-incorrect

    #"^FATAL: password authentication failed for user.*$"
    :password-incorrect

    #"^FATAL: .*$" ; all other FATAL messages: strip off the 'FATAL' part, capitalize, and add a period
    (let [[_ message] (re-matches #"^FATAL: (.*$)" message)]
      (str (str/capitalize message) \.))

    message))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

;; Wrap a HoneySQL datetime EXPRession in appropriate forms to cast/bucket it as UNIT.
(defmethod sql.qp/date [:iseries :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:iseries :minute]          [_ _ expr] (h2x/minute expr))
;;(defmethod sql.qp/date [:iseries :minute-of-hour]  [_ _ expr] (::h2x/extract :minute expr))
(defmethod sql.qp/date [:iseries :hour]            [_ _ expr] (h2x/hour expr))
;;(defmethod sql.qp/date [:iseries :hour-of-day]     [_ _ expr] (::h2x/extract :hour expr))
(defmethod sql.qp/date [:iseries :day]             [_ _ expr] expr) 
(defmethod sql.qp/date [:iseries :day-of-month]    [_ _ expr] (::h2x/extract :day expr))
(defmethod sql.qp/date [:iseries :week]
  [_ _ expr]
  (let [expr-sql (first (sql/format expr {:nested true}))]
    [:- expr
     [:raw (format "MOD(DAYOFWEEK(%s) + 5, 7) days" expr-sql)]]))
(defmethod sql.qp/date [:iseries :month]           [_ _ expr] [:first_day expr])
;;(defmethod sql.qp/date [:iseries :month-of-year]   [_ _ expr] (::h2x/extract :month expr))
(defmethod sql.qp/date [:iseries :quarter]         [_ _ expr] (h2x/quarter expr))
(defmethod sql.qp/date [:iseries :year]            [_ _ expr] (h2x/year expr))
;;(defmethod sql.qp/date [:iseries :week-of-year]    [_ _ expr] (h2x/week expr))
(defmethod sql.qp/date [:iseries :day-of-week]     [_ _ expr] (::h2x/extract :dayofweek expr))
(defmethod sql.qp/date [:iseries :day-of-year]     [_ _ expr] (::h2x/extract :dayofyear expr))
;;(defmethod sql.qp/date [:iseries :quarter-of-year] [_ _ expr] (::h2x/extract :quarter expr))
;;(defmethod sql.qp/date [:iseries :year-of-era]      [_ _ expr] (::h2x/year expr))

(defmethod sql.qp/add-interval-honeysql-form :iseries [_ hsql-form amount unit]
  (h2x/+ (h2x/->timestamp hsql-form) (case unit
    :second  [:raw (format "%d seconds" (int amount))]
    :minute  [:raw (format "%d minutes" (int amount))]
    :hour    [:raw (format "%d hours" (int amount))]
    :day     [:raw (format "%d days" (int amount))]
    :week    [:raw (format "%d days" (int (* amount 7)))]
    :month   [:raw (format "%d months" (int amount))]
    :quarter [:raw (format "%d months" (int (* amount 3)))]
    :year    [:raw (format "%d years" (int amount))]
  )))

(defmethod sql.qp/unix-timestamp->honeysql [:iseries :seconds] [_ _ expr]
  (h2x/+ [:raw "timestamp('1970-01-01 00:00:00')"] [:raw (format "%d seconds" (int expr))])

(defmethod sql.qp/unix-timestamp->honeysql [:iseries :milliseconds] [_ _ expr]
  (h2x/+ [:raw "timestamp('1970-01-01 00:00:00')"] [:raw (format "%d seconds" (int (/ expr 1000)))])))

(def ^:private now [:raw "current timestamp"])

(defmethod sql.qp/current-datetime-honeysql-form :iseries [_] now)

(defmethod sql.qp/->honeysql [:iseries Boolean]
  [_ bool]
  (if bool 1 0))

(defmethod sql.qp/->honeysql [:iseries :substring]
  [driver [_ arg start length]]
  		(if length
    	[:substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver start) [:min [:length (sql.qp/->honeysql driver arg)] (sql.qp/->honeysql driver length)]]
    	[:substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver start)]))


;; Use LIMIT OFFSET support DB2 v9.7 https://www.ibm.com/developerworks/community/blogs/SQLTips4DB2LUW/entry/limit_offset?lang=en
;; Maybe it could not to be necessary with the use of DB2_COMPATIBILITY_VECTOR
(defmethod sql.qp/apply-top-level-clause [:iseries :limit]
  [_ _ honeysql-query {value :limit}]
  {:select [:*]
   ;; if `honeysql-query` doesn't have a `SELECT` clause yet (which might be the case when using a source query) fall
   ;; back to including a `SELECT *` just to make sure a valid query is produced
   :from   [(-> (merge {:select [:*]}
                       honeysql-query)
                (update :select sql.u/select-clause-deduplicate-aliases))]
   :fetch  [:raw value]})

(defmethod sql.qp/apply-top-level-clause [:iseries :page]
  [driver _ honeysql-query {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can use the single-nesting implementation for `apply-limit`
      (sql.qp/apply-top-level-clause driver :limit honeysql-query {:limit items})
      ;; if we need to do an offset we have to do double-nesting
      {:select [:*]
       :from   [{:select [:tmp.* [[:raw "ROW_NUMBER() OVER()"] :rn]]
                 :from   [[(merge {:select [:*]}
                                  honeysql-query)
                           :tmp]]}]
       :where  [:raw (format "rn BETWEEN %d AND %d" offset (+ offset items))]})))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql date workarounds                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

;; Filtering with dates causes a -245 error. ;;v0.33.x
;; Explicit cast to timestamp when Date function is called to prevent db2 unknown parameter type.
;; Maybe it could not to be necessary with the use of DB2_DEFERRED_PREPARE_SEMANTICS
(defmethod sql.qp/->honeysql [:iseries Date]
  [_ date]
  		(h2x/->timestamp (t/format "yyyy-MM-dd HH:mm:ss" date))) ;;v0.34.x needs it?

(defmethod sql.qp/->honeysql [:iseries Timestamp]
  [_ date]
  		(h2x/->timestamp (t/format "yyyy-MM-dd HH:mm:ss" date)))


;; MEGA HACK from sqlite.clj ;;v0.34.x
;; Fix to Unrecognized JDBC type: 2014. ERRORCODE=-4228
(defn- zero-time? [t]
  (= (t/local-time t) (t/local-time 0)))

(defmethod sql.qp/->honeysql [:iseries LocalDate]
  [_ t]
  [:date (h2x/literal (u.date/format-sql t))])

(defmethod sql.qp/->honeysql [:iseries LocalDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    [:datetime (h2x/literal (u.date/format-sql t))]))

(defmethod sql.qp/->honeysql [:iseries LocalTime]
  [_ t]
  [:time (h2x/literal (u.date/format-sql t))])

(defmethod sql.qp/->honeysql [:iseries OffsetDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    [:datetime (h2x/literal (u.date/format-sql t))]))

(defmethod sql.qp/->honeysql [:iseries OffsetTime]
  [_ t]
  [:time (h2x/literal (u.date/format-sql t))])

(defmethod sql.qp/->honeysql [:iseries ZonedDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    [:datetime (h2x/literal (u.date/format-sql t))]))

;; DB2 doesn't like Temporal values getting passed in as prepared statement args, so we need to convert them to
;; date literal strings instead to get things to work (fix from sqlite.clj)
(s/defmethod driver.sql/->prepared-substitution [:iseries Temporal] :- driver.sql/PreparedStatementSubstitution
  [_driver date]
  ;; for anything that's a Temporal value convert it to a yyyy-MM-dd formatted date literal string
  ;; For whatever reason the SQL generated from parameters ends up looking like `WHERE date(some_field) = ?`
  ;; sometimes so we need to use just the date rather than a full ISO-8601 string
  (sql.params.substitution/make-stmt-subs "?" [(t/format "yyyy-MM-dd" date)]))


;; (.getObject rs i LocalDate) doesn't seem to work, nor does `(.getDate)`; ;;v0.34.x
;; Fixes merged from vertica.clj e sqlite.clj.
;; Fix to Invalid data conversion: Wrong result column type for requested conversion. ERRORCODE=-4461

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/DATE]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (let [t (u.date/parse s)]
        (log/tracef "(.getString rs %d) [DATE] -> %s -> %s" i s t)
        t))))

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/TIME]
  [_driver ^ResultSet rs _rsmeta ^Long i]
  (fn read-time []
    (when-let [s (.getString rs i)]
      (let [t (u.date/parse s)]
        (log/tracef "(.getString rs %d) [TIME] -> %s -> %s" i s t)
        t))))

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/TIMESTAMP]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (let [t (u.date/parse s)]
        (log/tracef "(.getString rs %d) [TIMESTAMP] -> %s -> %s" i s t)
        t))))

;; instead of returning a CLOB object, return the String
(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/CLOB]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (.getString rs i)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.conn/connection-details->spec :iseries [_ {:keys [host port dbname ssl]
                                                           :or   {host "localhost", port 8471, dbname ""}
                                                           :as   details}]
  (-> (merge {:classname   "com.ibm.as400.access.AS400JDBCDriver"
              :subprotocol "as400"
              :subname     (str "//" host ":" port "/" dbname)
              :sslConnection (boolean ssl)
              }
             (dissoc details :host :port :dbname :ssl))
      (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon))) ;; todo comprendre pourquoi changer seperator en separator ne fonctionne pas

(defmethod driver/can-connect? :iseries [driver details]
  (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel! details))]
    (= 1 (first (vals (first (jdbc/query connection ["SELECT 1 FROM SYSIBM.SYSDUMMY1"])))))))

;; custom DB2 type handling
(def ^:private database-type->base-type
  (some-fn (sql-jdbc.sync/pattern-based-database-type->base-type
            [])  ; no changes needed here
           {
            :BIGINT       :type/BigInteger
            :BINARY       :type/*
            :BLOB         :type/*
            :BOOLEAN      :type/Boolean
            :CHAR         :type/Text
            :NCHAR        :type/Text
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
            :VARCHAR      :type/Text
            :NVARCHAR     :type/Text
            :VARGRAPHIC   :type/Text
            :XML          :type/*
            (keyword "CHAR () FOR BIT DATA")      :type/*
            (keyword "CHAR() FOR BIT DATA") :type/*
            (keyword "LONG VARCHAR")              :type/*
            (keyword "LONG VARCHAR FOR BIT DATA") :type/*
            (keyword "LONG VARGRAPHIC")           :type/*
            (keyword "VARCHAR () FOR BIT DATA")   :type/*
            (keyword "VARCHAR() FOR BIT DATA")   :type/*})) ; interval literal

;; Use the same types as we use for PostgreSQL - with the above modifications
(defmethod sql-jdbc.sync/database-type->base-type :iseries
  [driver column-type]
  (or (database-type->base-type column-type)
      ((get-method sql-jdbc.sync/database-type->base-type :postgres) driver column-type)))

(defmethod sql-jdbc.sync/excluded-schemas :iseries [_]
  #{"SQLJ"
    "QCCA"
    "QCLUSTER"
    "QDNS"
    "QDSNX"
    "QFNTCPL"
    "QFNTWT"
    "QFPNTWE"
    "QGDDM"
    "QICSS"
    "QICU"
    "QIWS"
    "QJRNL"
    "QMSE"
    "QNAVSRV"
    "QNEWNAVSRV"
    "QPASE"
    "QPFRDATA"
    "QQALIB"
    "QRCL"
    "QRECOVERY"
    "QRPLOBJ"
    "QSHELL"
    "QSMP"
    "QSOC"
    "QSPL"
    "QSR"
    "QSRV"
    "QSRVAGT"
    "QSYSCGI"
    "QSYSDIR"
    "QSYSINC"
    "QSYSLOCALE"
    "QSYSNLS"
    "QSYS2"
    "QTEMP"
    "QUSRBRM"
    "QUSRDIRDB"
    "QUSRTEMP"
    "QUSRTOOL"
    "QTCP"
    "QSYS"
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

(defmethod driver/describe-database :iseries
  [_ database]
  {:tables
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (set
      (for [{:keys [schema, table, description ]} (jdbc/query {:connection conn} ["select schema, table, coalesce(case when table_text = '' then null else table_text end,'') as description from etllib.metabase_table_metadata allowed left join qsys2.systables on schema = table_schema and table_name = table"])]
        {:name   (not-empty table) ; column name differs depending on server (SparkSQL, hive, Impala)
         :schema (not-empty schema)
         :description (not-empty description)})))})


(defmethod sql-jdbc.execute/set-timezone-sql :iseries
  [_]
  "SET SESSION TIME ZONE = %s;")

(defmethod driver/db-default-timezone :iseries
  [driver database]
  (sql-jdbc.execute/do-with-connection-with-options
   driver database nil
   (fn [^java.sql.Connection conn]
     (with-open [stmt (.prepareStatement conn "SELECT CURRENT_TIMEZONE FROM SYSIBM.SYSDUMMY1")
                 rset (.executeQuery stmt)]
       (when (.next rset)
         (let [db2-offset (.getLong rset 1) ; Retrieve as Long
               ;; Convert Long to string and format as ISO-8601
               formatted-offset (format "%+03d:%02d"
                                        (quot db2-offset 10000) ; Hours
                                        (mod (quot db2-offset 100) 100)) ; Minutes
               zone-offset (java.time.ZoneOffset/of formatted-offset)]
           zone-offset)))))) ; Return ZoneOffset

