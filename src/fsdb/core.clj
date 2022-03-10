(ns fsdb.core
  (:require
    clojure.edn
    [clojure.java.io :as io]
    [me.raynes.fs :as fs])) 

;;; ----------------------------------------------------------------------------
;;; Filesystem-based Database
;;; ----------------------------------------------------------------------------

(comment
  "What's the purpose of this project?
  - I want to come up with a database that is ideal for prototyping projects.
  Basically I want to test ideas, iterate fast, and having do deal with a
  relational database is a hassle, because I have to come up with a schema
  and then if I want to change data schema I have to reset all the data and
  sometimes there will be some error and I will lose a lot of time figuring
  out what the fuck happened.
  
  What do I need to write to get a basic version of this database running?
  - I need to figure out how to represent a db, tables, and rows.
  
  Rows
  - I have a couple of options for this.
  - 1) Represent each row as a map in a file.
  - 2) Represent each row as a map, but have all the rows inside a single
  file. So the maps will be conj'ed onto a vector.
  - It seems pg used the 1st option.
  - pg uses the id as the file name. And then he stores the data as plain
  lisp objects.
  - I'm not sure I should go the route of the 2nd option. Mainly because 
  if I do that I'd just be reinventing the wheel. For the 2nd option there
  are some libs like Datomic and Datahike.
  - So I guess I'll be trying the 1st option.
  
  Tables
  - Will be represented as folders.
  - Each entity will have its own folder inside `db`.
  - There can also be other folders for cached items.
  
  Actions needed
  - Get a particular item.
  - Get all items.
  - Create an items.
  - Update an item.
  - Delete an item.
  
  From reading pg's code I can see that there is no complexity, really. He
  just saves the data as a plain hash-table and then keeps using it normally 
  on his code.")

;;; ----------------------------------------------------------------------------
;;; Global vars, utils

(def db-dir (io/file fs/*cwd* "resources" "db"))
(def settings-path (io/file db-dir "settings.edn"))

(defn load-edn
  "Load edn from an io/reader source (filename or io/resource)."
  [source]
  (try
    (with-open [r (io/reader source)]
      (clojure.edn/read 
        (java.io.PushbackReader. 
          r)))

    (catch java.io.IOException e
      (printf "Couldn't open '%s': %s\n" source (.getMessage e)))
    (catch RuntimeException e
      (printf "Error parsing edn file '%s': %s\n" source (.getMessage e)))))

(defn save-edn!
  "Save edn data to file."
  [file data]
  (with-open [wrt (io/writer file)]
    (binding [*out* wrt]
      (prn data)))
  data)

(defn table-path
  "Returns the table's path to the dir where records are stored."
  [tname]
  (get-in @settings [:tables tname :path]))

(defn table-file
  "Return the file for the table dir/record, if it exists."
  ([tname] 
   (table-file tname nil))
  ([tname id]
   (when-let [path (table-path tname)]
     (let [file (or (and id (io/file path (str id)))
                    (io/file path))]
       (when (fs/exists? file)
         file)))))

;;; ----------------------------------------------------------------------------
;;; Settings

; Management of id increment

(def settings 
  "DB settings."
  (atom nil))

(defn load-settings! []
  (reset! settings (load-edn settings-path)))

(load-settings!)

(defn save-settings! []
  (save-edn! settings-path @settings))
 
(defn next-id! 
  "Increment the table's counter and return the incremented number."
  [tname]
  (get-in (swap! settings update-in [:tables tname :counter] inc)
          [:tables tname :counter]))

(defn setup!
  "Must be run the first time the program is used to setup the db dirs and
  settings file."
  []
  (when-not (fs/exists? db-dir)
    (fs/mkdirs db-dir))
  (when-not (fs/exists? settings-path)
    (save-edn! settings-path {}))
  (load-settings!))


;;; ----------------------------------------------------------------------------
;;; CREATE, DELETE TABLE

(defn create-table
  "Creates the settings for the table. These settings will be used when 
  querying the table."
  [tname]
  (when-not (table-path tname)
    (let [table-path (io/file db-dir (name tname))
          config {:path (str table-path)
                  :counter 0}]
      ;; Create a the dir where the records will be saved.
      (fs/mkdir table-path)
      ;; Update the settings with the new table config.
      (save-edn!
        settings-path
        (swap! settings assoc-in [:tables tname] config)))))

(defn delete-table!
  "Deletes all data and settings related to the given table."
  [tname]
  (when-let [dir (table-path tname)]
    (fs/delete-dir dir)
    (save-edn!
      settings-path
      (swap! settings update :tables dissoc tname))))


;;; ----------------------------------------------------------------------------
;;; GET, SAVE, DELETE

; All files are expected to contain edn objects, so we just use
; clojure.edn.read when loading them from the file.

(defn get-by-id
  "Reads and returns the contents of the given file."
  [tname id]
  (some-> (table-file tname id)
          load-edn))

(defn get-all
  "Reads and returns the contents of the given dir."
  [tname]
  (some->> (table-file tname)
           fs/list-dir
           (mapv get-by-path)))

(defn create!
  "Creates a new table record. Returns the data with the id."
  [tname data]
  ; We generate the next id for the table and assoc it to the data map before
  ; adding the data to the db.
  (let [id (next-id! tname)
        data (assoc data (keyword (name tname) "id") id)]
    (save-edn! (io/file
                (table-path tname)
                (str id))
              data)))

(defn update!
  "Updates the record for the given table id."
  [tname data]
  (when-let [f (table-file tname 
                           (get data (keyword (name tname) "id")))]
    (save-edn! f data)))

(defn delete!
  "Deletes the record at the given file. If successful returns true. If the
  file doesn't exist, returns false."
  [tname id]
  (some-> (table-file tname id)
          fs/delete))

; Now that I have completed some basic functionaly I realized that I am missing
; some crucial features. 
; - id counter 

(comment
  "tests"
  (delete-table! :user)
  (create-table :user)
  (get-by-id :user 1)
  (get-all :user)
  (create! :user {:user/name "Guest"})
  (update! :user {:user/id 1 :user/name "gu357" :user/age 18})
  (delete! :user 1))