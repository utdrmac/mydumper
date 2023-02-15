
#include <glib/gstdio.h>
#include <gio/gio.h>
#include <mysql.h>

extern gchar *defaults_file;
extern GList *all_dbts;
extern GOptionEntry common_filter_entries[];
extern GOptionEntry common_connection_entries[];
extern GOptionEntry common_entries[];
extern gboolean program_version;
extern guint verbose;
extern gboolean debug;
extern gchar *tables_list;
extern gchar *fields_enclosed_by_ld;
extern gchar *lines_starting_by_ld;
extern gchar *statement_terminated_by_ld;
extern gboolean insert_ignore;
extern gboolean hex_blob;
extern gchar *lines_terminated_by_ld;
extern gchar *fields_terminated_by_ld;
extern gboolean csv;
extern gboolean replace;
extern guint chunk_filesize;
extern gchar *ignore_engines;
extern int build_empty_files;
extern gboolean exit_if_broken_table_found;
extern gboolean data_checksums;
extern gboolean no_dump_views;
extern gboolean views_as_tables;
extern gboolean dump_checksums;
extern gboolean split_partitions;
extern guint char_deep;
extern guint64 max_rows;
extern gchar *exec_per_thread_extension;
extern gchar *exec_per_thread;
extern gboolean order_by_primary_key;
extern guint num_exec_threads;
extern guint snapshot_interval;
extern int killqueries;
extern int longquery;
extern int longquery_retry_interval;
extern int longquery_retries;
extern int lock_all_tables;
extern gboolean no_backup_locks;
extern gchar *logfile;
extern gboolean help;
extern GKeyFile * key_file;
extern char **tables;
extern FILE * (*m_open)(const char *filename, const char *);
extern GAsyncQueue *start_scheduled_dump;
extern GAsyncQueue *stream_queue;
extern gboolean daemon_mode;
extern gboolean dump_events;
extern gboolean dump_routines;
extern gboolean dump_tablespaces;
extern gboolean dump_triggers;
extern gboolean ignore_generated_fields;
extern gboolean less_locking;
extern gboolean load_data;
extern gboolean no_data;
extern gboolean no_delete;
extern gboolean no_locks;
extern gboolean no_schemas;
extern gboolean no_stream;
extern gboolean routine_checksums;
extern gboolean schema_checksums;
extern gboolean shutdown_triggered;
extern gboolean skip_definer;
extern gboolean stream;
extern gboolean success_on_1146;
extern gboolean use_fifo;
extern gboolean use_savepoints;
extern gchar *compress_extension;
extern gchar *db;
extern gchar *disk_limits;
extern gchar *dump_directory;
extern gchar *exec_command;
extern gchar *fields_escaped_by;
extern gchar *output_directory;
extern gchar *output_directory_param;
extern gchar *pmm_path;
extern gchar *pmm_resolution ;
extern gchar *rows_per_chunk;
extern gchar *set_names_statement;
extern gchar *set_names_str;
extern gchar *tables_skiplist_file;
extern gchar *tidb_snapshot;
extern gchar *where_option;
extern GHashTable *all_where_per_table;
extern gint database_counter;
extern gint non_innodb_done;
extern GList *innodb_table, *non_innodb_table;
extern GList *no_updated_tables;
extern GList *schema_post;
extern GList *table_schemas;
extern GList *trigger_schemas;
extern GList *view_schemas;
extern GMutex *ready_database_dump_mutex;
extern GMutex *ready_table_dump_mutex;
extern GString *set_global;
extern GString *set_global_back;
extern GString *set_session;
extern guint char_chunk;
extern guint complete_insert;
extern guint dump_number;
extern guint errors;
extern guint num_threads;
extern guint rows_per_file;
extern guint snapshot_count;
extern guint statement_size;
extern guint trx_consistency_only;
extern guint updated_since;
extern int compress_output;
extern int detected_server;
extern int errno;
extern int (*m_close)(void *file);
extern int (*m_write)(FILE * file, const char * buff, int len);
extern int need_dummy_read;
extern int need_dummy_toku_read;
extern int skip_tz;
extern MYSQL *main_connection;
extern struct configuration_per_table conf_per_table;
extern struct function_pointer pp;
extern gchar identifier_quote_character;

