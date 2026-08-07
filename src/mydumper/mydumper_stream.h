/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

        Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/
#define METADATA_PARTIAL_INTERVAL 2
#define MYDUMPER_STREAM_BUDGET_DEFAULT_MB 256U
void initialize_stream();
void *process_binary_stream(void *data);
void wait_stream_to_finish();
void stream_request_cancel(void);
void metadata_partial_queue_push(struct db_table *dbt);
void stream_queue_push(struct db_table *dbt,gchar *filename);
guint get_stream_queue_length();
void send_initial_metadata();
guint mydumper_stream_budget_mb_effective(void);

extern guint stream_budget_mb;
extern gboolean stream_budget_mb_user_set;
