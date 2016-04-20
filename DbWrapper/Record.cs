using DbWrapper.Read;
using DbWrapper.Remove;
using DbWrapper.Serializer;
using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading;
using System.Xml;

namespace DbWrapper {
	public enum DbMessage : byte {
		Success,
		Failed
	}

	[Serializable]
	public class Record : FlexObject {

		private Database _database;      // Database connection object
		private Record[] _recordList;    // List of records for when a query returns more than one
		private Query _query;         // SQL query used for SELECT statements
		private String _table;         // SQL table that the commands are pointed to
		private static String _searchProperty;// Property used for searching when doing Delete or Update
		private int _page;          // Determines what page the Query should use
		private int _recordIndex;   // Determines what record we are currently looking at

		/// <summary>
		/// Class constructor.
		/// </summary>
		public Record() : base() {
			_database = new Database();
			_table = String.Empty;
			_page = 1;
			_recordIndex = -1;
		}
		/// <summary>
		/// Class constructor.
		/// </summary>
		/// <param name="_db"></param>
		public Record(string table, Database db) : base() {
			_database = db;
			_table = table;
			_query = new Query(db);
			_page = 1;
			_recordIndex = -1;

			this.Initialize();
		}
		/// <summary>
		/// Class constructor.
		/// </summary>
		static Record() {
			_searchProperty = String.Empty;
		}

		/// <summary>
		/// Gets the list of records retrieved from the Read() and
		/// ReadAsync() methods.
		/// </summary>
		public Record[] Records {
			get { return _recordList; }
		}

		/// <summary>
		/// Returns the table that the record object is pointed to
		/// </summary>
		public string Table {
			get { return _table; }
		}

		public Database Database {
			get { return _database; }
		}

		public Query Query {
			get { return _query; }
		}

		/// <summary>
		/// Determines how many records are to be returned per page
		/// </summary>
		public static int PageSize {
			set { Query.PageSize = value; }
		}

		/// <summary>
		/// Determines what record property should be used when doing an
		/// update or a delete.  If this property is blank the API will 
		/// try to use all properties and their values.
		/// </summary>
		public static string SearchProperty {
			get { return _searchProperty; }
			set { _searchProperty = value; }
		}

		/// <summary>
		/// Gets the list of columns from the database for specified
		/// table and initializes the Record object with null values.
		/// </summary>
		private void Initialize() {
			_query.ColumnList = (Hashtable)Database.Schema[_table];
			base.PropertyListInfo = _query.ColumnList;
			base.IdentityColumns = Database.GetTableConstraints(_table);

			InitializeProperties();
		}

		private void InitializeProperties() {
			foreach (var column in base.PropertyListInfo.Keys) {
				Type t = (Type)base.PropertyListInfo[column];

				if (t.IsValueType) {
					if (t == typeof(DateTime))
						this.Properties.Add(column, SqlDateTime.MinValue);
					else
						this.Properties.Add(column, Activator.CreateInstance(t));
				}
				else if (!t.IsValueType && t == typeof(byte[])) {
					this.Properties.Add(column, new byte[0]);
				}
				else {
					this.Properties.Add(column, DBNull.Value);
				}
			}
		}

		/// <summary>
		/// Called by the Read and ReadAsync methods to retrieve
		/// the records from the database based on the query list
		/// provided.
		/// </summary>
		/// <returns></returns>
		private DbMessage GetRecords() {
			try {
				_query.Table = _table;
				_query.CreateCommand();
				_query.CurrentPage = _page;

				//TODO (Logan): Hide dataset details in the Query object.
				DataSet ds = _query.Execute();
				DataTable dt = null;

				if (_database.Engine == DatabaseEngine.SqlServer)
					dt = ds.Tables[1];
				else if (_database.Engine == DatabaseEngine.MySQL)
					dt = ds.Tables[0];

				_recordList = new Record[dt.Rows.Count];
				this.FillRecordList(dt);
				// Now I'm just getting the total number of records returned from the query
				_query.RecordCount = Convert.ToUInt32(dt.Rows.Count);

				return DbMessage.Success;
			}
			catch (Exception e) {
				return DbMessage.Failed;
			}
		}

		/// <summary>
		/// Creates a list of record objects from the SQL statement executed.
		/// </summary>
		/// <param name="dt"></param>
		private void FillRecordList(DataTable dt) {
			for (int i = 0; i < dt.Rows.Count; i++) {
				_recordList[i] = new Record();
				DataRow row = dt.Rows[i];
				this.FillPropertyList(dt.Columns, ref row, i);
			}
		}

		/// <summary>
		/// Fills the PropertyList object for a single record.
		/// </summary>
		/// <param name="columns"></param>
		/// <param name="row"></param>
		/// <param name="index"></param>
		private void FillPropertyList(DataColumnCollection columns,
									  ref DataRow row,
									  int index) {
			for (int j = 0; j < columns.Count; j++) {
				DataColumn col = columns[j];
				object dbValue = row[col];
				Type dataType = col.DataType;

				object val = new object();

				if (!DBNull.Value.Equals(row[col])) {
					// Use the data type to convert the object to the appropriate type
					val = Convert.ChangeType(dbValue, dataType);
				}
				else {
					val = null;
				}
				_recordList[index].Properties.Add(col.ColumnName, val);
			}
		}

		/// <summary>
		/// Resets the record index, sets the array back to zero and moves
		/// to the next page of information.  Each page requires another call
		/// to the database.
		/// </summary>
		private void NextPage() {
			_recordIndex = -1;
			Array.Resize(ref _recordList, 0);
			_page++;
			this.GetRecords();
		}

		/// <summary>
		/// Checks to see if there is another record available in the _recordList
		/// array.  If there is the propertylist is obtained into the current
		/// object and "true" is returned.  Otherwise it increments to the next
		/// page and tries again.
		/// </summary>
		/// <returns></returns>
		private bool NextRecord() {
			_recordIndex++;
			if (_recordIndex < _recordList.Length) {
				this.Properties = _recordList[_recordIndex].Properties;
				return true;
			}
			else
				return false;
		}

		/// <summary>
		/// Adds a WHERE query to the SQL command.
		/// </summary>
		/// <param name="query"></param>
		/// <param name="type"></param>
		public void AddQuery(
			string query,
			string table = "",
			ClauseType type = ClauseType.Neither
		) {
			if (_query == null)
				_query = new Query(_database);

			WhereClause clause = new WhereClause(query, table, type);

			_query.AddClause(clause, type);
		}

		/// <summary>
		/// Adds a WhERE query to the SQL command.
		/// </summary>
		/// <param name="query"></param>
		/// <param name="type"></param>
		public void AddQuery(string query) {
			this.AddQuery(query, "", ClauseType.Neither);
		}

		public void AddQuery(string query, ClauseType type = ClauseType.Neither) {
			this.AddQuery(query, "", type);
		}

		public void AddJoin(
			string joinTable,
			string joinColumn,
			string joinOn,
			JoinType type = JoinType.Inner
		) {
			if (_query == null)
				_query = new Query(_database);

			Join j = new Join() {
				Table = joinTable,
				JoinColumn = joinColumn,
				Column = joinOn,
				Type = type
			};

			_query.AddJoin(j, j.Type);
		}

		/// <summary>
		/// Gets the next record in the Record list array if one is available.
		/// </summary>
		/// <returns></returns>
		public bool Next() {
			if (this.NextRecord()) {
				return true;
			}

			this.NextPage();
			if (_recordList.Length > 0) {
				this.NextRecord();
				return true;
			}

			return false;
		}

		/// <summary>
		/// Fills the record list utilizing queries on its
		/// own seperate thread.
		/// </summary>
		public void ReadAsync() {
			Thread t = new Thread(new ThreadStart(Read));
			t.Start();
		}

		/// <summary>
		/// Fills the record list utilizing queries.
		/// </summary>
		/// <returns></returns>
		public void Read() {
			if (this.GetRecords() == DbMessage.Failed) {
				throw new Exception("Unable to retrieve data from database");
			}
		}

		/// <summary>
		/// Deletes the records within the record list.
		/// </summary>
		/// <returns></returns>
		public DbMessage Remove() {
			try {
				Delete deleteObj = new Delete(this, _table, _database);
				deleteObj.CreateCommand();
				deleteObj.Execute();

				return DbMessage.Success;
			}
			catch (SqlException e) {
				return DbMessage.Failed;
			}
		}
		/// <summary>
		/// Updates records within the record list.
		/// </summary>
		/// <returns></returns>
		public DbMessage Update() {
			Update updt = new Update(this, _table, _database);
			updt.CreateCommand();
			updt.Execute();
			return DbMessage.Success;
		}
		/// <summary>
		/// Creates the record object into database.
		/// </summary>
		/// <returns></returns>
		public DbMessage Create() {
			try {
				Insert insert = new Insert(this, _table, _database);
				insert.CreateCommand();
				insert.Execute();
				return DbMessage.Success;
			}
			catch (Exception e) {
				return DbMessage.Failed;
			}

		}
		/// <summary>
		/// Checks if column is in the list of TableConstraintInfo objects
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public bool IsIdentityColumn(string column) {
			return base.IdentityColumns.Any(x => x.Column.Equals(column));
		}

		/// <summary>
		/// Returns the constraint information for a particular column
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public TableConstraintInfo GetIdentityInfo(string column) {
			return base.IdentityColumns.Find(x => x.Column.Equals(column));
		}
	}
}