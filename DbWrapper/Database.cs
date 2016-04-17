using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace DbWrapper {
	public enum DatabaseEngine : sbyte {
		SqlServer,
		MySQL
	}

	[Serializable]
	public sealed class Database : IDisposable {
		/*
		 * Private class variables
		 */
		private static OdbcConnection _conn;
		private static Hashtable _schema;
		private List<string> _tableList;
		private string _catalog;
		private string _server;
		private string _username;
		private string _password;
		private string _provider;
		private DatabaseEngine _engine;

		[NonSerialized]
		private OdbcConnectionStringBuilder _conBuilder;

		/*
		 * Class constructors
		 */
		/// <summary>
		/// Constructor
		/// </summary>
		public Database() {
			Init();
		}

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="server"></param>
		/// <param name="catalog"></param>
		/// <param name="username"></param>
		/// <param name="password"></param>
		public Database(string server,
						string catalog,
						DatabaseEngine engine,
						string username = "",
						string password = "") {
			Init();

			_server = server;
			_catalog = catalog;
			_username = username;
			_password = password;
			_engine = engine;

			switch (_engine) {
				case DatabaseEngine.SqlServer:
					_provider = "SQL Server";
					break;
				case DatabaseEngine.MySQL:
					_provider = "MySQL ODBC 3.51 Driver";
					break;
				default:
					_provider = "UNKNOWN";
					break;
			}

			_conBuilder.Driver = _provider;
			_conBuilder["Server"] = _server;
			_conBuilder["Uid"] = _username;
			_conBuilder["Pwd"] = _password;
			_conBuilder["Database"] = _catalog;
			_conn.ConnectionString = _conBuilder.ToString();
			GetTableList(catalog);

			if (_schema == null) {
				_schema = new Hashtable();
				BuildSchema();
			}
		}

		/*
		 * Public class properties
		 */
		public static OdbcConnection Connection {
			get { return _conn; }
		}
		public List<string> Tables {
			get { return _tableList; }
		}
		public string Server {
			get { return _server; }
		}
		public string Username {
			get { return _username; }
		}
		public string Password {
			get { return _password; }
		}
		public string Catalog {
			get { return _catalog; }
		}
		public DatabaseEngine Engine {
			get { return _engine; }
		}
		public static Hashtable Schema {
			get { return _schema; }
		}

		public void Open() {
			if (_conn.State == ConnectionState.Closed)
				_conn.Open();
		}

		public void Close() {
			if (_conn.State == ConnectionState.Open)
				_conn.Close();
		}

		/// <summary>
		/// Disposes the object out of memory
		/// </summary>
		public void Dispose() {
			_conn.Close();
			_conn.Dispose();
			_conBuilder.Clear();

			_conn = null;
			_conBuilder = null;
		}

		/*
		 * Internal class methods
		 */
		/// <summary>
		/// Retrieves a list of columns and their data types in
		/// order to create the SELECT statement.
		/// </summary>
		/// <param name="table"></param>
		/// <returns></returns>
		internal static Hashtable GetColumns(string table) {
			_conn.Open();
			Hashtable colList = new Hashtable();
			DataTable dt = _conn.GetSchema("COLUMNS", new string[] { null, null, table, null });
			foreach (DataRow row in dt.Rows) {
				if (row["TABLE_NAME"].ToString().Equals(table)) {
					string val = row["TYPE_NAME"].ToString();
					OdbcType dataType = (OdbcType)Int32.Parse(row["DATA_TYPE"].ToString());
					Type t = Database.GetSystemType(val);
					colList.Add(row["COLUMN_NAME"].ToString(), t);
				}
			}
			_conn.Close();

			return colList;
		}

		/// <summary>
		/// Retrieves all of the constraint information about a particular table
		/// </summary>
		/// <param name="table"></param>
		/// <returns></returns>
		internal static List<TableConstraintInfo> GetTableConstraints(string table) {
			List<TableConstraintInfo> list = new List<TableConstraintInfo>();
			_conn.Open();
			DataTable fKeys = _conn.GetSchema(
				"indexes",
				new string[] { null, null, table, null }
			);
			DataTable pKeys = _conn.GetSchema(
				"indexes",
				new string[] { null, null, table, null }
			);
			_conn.Close();

			// TODO(Logan): Have two threads going at the same time and join them together.
			/*
			 * Go through the data table and get the foreign key information
			 */
			for (int i = 0; i < fKeys.Rows.Count; i++) {
				DataRow row = fKeys.Rows[i];

				if (row["TABLE_NAME"].ToString().Equals(table) &&
					row["INDEX_NAME"].ToString().Contains("FK")) {
					TableConstraintInfo info = new TableConstraintInfo();
					info.Column = row["COLUMN_NAME"].ToString();
					list.Add(info);
				}
			}

			/*
			 * Go through the same table to get the primary key information
			 */
			for (int i = 0; i < pKeys.Rows.Count; i++) {
				DataRow row = pKeys.Rows[i];

				if (row["TABLE_NAME"].ToString().Equals(table) &&
					row["INDEX_NAME"].ToString().Contains("PK")) {

					TableConstraintInfo info = new TableConstraintInfo();
					info.Column = row["COLUMN_NAME"].ToString();
					info.IsPrimaryKey = true;

					list.Add(info);
				}
			}
			return list;
		}
		/*
		 * Private static class methods
		 */
		/// <summary>
		/// Takes the SQL data type and converts it to the CLR type
		/// </summary>
		/// <param name="sqlType"></param>
		/// <returns></returns>
		private static Type GetSystemType(string sqlType) {
			string[] array = sqlType.Split(new char[] { ' ' });
			switch (array[0].ToUpper()) {
				case "BIGINT": return typeof(long);
				case "BINARY":
				case "IMAGE": return typeof(byte[]);
				case "BIT": return typeof(bool);
				case "VARCHAR":
				case "NVARCHAR":
				case "NTEXT":
				case "TEXT":
				case "SYSNAME": return typeof(string);
				case "DATE":
				case "DATETIME": return typeof(DateTime);
				case "TIME": return typeof(TimeSpan);
				case "DECIMAL": return typeof(decimal);
				case "DOUBLE": return typeof(double);
				case "SINGLE":
				case "FLOAT": return typeof(float);
				case "INTEGER":
				case "INT": return typeof(int);
				case "UNIQUEIDENTIFIER": return typeof(Guid);
				case "SMALLINT": return typeof(short);
				case "TINYINT": return typeof(byte);
				default:
					throw new ArgumentException("Type not found");
			}
		}
		/*
		 * Private class methods
		 */
		/// <summary>
		/// Used to initialize the object in all constructor methods
		/// </summary>
		private void Init() {
			_conBuilder = new OdbcConnectionStringBuilder();
			_tableList = new List<string>();
			_catalog = String.Empty;
			_server = String.Empty;

			if (_conn == null)
				_conn = new OdbcConnection();
		}

		private void GetTableList(string db) {
			if (_tableList.Count > 0)
				_tableList.Clear();

			_conn.Open();
			//DataTable dt = _conn.GetSchema("Tables", new string[] {null, "dbo", null});
			DataTable dt = _conn.GetSchema("Tables");

			foreach (DataRow row in dt.Rows) {
				string rowSchema = row["TABLE_SCHEM"].ToString();
				if (!rowSchema.Equals("sys", StringComparison.OrdinalIgnoreCase) &&
					!rowSchema.Equals("INFORMATION_SCHEM")) {

					_tableList.Add(row["TABLE_NAME"].ToString());
				}
			}
			_conn.Close();
		}

		/// <summary>
		/// Builds the entire schema of the database
		/// </summary>
		private void BuildSchema() {
			for (short i = 0; i < _tableList.Count; i++) {
				Hashtable tblSchema = GetColumns(_tableList[i]);
				_schema.Add(_tableList[i], tblSchema);
			}
		}

		private static string GetConstraintTable(string constraint) {
			string matchText = String.Empty;
			string pattern = @"(\w{2}_FK_)(\w+)";
			Match match = Regex.Match(constraint, pattern, RegexOptions.IgnoreCase);

			if (match.Success) {
				matchText = match.Groups[2].Value;
			}

			return matchText;
		}
	}
}
