using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Odbc;
using System.Text.RegularExpressions;
using System.Threading;

namespace DbWrapper.Contracts {
	public enum DatabaseEngine : sbyte {
		SqlServer,
		MySQL
	}

	[Serializable]
	public sealed class Database : IDisposable {

		private List<string> _tableList;
		private string _provider;

		[NonSerialized]
		private OdbcConnectionStringBuilder _conBuilder;

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
		public Database(
			string server,
			string catalog,
			DatabaseEngine engine,
			string username = "",
			string password = ""
		) : this() {

			Server = server;
			Catalog = catalog;
			Username = username;
			Password = password;
			Engine = engine;

			switch (Engine) {
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
			_conBuilder["Server"] = Server;
			_conBuilder["Uid"] = username;
			_conBuilder["Pwd"] = password;
			_conBuilder["Database"] = Catalog;
			Connection.ConnectionString = _conBuilder.ToString();
			GetTableList(catalog);

			if (Schema == null) {
				Schema = new Hashtable();
				BuildSchema();
			}
		}

		public static OdbcConnection Connection { get; private set; }
		public List<string> Tables { get; private set; }
		public string Server { get; private set; }
		public string Username { get; private set; }
		public string Password { get; private set; }
		public string Catalog { get; private set; }
		public DatabaseEngine Engine { get; private set; }
		public static Hashtable Schema { get; private set; }

		public void Open() {
			if (Connection.State == ConnectionState.Closed) {
				Connection.Open();
			}
		}

		public void Close() {
			if (Connection.State == ConnectionState.Open) {
				Connection.Close();
			}
		}

		/// <summary>
		/// Disposes the object out of memory
		/// </summary>
		public void Dispose() {
			Connection.Close();
			Connection.Dispose();
			_conBuilder.Clear();

			Connection = null;
			_conBuilder = null;
		}

		/// <summary>
		/// Retrieves a list of columns and their data types in
		/// order to create the SELECT statement.
		/// </summary>
		/// <param name="table"></param>
		/// <returns></returns>
		public static Hashtable GetColumnsFor(string table) {
			Connection.Open();

			Hashtable colList = new Hashtable();
			DataTable dt = Connection.GetSchema(
				"COLUMNS",
				new string[] { null, null, table, null }
			);

			foreach (DataRow row in dt.Rows) {
				if (row["TABLE_NAME"].ToString().Equals(table)) {
					string typeStr = row["TYPE_NAME"].ToString();
					OdbcType dataType = (OdbcType)Int32.Parse(row["DATA_TYPE"].ToString());
					colList.Add(row["COLUMN_NAME"].ToString(), GetSystemType(typeStr));
				}
			}

			Connection.Close();
			return colList;
		}

		/// <summary>
		/// Retrieves all of the constraint information about a particular table
		/// </summary>
		/// <param name="table"></param>
		/// <returns></returns>
		public static List<TableConstraintInfo> GetTableConstraints(string table) {
			List<TableConstraintInfo> list = new List<TableConstraintInfo>();
			Connection.Open();
			DataTable foreignKeys = Connection.GetSchema(
				"indexes",
				new string[] { null, null, table, null }
			);
			DataTable primaryKeys = Connection.GetSchema(
				"indexes",
				new string[] { null, null, table, null }
			);
			Connection.Close();

			Thread[] threadArray = new Thread[2];
			Thread foreignKeyThread = new Thread(() => {
				/*
				 * Go through the data table and get the foreign key information
				 */
				for (int i = 0; i < foreignKeys.Rows.Count; i++) {
					DataRow row = foreignKeys.Rows[i];

					if (row["TABLE_NAME"].ToString().Equals(table) &&
						row["INDEX_NAME"].ToString().Contains("FK")) {
						TableConstraintInfo info = new TableConstraintInfo();
						info.Column = row["COLUMN_NAME"].ToString();
						list.Add(info);
					}
				}
			});
			threadArray[0] = foreignKeyThread;
			foreignKeyThread.Start();

			Thread primaryKeyThread = new Thread(() => {
				/*
				 * Go through the same table to get the primary key information
				 */
				for (int i = 0; i < primaryKeys.Rows.Count; i++) {
					DataRow row = primaryKeys.Rows[i];

					if (row["TABLE_NAME"].ToString().Equals(table) &&
						row["INDEX_NAME"].ToString().Contains("PK")) {

						TableConstraintInfo info = new TableConstraintInfo();
						info.Column = row["COLUMN_NAME"].ToString();
						info.IsPrimaryKey = true;

						list.Add(info);
					}
				}
			});
			threadArray[1] = primaryKeyThread;
			primaryKeyThread.Start();

			for (sbyte i = 0; i < threadArray.Length; i++) {
				threadArray[i].Join();
			}

			return list;
		}

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

		/// <summary>
		/// Used to initialize the object in all constructor methods
		/// </summary>
		private void Init() {
			_conBuilder = new OdbcConnectionStringBuilder();
			_tableList = new List<string>();
			Catalog = String.Empty;
			Server = String.Empty;

			if (Connection == null)
				Connection = new OdbcConnection();
		}

		private void GetTableList(string db) {
			if (_tableList.Count > 0)
				_tableList.Clear();

			Connection.Open();
			DataTable dt = Connection.GetSchema("Tables");

			foreach (DataRow row in dt.Rows) {
				string rowSchema = row["TABLE_SCHEM"].ToString();
				if (!rowSchema.Equals("sys", StringComparison.OrdinalIgnoreCase) &&
					!rowSchema.Equals("INFORMATION_SCHEM")) {

					_tableList.Add(row["TABLE_NAME"].ToString());
				}
			}
			Connection.Close();
		}

		/// <summary>
		/// Builds the entire schema of the database
		/// </summary>
		private void BuildSchema() {
			for (short i = 0; i < _tableList.Count; i++) {
				Hashtable tblSchema = GetColumnsFor(_tableList[i]);
				Schema.Add(_tableList[i], tblSchema);
			}
		}

	}

}
