using System;
using System.Collections;
using System.Data.SqlClient;
using System.Text;
using DbWrapper.Contracts;

namespace DbWrapper.Read {
	[Serializable]
	public class Query : Command {

		private static int _pageSize;      // Determines how many records are to be in a page of data
		private uint _recordCount;   // Stores the total amount of records in a record set
		private int _currentPage;   // Shows what page the object is currently on
		private int _rowStart;      // Tells SQL what row the page should start on
		private int _rowEnd;        // Tells SQL what row the page should end on

		/// <summary>
		/// Class constructor.  Sets the size of each data page to a default
		/// of 25 records per page.
		/// </summary>
		/// <param name="db"></param>
		public Query(DynamicDatabase db)
			: base(db) {

			_recordCount = 0;

			if (_pageSize == 0)
				_pageSize = 1000;
		}

		/// <summary>
		/// Class constructor.  Sets the size of each data page to whatever
		/// is passed into the pageSize parameter.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="pageSize"></param>
		public Query(DynamicDatabase db, int pageSize)
			: base(db) {

			_recordCount = 0;
			_pageSize = pageSize;
		}

		/// <summary>
		/// Total count of records in parent table being queries
		/// </summary>
		public uint RecordCount {
			get { return this._recordCount; }
			set { this._recordCount = value; }
		}

		/// <summary>
		/// Total number of records per page of data
		/// </summary>
		public static int PageSize {
			get { return _pageSize; }
			set { _pageSize = value; }
		}

		/// <summary>
		/// Returns what page the object is currently on
		/// </summary>
		public int CurrentPage {
			get { return _currentPage; }
			set { _currentPage = value; }
		}

		/// <summary>
		/// States where the first record in SQL should begin
		/// </summary>
		public int RowStart {
			get {
				_rowStart = _pageSize * _currentPage + 1;
				return _rowStart;
			}
		}

		/// <summary>
		/// States where the last record in SQL should end
		/// </summary>
		public int RowEnd {
			get {
				_rowEnd = _rowStart + _pageSize - 1;
				return _rowEnd;
			}
		}

		internal Hashtable ColumnList {
			get { return base._columnList; }
			set { base._columnList = value; }
		}

		public override string ToString() {
			return base.ToString();
		}

		/// <summary>
		/// Creates the SQL SELECT Command based on the list of clauses
		/// </summary>
		public override void CreateCommand() {
			string table = _table;
			base.CreateCommand();

			if (_db.Engine == DatabaseEngine.SqlServer) {
				table = String.Format("{0}Tbl", _table);
				base.CreateCTESection();
			}
			else if (_db.Engine == DatabaseEngine.MySQL) {
				table = String.Format(
					"{0}{1}{2}",
					EscapeCharacters[0],
					_table,
					EscapeCharacters[1]
				);
			}

			_commandStr.Append("SELECT ");

			short i = 0;
			foreach (var columnName in _columnList.Keys) {
				if (i != _columnList.Count - 1) {
					_commandStr.Append(
						String.Format(
							"{0}.{2}{1}{3}\n\t, ",
							table,
							columnName,
							EscapeCharacters[0],
							EscapeCharacters[1]
						)
					);
				}
				else {
					_commandStr.Append(
						String.Format(
							"{0}.{2}{1}{3}\n",
							table,
							columnName,
							EscapeCharacters[0],
							EscapeCharacters[1]
						)
					);
				}
				i++;
			}

			_commandStr.Append("FROM " + table);

			base.CreateJoinSection(false); // The 'false' value states that it is not for CTE generation
			base.CreateWhereSection(this.RowStart, this.RowEnd); // The int values that aren't 0 says it is not for CTE
			_command.CommandText = _commandStr.ToString();
		}
	}
}