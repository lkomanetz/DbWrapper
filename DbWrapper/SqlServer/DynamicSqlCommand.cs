using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using DbWrapper.Contracts;
using System.Collections;
using System.Text.RegularExpressions;

namespace DbWrapper.SqlServer {

	public abstract class DynamicSqlCommand : IDynamicCommand {

		private OdbcCommand _command;
		private StringBuilder _commandStr;
		private Hashtable _columnList;

		public DynamicSqlCommand() {
			this.EscapeCharacters = new char[] { '[', ']' };
			this.Clauses = new List<WhereClause>();
			this.Joins = new Dictionary<string, Join>();
			this.Table = String.Empty;
		}

		public DynamicSqlCommand(Database db)
			: this() {
			this.Database = db;
		}

		public List<WhereClause> Clauses { get; set; }
		public Database Database { get; set; }
		public char[] EscapeCharacters { get; }
		public Dictionary<string, Join> Joins { get; set; }
		public string Table { get; set; }

		public void AddClause(WhereClause clause, ClauseType type = ClauseType.Neither) {
			if (!this.Clauses.Contains(clause)) {
				this.Clauses.Add(clause);
			}
		}

		public void AddJoin(Join join, JoinType type = JoinType.Inner) {
			if (!this.Joins.ContainsKey(join.Table)) {
				this.Joins.Add(join.Table, join);
			}
		}

		public override string ToString() {
			return _command.CommandText;
		}

		public virtual void CreateCommand() {
			if (_command == null) {
				_command = new OdbcCommand();
			}

			if (_commandStr == null) {
				_commandStr = new StringBuilder();
			}

			_columnList = Database.GetColumnsFor(this.Table);
		}

		public void Dispose() {
			_commandStr.Length = 0;
			_command.Dispose();
			GC.SuppressFinalize(this);
		}

		public DataSet Execute() {
			throw new NotImplementedException();
		}

		internal void CreateCTESection() {
			// Get the total number of records in the table
			_commandStr.AppendFormat("SELECT COUNT(*) AS RecordCount\n" +
									"FROM [{0}];\n\n", this.Table);

			/*
			 * This WITH statement returns the number of rows in the table.
			 * It is used when retrieving data to make sure I am retrieving
			 * only a subset of data each time.  Primary keys can't be used
			 * since once records get deleted it throws everything off.
			 */
			_commandStr.AppendFormat("WITH {0}Tbl AS\n(\nSELECT ", this.Table);

			short i = 0;
			string firstCol = String.Empty;
			foreach (string columnName in _columnList.Keys) {
				if (i == 0) {
					firstCol = columnName;
				}

				if (i != _columnList.Count - 1) {
					_commandStr.Append(String.Format("[{0}].[{1}]\n\t, ", this.Table, columnName));
				}
				else {
					_commandStr.Append(String.Format("[{0}].[{1}]\n", this.Table, columnName));
				}

				i++;
			}
			_commandStr.AppendFormat(
				"\t, ROW_NUMBER() OVER(ORDER BY [{0}].[{1}]) AS RowNumber\n",
				this.Table,
				firstCol
			);
			_commandStr.AppendFormat("FROM [{0}]", this.Table);
			CreateCTEJoin();
			CreateJoinSection(true); // The 'true' parameter says it is for CTE generation
			CreateWhereSection(0, 0); // The '0, 0' int parameters says it is for CTE generation
			_commandStr.Append("\n)\n");
		}

		internal void CreateJoinSection() {

		}

		private string CreateCTEJoin() {
		}

		private string RemoveIllegalCharacters(string parameterName) {
			return Regex.Replace(parameterName, @"\.", "");
		}

	}

}
