using DbWrapper.Contracts;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;

namespace DbWrapper.SqlServer {

	[Serializable]
	public class DynamicSqlQuery : DynamicSqlCommand, IDynamicQuery {

		public DynamicSqlQuery(DynamicDatabase db) 
			: base(db) { }

		public int CurrentPage { get; set; }
		public static int PageSize => 1000;

		public int RowStart {
			get { return PageSize * this.CurrentPage + 1; }
		}

		public int RowEnd {
			get { return this.RowStart + PageSize - 1; }
		}

		public override void InitializeCommand() {
			base.InitializeCommand();
			string table = this.Table + "Tbl";

			base.AppendCTESection();
			_commandStr.Append("SELECT ");

			// This loop creates the columns to return in the following format:
			// <Table>.[<ColumnName>]
			short i = 0;
			int endOfList = _columnList.Count - 1;
			foreach(object columnName in _columnList.Keys) {
				if (i != endOfList) {
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
			this.AppendJoinSection();
			this.AppendWhereSection();

			_command.CommandText = _commandStr.ToString();
		}

		protected override void AppendJoinSection() {
			string joinStr = "\n\t{3} JOIN {1}{0}{2}";
			string joinTypeStr = String.Empty;

			foreach (var kvp in this.Joins) {
				Join join = kvp.Value;

				_commandStr.AppendFormat(
					joinStr,
					join.Table,
					EscapeCharacters[0],
					EscapeCharacters[1],
					join.Type.ToString("G").ToUpper()
				);

				_commandStr.AppendFormat(
					"\n\t\tON {0}.{4}{1}{5} = {4}{2}{5}.{4}{3}{5}",
					this.Table + "Tbl",
					join.Column,
					join.Table,
					join.JoinColumn,
					EscapeCharacters[0],
					EscapeCharacters[1]
				);
			}
		}

		protected override void AppendWhereSection() {
			if (this.Clauses == null || this.Clauses.Count == 0) {
				_commandStr.AppendFormat(
					"\nWHERE (RowNumber >= {0} AND\n\tRowNumber <= {1})",
					this.RowStart,
					this.RowEnd
				);

				return;
			}

			_commandStr.Append("\nWHERE ");

			for (short i = 0; i < this.Clauses.Count; i++) {
				WhereClause clause = this.Clauses[i];
				OdbcParameter param = clause.BuildParameter();
				string table = GetTable(clause);
				string clauseTypeStr = String.Empty;
				string formattedStr = String.Empty;

				if (clause.Type == ClauseType.And ||
					clause.Type == ClauseType.Or) {

					clauseTypeStr = (clause.Type == ClauseType.And) ? "AND" : "OR";
				}


				if (String.IsNullOrEmpty(clauseTypeStr)) {
					_commandStr.AppendFormat(
						"{3}{0}{4}.{3}{1}{4} {2} ?",
						table,
						clause.Column,
						clause.Operator,
						EscapeCharacters[0],
						EscapeCharacters[1]
					);
				}
				else {
					_commandStr.AppendFormat(
						"{0}.{4}{1}{5} {2} ? {3}\n\t",
						table,
						clause.Column,
						clause.Operator,
						clauseTypeStr,
						EscapeCharacters[0],
						EscapeCharacters[1]
					);
				}

				_command.Parameters.Add(param);
			}

			if (this.RowStart != 0 && this.RowEnd != 0) {
				_commandStr.Append($" AND\n\t(RowNumber >= {this.RowStart} AND\n\tRowNumber <= {this.RowEnd})");
			}
		}

		private string GetTable(WhereClause clause) {
			string tableName = String.IsNullOrEmpty(clause.Table) ? this.Table : clause.Table;
			return $"{tableName}Tbl";
		}

	}

}
