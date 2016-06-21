using DbWrapper.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DbWrapper.SqlServer {

	[Serializable]
	public class DynamicSqlQuery : DynamicSqlCommand, IDynamicQuery {

		public DynamicSqlQuery(DynamicDatabase db) 
			: base(db) { }

		public int CurrentPage { get; set; }
		public static int PageSize { get; set; }

		public int RowStart {
			get { return PageSize * this.CurrentPage + 1; }
		}

		public int RowEnd {
			get { return this.RowStart + PageSize - 1; }
		}

		public override void CreateCommand() {
			base.CreateCommand();
			string table = this.Table + "Tbl";

			base.CreateCTESection();
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
			this.CreateJoinSection();
			this.CreateWhereSection();

			_command.CommandText = _commandStr.ToString();
		}

		protected override void CreateJoinSection() {
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
					join.Table + "Tbl",
					join.Column,
					join.Table,
					join.JoinColumn,
					EscapeCharacters[0],
					EscapeCharacters[1]
				);
			}
		}

		protected override void CreateWhereSection() {
			if (this.Clauses == null || this.Clauses.Count == 0) {
				_commandStr.AppendFormat(
					" WHERE (RowNumber >= {0} AND\n\tRowNumber <= {1})",
					this.RowStart,
					this.RowEnd
				);

				return;
			}

		}

	}

}
